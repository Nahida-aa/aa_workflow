use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::define::{NodeRunFn, NodeSpec, Workflow};
use crate::error::WorkflowError;
use crate::event::{fold_node_states, NodeAttempt, NodeState, NodeStatus, RunEvent, RunStatus};
use crate::resource::Gate;
use crate::store::{RunState, RunStore};

/// Messages sent from worker threads back to the scheduler.
pub(crate) enum WorkerMsg {
    Done {
        node_id: String,
        result: Option<serde_json::Value>,
        attempts: Vec<NodeAttempt>,
    },
    Failed {
        node_id: String,
        error: String,
        attempts: Vec<NodeAttempt>,
    },
    Progress {
        node_id: String,
        value: f64,
    },
}

/// Per-invocation view handed to node closures. Nodes do real work on disk /
/// subprocesses, so this is intentionally minimal; `progress` is an
/// emit-only observability signal.
#[derive(Clone)]
pub struct RunCtx {
    pub run_id: String,
    pub node_id: String,
    pub input: serde_json::Value,
    pub(crate) scheduler_tx: Option<Sender<WorkerMsg>>,
}

impl RunCtx {
    pub fn progress(&self, value: f64) {
        if let Some(tx) = &self.scheduler_tx {
            let _ = tx.send(WorkerMsg::Progress {
                node_id: self.node_id.clone(),
                value,
            });
        }
    }
}

/// Per-invocation options. `run_id`, `continue_from`, `target_stage` and
/// `max_concurrency` are invocation options, NOT persisted — persistent state
/// lives in the event log.
pub struct RunOptions {
    pub run_id: Option<String>,
    pub input: serde_json::Value,
    pub target_stage: Option<String>,
    pub continue_from: Option<String>,
    pub max_concurrency: Option<usize>,
}

impl RunOptions {
    pub fn new(input: serde_json::Value) -> Self {
        Self {
            run_id: None,
            input,
            target_stage: None,
            continue_from: None,
            max_concurrency: None,
        }
    }

    pub fn run_id(mut self, v: impl Into<String>) -> Self {
        self.run_id = Some(v.into());
        self
    }

    pub fn target_stage(mut self, v: impl Into<String>) -> Self {
        self.target_stage = Some(v.into());
        self
    }

    pub fn continue_from(mut self, v: impl Into<String>) -> Self {
        self.continue_from = Some(v.into());
        self
    }

    pub fn max_concurrency(mut self, v: usize) -> Self {
        self.max_concurrency = Some(v);
        self
    }
}

#[derive(Debug)]
pub struct RunOutcome {
    pub run_id: String,
    pub status: RunStatus,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// All downstream nodes reachable from `start` following `dependents` edges
/// (start itself included). Used by `continue_from` to reset a suffix of the
/// graph.
fn closure_downstream(start: &str, dependents: &HashMap<String, Vec<String>>) -> HashSet<String> {
    let mut out = HashSet::new();
    let mut stack = vec![start.to_string()];
    while let Some(n) = stack.pop() {
        if !out.insert(n.clone()) {
            continue;
        }
        if let Some(deps) = dependents.get(&n) {
            for d in deps {
                stack.push(d.clone());
            }
        }
    }
    out
}

fn ready(node_id: &str, needs_of: &HashMap<String, Vec<String>>, remaining: &HashSet<String>) -> bool {
    needs_of
        .get(node_id)
        .map_or(true, |deps| deps.iter().all(|d| !remaining.contains(d)))
}

/// Produces a per-node probe context (scheduler_tx = None) for the
/// `enabled` / `resource` / `up_to_date` selector closures.
fn probe(run_id: &str, node_id: &str, input: &serde_json::Value) -> RunCtx {
    RunCtx {
        run_id: run_id.to_string(),
        node_id: node_id.to_string(),
        input: input.clone(),
        scheduler_tx: None,
    }
}

/// Executes a node's `run` fn with retry + optional wall-clock timeout.
/// Timeout uses `recv_timeout`: on expiry the node is marked failed and the
/// egger thread is left running (documented limitation — subprocesses cannot
/// be force-killed from Rust anyway).
fn run_node_with_retry(
    node: &NodeSpec,
    ctx: &RunCtx,
) -> (Result<Option<serde_json::Value>, String>, Vec<NodeAttempt>) {
    let max = node.retry.as_ref().map(|p| p.max_attempts).unwrap_or(1);
    let mut attempts = Vec::with_capacity(max);
    for attempt in 1..=max {
        let started_at = now_ms();
        let outcome = match node.timeout {
            Some(t) => run_with_timeout(&node.run, ctx, t).map_err(|e| e.to_string()),
            None => (node.run)(ctx.clone()).map_err(|e| e.to_string()),
        };
        let finished_at = now_ms();
        match outcome {
            Ok(result) => {
                attempts.push(NodeAttempt {
                    attempt,
                    started_at,
                    finished_at,
                    result: result.clone(),
                    error: None,
                });
                return (Ok(result), attempts);
            }
            Err(err) => {
                attempts.push(NodeAttempt {
                    attempt,
                    started_at,
                    finished_at,
                    result: None,
                    error: Some(err.clone()),
                });
                if node.retry.is_some() && attempt < max {
                    let delay = node
                        .retry
                        .as_ref()
                        .map(|p| p.backoff.delay_ms(attempt))
                        .unwrap_or(0);
                    std::thread::sleep(Duration::from_millis(delay));
                    continue;
                }
                return (Err(err), attempts);
            }
        }
    }
    unreachable!()
}

fn run_with_timeout(
    run: &NodeRunFn,
    ctx: &RunCtx,
    timeout: Duration,
) -> anyhow::Result<Option<serde_json::Value>> {
    let (tx, rx) = mpsc::channel();
    let run = run.clone();
    let ctx = ctx.clone();
    std::thread::spawn(move || {
        let _ = tx.send(run(ctx));
    });
    match rx.recv_timeout(timeout) {
        Ok(res) => res,
        Err(_) => Err(anyhow::anyhow!(
            "step timed out after {timeout:?}; worker thread leaks in the background"
        )),
    }
}

/// Shared handler for worker completions. Used both by the non-blocking drain
/// and by the blocking `recv` after a wait, so a received message is never
/// dropped.
#[allow(clippy::too_many_arguments)]
fn handle_worker(
    msg: WorkerMsg,
    running: &mut HashSet<String>,
    remaining: &mut HashSet<String>,
    live: &mut HashMap<String, NodeState>,
    store: &dyn RunStore,
    run_id: &str,
    log_len: &mut usize,
    publisher: Option<&dyn Fn(&RunEvent)>,
    target_stage: Option<&str>,
    target_reached: &mut bool,
    run_error: &mut Option<String>,
) -> Result<(), WorkflowError> {
    let publish = |ev: &RunEvent| {
        if let Some(p) = publisher {
            p(ev);
        }
    };
    match msg {
        WorkerMsg::Done { node_id, result, attempts } => {
            running.remove(&node_id);
            remaining.remove(&node_id);
            let st = live.entry(node_id.clone()).or_default();
            st.status = NodeStatus::Success;
            st.result = result.clone();
            st.error = None;
            st.finished_at = Some(now_ms());
            let ev = RunEvent::NodeFinished {
                ts: now_ms(),
                run_id: run_id.to_string(),
                node_id: node_id.clone(),
                result,
                attempts,
            };
            store.append_event(run_id, *log_len, &ev)?;
            *log_len += 1;
            publish(&ev);
            if target_stage == Some(node_id.as_str()) {
                *target_reached = true;
            }
        }
        WorkerMsg::Failed { node_id, error, attempts } => {
            running.remove(&node_id);
            remaining.remove(&node_id);
            let st = live.entry(node_id.clone()).or_default();
            st.status = NodeStatus::Failed;
            st.error = Some(error.clone());
            st.finished_at = Some(now_ms());
            let ev = RunEvent::NodeFailed {
                ts: now_ms(),
                run_id: run_id.to_string(),
                node_id: node_id.clone(),
                error: error.clone(),
                attempts,
            };
            store.append_event(run_id, *log_len, &ev)?;
            *log_len += 1;
            publish(&ev);
            if run_error.is_none() {
                *run_error = Some(error);
            }
        }
        WorkerMsg::Progress { node_id, value } => {
            publish(&RunEvent::NodeProgress {
                ts: now_ms(),
                run_id: run_id.to_string(),
                node_id,
                value,
            });
        }
    }
    Ok(())
}

/// Runs (or resumes) a workflow.
///
/// Resume semantics:
/// - Load the run's event log and fold it into per-node states.
/// - `continue_from`: reset that node + all its downstream descendants and run
///   only that suffix, leaving the (successful) prefix untouched.
/// - Otherwise: run every active, non-Success node (failed nodes get a fresh
///   attempt) plus any Success node whose `up_to_date` hook reports stale.
/// - `target_stage`: stop scheduling once that node succeeds (downstream never
///   runs); the run still finishes as `Finished`.
///
/// On the first terminal node failure the run stops scheduling and reports
/// `Errored`. `max_concurrency` caps simultaneously-running nodes; resource
/// gates (e.g. GPU) are honored during dispatch.
pub fn run(
    workflow: &mut Workflow,
    store: &dyn RunStore,
    opts: &RunOptions,
    publisher: Option<&dyn Fn(&RunEvent)>,
) -> Result<RunOutcome, WorkflowError> {
    workflow.validate()?;

    let run_id = opts.run_id.clone().unwrap_or_else(|| format!("run_{}", now_ms()));
    let ts = now_ms();

    let run_state = match store.get_run_state(&run_id)? {
        Some(mut st) => {
            st.status = RunStatus::Running;
            st.updated_at = ts;
            st
        }
        None => RunState {
            run_id: run_id.clone(),
            workflow_id: workflow.id.clone(),
            workflow_version: workflow.version.clone(),
            status: RunStatus::Running,
            input: opts.input.clone(),
            output: None,
            error: None,
            created_at: ts,
            updated_at: ts,
        },
    };
    store.set_run_state(&run_id, &run_state)?;

    let publish = |ev: &RunEvent| {
        if let Some(p) = publisher {
            p(ev);
        }
    };
    publish(&RunEvent::RunStarted {
        ts,
        run_id: run_id.clone(),
    });

    // ---- derive current state from the persisted log (resume) ----
    let events = store.get_events(&run_id)?;
    let mut live = fold_node_states(&events);
    let mut log_len = events.len();

    // ---- active set: `enabled` filters (LocalDub get_stages selection) ----
    let mut active: Vec<NodeSpec> = Vec::new();
    for node in &workflow.nodes {
        let on = node
            .enabled
            .as_ref()
            .map_or(true, |f| f(&probe(&run_id, &node.id, &opts.input)));
        if on {
            active.push(node.clone());
        }
    }

    let active_ids: HashSet<String> = active.iter().map(|n| n.id.clone()).collect();
    let mut needs_of: HashMap<String, Vec<String>> = HashMap::new();
    let mut dependents_of: HashMap<String, Vec<String>> = HashMap::new();
    for n in &active {
        let deps: Vec<String> = n
            .needs
            .iter()
            .filter(|d| active_ids.contains(*d))
            .cloned()
            .collect();
        for d in &deps {
            dependents_of.entry(d.clone()).or_default().push(n.id.clone());
        }
        needs_of.insert(n.id.clone(), deps);
    }

    if let Some(t) = &opts.target_stage {
        if !active_ids.contains(t) {
            return Err(WorkflowError::Validation(format!(
                "target_stage `{t}` is not an active node"
            )));
        }
    }

    // ---- decide which nodes need to run ----
    let mut remaining: HashSet<String> = HashSet::new();
    if let Some(cf) = &opts.continue_from {
        if !active_ids.contains(cf) {
            return Err(WorkflowError::Validation(format!(
                "continue_from `{cf}` is not an active node"
            )));
        }
        remaining = closure_downstream(cf, &dependents_of);
    } else {
        for n in &active {
            let st = live.get(n.id.as_str());
            let stale = match (st, &n.up_to_date) {
                (Some(st), Some(f)) => {
                    !f(&probe(&run_id, &n.id, &opts.input), st)
                }
                _ => false,
            };
            let needs_rerun = st.map(|s| s.status != NodeStatus::Success).unwrap_or(true) || stale;
            if needs_rerun {
                remaining.insert(n.id.clone());
            }
        }
    }

    // ---- dispatch loop ----
    let gate = Arc::new(Gate::new());
    let (tx, rx) = mpsc::channel::<WorkerMsg>();
    let cap = opts.max_concurrency.unwrap_or(active.len().max(1));

    let mut running: HashSet<String> = HashSet::new();
    let mut target_reached = false;
    let mut run_error: Option<String> = None;

    loop {
        // 1. drain queued messages (non-blocking)
        while let Ok(msg) = rx.try_recv() {
            handle_worker(
                msg,
                &mut running,
                &mut remaining,
                &mut live,
                store,
                &run_id,
                &mut log_len,
                publisher,
                opts.target_stage.as_deref(),
                &mut target_reached,
                &mut run_error,
            )?;
        }

        // 2. dispatch ready nodes
        if run_error.is_none() && !target_reached {
            for n in &active {
                if running.len() >= cap {
                    break;
                }
                if !remaining.contains(&n.id) || running.contains(&n.id) {
                    continue;
                }
                if !ready(&n.id, &needs_of, &remaining) {
                    continue;
                }
                let rkey_opt = n.resource.as_ref().map(|f| f(&probe(&run_id, &n.id, &opts.input)));
                let guard = match rkey_opt {
                    Some(rkey) => match gate.try_acquire(&rkey) {
                        Some(g) => Some(g),
                        None => continue, // resource busy: try next ready node
                    },
                    None => None, // no resource spec: no serialization
                };
                let ctx = RunCtx {
                    run_id: run_id.clone(),
                    node_id: n.id.clone(),
                    input: opts.input.clone(),
                    scheduler_tx: Some(tx.clone()),
                };
                let node = n.clone();
                let tx = tx.clone();
                std::thread::spawn(move || {
                    let _guard = guard; // held for the node's whole run
                    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        run_node_with_retry(&node, &ctx)
                    }));
                    drop(_guard);
                    let (res, attempts) = match run {
                        Ok(v) => v,
                        Err(payload) => {
                            let msg = payload
                                .downcast_ref::<&str>()
                                .map(|s| s.to_string())
                                .or_else(|| payload.downcast_ref::<String>().cloned())
                                .unwrap_or_else(|| "node panicked".to_string());
                            (Err(msg), Vec::new())
                        }
                    };
                    let msg = match res {
                        Ok(result) => WorkerMsg::Done {
                            node_id: node.id.clone(),
                            result,
                            attempts,
                        },
                        Err(error) => WorkerMsg::Failed {
                            node_id: node.id.clone(),
                            error,
                            attempts,
                        },
                    };
                    let _ = tx.send(msg);
                });
                running.insert(n.id.clone());
                publish(&RunEvent::NodeStarted {
                    ts: now_ms(),
                    run_id: run_id.clone(),
                    node_id: n.id.clone(),
                });
            }
        }

        // 3. termination or wait
        if running.is_empty() {
            if run_error.is_some() || remaining.is_empty() || target_reached {
                break;
            }
            // Invariant: any remaining ready node must have a free gate when
            // nothing is running, so this is a schedule bug.
            return Err(WorkflowError::Internal(format!(
                "deadlock guard: nothing running but {} nodes remain",
                remaining.len()
            )));
        }
        match rx.recv() {
            Ok(msg) => {
                handle_worker(
                    msg,
                    &mut running,
                    &mut remaining,
                    &mut live,
                    store,
                    &run_id,
                    &mut log_len,
                    publisher,
                    opts.target_stage.as_deref(),
                    &mut target_reached,
                    &mut run_error,
                )?;
            }
            Err(_) => return Err(WorkflowError::Internal("worker channel disconnected".into())),
        }
    }

    // ---- finalize / terminal event ----
    let mut output = None;
    if let Some(fin) = &workflow.finalize {
        match fin(&live) {
            Ok(o) => output = o,
            Err(e) => {
                let msg = e.to_string();
                let ev = RunEvent::RunErrored {
                    ts: now_ms(),
                    run_id: run_id.clone(),
                    error: msg.clone(),
                };
                store.append_event(&run_id, log_len, &ev)?;
                publish(&ev);
                let mut st = run_state.clone();
                st.status = RunStatus::Errored;
                st.error = Some(msg.clone());
                st.updated_at = now_ms();
                store.set_run_state(&run_id, &st)?;
                return Ok(RunOutcome {
                    run_id,
                    status: RunStatus::Errored,
                    output: None,
                    error: Some(msg),
                });
            }
        }
    }

    let (status, error) = match run_error {
        Some(e) => (RunStatus::Errored, Some(e)),
        None => (RunStatus::Finished, None),
    };
    let ev = match &error {
        Some(e) => RunEvent::RunErrored {
            ts: now_ms(),
            run_id: run_id.clone(),
            error: e.clone(),
        },
        None => RunEvent::RunFinished {
            ts: now_ms(),
            run_id: run_id.clone(),
            output: output.clone(),
        },
    };
    store.append_event(&run_id, log_len, &ev)?;
    publish(&ev);

    let mut st = run_state;
    st.status = status;
    st.output = if status == RunStatus::Finished {
        output.clone()
    } else {
        None
    };
    st.error = error.clone();
    st.updated_at = now_ms();
    store.set_run_state(&run_id, &st)?;

    Ok(RunOutcome {
        run_id,
        status,
        output,
        error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::define::{Backoff, RetryPolicy};
    use crate::error::StoreError;
    use crate::store::InMemoryStore;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct TestLog {
        started: Vec<String>,
        finished: Vec<String>,
        timeline: Vec<String>,
        active: usize,
        peaks: Vec<usize>,
        runs: HashMap<String, usize>,
    }

    impl TestLog {
        fn note_start(&mut self, id: &str) {
            self.started.push(id.to_string());
            self.timeline.push(format!(">{id}"));
            self.active += 1;
            self.peaks.push(self.active);
            *self.runs.entry(id.to_string()).or_insert(0) += 1;
        }
        fn note_finish(&mut self, id: &str) {
            self.finished.push(id.to_string());
            self.timeline.push(format!("<{id}"));
            self.active -= 1;
            self.peaks.push(self.active);
        }
    }

    fn rec_node(
        id: &str,
        log: &std::sync::Arc<Mutex<TestLog>>,
        work: impl Fn() + Send + Sync + 'static,
    ) -> NodeSpec {
        let log = log.clone();
        let id = id.to_string();
        NodeSpec::new(id.clone(), std::sync::Arc::new(move |_ctx: RunCtx| {
            {
                let mut l = log.lock().unwrap();
                l.note_start(&id);
            }
            work();
            {
                let mut l = log.lock().unwrap();
                l.note_finish(&id);
            }
            Ok(None)
        }))
    }

    fn idx(v: &[String], s: &str) -> usize {
        v.iter().position(|x| x == s).unwrap_or(usize::MAX)
    }

    fn simple_run(
        wf: &mut Workflow,
        store: &dyn RunStore,
        opts: RunOptions,
    ) -> Result<RunOutcome, WorkflowError> {
        run(wf, store, &opts, None)
    }

    #[test]
    fn linear_dag_runs_in_order() {
        let store = InMemoryStore::new();
        let log = std::sync::Arc::new(Mutex::new(TestLog::default()));
        let mut wf = Workflow::new("w")
            .node(rec_node("a", &log, || {}))
            .node(rec_node("b", &log, || {}).needs(["a"]))
            .node(rec_node("c", &log, || {}).needs(["b"]));
        let out = simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({})).max_concurrency(2))
            .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 1);
        assert_eq!(l.runs["c"], 1);
        assert_eq!(l.started, vec!["a", "b", "c"]);
        assert_eq!(l.finished, vec!["a", "b", "c"]);
        // c must not start before b finished
        assert!(idx(&l.timeline, ">c") > idx(&l.timeline, "<b"));
    }

    #[test]
    fn diamond_fan_in() {
        let store = InMemoryStore::new();
        let log = std::sync::Arc::new(Mutex::new(TestLog::default()));
        let mut wf = Workflow::new("w")
            .node(rec_node("a", &log, || {}))
            .node(rec_node("b", &log, || {}).needs(["a"]))
            .node(rec_node("c", &log, || {}).needs(["a", "b"]));
        simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({})).max_concurrency(2)).unwrap();
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 1);
        assert_eq!(l.runs["c"], 1);
        assert!(idx(&l.timeline, ">c") > idx(&l.timeline, "<a"));
        assert!(idx(&l.timeline, ">c") > idx(&l.timeline, "<b"));
    }

    #[test]
    fn fan_out_parallel_siblings() {
        let store = InMemoryStore::new();
        let log = std::sync::Arc::new(Mutex::new(TestLog::default()));
        let mut wf = Workflow::new("w")
            .node(rec_node("a", &log, || {}))
            .node(rec_node("b", &log, || std::thread::sleep(Duration::from_millis(60))).needs(["a"]))
            .node(rec_node("c", &log, || std::thread::sleep(Duration::from_millis(60))).needs(["a"]));
        simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({})).max_concurrency(3)).unwrap();
        let l = log.lock().unwrap();
        // both siblings overlap: each starts before the other finishes
        assert!(idx(&l.timeline, ">b") < idx(&l.timeline, "<c"));
        assert!(idx(&l.timeline, ">c") < idx(&l.timeline, "<b"));
    }

    #[test]
    fn resume_reruns_failed_nodes() {
        let store = InMemoryStore::new();
        let log = std::sync::Arc::new(Mutex::new(TestLog::default()));
        let fail_once = std::sync::Arc::new(AtomicBool::new(true));
        let fail_ref = fail_once.clone();
        let b_id = "b".to_string();
        let b_log = log.clone();
        let node_b = NodeSpec::new("b", std::sync::Arc::new(move |_ctx: RunCtx| {
            {
                let mut l = b_log.lock().unwrap();
                l.note_start(&b_id);
            }
            let res = if fail_ref.swap(false, Ordering::SeqCst) {
                Err(anyhow::anyhow!("boom"))
            } else {
                Ok(None)
            };
            {
                let mut l = b_log.lock().unwrap();
                l.note_finish(&b_id);
            }
            res
        }))
        .needs(["a"]);
        let mut wf = Workflow::new("w")
            .node(rec_node("a", &log, || {}))
            .node(node_b)
            .node(rec_node("c", &log, || {}).needs(["b"]));

        // first run: b fails, c never starts
        let out = simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({})).max_concurrency(2))
            .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        // resume with the same run_id: only b+c rerun
        let out = simple_run(
            &mut wf,
            &store,
            RunOptions::new(serde_json::json!({})).run_id(out.run_id).max_concurrency(2),
        )
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1, "successful prefix not re-run");
        assert_eq!(l.runs["b"], 2, "failed node re-attempted");
        assert_eq!(l.runs["c"], 1);
    }

    #[test]
    fn plain_resume_skips_success() {
        let store = InMemoryStore::new();
        let log = std::sync::Arc::new(Mutex::new(TestLog::default()));
        let mut wf = Workflow::new("w")
            .node(rec_node("a", &log, || {}))
            .node(rec_node("b", &log, || {}).needs(["a"]));
        let out = simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({}))).unwrap();
        let second = simple_run(
            &mut wf,
            &store,
            RunOptions::new(serde_json::json!({})).run_id(out.run_id),
        )
        .unwrap();
        assert_eq!(second.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 1);
    }

    #[test]
    fn continue_from_resets_downstream() {
        let store = InMemoryStore::new();
        let log = std::sync::Arc::new(Mutex::new(TestLog::default()));
        let mut wf = Workflow::new("w")
            .node(rec_node("a", &log, || {}))
            .node(rec_node("b", &log, || {}).needs(["a"]))
            .node(rec_node("c", &log, || {}).needs(["b"]));
        let out = simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({}))).unwrap();
        let second = simple_run(
            &mut wf,
            &store,
            RunOptions::new(serde_json::json!({})).run_id(out.run_id).continue_from("b"),
        )
        .unwrap();
        assert_eq!(second.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1, "prefix before continue_from untouched");
        assert_eq!(l.runs["b"], 2);
        assert_eq!(l.runs["c"], 2);
    }

    #[test]
    fn stale_up_to_date_reruns() {
        let store = InMemoryStore::new();
        let log = std::sync::Arc::new(Mutex::new(TestLog::default()));
        let fresh_a = std::sync::Arc::new(AtomicBool::new(true));
        let fresh_b = std::sync::Arc::new(AtomicBool::new(true));
        let (fresh_a_ref, fresh_b_ref) = (fresh_a.clone(), fresh_b.clone());
        let mut wf = Workflow::new("w")
            .node(rec_node("a", &log, || {}).up_to_date(move |_, _| fresh_a_ref.load(Ordering::SeqCst)))
            .node(rec_node("b", &log, || {}).up_to_date(move |_, _| fresh_b_ref.load(Ordering::SeqCst)));
        let out = simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({}))).unwrap();
        // mark a as stale, rerun: a must rerun, b stays
        fresh_a.store(false, Ordering::SeqCst);
        let second = simple_run(
            &mut wf,
            &store,
            RunOptions::new(serde_json::json!({})).run_id(out.run_id),
        )
        .unwrap();
        assert_eq!(second.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 2, "stale node re-ran");
        assert_eq!(l.runs["b"], 1, "up-to-date node skipped");
    }

    #[test]
    fn target_stage_stops_downstream() {
        let store = InMemoryStore::new();
        let log = std::sync::Arc::new(Mutex::new(TestLog::default()));
        let mut wf = Workflow::new("w")
            .node(rec_node("a", &log, || {}))
            .node(rec_node("b", &log, || {}).needs(["a"]))
            .node(rec_node("c", &log, || {}).needs(["b"]));
        let out = simple_run(
            &mut wf,
            &store,
            RunOptions::new(serde_json::json!({})).target_stage("b").max_concurrency(2),
        )
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 1);
        assert_eq!(l.runs.get("c").copied().unwrap_or(0), 0, "downstream never ran");
    }

    #[test]
    fn resource_gate_serializes_same_key() {
        let store = InMemoryStore::new();
        let log = std::sync::Arc::new(Mutex::new(TestLog::default()));
        let mut wf = Workflow::new("w")
            .node(rec_node("x", &log, || std::thread::sleep(Duration::from_millis(60))).resource(|_| "gpu:0".into()))
            .node(rec_node("y", &log, || std::thread::sleep(Duration::from_millis(60))).resource(|_| "gpu:0".into()));
        simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({})).max_concurrency(2)).unwrap();
        let l = log.lock().unwrap();
        // y (workflow-order second) must not start until x fully finished
        assert!(
            idx(&l.timeline, ">y") > idx(&l.timeline, "<x"),
            "same-resource nodes must not overlap"
        );
        assert_eq!(l.runs["x"], 1);
        assert_eq!(l.runs["y"], 1);
    }

    #[test]
    fn max_concurrency_caps_parallelism() {
        let store = InMemoryStore::new();
        let log = std::sync::Arc::new(Mutex::new(TestLog::default()));
        let mut wf = Workflow::new("w");
        for i in 0..5 {
            let id = format!("n{i}");
            wf = wf.node(rec_node(&id, &log, || std::thread::sleep(Duration::from_millis(30))));
        }
        simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({})).max_concurrency(2)).unwrap();
        let l = log.lock().unwrap();
        let peak = l.peaks.iter().copied().max().unwrap_or(0);
        assert!(peak <= 2, "peak concurrency was {peak}");
        assert_eq!(l.runs.len(), 5);
    }

    #[test]
    fn retry_records_attempts_then_succeeds() {
        let store = InMemoryStore::new();
        let fail = std::sync::Arc::new(AtomicBool::new(true));
        let fail_ref = fail.clone();
        let mut wf = Workflow::new("w").node(
            NodeSpec::new("a", std::sync::Arc::new(move |_ctx: RunCtx| {
                if fail_ref.swap(false, Ordering::SeqCst) {
                    Err(anyhow::anyhow!("transient"))
                } else {
                    Ok(None)
                }
            }))
            .retry(RetryPolicy::new(2, Backoff::Fixed { base_ms: 1 })),
        );
        let out = simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({}))).unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let events = store.get_events(&out.run_id).unwrap();
        let fin = events.iter().find_map(|e| match e {
            RunEvent::NodeFinished { node_id, attempts, .. } if node_id == "a" => Some(attempts),
            _ => None,
        });
        assert_eq!(fin.map(|a| a.len()), Some(2));
    }

    #[test]
    fn exhausted_retries_error() {
        let store = InMemoryStore::new();
        let mut wf = Workflow::new("w").node(
            NodeSpec::new("a", std::sync::Arc::new(move |_ctx: RunCtx| {
                Err(anyhow::anyhow!("boom"))
            }))
            .retry(RetryPolicy::new(2, Backoff::Fixed { base_ms: 1 })),
        );
        let out = simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({}))).unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(out.error.as_deref().unwrap().contains("boom"));
        let events = store.get_events(&out.run_id).unwrap();
        let failed = events.iter().find_map(|e| match e {
            RunEvent::NodeFailed { node_id, attempts, .. } if node_id == "a" => Some(attempts),
            _ => None,
        });
        assert_eq!(failed.map(|a| a.len()), Some(2));
    }

    #[test]
    fn enabled_disabled_edge_is_ignored() {
        let store = InMemoryStore::new();
        let log = std::sync::Arc::new(Mutex::new(TestLog::default()));
        let mut wf = Workflow::new("w")
            .node(rec_node("a", &log, || {}).enabled(|_| false))
            .node(rec_node("b", &log, || {}).needs(["a"]));
        let out = simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({}))).unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs.get("a").copied().unwrap_or(0), 0);
        assert_eq!(l.runs["b"], 1);
    }

    #[test]
    fn progress_and_subscribe_fan_out() {
        let store = InMemoryStore::new();
        let rx = store.subscribe("prog_run").unwrap();
        let events = std::sync::Arc::new(Mutex::new(Vec::new()));
        let sink = events.clone();
        let mut wf = Workflow::new("w").node(NodeSpec::new(
            "a",
            std::sync::Arc::new(move |ctx: RunCtx| {
                ctx.progress(0.5);
                Ok(None)
            }),
        ));
        let out = run(
            &mut wf,
            &store,
            &RunOptions::new(serde_json::json!({})).run_id("prog_run"),
            Some(&move |e: &RunEvent| sink.lock().unwrap().push(e.clone())),
        )
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);

        // publisher saw the emit-only progress + all checkpoints
        let evs = events.lock().unwrap();
        assert!(evs.iter().any(|e| matches!(e, RunEvent::NodeProgress { value: 0.5, .. })));
        assert!(evs.iter().any(|e| matches!(e, RunEvent::RunStarted { .. })));
        assert!(evs.iter().any(|e| matches!(e, RunEvent::NodeFinished { .. })));

        // subscriber (store log) saw checkpoints but NOT emit-only events
        let mut saw_finish = false;
        let mut saw_shared = false;
        let first = rx.recv_timeout(Duration::from_secs(2)).expect("got first event");
        saw_finish |= matches!(first, RunEvent::NodeFinished { .. });
        assert!(saw_finish, "first appended event should be NodeFinished");
        while let Ok(e) = rx.try_recv() {
            match e {
                RunEvent::NodeFinished { .. } => saw_finish = true,
                RunEvent::NodeProgress { .. } | RunEvent::NodeStarted { .. } => saw_shared = true,
                _ => {}
            }
        }
        assert!(saw_finish);
        assert!(!saw_shared, "emit-only events must not reach the log");
    }

    #[test]
    fn cas_conflict_detected() {
        let store = InMemoryStore::new();
        let ev = RunEvent::NodeFinished {
            ts: 1,
            run_id: "r1".into(),
            node_id: "a".into(),
            result: None,
            attempts: vec![],
        };
        store.append_event("r1", 0, &ev).unwrap();
        store.append_event("r1", 1, &ev).unwrap();
        let err = store.append_event("r1", 1, &ev).unwrap_err();
        match err {
            StoreError::Conflict { expected, actual, .. } => {
                assert_eq!(expected, 1);
                assert_eq!(actual, 2);
            }
            _ => panic!("expected Conflict"),
        }
    }

    #[test]
    fn finalize_produces_output() {
        let store = InMemoryStore::new();
        let mut wf = Workflow::new("w")
            .node(NodeSpec::new("a", std::sync::Arc::new(|_ctx: RunCtx| Ok(Some(serde_json::json!({"x": 1}))))))
            .finalize_with(|states| {
                let r = states
                    .get("a")
                    .and_then(|s| s.result.clone())
                    .unwrap_or(serde_json::Value::Null);
                Ok(Some(serde_json::json!({ "out": r })))
            });
        let out = simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({}))).unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "out": { "x": 1 } })));
        let st = store.get_run_state(&out.run_id).unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Finished);
        assert_eq!(st.output, Some(serde_json::json!({ "out": { "x": 1 } })));
    }

    #[test]
    fn validation_errors() {
        let mut dup = Workflow::new("w")
            .node(NodeSpec::new("a", std::sync::Arc::new(|_| Ok(None))))
            .node(NodeSpec::new("a", std::sync::Arc::new(|_| Ok(None))));
        assert!(dup.validate().is_err());

        let mut unknown = Workflow::new("w").node(
            NodeSpec::new("a", std::sync::Arc::new(|_| Ok(None))).needs(["ghost"]),
        );
        assert!(unknown.validate().is_err());

        let mut cycle = Workflow::new("w")
            .node(NodeSpec::new("a", std::sync::Arc::new(|_| Ok(None))).needs(["b"]))
            .node(NodeSpec::new("b", std::sync::Arc::new(|_| Ok(None))).needs(["a"]));
        assert!(cycle.validate().is_err());
    }

    #[test]
    fn timeout_marks_node_failed() {
        let store = InMemoryStore::new();
        let mut wf = Workflow::new("w").node(
            NodeSpec::new("a", std::sync::Arc::new(|_ctx: RunCtx| {
                std::thread::sleep(Duration::from_millis(200));
                Ok(None)
            }))
            .timeout(Duration::from_millis(5)),
        );
        let out = simple_run(&mut wf, &store, RunOptions::new(serde_json::json!({}))).unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(out.error.as_deref().unwrap().contains("timed out"));
    }

    #[test]
    fn continue_from_unknown_rejected() {
        let store = InMemoryStore::new();
        let mut wf = Workflow::new("w").node(NodeSpec::new("a", std::sync::Arc::new(|_| Ok(None))));
        let err = simple_run(
            &mut wf,
            &store,
            RunOptions::new(serde_json::json!({})).continue_from("nope"),
        )
        .unwrap_err();
        assert!(matches!(err, WorkflowError::Validation(_)));
    }
}