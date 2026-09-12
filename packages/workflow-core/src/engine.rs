use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::define::{StepCtx, StepOptions, Workflow};
use crate::error::WorkflowError;
use crate::event::{StepAttempt, StepState, StepStatus, RunEvent, RunStatus, fold_step_states};
use crate::resource::Gate;
use crate::store::{RunState, RunStore};

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Internal sentinel: the engine stopped driving the handler after
/// `target_step` succeeded. Becomes a `Finished` run, never `Errored`.
#[derive(Debug)]
pub struct StepHalt;

impl std::fmt::Display for StepHalt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "target_step reached, handler halted")
    }
}

impl std::error::Error for StepHalt {}

/// Shared driver state handed to every step (and to the `<WorkflowCtx>`).
/// This is the code-as-DAG substrate: the "graph" is just this state plus the
/// handler's control flow, discovered as the handler runs.
pub struct DrvInner {
    pub run_id: String,
    pub input: serde_json::Value,
    pub store: Arc<dyn RunStore>,
    pub gate: Arc<Gate>,
    /// Monotonic next-appendix index. Concurrent steps (via `try_join!`)
    /// CAS-retry on it so the append-only log never corrupts.
    log_len: AtomicUsize,
    /// Derived per-step states (replay short-circuit + result cache).
    lives: Mutex<std::collections::HashMap<String, StepState>>,
    pub target_step: Option<String>,
    target_reached: AtomicBool,
    pub publisher: Option<Arc<dyn Fn(&RunEvent) + Send + Sync>>,
}

impl DrvInner {
    /// Appends a checkpoint with optimistic CAS; on a lost race re-bases the
    /// cursor and retries (the other writer won, its event precedes ours).
    fn append(&self, ev: &RunEvent) -> Result<(), WorkflowError> {
        loop {
            let idx = self.log_len.load(Ordering::Acquire);
            match self.store.append_event(&self.run_id, idx, ev) {
                Ok(()) => {
                    self.log_len.store(idx + 1, Ordering::Release);
                    return Ok(());
                }
                Err(crate::error::StoreError::Conflict { actual, .. }) => {
                    self.log_len.store(actual, Ordering::Release);
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn publish(&self, ev: &RunEvent) {
        if let Some(p) = &self.publisher {
            p(ev);
        }
    }

    pub fn publish_progress(&self, step_id: &str, value: f64) {
        self.publish(&RunEvent::StepProgress {
            ts: now_ms(),
            run_id: self.run_id.clone(),
            step_id: step_id.to_string(),
            value,
        });
    }

    fn set_live(&self, step_id: &str, st: StepState) {
        if let Ok(mut lives) = self.lives.lock() {
            lives.insert(step_id.to_string(), st);
        }
    }

    fn live(&self, step_id: &str) -> Option<StepState> {
        self.lives.lock().ok()?.get(step_id).cloned()
    }

    fn is_target(&self, step_id: &str) -> bool {
        self.target_step.as_deref() == Some(step_id)
    }

    fn mark_target_reached(&self, step_id: &str) {
        if self.is_target(step_id) {
            self.target_reached.store(true, Ordering::Release);
        }
    }

    fn halted(&self) -> bool {
        self.target_reached.load(Ordering::Acquire)
    }
}

/// Runs one durable step: replay short-circuit, fresh execution with
/// retry/timeout/gate, checkpoint append. Returns the step's result.
pub(crate) async fn exec_step<F, Fut>(
    inner: &Arc<DrvInner>,
    step_id: &str,
    opts: &StepOptions,
    run: F,
) -> anyhow::Result<serde_json::Value>
where
    F: FnOnce(StepCtx) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = anyhow::Result<serde_json::Value>> + Send + 'static,
{
    // target_step reached → halt the whole handler.
    if inner.halted() {
        return Err(StepHalt.into());
    }

    // Replay short-circuit: a succeeded checkpoint returns the cached result
    // without re-running `run`. A failed checkpoint rethrows the stored error
    // (failures are terminal; retry via continue_from / a new run).
    if let Some(st) = inner.live(step_id) {
        match st.status {
            StepStatus::Success => {
                let fresh = match &opts.up_to_date {
                    Some(utd) => {
                        let probe = StepCtx {
                            inner: inner.clone(),
                            step_id: step_id.to_string(),
                            attempt: 0,
                        };
                        utd(&probe, &st)
                    }
                    None => true,
                };
                if fresh {
                    return Ok(st.result.clone().unwrap_or(serde_json::Value::Null));
                }
                // stale → fall through and re-execute below.
            }
            StepStatus::Failed => {
                let msg = st.error.clone().unwrap_or_else(|| "failed".to_string());
                return Err(anyhow::anyhow!("step `{step_id}` previously failed: {msg}"));
            }
            // Running can only appear from a live concurrent duplicate id —
            // a user error we surface loudly instead of double-appending.
            StepStatus::Running | StepStatus::Pending => {}
        }
    }
    let running_marker = StepState {
        status: StepStatus::Running,
        ..Default::default()
    };
    inner.set_live(step_id, running_marker.clone());

    inner.publish(&RunEvent::StepStarted {
        ts: now_ms(),
        run_id: inner.run_id.clone(),
        step_id: step_id.to_string(),
    });

    // Resource gate: same key ⇒ serialized (capacity-1 async semaphore).
    let _gate = match &opts.resource {
        Some(key) => Some(inner.gate.acquire(key).await),
        None => None,
    };

    let max_attempts = opts.retry.as_ref().map(|p| p.max_attempts).unwrap_or(1);
    let mut attempts: Vec<StepAttempt> = Vec::with_capacity(max_attempts);

    for attempt in 1..=max_attempts {
        let step_ctx = StepCtx {
            inner: inner.clone(),
            step_id: step_id.to_string(),
            attempt,
        };
        let started_at = now_ms();
        let run_fut = run.clone()(step_ctx);
        let outcome = match opts.timeout {
            Some(t) => match tokio::time::timeout(t, run_fut).await {
                Ok(res) => res.map_err(|e| e.to_string()),
                Err(_) => Err(format!(
                    "step \"{step_id}\" timed out after {t:?}; underlying future continues on the runtime"
                )),
            },
            None => run_fut.await.map_err(|e| e.to_string()),
        };
        let finished_at = now_ms();

        match outcome {
            Ok(result) => {
                attempts.push(StepAttempt {
                    attempt,
                    started_at,
                    finished_at,
                    result: Some(result.clone()),
                    error: None,
                });
                let ev = RunEvent::StepFinished {
                    ts: now_ms(),
                    run_id: inner.run_id.clone(),
                    step_id: step_id.to_string(),
                    result: Some(result.clone()),
                    attempts,
                };
                inner.append(&ev)?;
                inner.set_live(
                    step_id,
                    StepState {
                        status: StepStatus::Success,
                        result: Some(result.clone()),
                        error: None,
                        started_at: Some(started_at),
                        finished_at: Some(finished_at),
                    },
                );
                inner.publish(&ev);
                inner.mark_target_reached(step_id);
                return Ok(result);
            }
            Err(err) => {
                attempts.push(StepAttempt {
                    attempt,
                    started_at,
                    finished_at,
                    result: None,
                    error: Some(err.clone()),
                });
                let retrying = opts.retry.is_some() && attempt < max_attempts;
                if retrying {
                    let delay = opts
                        .retry
                        .as_ref()
                        .map(|p| p.backoff.delay_ms(attempt))
                        .unwrap_or(0);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    continue;
                }
                let msg = format!("step `{step_id}` failed: {err}");
                let ev = RunEvent::StepFailed {
                    ts: now_ms(),
                    run_id: inner.run_id.clone(),
                    step_id: step_id.to_string(),
                    error: err.clone(),
                    attempts,
                };
                inner.append(&ev)?;
                inner.set_live(
                    step_id,
                    StepState {
                        status: StepStatus::Failed,
                        result: None,
                        error: Some(err),
                        started_at: Some(started_at),
                        finished_at: Some(finished_at),
                    },
                );
                inner.publish(&ev);
                return Err(anyhow::anyhow!(msg));
            }
        }
    }
    unreachable!("retry loop always returns")
}

/// Per-invocation options. `run_id`, `continue_from` and `target_step` are
/// invocation options, NOT persisted — persistent state lives in the event
/// log. `max_concurrency` is gone: parallelism is explicit handler code
/// (`tokio::try_join!`).
#[derive(Default)]
pub struct RunOptions {
    pub run_id: Option<String>,
    pub input: serde_json::Value,
    pub target_step: Option<String>,
    pub continue_from: Option<String>,
}

impl RunOptions {
    pub fn new(input: serde_json::Value) -> Self {
        Self {
            run_id: None,
            input,
            target_step: None,
            continue_from: None,
        }
    }

    pub fn run_id(mut self, v: impl Into<String>) -> Self {
        self.run_id = Some(v.into());
        self
    }

    pub fn target_step(mut self, v: impl Into<String>) -> Self {
        self.target_step = Some(v.into());
        self
    }

    pub fn continue_from(mut self, v: impl Into<String>) -> Self {
        self.continue_from = Some(v.into());
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

/// Runs (or resumes) a workflow by driving its async handler.
///
/// Inputs:
/// - `continue_from`: the store truncates everything at `step_id`'s latest
///   terminal checkpoint (inclusive) before re-running the handler, so the
///   prefix short-circuits and the suffix re-executes from scratch.
/// - `target_step`: once that step succeeds the engine raises [`StepHalt`],
///   the handler unwinds (users should propagate with `?`), and the run ends
///   `Finished` with no output.
///
/// On resume, `ctx.step` short-circuits succeeded checkpoints (cached result,
/// `run` not re-executed) and rethrows failed ones. Multiplex step results
/// however you like — the log is the only source of truth.
pub async fn run_workflow(
    workflow: &Workflow,
    store: Arc<dyn RunStore>,
    opts: &RunOptions,
    publisher: Option<Arc<dyn Fn(&RunEvent) + Send + Sync>>,
) -> Result<RunOutcome, WorkflowError> {
    let run_id = opts
        .run_id
        .clone()
        .unwrap_or_else(|| format!("run_{}", now_ms()));
    let ts = now_ms();

    // continue_from lives at the store layer: cut the log at the step's latest
    // terminal checkpoint so the replayed handler re-runs that suffix.
    if let Some(cf) = &opts.continue_from {
        store.truncate_runs(&run_id, cf)?;
    }

    let run_state = match store.get_run_state(&run_id)? {
        Some(mut st) => {
            st.status = RunStatus::Running;
            st.error = None;
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

    let events = store.get_events(&run_id)?;
    let lives = fold_step_states(&events);
    let log_len = events.len();

    let target_reached = match &opts.target_step {
        Some(t) => lives
            .get(t)
            .map(|s| s.status == StepStatus::Success)
            .unwrap_or(false),
        None => false,
    };

    let inner = Arc::new(DrvInner {
        run_id: run_id.clone(),
        input: opts.input.clone(),
        store: store.clone(),
        gate: Arc::new(Gate::new()),
        log_len: AtomicUsize::new(log_len),
        lives: Mutex::new(lives),
        target_step: opts.target_step.clone(),
        target_reached: AtomicBool::new(target_reached),
        publisher,
    });
    inner.publish(&RunEvent::RunStarted { ts, run_id: run_id.clone() });

    let ctx = crate::define::WorkflowCtx { inner: inner.clone() };
    let handler_result = (workflow.handler)(ctx).await;

    let (status, output, error) = match handler_result {
        Ok(output) => (RunStatus::Finished, Some(output), None),
        Err(e) if e.downcast_ref::<StepHalt>().is_some() => {
            (RunStatus::Finished, None, None)
        }
        Err(e) => {
            let msg = e.to_string();
            (RunStatus::Errored, None, Some(msg))
        }
    };

    let terminal = match &error {
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
    inner.append(&terminal)?;
    inner.publish(&terminal);

    let mut st = run_state;
    st.status = status;
    st.output = output.clone();
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

/// Sync convenience over a local multi-thread runtime for callers that are
/// not async themselves (e.g. LocalDub's CLI entrypoint).
pub fn run_workflow_sync(
    workflow: &Workflow,
    store: Arc<dyn RunStore>,
    opts: &RunOptions,
    publisher: Option<Arc<dyn Fn(&RunEvent) + Send + Sync>>,
) -> Result<RunOutcome, WorkflowError> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .map_err(|e| WorkflowError::Internal(format!("tokio runtime: {e}")))?;
    rt.block_on(run_workflow(workflow, store, opts, publisher))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::define::{Backoff, RetryPolicy, WorkflowCtx};
    use crate::store::InMemoryStore;
    use std::sync::atomic::AtomicBool;
    use tokio::try_join;

    #[derive(Default)]
    struct TestLog {
        started: Vec<String>,
        finished: Vec<String>,
        timeline: Vec<String>,
        active: usize,
        peaks: Vec<usize>,
        runs: std::collections::HashMap<String, usize>,
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

    fn idx(v: &[String], s: &str) -> usize {
        v.iter().position(|x| x == s).unwrap_or(usize::MAX)
    }

    #[tokio::test]
    async fn serial_handler_runs_in_order() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = Workflow::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
            let log = log.clone();
            async move {
                for id in ["a", "b", "c"] {
                    let log = log.clone();
                    let id = id.to_string();
                    let id_c = id.clone();
                    ctx.step(&id, move |_sc: StepCtx| {
                        let (log, id) = (log.clone(), id_c.clone());
                        async move {
                            log.lock().unwrap().note_start(&id);
                            log.lock().unwrap().note_finish(&id);
                            Ok(serde_json::Value::Null)
                        }
                    })
                    .await?;
                }
                Ok(serde_json::Value::Null)
            }
            }
        });
        let out = run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.started, vec!["a", "b", "c"]);
        assert_eq!(l.finished, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn parallel_siblings_share_harness() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = Workflow::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
            let log = log.clone();
            async move {
                let a = {
                    let log = log.clone();
                    ctx.step("a", move |_sc| {
                        let log = log.clone();
                        async move {
                            log.lock().unwrap().note_start("a");
                            tokio::time::sleep(Duration::from_millis(60)).await;
                            log.lock().unwrap().note_finish("a");
                            Ok(serde_json::Value::Null)
                        }
                    })
                };
                let b = {
                    let log = log.clone();
                    ctx.step("b", move |_sc| {
                        let log = log.clone();
                        async move {
                            log.lock().unwrap().note_start("b");
                            tokio::time::sleep(Duration::from_millis(60)).await;
                            log.lock().unwrap().note_finish("b");
                            Ok(serde_json::Value::Null)
                        }
                    })
                };
                let _ = try_join!(a, b)?;
                Ok(serde_json::Value::Null)
            }
            }
        });
        run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 1);
        assert!(idx(&l.timeline, ">a") < idx(&l.timeline, "<b"));
        assert!(idx(&l.timeline, ">b") < idx(&l.timeline, "<a"));
    }

    #[tokio::test]
    async fn parallel_serial_chain_a_and_bcd() {
        // The motivating case: a runs concurrently with the serial chain
        // b → c → d, then everything joins before e.
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = Workflow::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
            let log = log.clone();
            async move {
                // Step closures capture their own `log` clone (per-call block)
                // so `log` stays borrowable for the peer chain + `e`.
                let a = {
                    let log = log.clone();
                    ctx.step("a", move |_sc: StepCtx| {
                        let log = log.clone();
                        async move {
                            log.lock().unwrap().note_start("a");
                            tokio::time::sleep(Duration::from_millis(40)).await;
                            log.lock().unwrap().note_finish("a");
                            Ok(serde_json::Value::Null)
                        }
                    })
                };
                let b = {
                    let log = log.clone();
                    ctx.step("b", move |_sc: StepCtx| {
                        let log = log.clone();
                        async move {
                            log.lock().unwrap().note_start("b");
                            tokio::time::sleep(Duration::from_millis(40)).await;
                            log.lock().unwrap().note_finish("b");
                            Ok(serde_json::Value::Null)
                        }
                    })
                };
                // wait for b, then c, then d — serial, but concurrent with `a`.
                let chain_ctx = ctx.clone();
                let chain_log = log.clone();
                let chain = async move {
                    let _ = b.await?;
                    let _ = {
                        let log = chain_log.clone();
                        chain_ctx.step("c", move |_sc: StepCtx| {
                            let log = log.clone();
                            async move {
                                log.lock().unwrap().note_start("c");
                                tokio::time::sleep(Duration::from_millis(40)).await;
                                log.lock().unwrap().note_finish("c");
                                Ok(serde_json::Value::Null)
                            }
                        })
                    }
                    .await?;
                    let _ = {
                        let log = chain_log.clone();
                        chain_ctx.step("d", move |_sc: StepCtx| {
                            let log = log.clone();
                            async move {
                                log.lock().unwrap().note_start("d");
                                tokio::time::sleep(Duration::from_millis(40)).await;
                                log.lock().unwrap().note_finish("d");
                                Ok(serde_json::Value::Null)
                            }
                        })
                    }
                    .await?;
                    Ok(())
                };
                let _ = try_join!(a, chain)?;
                let _ = {
                    let log = log.clone();
                    ctx.step("e", move |_sc: StepCtx| {
                        let log = log.clone();
                        async move {
                            log.lock().unwrap().note_start("e");
                            tokio::time::sleep(Duration::from_millis(40)).await;
                            log.lock().unwrap().note_finish("e");
                            Ok(serde_json::Value::Null)
                        }
                    })
                }
                .await?;
                Ok(serde_json::Value::Null)
            }
            }
        });
        run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        let l = log.lock().unwrap();
        for id in ["a", "b", "c", "d", "e"] {
            assert_eq!(l.runs[id], 1, "{id} ran exactly once");
        }
        // b stops before both c and d start; e waits for a, d.
        assert!(idx(&l.timeline, ">c") > idx(&l.timeline, "<b"));
        assert!(idx(&l.timeline, ">d") > idx(&l.timeline, "<c"));
        assert!(idx(&l.timeline, ">e") > idx(&l.timeline, "<d"));
        // a overlapped the chain: a started before c (mid-chain) finished.
        assert!(idx(&l.timeline, ">a") < idx(&l.timeline, "<c"));
    }

    #[tokio::test]
    async fn resume_short_circuits_success() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = Workflow::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
            let log = log.clone();
            async move {
                for id in ["a", "b"] {
                    let log = log.clone();
                    let id = id.to_string();
                    let id_c = id.clone();
                    ctx.step(&id, move |_sc: StepCtx| {
                        let (log, id) = (log.clone(), id_c.clone());
                        async move {
                            log.lock().unwrap().note_start(&id);
                            log.lock().unwrap().note_finish(&id);
                            Ok(serde_json::Value::Null)
                        }
                    })
                    .await?;
                }
                Ok(serde_json::Value::Null)
            }
            }
        });
        let out = run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        let second = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({})).run_id(out.run_id),
            None,
        )
        .await
        .unwrap();
        assert_eq!(second.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 1);
    }

    #[tokio::test]
    async fn failed_is_terminal_until_continue_from() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let fail = Arc::new(AtomicBool::new(true));
        let wf = Workflow::new("w").handler({
            let log = log.clone();
            let fail = fail.clone();
            move |ctx: WorkflowCtx| {
            let log = log.clone();
            let fail = fail.clone();
            async move {
                for id in ["a", "b"] {
                    let (log, fail) = (log.clone(), fail.clone());
                    let id = id.to_string();
                    let id_c = id.clone();
                    ctx.step(&id, move |_sc: StepCtx| {
                        let (log, fail, id) = (log.clone(), fail.clone(), id_c.clone());
                        async move {
                            log.lock().unwrap().note_start(&id);
                            let res = if id == "b" && fail.swap(false, Ordering::SeqCst) {
                                Err(anyhow::anyhow!("boom"))
                            } else {
                                Ok(serde_json::Value::Null)
                            };
                            log.lock().unwrap().note_finish(&id);
                            res
                        }
                    })
                    .await?;
                }
                Ok(serde_json::Value::Null)
            }
            }
        });

        // first run: b fails, run errors
        let out = run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(out.error.as_deref().unwrap().contains("boom"));
        assert_eq!(log.lock().unwrap().runs["b"], 1);
        let run_id = out.run_id.clone();

        // plain resume: failed checkpoint rethrows → still errored, no rerun
        let again = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({})).run_id(run_id.clone()),
            None,
        )
        .await
        .unwrap();
        assert_eq!(again.status, RunStatus::Errored);
        assert_eq!(log.lock().unwrap().runs["b"], 1, "no re-execution on plain resume");

        // continue_from "b": truncate b's checkpoint + suffix, replay reruns b
        let resumed = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({}))
                .run_id(run_id)
                .continue_from("b"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(resumed.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1, "prefix before continue_from untouched");
        assert_eq!(l.runs["b"], 2, "suffix reran via continue_from");
    }

    #[tokio::test]
    async fn continue_from_resets_downstream() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = Workflow::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
            let log = log.clone();
            async move {
                for id in ["a", "b", "c"] {
                    let log = log.clone();
                    let id = id.to_string();
                    let id_c = id.clone();
                    ctx.step(&id, move |_sc: StepCtx| {
                        let (log, id) = (log.clone(), id_c.clone());
                        async move {
                            log.lock().unwrap().note_start(&id);
                            log.lock().unwrap().note_finish(&id);
                            Ok(serde_json::Value::Null)
                        }
                    })
                    .await?;
                }
                Ok(serde_json::Value::Null)
            }
            }
        });
        let out = run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        let second = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({}))
                .run_id(out.run_id)
                .continue_from("b"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(second.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 2);
        assert_eq!(l.runs["c"], 2);
    }

    #[tokio::test]
    async fn stale_up_to_date_reruns() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let fresh_a = Arc::new(AtomicBool::new(true));
        let fresh_b = Arc::new(AtomicBool::new(true));
        let wf = Workflow::new("w").handler({
            let log = log.clone();
            let fresh_a = fresh_a.clone();
            let fresh_b = fresh_b.clone();
            move |ctx: WorkflowCtx| {
            let log = log.clone();
            let fresh_a = fresh_a.clone();
            let fresh_b = fresh_b.clone();
            async move {
                let opts_a = StepOptions::new().up_to_date(move |_, _| fresh_a.load(Ordering::SeqCst));
                let opts_b = StepOptions::new().up_to_date(move |_, _| fresh_b.load(Ordering::SeqCst));
                for (id, opts) in [("a", opts_a), ("b", opts_b)] {
                    let log = log.clone();
                    let id = id.to_string();
                    let id_c = id.clone();
                    ctx.step_with(&id, opts, move |_sc: StepCtx| {
                        let (log, id) = (log.clone(), id_c.clone());
                        async move {
                            log.lock().unwrap().note_start(&id);
                            log.lock().unwrap().note_finish(&id);
                            Ok(serde_json::Value::Null)
                        }
                    })
                    .await?;
                }
                Ok(serde_json::Value::Null)
            }
            }
        });
        let out = run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        // mark a stale, rereun: a reruns, b stays cached
        fresh_a.store(false, Ordering::SeqCst);
        let second = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({})).run_id(out.run_id),
            None,
        )
        .await
        .unwrap();
        assert_eq!(second.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 2);
        assert_eq!(l.runs["b"], 1);
    }

    #[tokio::test]
    async fn target_step_stops_downstream() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = Workflow::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
            let log = log.clone();
            async move {
                for id in ["a", "b", "c"] {
                    let log = log.clone();
                    let id = id.to_string();
                    let id_c = id.clone();
                    ctx.step(&id, move |_sc: StepCtx| {
                        let (log, id) = (log.clone(), id_c.clone());
                        async move {
                            log.lock().unwrap().note_start(&id);
                            log.lock().unwrap().note_finish(&id);
                            Ok(serde_json::Value::Null)
                        }
                    })
                    .await?;
                }
                Ok(serde_json::Value::Null)
            }
            }
        });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({})).target_step("b"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 1);
        assert_eq!(l.runs.get("c").copied().unwrap_or(0), 0, "downstream never ran");
    }

    #[tokio::test]
    async fn resource_gate_serializes_same_key() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = Workflow::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
            let log = log.clone();
            async move {
                let ser = |id: &'static str| {
                    let log = log.clone();
                    let opts = StepOptions::new().resource("gpu:0");
                    ctx.step_with(id, opts, move |_sc: StepCtx| {
                        let log = log.clone();
                        async move {
                            log.lock().unwrap().note_start(id);
                            tokio::time::sleep(Duration::from_millis(60)).await;
                            log.lock().unwrap().note_finish(id);
                            Ok(serde_json::Value::Null)
                        }
                    })
                };
                let a = ser("a");
                let b = ser("b");
                let _ = try_join!(a, b)?;
                Ok(serde_json::Value::Null)
            }
            }
        });
        run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        let l = log.lock().unwrap();
        assert!(idx(&l.timeline, ">b") > idx(&l.timeline, "<a"));
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 1);
    }

    #[tokio::test]
    async fn retry_records_attempts_then_succeeds() {
        let store = Arc::new(InMemoryStore::new());
        let fail = Arc::new(AtomicBool::new(true));
        let wf = Workflow::new("w").handler(move |ctx: WorkflowCtx| {
            let fail = fail.clone();
            async move {
                let fail = fail.clone();
                let retry = RetryPolicy::new(2, Backoff::Fixed { base_ms: 1 });
                ctx.step_with(
                    "a",
                    StepOptions::new().retry(retry),
                    move |_sc: StepCtx| {
                        let fail = fail.clone();
                        async move {
                            if fail.swap(false, Ordering::SeqCst) {
                                Err(anyhow::anyhow!("transient"))
                            } else {
                                Ok(serde_json::Value::Null)
                            }
                        }
                    },
                )
                .await
            }
        });
        let out = run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let events = store.get_events(&out.run_id).unwrap();
        let fin = events.iter().find_map(|e| match e {
            RunEvent::StepFinished { step_id, attempts, .. } if step_id == "a" => Some(attempts),
            _ => None,
        });
        assert_eq!(fin.map(|a| a.len()), Some(2));
    }

    #[tokio::test]
    async fn exhausted_retries_error() {
        let store = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("w").handler(move |ctx: WorkflowCtx| {
            async move {
                let retry = RetryPolicy::new(2, Backoff::Fixed { base_ms: 1 });
                ctx.step_with(
                    "a",
                    StepOptions::new().retry(retry),
                    move |_sc: StepCtx| async move { Err(anyhow::anyhow!("boom")) },
                )
                .await
            }
        });
        let out = run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(out.error.as_deref().unwrap().contains("boom"));
        let events = store.get_events(&out.run_id).unwrap();
        let failed = events.iter().find_map(|e| match e {
            RunEvent::StepFailed { step_id, attempts, .. } if step_id == "a" => Some(attempts),
            _ => None,
        });
        assert_eq!(failed.map(|a| a.len()), Some(2));
    }

    #[tokio::test]
    async fn handler_output_is_run_output() {
        let store = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("w").handler(|ctx: WorkflowCtx| async move {
            let v = ctx
                .step("a", move |_sc: StepCtx| async move {
                    Ok(serde_json::json!({"x": 1}))
                })
                .await?;
            Ok(serde_json::json!({ "out": v }))
        });
        let out = run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "out": { "x": 1 } })));
        let st = store.get_run_state(&out.run_id).unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Finished);
        assert_eq!(st.output, Some(serde_json::json!({ "out": { "x": 1 } })));
    }

    #[tokio::test]
    async fn timeout_marks_step_failed() {
        let store = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("w").handler(|ctx: WorkflowCtx| async move {
            let _ = ctx
                .step_with(
                    "a",
                    StepOptions::new().timeout(Duration::from_millis(5)),
                    move |_sc: StepCtx| async move {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        Ok(serde_json::Value::Null)
                    },
                )
                .await?;
            Ok(serde_json::Value::Null)
        });
        let out = run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(out.error.as_deref().unwrap().contains("timed out"));
    }

    #[tokio::test]
    async fn progress_and_subscribe_fan_out() {
        let store = Arc::new(InMemoryStore::new());
        let rx = store.subscribe("prog_run").unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = events.clone();
        let wf = Workflow::new("w").handler(|ctx: WorkflowCtx| async move {
            ctx.step("a", move |sc: StepCtx| async move {
                sc.progress(0.5);
                Ok(serde_json::Value::Null)
            })
            .await
        });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({})).run_id("prog_run"),
            Some(Arc::new(move |e: &RunEvent| sink.lock().unwrap().push(e.clone()))),
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);

        // publisher saw emit-only progress + checkpoints
        let evs = events.lock().unwrap();
        assert!(evs.iter().any(|e| matches!(e, RunEvent::StepProgress { value: 0.5, .. })));
        assert!(evs.iter().any(|e| matches!(e, RunEvent::RunStarted { .. })));
        assert!(evs.iter().any(|e| matches!(e, RunEvent::StepFinished { .. })));

        // subscriber (store log) saw the checkpoint but not emit-only events
        let mut saw_finish = false;
        let mut saw_shared = false;
        let first = rx.recv_timeout(Duration::from_secs(2)).expect("got first event");
        saw_finish |= matches!(first, RunEvent::StepFinished { .. });
        assert!(saw_finish, "first appended event should be StepFinished");
        while let Ok(e) = rx.try_recv() {
            match e {
                RunEvent::StepFinished { .. } => saw_finish = true,
                RunEvent::StepProgress { .. } | RunEvent::StepStarted { .. } => saw_shared = true,
                _ => {}
            }
        }
        assert!(saw_finish);
        assert!(!saw_shared, "emit-only events must not reach the log");
    }

    #[tokio::test]
    async fn cas_conflict_detected() {
        let store = Arc::new(InMemoryStore::new());
        let ev = RunEvent::StepFinished {
            ts: 1,
            run_id: "r1".into(),
            step_id: "a".into(),
            result: None,
            attempts: vec![],
        };
        store.append_event("r1", 0, &ev).unwrap();
        store.append_event("r1", 1, &ev).unwrap();
        let err = store.append_event("r1", 1, &ev).unwrap_err();
        match err {
            crate::error::StoreError::Conflict { expected, actual, .. } => {
                assert_eq!(expected, 1);
                assert_eq!(actual, 2);
            }
            _ => panic!("expected Conflict"),
        }
    }
}