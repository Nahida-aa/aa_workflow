//! 引擎驱动：代码即 DAG 的执行器。`EngineRuntime` 是共享驱动态（step 重放 /
//! 结果缓存 / CAS 追加），`exec_step` 跑单个 durable step，
//! `exec_pause` + `signal_run` 实现 durable 等待（approval / sleep）。
//! 单次 invocation 的顶层编排（`run_workflow` / `run_workflow_sync` /
//! `RunOptions` / `RunOutcome`）在 [`run_workflow`](run_workflow) 子模块。

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::define::{StepCtx, StepOptions};
use crate::error::{StoreError, WorkflowError};
use crate::event::{RunEvent, RunStatus, StepAttempt, StepState, StepStatus};
use crate::resource::Gate;
use crate::store::RunStore;

mod run_workflow;
pub use run_workflow::{run_workflow, run_workflow_sync, RunOptions, RunOutcome};

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

/// Internal sentinel: [`cancel_run`] set the run to `Aborted` while the
/// handler was driving. Step closures are not interruptible mid-`await`,
/// so the error surfaces at the next engine boundary (step entry / attempt
/// boundary / pause loop tick). Becomes an `Aborted` run, never `Errored`.
#[derive(Debug)]
pub struct WorkflowCancelled;

impl std::fmt::Display for WorkflowCancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "workflow aborted")
    }
}

impl std::error::Error for WorkflowCancelled {}

/// True when `run_id`'s run state was set to `Aborted` by [`cancel_run`].
/// Polled at step entry and inside the pause loop; store-agnostic (works for
/// any [`RunStore`], including filesystem stores without a notification
/// channel).
fn is_cancelled(store: &Arc<dyn RunStore>, run_id: &str) -> bool {
    store
        .get_run_state(run_id)
        .ok()
        .flatten()
        .map(|st| st.status == RunStatus::Aborted)
        .unwrap_or(false)
}

/// Cancels a live or parked run: flips its state to terminal `Aborted`. The
/// engine spots it on its next poll (≤ `RESUME_POLL_MS` when parked, at the
/// next step/attempt boundary when running) and returns
/// [`WorkflowCancelled`], so the drive ends `Aborted` rather than `Errored`.
/// A second cancel on an already-terminal run is a no-op `Ok(())`.
pub fn cancel_run(store: &dyn RunStore, run_id: &str) -> Result<(), WorkflowError> {
    let Some(mut st) = store.get_run_state(run_id)? else {
        return Err(WorkflowError::RunNotFound(run_id.to_string()));
    };
    if matches!(
        st.status,
        RunStatus::Finished | RunStatus::Errored | RunStatus::Aborted
    ) {
        return Ok(());
    }
    st.status = RunStatus::Aborted;
    st.error = Some(WorkflowCancelled.to_string());
    st.waiting_for = None;
    st.pending_approval = None;
    st.updated_at = now_ms();
    store.set_run_state(run_id, &st)?;
    Ok(())
}

/// Shared driver state handed to every step (and to the `<WorkflowCtx>`).
/// This is the code-as-DAG substrate: the "graph" is just this state plus the
/// handler's control flow, discovered as the handler runs.
pub struct EngineRuntime {
    pub run_id: String,
    pub input: serde_json::Value,
    /// Per-invocation state, rebuilt from `initialize(input)` on every start
    /// and resume (see `define::Workflow::initialize`). Guarded by a
    /// `std::sync::RwLock`; it is the live image that `BaseCtx::state` (the
    /// handler's field working copy) snapshots at drive start and flushes back
    /// to before every durable primitive. Never persisted.
    pub state: Arc<std::sync::RwLock<serde_json::Value>>,
    pub store: Arc<dyn RunStore>,
    pub gate: Arc<Gate>,
    /// Monotonic next-appendix index. Concurrent steps (via `try_join!`)
    /// CAS-retry on it so the append-only log never corrupts.
    log_len: AtomicUsize,
    /// Derived per-step states (replay short-circuit + result cache).
    lives: Mutex<std::collections::HashMap<String, StepState>>,
    pub target_step: Option<String>,
    target_reached: AtomicBool,
    /// Per-invocation positional counters for `ctx.now()` / `ctx.uuid()`
    /// (rebuilt on every drive, mirroring TanStack's per-run `engine.counters`);
    /// used to derive the checkpoint ids `__now-{n}` / `__uuid-{n}`.
    now_counter: AtomicUsize,
    uuid_counter: AtomicUsize,
    /// Runtime budget (TanStack `deadline`): absolute UTC ms for this drive.
    pub deadline: Option<i64>,
    /// Headroom threshold for `should_yield()` (TanStack `minYieldRemainingMs`).
    pub min_yield_remaining_ms: u64,
    /// Absolute ms at which `ctx.yield_()` re-wakes (TanStack `yieldResumeAt`).
    pub yield_resume_at: Option<i64>,
    /// Positional counter for `__yield-{n}` pause keys (per-invocation).
    pub(crate) yield_counter: AtomicUsize,
    pub publisher: Option<Arc<dyn Fn(&RunEvent) + Send + Sync>>,
}

impl EngineRuntime {
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

    pub(crate) fn publish(&self, ev: &RunEvent) {
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
    inner: &Arc<EngineRuntime>,
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

    // Cancelled run → stop driving at this boundary.
    if is_cancelled(&inner.store, &inner.run_id) {
        return Err(WorkflowCancelled.into());
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
            // Paused likewise: it is a projection for an approve/sleep wait
            // key, and re-using such a key as a step id is a user error.
            StepStatus::Running | StepStatus::Pending | StepStatus::Paused => {}
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
                    if is_cancelled(&inner.store, &inner.run_id) {
                        return Err(WorkflowCancelled.into());
                    }
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

/// How often the engine re-reads the log to notice an external signal.
/// Polling (rather than a subscription) keeps pause/resume working against any
/// [`RunStore`], including LocalDub's `FsRunStore` which has no `subscribe`.
const RESUME_POLL_MS: Duration = Duration::from_millis(25);

/// Default `minYieldRemainingMs` (TanStack): `should_yield()` turns true once
/// fewer than this many ms of the runtime budget remain.
pub const DEFAULT_MIN_YIELD_REMAINING_MS: u64 = 1000;

/// Deterministic wall-clock behind [`WorkflowCtx::now`](crate::define::WorkflowCtx::now)
/// (TanStack `ctx.now`): the call's timestamp, recorded as a `NowRecorded`
/// checkpoint. On replay the recorded value is served from the log — a run
/// sees the **same** clock across resumes. The checkpoint id uses a
/// per-invocation atomic counter (`__now-0`, `__now-1`, …) so concurrent
/// calls stay unique; replay permutes them to the same ids and reads the cache.
pub fn exec_now(inner: &Arc<EngineRuntime>) -> anyhow::Result<i64> {
    let k = inner.now_counter.fetch_add(1, Ordering::SeqCst);
    let step_id = format!("__now-{k}");
    for ev in inner.store.get_events(&inner.run_id)? {
        if let RunEvent::NowRecorded { step_id: id, value, .. } = &ev
            && id == &step_id
        {
            return Ok(*value);
        }
    }
    let value = now_ms();
    let ev = RunEvent::NowRecorded {
        ts: value,
        run_id: inner.run_id.clone(),
        step_id,
        value,
    };
    inner.append(&ev)?;
    inner.publish(&ev);
    Ok(value)
}

/// Deterministic id behind [`WorkflowCtx::uuid`](crate::define::WorkflowCtx::uuid)
/// (TanStack `ctx.uuid`): a generated UUIDv4, recorded as a `UuidRecorded`
/// checkpoint. On replay the recorded id is served — stable across resumes.
pub fn exec_uuid(inner: &Arc<EngineRuntime>) -> anyhow::Result<String> {
    let k = inner.uuid_counter.fetch_add(1, Ordering::SeqCst);
    let step_id = format!("__uuid-{k}");
    for ev in inner.store.get_events(&inner.run_id)? {
        if let RunEvent::UuidRecorded { step_id: id, value, .. } = &ev
            && id == &step_id
        {
            return Ok(value.clone());
        }
    }
    let value = uuid::Uuid::new_v4().to_string();
    let ev = RunEvent::UuidRecorded {
        ts: now_ms(),
        run_id: inner.run_id.clone(),
        step_id,
        value: value.clone(),
    };
    inner.append(&ev)?;
    inner.publish(&ev);
    Ok(value)
}

/// Durable wait implemented by [`WorkflowCtx::approve`](crate::define::WorkflowCtx::approve)
/// / [`WorkflowCtx::sleep`](crate::define::WorkflowCtx::sleep) /
/// [`WorkflowCtx::wait_for_event`](crate::define::WorkflowCtx::wait_for_event).
/// `step_id` is the pause key == signal id; it must not collide with any step
/// id. `signal_name` is the channel the run parks on (`"__approval"` for
/// approvals, `"__timer"` for sleeps, a user event name for named waits).
///
/// Replay fast path: a `StepResume` already in the log resolves immediately,
/// so a crashed-paused run re-waits exactly once and never re-appends its
/// `StepPaused`. Otherwise the pause checkpoint is persisted (idempotently),
/// the run state flips to `Paused`, and the handler parks:
/// - approvals wait until an external [`signal_run`] appends `StepResume`;
/// - named waits wait until an external [`signal_event`] appends `StepResume`;
/// - sleeps also arm a `tokio` timer that auto-delivers the resume.
pub async fn exec_pause(
    inner: &Arc<EngineRuntime>,
    step_id: &str,
    signal_name: &str,
    reason: &str,
    dur: Option<Duration>,
) -> anyhow::Result<serde_json::Value> {
    if let Some(payload) = find_resume(&inner.store, &inner.run_id, step_id) {
        clear_run_wait(&inner.store, &inner.run_id);
        return Ok(payload);
    }

    let due_at = dur.map(|d| now_ms() + d.as_millis() as i64);

    let already_paused = {
        let events = inner.store.get_events(&inner.run_id)?;
        events
            .iter()
            .any(|ev| matches!(ev, RunEvent::StepPaused { step_id: id, .. } if id == step_id))
    };
    if !already_paused {
        let ev = RunEvent::StepPaused {
            ts: now_ms(),
            run_id: inner.run_id.clone(),
            step_id: step_id.to_string(),
            signal_name: signal_name.to_string(),
            due_at,
            reason: reason.to_string(),
        };
        inner.append(&ev)?;
        inner.publish(&ev);
    }
    // 投影 run.json（fresh 与崩溃后 replay 一致：都展示 Paused + 等待说明）。
    project_run_wait(
        &inner.store,
        &inner.run_id,
        if signal_name == "__approval" {
            WaitKind::Approval {
                step_id: step_id.to_string(),
                approval_id: step_id.to_string(),
                title: reason.to_string(),
                description: None,
            }
        } else {
            WaitKind::Signal {
                step_id: step_id.to_string(),
                signal_name: signal_name.to_string(),
                deadline: due_at,
            }
        },
    );

    // Sleeps: poll until `due_at` passes, then deliver the resume ourselves.
    // Approvals: poll until an external `signal_run` appends the resume.
    loop {
        if let Some(payload) = find_resume(&inner.store, &inner.run_id, step_id) {
            clear_run_wait(&inner.store, &inner.run_id);
            return Ok(payload);
        }
        if is_cancelled(&inner.store, &inner.run_id) {
            return Err(WorkflowCancelled.into());
        }
        let wait = match due_at {
            Some(due) => {
                // `i64::saturating_sub` does NOT clamp negatives to 0 — the
                // difference fits an i64 and comes back negative, which would
                // wrap to a huge u64 under `as u64`. Branch explicitly.
                let rem_ms = if due > now_ms() { (due - now_ms()) as u64 } else { 0 };
                let remaining = Duration::from_millis(rem_ms);
                if remaining.is_zero() {
                    signal_run(inner.store.as_ref(), &inner.run_id, step_id, serde_json::Value::Null)?;
                    continue;
                }
                remaining.min(RESUME_POLL_MS)
            }
            None => RESUME_POLL_MS,
        };
        tokio::time::sleep(wait).await;
    }
}

/// Last delivered signal payload for `step_id`, if any.
fn find_resume(
    store: &Arc<dyn RunStore>,
    run_id: &str,
    step_id: &str,
) -> Option<serde_json::Value> {
    let events = store.get_events(run_id).ok()?;
    events.iter().find_map(|ev| match ev {
        RunEvent::StepResume { step_id: id, payload, .. } if id == step_id => {
            Some(payload.clone().unwrap_or(serde_json::Value::Null))
        }
        _ => None,
    })
}

/// 挂起时的 run.json 投影种类。
enum WaitKind {
    Signal {
        step_id: String,
        signal_name: String,
        deadline: Option<i64>,
    },
    Approval {
        step_id: String,
        approval_id: String,
        title: String,
        description: Option<String>,
    },
}

/// 挂起时把 run.json 投影成 `Paused` + `waiting_for` / `pending_approval`
/// （对齐 TanStack `RunState`）。fresh 与崩溃后 replay 都会调用，保证
/// observer 不需要扫事件日志就知道 run 在等什么。best-effort：失败忽略，
/// 事件日志仍为准。
fn project_run_wait(store: &Arc<dyn RunStore>, run_id: &str, kind: WaitKind) {
    if let Ok(Some(mut st)) = store.get_run_state(run_id) {
        // 已终局（含被 cancel_run 置为 Aborted）——不覆盖成 Paused。
        if matches!(
            st.status,
            RunStatus::Finished | RunStatus::Errored | RunStatus::Aborted
        ) {
            return;
        }
        st.status = RunStatus::Paused;
        st.updated_at = now_ms();
        match kind {
            WaitKind::Signal { step_id, signal_name, deadline } => {
                st.waiting_for = Some(crate::store::WaitForState {
                    step_id,
                    signal_name,
                    deadline,
                });
                st.pending_approval = None;
            }
            WaitKind::Approval { step_id, approval_id, title, description } => {
                st.pending_approval = Some(crate::store::PendingApproval {
                    step_id,
                    approval_id,
                    title,
                    description,
                });
                st.waiting_for = None;
            }
        }
        let _ = store.set_run_state(run_id, &st);
    }
}

/// 恢复时清除 wait 投影并回到 `Running`。best-effort。
fn clear_run_wait(store: &Arc<dyn RunStore>, run_id: &str) {
    if let Ok(Some(mut st)) = store.get_run_state(run_id) {
        st.status = RunStatus::Running;
        st.updated_at = now_ms();
        st.waiting_for = None;
        st.pending_approval = None;
        let _ = store.set_run_state(run_id, &st);
    }
}

/// Appends a `StepResume` for a paused run — the external side of
/// [`WorkflowCtx::approve`](crate::define::WorkflowCtx::approve). Safe to call
/// any time: a live instance picks it up via polling, and a later replay
/// resolves from the log without re-waiting. The first append per
/// `(run_id, step_id)` wins; duplicates drop via the store's CAS append.
/// `payload` is what the parked `approve` call returns.
pub fn signal_run(
    store: &dyn RunStore,
    run_id: &str,
    step_id: &str,
    payload: serde_json::Value,
) -> Result<(), WorkflowError> {
    loop {
        let n = store.get_events(run_id)?.len();
        let ev = RunEvent::StepResume {
            ts: now_ms(),
            run_id: run_id.to_string(),
            step_id: step_id.to_string(),
            payload: Some(payload.clone()),
        };
        match store.append_event(run_id, n, &ev) {
            Ok(()) => return Ok(()),
            Err(StoreError::Conflict { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// Appends a `StepResume` for the run currently parked waiting for the event
/// `event_name` — the external side of
/// [`WorkflowCtx::wait_for_event`](crate::define::WorkflowCtx::wait_for_event).
/// Locates the paused step via its `signal_name` (the channel, not the pause
/// key), so a host needs only the event name. Fails with
/// [`WorkflowError::SignalLost`] when nothing is parked on that name.
pub fn signal_event(
    store: &dyn RunStore,
    run_id: &str,
    event_name: &str,
    payload: serde_json::Value,
) -> Result<(), WorkflowError> {
    let events = store.get_events(run_id)?;
    let step_id = events.iter().rev().find_map(|ev| match ev {
        RunEvent::StepPaused { step_id, signal_name, .. } if signal_name == event_name => {
            Some(step_id.clone())
        }
        _ => None,
    });
    match step_id {
        Some(step_id) => signal_run(store, run_id, &step_id, payload),
        None => Err(WorkflowError::SignalLost(format!(
            "no step in run `{run_id}` is paused waiting on event `{event_name}`"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::define::{Backoff, RetryPolicy, Workflow, WorkflowCtx};
    use crate::store::{InMemoryStore, RunState};
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

    async fn wait_until(store: &Arc<dyn RunStore>, run_id: &str, pred: impl Fn(&RunEvent) -> bool) {
        for _ in 0..2000 {
            if store.get_events(run_id).unwrap().iter().any(|e| pred(e)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition never met for run {run_id}");
    }

    fn count_events(store: &Arc<dyn RunStore>, run_id: &str, pred: impl Fn(&RunEvent) -> bool) -> usize {
        store
            .get_events(run_id)
            .unwrap()
            .iter()
            .filter(|e| pred(e))
            .count()
    }

    #[tokio::test]
    async fn approve_pauses_until_signal_resumes() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let decided = Arc::new(Mutex::new(None::<serde_json::Value>));
        let decided_in = decided.clone();
        let wf = Workflow::new("approval").handler(move |ctx: WorkflowCtx| {
            let decided = decided_in.clone();
            async move {
                ctx.step("charge", move |_sc: StepCtx| async move {
                    Ok(serde_json::json!({ "ok": true }))
                })
                .await?;
                let d = ctx.approve("release", "Approve the release?").await?;
                *decided.lock().unwrap() = Some(d.clone());
                ctx.step("ship", move |_sc: StepCtx| async move {
                    Ok(serde_json::json!({ "shipped": true }))
                })
                .await?;
                Ok(d)
            }
        });
        let store2 = store.clone();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(&wf2, store2, &RunOptions::new(serde_json::json!({})).run_id("r1"), None).await
        });
        wait_until(
            &store,
            "r1",
            |e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "release"),
        )
        .await;
        let st = store.get_run_state("r1").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Paused);
        let pa = st.pending_approval.as_ref().expect("run.json 应投影 pending_approval");
        assert_eq!(pa.step_id, "release");
        assert_eq!(pa.title, "Approve the release?");
        assert!(st.waiting_for.is_none());

        signal_run(store.as_ref(), "r1", "release", serde_json::json!({ "approved": true })).unwrap();
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "approved": true })));
        let st = store.get_run_state("r1").unwrap().unwrap();
        assert!(st.waiting_for.is_none() && st.pending_approval.is_none(), "恢复后投影应被清除");
        assert_eq!(
            decided.lock().unwrap().as_ref(),
            Some(&serde_json::json!({ "approved": true }))
        );
        assert_eq!(
            count_events(&store, "r1", |e| matches!(
                e,
                RunEvent::StepPaused { step_id, .. } if step_id == "release"
            )),
            1
        );
        assert_eq!(
            count_events(&store, "r1", |e| matches!(
                e,
                RunEvent::StepResume { step_id, .. } if step_id == "release"
            )),
            1
        );
        assert!(store.get_events("r1").unwrap().iter().any(|e| matches!(
            e,
            RunEvent::StepFinished { step_id, .. } if step_id == "ship"
        )));
    }

    #[tokio::test]
    async fn aborted_run_replays_pause_without_reappending() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let decided = Arc::new(Mutex::new(None::<serde_json::Value>));
        let decided_in = decided.clone();
        let wf = Workflow::new("approval").handler(move |ctx: WorkflowCtx| {
            let decided = decided_in.clone();
            async move {
                ctx.step("charge", move |_sc: StepCtx| async move {
                    Ok(serde_json::json!({ "ok": true }))
                })
                .await?;
                let d = ctx.approve("release", "Approve the release?").await?;
                *decided.lock().unwrap() = Some(d.clone());
                Ok(d)
            }
        });

        // Run 1: parks at the pause, then the instance "crashes" (aborted mid-wait).
        let store2 = store.clone();
        let wf2 = wf.clone();
        let t1 = tokio::spawn(async move {
            run_workflow(&wf2, store2, &RunOptions::new(serde_json::json!({})).run_id("r2"), None).await
        });
        wait_until(
            &store,
            "r2",
            |e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "release"),
        )
        .await;
        t1.abort();
        let _ = t1.await;

        // Run 2 (same run_id): succeeded checkpoint short-circuits, the pause
        // re-waits but must NOT append a second StepPaused.
        let store3 = store.clone();
        let wf3 = wf.clone();
        let t2 = tokio::spawn(async move {
            run_workflow(&wf3, store3, &RunOptions::new(serde_json::json!({})).run_id("r2"), None).await
        });
        wait_until(
            &store,
            "r2",
            |e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "release"),
        )
        .await;
        assert_eq!(
            count_events(&store, "r2", |e| matches!(
                e,
                RunEvent::StepPaused { step_id, .. } if step_id == "release"
            )),
            1,
            "replay must not re-append the pause checkpoint"
        );

        signal_run(store.as_ref(), "r2", "release", serde_json::json!({ "approved": true })).unwrap();
        let out = t2.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "approved": true })));
        assert_eq!(
            count_events(&store, "r2", |e| matches!(
                e,
                RunEvent::StepFinished { step_id, .. } if step_id == "charge"
            )),
            1,
            "replay must short-circuit the succeeded step"
        );
        assert_eq!(
            count_events(&store, "r2", |e| matches!(
                e,
                RunEvent::StepResume { step_id, .. } if step_id == "release"
            )),
            1
        );
    }

#[tokio::test]
    async fn sleep_pauses_then_auto_resumes() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("sleeper").handler(|ctx: WorkflowCtx| async move {
            ctx.sleep("cooldown", Duration::from_millis(250)).await?;
            ctx.step("after", move |_sc: StepCtx| async move {
                Ok(serde_json::json!({ "done": true }))
            })
            .await?;
            Ok(serde_json::Value::Null)
        });
        let store2 = store.clone();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(&wf2, store2, &RunOptions::new(serde_json::json!({})).run_id("s1"), None)
                .await
        });

        // 等待进入 sleep：status=Paused + waiting_for{signal_name,deadline}
        let mut paused: Option<RunState> = None;
        for _ in 0..2000 {
            if let Some(st) = store.get_run_state("s1").unwrap() {
                if st.status == RunStatus::Paused && st.waiting_for.is_some() {
                    paused = Some(st);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let paused = paused.expect("sleep 中应投影 status=Paused + waiting_for");
        let wf2_state = paused.waiting_for.as_ref().unwrap();
        assert_eq!(wf2_state.step_id, "cooldown", "sleep 的 step_id 是 pause key");
        assert_eq!(wf2_state.signal_name, "__timer", "sleep 的通道是内部 __timer");
        assert!(wf2_state.deadline.is_some(), "sleep 的 due_at 应投影为 deadline");

        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let st = store.get_run_state("s1").unwrap().unwrap();
        assert!(
            st.waiting_for.is_none() && st.pending_approval.is_none(),
            "autoresume 后投影应清除"
        );

        let evs = store.get_events("s1").unwrap();
        let resume = evs.iter().find_map(|e| match e {
            RunEvent::StepResume { step_id, payload, .. } if step_id == "cooldown" => payload.clone(),
            _ => None,
        });
        assert_eq!(resume, Some(serde_json::Value::Null));
        assert!(evs.iter().any(|e| matches!(
            e,
            RunEvent::StepFinished { step_id, .. } if step_id == "after"
        )));
    }

    #[tokio::test]
    async fn resumed_run_replays_from_log_without_rewaiting() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("approval").handler(|ctx: WorkflowCtx| async move {
            let d = ctx.approve("gate", "Approve this?").await?;
            ctx.step("consume", move |_sc: StepCtx| async move {
                Ok(serde_json::json!({ "consumed": true }))
            })
            .await?;
            Ok(d)
        });

        // Run 1: pause, signal, finish.
        let store2 = store.clone();
        let wf2 = wf.clone();
        let t = tokio::spawn(async move {
            run_workflow(&wf2, store2, &RunOptions::new(serde_json::json!({})).run_id("r3"), None).await
        });
        wait_until(
            &store,
            "r3",
            |e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "gate"),
        )
        .await;
        signal_run(store.as_ref(), "r3", "gate", serde_json::json!({ "yes": 1 })).unwrap();
        let out = t.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "yes": 1 })));

        // Run 2, same run_id: the StepResume is already in the log, so
        // approve resolves from the log — no new checkpoints appended.
        let store3 = store.clone();
        let wf3 = wf.clone();
        let out2 = run_workflow(&wf3, store3, &RunOptions::new(serde_json::json!({})).run_id("r3"), None)
            .await
            .unwrap();
        assert_eq!(out2.status, RunStatus::Finished);
        assert_eq!(out2.output, Some(serde_json::json!({ "yes": 1 })));
        assert_eq!(
            count_events(&store, "r3", |e| matches!(
                e,
                RunEvent::StepPaused { step_id, .. } if step_id == "gate"
            )),
            1
        );
        assert_eq!(
            count_events(&store, "r3", |e| matches!(
                e,
                RunEvent::StepResume { step_id, .. } if step_id == "gate"
            )),
            1
        );
    }

    #[tokio::test]
    async fn wait_for_event_fulfilled_by_signal_event() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let received = Arc::new(Mutex::new(None::<serde_json::Value>));
        let rx = received.clone();
        let wf = Workflow::new("named-event").handler(move |ctx: WorkflowCtx| {
            let rx = rx.clone();
            async move {
                let v = ctx.wait_for_event("review", "review-approved").await?;
                *rx.lock().unwrap() = Some(v.clone());
                Ok(v)
            }
        });
        let store2 = store.clone();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(&wf2, store2, &RunOptions::new(serde_json::json!({})).run_id("ne1"), None).await
        });
        wait_until(
            &store,
            "ne1",
            |e| matches!(e, RunEvent::StepPaused { signal_name, .. } if signal_name == "review-approved"),
        )
        .await;
        let st = store.get_run_state("ne1").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Paused);
        let w = st.waiting_for.as_ref().expect("named wait 投影 waiting_for");
        assert_eq!(w.signal_name, "review-approved");
        assert_eq!(w.step_id, "review");
        assert!(st.pending_approval.is_none(), "named event 不是 approval");

        signal_event(store.as_ref(), "ne1", "review-approved", serde_json::json!({ "ok": true })).unwrap();
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "ok": true })));
        assert_eq!(received.lock().unwrap().as_ref(), Some(&serde_json::json!({ "ok": true })));
    }

    #[tokio::test]
    async fn wait_for_event_resolves_from_log_on_replay() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("named-event").handler(|ctx: WorkflowCtx| async move {
            let v = ctx.wait_for_event("gate", "go").await?;
            let out = v.clone();
            ctx.step("consume", move |_sc| async move { Ok(v.clone()) }).await?;
            Ok(out)
        });
        let store2 = store.clone();
        let wf2 = wf.clone();
        let t = tokio::spawn(async move {
            run_workflow(&wf2, store2, &RunOptions::new(serde_json::json!({})).run_id("ne2"), None).await
        });
        wait_until(
            &store,
            "ne2",
            |e| matches!(e, RunEvent::StepPaused { signal_name, .. } if signal_name == "go"),
        )
        .await;
        signal_event(store.as_ref(), "ne2", "go", serde_json::json!(42)).unwrap();
        let out = t.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!(42)));

        // 同名 run 重跑：StepResume 已在日志，replay 从日志取，不重新等待或加 checkpoint
        let store3 = store.clone();
        let wf3 = wf.clone();
        let out2 = run_workflow(
            &wf3,
            store3,
            &RunOptions::new(serde_json::json!({})).run_id("ne2"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out2.status, RunStatus::Finished);
        assert_eq!(out2.output, Some(serde_json::json!(42)));
        assert_eq!(
            count_events(&store, "ne2", |e| matches!(
                e,
                RunEvent::StepPaused { signal_name, .. } if signal_name == "go"
            )),
            1
        );
    }

    #[tokio::test]
    async fn sleep_until_past_resolves_immediately() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("sleeper").handler(|ctx: WorkflowCtx| async move {
            let ts = crate::engine::now_ms() - 5000;
            ctx.sleep_until("cooldown", ts).await?;
            ctx.step("after", move |_sc: StepCtx| async move {
                Ok(serde_json::json!({ "done": true }))
            })
            .await?;
            Ok(serde_json::Value::Null)
        });
        let out = run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let evs = store.get_events(&out.run_id).unwrap();
        assert!(evs.iter().any(|e| matches!(
            e,
            RunEvent::StepFinished { step_id, .. } if step_id == "after"
        )));
        // 过去时间点 → 引擎自我投递 "__timer" resume，立即放行
        assert!(evs.iter().any(|e| matches!(
            e,
            RunEvent::StepResume { step_id, .. } if step_id == "cooldown"
        )));
    }

    #[tokio::test]
    async fn sleep_until_schedules_timer() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("sleeper").handler(|ctx: WorkflowCtx| async move {
            let ts = crate::engine::now_ms() + 300;
            ctx.sleep_until("cooldown", ts).await?;
            Ok(serde_json::Value::Null)
        });
        let store2 = store.clone();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(&wf2, store2, &RunOptions::new(serde_json::json!({})).run_id("stu1"), None).await
        });
        let mut saw_deadline = false;
        for _ in 0..2000 {
            if let Some(st) = store.get_run_state("stu1").unwrap()
                && let Some(w) = &st.waiting_for
                && w.step_id == "cooldown"
                && w.signal_name == "__timer"
                && w.deadline.is_some()
            {
                saw_deadline = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(saw_deadline, "sleep_until 应投影 waiting_for + deadline");
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
    }

    #[tokio::test]
    async fn emit_reaches_publisher_but_not_log() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let wf = Workflow::new("emitter").handler(|ctx: WorkflowCtx| async move {
            ctx.emit("ping", serde_json::json!({ "x": 1 }));
            ctx.step("a", move |_sc: StepCtx| async move {
                Ok(serde_json::Value::Null)
            })
            .await?;
            Ok(serde_json::Value::Null)
        });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({})).run_id("em1"),
            Some(Arc::new(move |e: &RunEvent| sink.lock().unwrap().push(e.clone()))),
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert!(seen
            .lock()
            .unwrap()
            .iter()
            .any(|e| matches!(e, RunEvent::Custom { name, .. } if name == "ping")));
        assert!(store
            .get_events("em1")
            .unwrap()
            .iter()
            .all(|e| !matches!(e, RunEvent::Custom { .. })));
    }

    #[tokio::test]
    async fn signal_event_without_awaiter_errors() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let err =
            signal_event(store.as_ref(), "nowhere", "ghost", serde_json::Value::Null).unwrap_err();
        assert!(matches!(err, crate::error::WorkflowError::SignalLost(_)));
        assert!(
            store.get_events("nowhere").unwrap().is_empty(),
            "失败时不 append StepResume"
        );
    }

    #[tokio::test]
    async fn now_and_uuid_deterministic_across_resume() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("det").handler(|ctx: WorkflowCtx| async move {
            // 两个调用位点各自拿到稳定值；approve 强制产生一次 replay 边界。
            let t1 = ctx.now()?;
            let u1 = ctx.uuid()?;
            ctx.approve("gate", "go").await?;
            let t2 = ctx.now()?;
            let u2 = ctx.uuid()?;
            Ok(serde_json::json!({ "now": t1, "uuid": u1, "now2": t2, "uuid2": u2 }))
        });
        let store2 = store.clone();
        let wf2 = wf.clone();
        let t = tokio::spawn(async move {
            run_workflow(&wf2, store2, &RunOptions::new(serde_json::json!({})).run_id("det1"), None).await
        });
        wait_until(
            &store,
            "det1",
            |e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "gate"),
        )
        .await;
        signal_run(store.as_ref(), "det1", "gate", serde_json::json!(true)).unwrap();
        let out = t.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let o1 = out.output.clone().unwrap();

        // 重跑（崩溃恢复模拟）：now/uuid 全部从日志缓存值取，输出逐字节一致。
        let store3 = store.clone();
        let wf3 = wf.clone();
        let out2 = run_workflow(&wf3, store3, &RunOptions::new(serde_json::json!({})).run_id("det1"), None)
            .await
            .unwrap();
        assert_eq!(out2.status, RunStatus::Finished);
        assert_eq!(out2.output.clone().unwrap(), o1, "replay 后 now/uuid 与首跑一致");
        assert_eq!(
            count_events(&store, "det1", |e| matches!(e, RunEvent::NowRecorded { .. })),
            2,
            "每个调用位点只记录一次，replay 不重复 append"
        );
        assert_eq!(
            count_events(&store, "det1", |e| matches!(e, RunEvent::UuidRecorded { .. })),
            2
        );
        // 两对 now/uuid 都用独立的确定性 id（__now-0/__now-1、__uuid-0/__uuid-1）。
        let ids: Vec<String> = store
            .get_events("det1")
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                RunEvent::NowRecorded { step_id, .. } => Some(format!("now:{step_id}")),
                RunEvent::UuidRecorded { step_id, .. } => Some(format!("uuid:{step_id}")),
                _ => None,
            })
            .collect();
        assert!(!ids.iter().any(|a| ids.iter().filter(|b| b == &a).count() > 1), "ids 唯一: {ids:?}");
    }

    #[tokio::test]
    async fn concurrent_now_calls_stay_unique() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("conc").handler(|ctx: WorkflowCtx| {
            async move {
                let (a, b) = {
                    let (a, b) = (ctx.clone(), ctx.clone());
                    tokio::join!(
                        async move { a.now() },
                        async move { b.now() },
                    )
                };
                Ok(serde_json::json!({ "a": a?, "b": b? }))
            }
        });
        let out = run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let v = out.output.unwrap();
        assert!(v["a"].as_i64().is_some() && v["b"].as_i64().is_some(), "两个 now 都应成功");
        assert_eq!(
            count_events(&store, &out.run_id, |e| matches!(e, RunEvent::NowRecorded { .. })),
            2,
            "并发调用各自落一个 NowRecorded，无重复 id"
        );

        // 重跑：并发 again，replay 命中 __now-0/__now-1 缓存，事件数不变。
        let store2 = store.clone();
        let wf2 = wf.clone();
        let out2 = run_workflow(&wf2, store2, &RunOptions::new(serde_json::json!({})).run_id(&out.run_id), None)
            .await
            .unwrap();
        assert_eq!(out2.status, RunStatus::Finished);
        assert_eq!(out2.output.clone().unwrap(), v, "并发 replay 也一致");
        assert_eq!(
            count_events(&store, &out.run_id, |e| matches!(e, RunEvent::NowRecorded { .. })),
            2
        );
    }

    #[tokio::test]
    async fn cancel_parked_approval_aborts() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("cancel").handler(|ctx: WorkflowCtx| async move {
            let _ = ctx.approve("release", "Approve?").await?;
            ctx.step("ship", move |_sc: StepCtx| async move {
                Ok(serde_json::Value::Null)
            })
            .await?;
            Ok(serde_json::Value::Null)
        });
        let store2 = store.clone();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(&wf2, store2, &RunOptions::new(serde_json::json!({})).run_id("ca1"), None).await
        });
        wait_until(
            &store,
            "ca1",
            |e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "release"),
        )
        .await;
        let st = store.get_run_state("ca1").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Paused);

        cancel_run(store.as_ref(), "ca1").unwrap();
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Aborted, "cancel 后 run 以 Aborted 终局");
        assert!(out.error.as_deref().unwrap().contains("aborted"));
        assert!(out.output.is_none());

        let st = store.get_run_state("ca1").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Aborted, "run.json 终态应为 Aborted");
        assert!(st.waiting_for.is_none() && st.pending_approval.is_none());

        // 日志没有 Finished 终态，也不应落在 running/paused（cancel 后不再驱动）
        let evs = store.get_events("ca1").unwrap();
        let terminal_finished = evs
            .iter()
            .any(|e| matches!(e, RunEvent::RunFinished { .. }));
        assert!(!terminal_finished);
        assert!(!evs.iter().any(|e| matches!(
            e,
            RunEvent::StepFinished { step_id, .. } if step_id == "ship"
        )));
    }

    #[tokio::test]
    async fn cancel_sleeping_run_aborts_and_is_recoverable() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("cancel-sleep").handler(|ctx: WorkflowCtx| async move {
            ctx.sleep("hold", Duration::from_secs(1)).await?;
            Ok(serde_json::json!({ "done": true }))
        });
        let store2 = store.clone();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(&wf2, store2, &RunOptions::new(serde_json::json!({})).run_id("ca2"), None).await
        });
        wait_until(
            &store,
            "ca2",
            |e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "hold"),
        )
        .await;
        cancel_run(store.as_ref(), "ca2").unwrap();
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Aborted);

        // 重复 cancel：已终局 → no-op Ok
        cancel_run(store.as_ref(), "ca2").unwrap();
        // 未知 run → RunNotFound
        let err = cancel_run(store.as_ref(), "missing").unwrap_err();
        assert!(matches!(err, crate::error::WorkflowError::RunNotFound(_)));

        // 取消的 run 可再次驱动：sleep 重新计时（1s）后自动放行 → Finished。
        let store3 = store.clone();
        let wf3 = wf.clone();
        let out2 = run_workflow(
            &wf3,
            store3,
            &RunOptions::new(serde_json::json!({})).run_id("ca2"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out2.status, RunStatus::Finished, "cancel 后重跑可恢复");
        assert_eq!(out2.output, Some(serde_json::json!({ "done": true })));
    }

    #[tokio::test]
    async fn ctx_reports_not_cancelled_in_normal_run() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("not-cancelled").handler(|ctx: WorkflowCtx| async move {
            assert!(!ctx.is_cancelled());
            Ok(serde_json::json!({ "ok": true }))
        });
        let out = run_workflow(&wf, store.clone(), &RunOptions::new(serde_json::json!({})), None)
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
    }

    #[tokio::test]
    async fn deadline_exhausted_flags_should_yield() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let seen = Arc::new(Mutex::new((None::<Option<i64>>, false, false)));
        let sink = seen.clone();
        let wf = Workflow::new("budget").handler(move |ctx: WorkflowCtx| {
            let sink = sink.clone();
            async move {
                let (d, tr, sy) = (ctx.deadline(), ctx.time_remaining(), ctx.should_yield());
                *sink.lock().unwrap() = (Some(d), tr < 1000, sy);
                ctx.step("a", move |sc: StepCtx| {
                    let sink = sink.clone();
                    async move {
                        *sink.lock().unwrap() = (Some(sc.deadline()), sc.time_remaining() < 1000, sc.should_yield());
                        Ok(serde_json::json!({ "d": sc.deadline(), "tr": sc.time_remaining() }))
                    }
                })
                .await?;
                Ok(serde_json::Value::Null)
            }
        });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({}))
                .deadline(crate::engine::now_ms() + 300), // 500ms 内到期 → headroom < 1000
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        {
            let g = seen.lock().unwrap();
            let (d, _, _) = *g;
            assert!(d.is_some(), "deadline 传入");
        }
        // 至少一次观测到 budget 接近耗尽 → should_yield 在 ctx 或 step 侧为 true
        let (_, tr_in_step, _) = *seen.lock().unwrap();
        assert!(tr_in_step, "should_yield 应因 deadline 逼近而为 true");
    }

    #[tokio::test]
    async fn yield_parks_and_replays() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("yielder").handler(|ctx: WorkflowCtx| async move {
            ctx.yield_().await?;
            if ctx.should_yield() {
                ctx.yield_().await?;
            }
            ctx.step("after", move |_sc: StepCtx| async move {
                Ok(serde_json::json!({ "done": true }))
            })
            .await?;
            Ok(serde_json::json!({ "ok": true }))
        });
        let store2 = store.clone();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(&wf2, store2, &RunOptions::new(serde_json::json!({})).run_id("y1"), None).await
        });
        // 第一个 yield 会 park（default 到期 ≈ now+1ms，非常快）；等待其 StepPaused。
        wait_until(
            &store,
            "y1",
            |e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id.starts_with("__yield-")),
        )
        .await;
        // 到期自动 resume（timer 自动投递）→ 继续跑完
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert!(store
            .get_events("y1")
            .unwrap()
            .iter()
            .any(|e| matches!(e, RunEvent::StepFinished { step_id, .. } if step_id == "after")));

        // replay：yield 的 StepResume 已在日志 → 短路径立即放行，不新增 checkpoint
        let store3 = store.clone();
        let wf3 = wf.clone();
        let out2 = run_workflow(&wf3, store3, &RunOptions::new(serde_json::json!({})).run_id("y1"), None)
            .await
            .unwrap();
        assert_eq!(out2.status, RunStatus::Finished);
        assert_eq!(
            count_events(&store, "y1", |e| matches!(e, RunEvent::StepPaused { .. })),
            1,
            "replay 不重复 yield 的 pause"
        );
    }

    #[tokio::test]
    async fn yield_waits_until_yield_resume_at() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("yielder").handler(|ctx: WorkflowCtx| async move {
            ctx.yield_().await?;
            Ok(serde_json::json!({ "ok": true }))
        });
        let resume_at = crate::engine::now_ms() + 250;
        let store2 = store.clone();
        let wf2 = wf.clone();
        let start = crate::engine::now_ms();
        let task = tokio::spawn(async move {
            run_workflow(
                &wf2,
                store2,
                &RunOptions::new(serde_json::json!({})).run_id("y2").yield_resume_at(resume_at),
                None,
            )
            .await
        });
        wait_until(
            &store,
            "y2",
            |e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id.starts_with("__yield-")),
        )
        .await;
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert!(
            crate::engine::now_ms() - start >= 200,
            "yield 应睡到 yield_resume_at 才放行"
        );
    }
}