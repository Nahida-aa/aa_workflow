//! 引擎驱动：代码即 DAG 的执行器。`EngineRuntime` 是共享驱动态（step 重放 /
//! 结果缓存 / CAS 追加），`exec_step` 跑单个 durable step，
//! `exec_pause` + `signal_run` 实现 durable 等待（approval / sleep）。
//! 单次 invocation 的顶层编排（`run_workflow` / `run_workflow_sync` /
//! `RunWorkflowOptions` / `RunOutcome`）在 `run_workflow` 子模块。

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::define::{AnyWorkflowDefinition, StepCtx, StepOptions};
use crate::error::{RunError, StoreError, WorkflowError};
use crate::event::{RunStatus, StepAttempt, StepState, StepStatus, WorkflowEvent};
use crate::resource::Gate;
use crate::run_store::RunStore;

mod run_workflow;
pub mod state_diff;
pub use run_workflow::{
    PublisherFn, RunId, RunOutcome, RunWorkflowOptions, run_workflow, run_workflow_sync,
    select_workflow_version,
};
pub use state_diff::{Operation, diff_state, snapshot_state};

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

/// Internal sentinel: the engine parked the run at a durable wait point
/// ([`exec_pause`]). The pause checkpoint and the `Paused` projection were
/// already written; this only unwinds the handler so the drive can end
/// `Paused` without appending a terminal event. Never `Errored`.
///
/// Mirrors TanStack's `engine.paused` flag + `throw new WorkflowPaused()`
/// (`run-workflow.ts:970`), whose catch does `if (engine.paused) return`.
#[derive(Debug)]
pub struct WorkflowParked;

impl std::fmt::Display for WorkflowParked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "workflow parked at a durable wait point")
    }
}

impl std::error::Error for WorkflowParked {}

/// Internal sentinel: [`cancel_run`] set the run to `Aborted` while the
/// handler was driving. Step closures are not interruptible mid-`await`,
/// so the error surfaces at the next engine boundary (step entry / attempt
/// boundary). Becomes an `Aborted` run, never `Errored`.
#[derive(Debug)]
pub struct WorkflowCancelled;

impl std::fmt::Display for WorkflowCancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "workflow aborted")
    }
}

impl std::error::Error for WorkflowCancelled {}

/// True when `run_id`'s run state was set to `Aborted` by [`cancel_run`].
/// Checked at step entry and at attempt boundaries; store-agnostic (works for
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
/// engine spots it on its next step/attempt boundary and returns
/// [`WorkflowCancelled`], so the drive ends `Aborted` rather than `Errored`.
/// A **parked** run is not running at all, so nothing is watching: the flag is
/// only observed when a driver re-invokes `run_workflow`, which then ends the
/// run `Aborted` at its first boundary. A second cancel on an already-terminal
/// run is a no-op `Ok(())`.
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
    st.error = Some(RunError::cancelled());
    st.waiting_for = None;
    st.pending_approval = None;
    st.updated_at = now_ms();
    store.set_run_state(run_id, &st)?;
    Ok(())
}

/// Shared driver state handed to every step (and to the `<WorkflowCtx>`).
/// This is the code-as-DAG substrate: the "graph" is just this state plus the
/// handler's control flow, discovered as the handler runs.
///
/// 事件扇出通道直接传 `WorkflowEvent`：收尾**靠关通道**（`run_workflow` 返回的
/// [`RunEventStream`](crate::RunEventStream) 靠 `poll_recv` 见到 `None` 结束），
/// 不需要显式的 shutdown 哨兵——`EngineRuntime` 随引擎 future 一起 drop，
/// 它是最后一个 sender。
pub struct EngineRuntime {
    pub run_id: String,
    pub input: serde_json::Value,
    /// Per-invocation state, rebuilt from `initialize(input)` on every start
    /// and resume (see `define::WorkflowDefinition::initialize`). Guarded by a
    /// `std::sync::RwLock`; it is the live image that `BaseCtx::state` (the
    /// handler's field working copy) snapshots at drive start and flushes back
    /// to before every durable primitive. Never persisted.
    /// Engine-side serialized view of the workflow state, **shared with the
    /// handler's `StateHandle`**（handle 在变更语句与 drop 时同步进这里）。
    /// `STATE_DELTA` 的 diff 读它；不含 `prev_state_snapshot` 的历史。
    pub state_mirror: Arc<Mutex<serde_json::Value>>,
    /// State as of the last flush — the "before" side of the
    /// [`WorkflowEvent::StateDelta`] diff. Reset at drive start (upstream
    /// `prevStateSnapshot: snapshotState(state)`).
    pub prev_state_snapshot: Mutex<serde_json::Value>,
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
    /// 事件扇出通道。**引擎侧只做一次 `send`**（非阻塞），await publisher 的
    /// 动作在**消费侧**——对齐上游 `runWorkflow` 的 async generator：
    /// `queue.shift()` 后 `await publish`，而执行在另一个 task 里继续往 queue 推
    /// （`run-workflow.ts:85-134`）。所以慢 publisher 拖慢消费端，不拖慢引擎。
    pub(crate) publish_tx: Option<tokio::sync::mpsc::UnboundedSender<WorkflowEvent>>,
    /// WorkflowDefinition-level fallback retry (TanStack `defaultStepRetry`); steps that
    /// declare their own [`StepOptions::retry`](crate::define::StepOptions::retry)
    /// win.
    pub(crate) default_step_retry: Option<crate::define::RetryPolicy>,
}

impl EngineRuntime {
    /// Appends a checkpoint with optimistic CAS; on a lost race re-bases the
    /// cursor and retries (the other writer won, its event precedes ours).
    fn append(&self, ev: &WorkflowEvent) -> Result<(), WorkflowError> {
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

    /// Best-effort fan-out to the host's publisher. **Never propagates**, and
    /// **never blocks the engine** — this only hands the event to the drain task
    /// (see [`Self::publish_tx`]); awaiting the publisher happens there.
    ///
    /// Upstream's shape is an `async function*` whose loop does
    /// `queue.shift()` → `await publish` → `yield`, while execution runs in a
    /// *separate* task pushing into that queue (`run-workflow.ts:85-134`). So a
    /// slow publisher costs upstream throughput, not progress — we match that
    /// by not awaiting on the engine's path.
    ///
    /// The queue is **unbounded**, like upstream's plain array: a slow
    /// publisher grows memory rather than stalling the run. Hosts that need
    /// backpressure should apply it inside their own publisher.
    ///
    /// Panics are swallowed so host code cannot destroy a durable run
    /// (upstream: *"A misbehaving publisher must not break the run — swallow
    /// and continue."*). Swallowed **silently** — this crate has no logging
    /// dependency; hosts that want diagnostics catch inside their own publisher.
    ///
    /// `ev.clone()` 是**必需**的（调用点只有 `&self`，事件要活过本次调用）；
    /// 但它也是**唯一**一次克隆——drain task 拿到所有权后按值交给 publisher，
    /// 引擎侧不再有任何额外拷贝。
    pub(crate) fn publish(&self, ev: &WorkflowEvent) {
        if let Some(tx) = &self.publish_tx {
            let _ = tx.send(ev.clone());
        }
    }

    /// Diff [`prev_state_snapshot`](Self::prev_state_snapshot) vs
    /// [`state_mirror`](Self::state_mirror)，非空则 **emit-only** 发一条
    /// [`WorkflowEvent::StateDelta`] 并推进快照。
    ///
    /// 调用点：耐久边界（`flush_state`）与 drive 收尾（尾段 delta，对齐上游
    /// handler 返回 / catch 处的 `flushStateDelta`）。
    pub(crate) fn emit_state_delta(&self) {
        eprintln!("[trace] emit_state_delta enter");
        let mirror = self
            .state_mirror
            .lock()
            .expect("state mirror lock poisoned")
            .clone();
        let delta = {
            let mut prev = self
                .prev_state_snapshot
                .lock()
                .expect("state snapshot lock poisoned");
            let delta = state_diff::diff_state(&prev, &mirror);
            if !delta.is_empty() {
                *prev = mirror;
            }
            delta
        };
        eprintln!("[trace] emit_state_delta delta_len={}", delta.len());
        if !delta.is_empty() {
            self.publish(&WorkflowEvent::StateDelta {
                ts: now_ms(),
                run_id: self.run_id.clone(),
                delta,
            });
        }
    }

    pub fn publish_progress(&self, step_id: &str, value: f64) {
        self.publish(&WorkflowEvent::StepProgress {
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
/// Runs one durable step and resolves to the closure's own `T`.
///
/// Two paths, deliberately asymmetric:
///
/// - **Fresh** — runs the closure and hands back its `T` *untouched*. A `Value`
///   is derived from it only to write `STEP_FINISHED`. So the first drive returns
///   exactly what the closure produced; a lossy JSON round-trip can't corrupt it.
/// - **Replayed** — the log is the only source of truth, so the recorded
///   `Value` is deserialized back into `T`. This is the one place a mismatch
///   between the recorded shape and `T` can surface as an error, and it does so
///   loudly rather than handing back a silent `null`.
///
/// The log stays `serde_json::Value` regardless of `T` — durability is defined
/// by the event schema, not by the handler's types.
pub(crate) async fn exec_step<T, F, Fut>(
    inner: &Arc<EngineRuntime>,
    step_id: &str,
    opts: &StepOptions,
    run: F,
) -> anyhow::Result<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
    F: FnOnce(StepCtx) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = anyhow::Result<T>> + Send + 'static,
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
                            id: step_id.to_string(),
                            attempt: 0,
                        };
                        utd(&probe, &st)
                    }
                    None => true,
                };
                if fresh {
                    let raw = st.result.clone().unwrap_or(serde_json::Value::Null);
                    return serde_json::from_value(raw).map_err(|e| {
                        anyhow::anyhow!(
                            "step `{step_id}`: recorded result does not deserialize back into \
                             `{}` (the log says {}): {e}",
                            std::any::type_name::<T>(),
                            st.result
                                .as_ref()
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "null".into()),
                        )
                    });
                }
                // stale → fall through and re-execute below.
            }
            StepStatus::Failed => {
                let err = st
                    .error
                    .clone()
                    .unwrap_or_else(|| RunError::from_anyhow(&anyhow::anyhow!("failed")));
                return Err(anyhow::anyhow!("step `{step_id}` previously failed: {err}"));
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

    inner.publish(&WorkflowEvent::StepStarted {
        ts: now_ms(),
        run_id: inner.run_id.clone(),
        step_id: step_id.to_string(),
    });

    // Resource gate: same key ⇒ serialized (capacity-1 async semaphore).
    let _gate = match &opts.resource {
        Some(key) => Some(inner.gate.acquire(key).await),
        None => None,
    };

    // Effective retry: per-step policy wins, else the workflow's
    // `default_step_retry` (TanStack `defaultStepRetry`), else none.
    let retry = opts
        .retry
        .clone()
        .or_else(|| inner.default_step_retry.clone());

    let max_attempts = retry.as_ref().map(|p| p.max_attempts).unwrap_or(1);
    let mut attempts: Vec<StepAttempt> = Vec::with_capacity(max_attempts);

    for attempt in 1..=max_attempts {
        let step_ctx = StepCtx {
            inner: inner.clone(),
            id: step_id.to_string(),
            attempt,
        };
        let started_at = now_ms();
        // 保留 `anyhow::Error` 而非提前字符串化，这样 attempt 能记下结构化的
        // `RunError`（attempts 会落进 STEP_FAILED，供重试与审计回看）。
        let run_fut = run.clone()(step_ctx);
        let run_outcome: anyhow::Result<T> = match opts.timeout {
            Some(t) => match tokio::time::timeout(t, run_fut).await {
                Ok(res) => res,
                Err(_) => Err(anyhow::anyhow!(
                    "step \"{step_id}\" timed out after {t:?}; underlying future continues on the runtime"
                )),
            },
            None => run_fut.await,
        };
        let finished_at = now_ms();

        // 记录进日志需要 `Value`，闭包给的是 `T`。转换失败**按 step 失败处理**，
        // 复用既有的 STEP_FAILED + 重试路径——不能 `?` 提前返回，那会让 step
        // 永远停在 Running 且不追加任何事件。原始 `T` 一路带到 return，
        // 不做往返，避免首次 drive 就与闭包返回值不一致。
        let outcome: anyhow::Result<(T, serde_json::Value)> = match run_outcome {
            Ok(t) => serde_json::to_value(&t)
                .map(|recorded| (t, recorded))
                .map_err(|e| {
                    anyhow::anyhow!(
                        "step \"{step_id}\": result of type `{}` does not serialize: {e}",
                        std::any::type_name::<T>(),
                    )
                }),
            Err(e) => Err(e),
        };

        match outcome {
            Ok((result, recorded)) => {
                attempts.push(StepAttempt {
                    attempt,
                    started_at,
                    finished_at,
                    result: Some(recorded.clone()),
                    error: None,
                });
                let ev = WorkflowEvent::StepFinished {
                    ts: now_ms(),
                    run_id: inner.run_id.clone(),
                    step_id: step_id.to_string(),
                    result: Some(recorded.clone()),
                    attempts,
                };
                inner.append(&ev)?;
                inner.set_live(
                    step_id,
                    StepState {
                        status: StepStatus::Success,
                        result: Some(recorded),
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
                let run_err = RunError::from_anyhow(&err);
                attempts.push(StepAttempt {
                    attempt,
                    started_at,
                    finished_at,
                    result: None,
                    error: Some(run_err.clone()),
                });
                let retrying = retry.is_some() && attempt < max_attempts;
                if retrying {
                    if is_cancelled(&inner.store, &inner.run_id) {
                        return Err(WorkflowCancelled.into());
                    }
                    let delay = retry
                        .as_ref()
                        .map(|p| p.backoff.delay_ms(attempt))
                        .unwrap_or(0);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    continue;
                }
                let ev = WorkflowEvent::StepFailed {
                    ts: now_ms(),
                    run_id: inner.run_id.clone(),
                    step_id: step_id.to_string(),
                    error: run_err.clone(),
                    attempts,
                };
                inner.append(&ev)?;
                inner.set_live(
                    step_id,
                    StepState {
                        status: StepStatus::Failed,
                        result: None,
                        error: Some(run_err.clone()),
                        started_at: Some(started_at),
                        finished_at: Some(finished_at),
                    },
                );
                inner.publish(&ev);
                // 原样上抛，不加 `step `x` failed:` 前缀：step 身份已经在
                // `step_id` 字段里（结构化），再包一层会让同一个错误的 message
                // 在 step 层与 run 层变成两个值。TanStack 也是直接 rethrow
                // （run-workflow.ts:822）。
                return Err(err);
            }
        }
    }
    unreachable!("retry loop always returns")
}

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
        if let WorkflowEvent::NowRecorded {
            step_id: id, value, ..
        } = &ev
            && id == &step_id
        {
            return Ok(*value);
        }
    }
    let value = now_ms();
    let ev = WorkflowEvent::NowRecorded {
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
        if let WorkflowEvent::UuidRecorded {
            step_id: id, value, ..
        } = &ev
            && id == &step_id
        {
            return Ok(value.clone());
        }
    }
    let value = uuid::Uuid::new_v4().to_string();
    let ev = WorkflowEvent::UuidRecorded {
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
/// `deadline` is an **absolute** UTC ms timestamp, stored verbatim (it is what
/// a timer sweep indexes on) — `None` for waits with no time bound.
///
/// Replay fast path: a `StepResume` already in the log resolves immediately,
/// so a crashed-paused run re-waits exactly once and never re-appends its
/// `StepPaused`. Otherwise the pause checkpoint is persisted (idempotently)
/// and the run state flips to `Paused` + `waiting_for` / `pending_approval`.
///
/// **Then it parks**: the function returns [`WorkflowParked`] immediately — it
/// does NOT block. The drive ends `Paused`, the process is free to exit, and an
/// external driver is what delivers the wake-up (a `StepResume` via
/// [`signal_run`] / [`signal_event`], or a timer sweep noticing `deadline`).
/// Mirrors TanStack, where the awaited primitive throws `WorkflowPaused` and
/// the drive returns (`run-workflow.ts:970` / `:529`).
pub async fn exec_pause(
    inner: &Arc<EngineRuntime>,
    step_id: &str,
    signal_name: &str,
    reason: &str,
    deadline: Option<i64>,
) -> anyhow::Result<serde_json::Value> {
    exec_pause_with(
        inner,
        step_id,
        signal_name,
        reason,
        deadline,
        None,
        None,
    )
    .await
}

/// [`exec_pause`] 的完整形态：额外带 `meta`（落进 `StepPaused` checkpoint 与
/// `RunState` 的三处投影）和 `validator`（恢复时校验 payload 形状）。
///
/// `meta` 落进 checkpoint 是有意的：观察者只读 `RunState` 就能渲染，不必回放
/// 日志（对齐 `DurableOperationOptions.meta`「copied into the operation's log
/// event」，`types.ts:29-31`）。
///
/// `validator` **不落盘** —— 它是代码而非数据，replay 时同一个
/// `wait_for_event_with(..)` 调用点会重新装上同一个校验器。
pub async fn exec_pause_with(
    inner: &Arc<EngineRuntime>,
    step_id: &str,
    signal_name: &str,
    reason: &str,
    deadline: Option<i64>,
    meta: Option<serde_json::Value>,
    validator: Option<crate::define::PayloadValidator>,
) -> anyhow::Result<serde_json::Value> {
    if let Some(payload) = find_resume(&inner.store, &inner.run_id, step_id) {
        // 恢复路径：投递进来的 payload 先过声明的 schema 再交给 workflow。
        // 校验失败 = run 报错（不是 park），因为数据已经在这了、run 不会
        // 重新等待。
        if let Some(v) = &validator {
            v(&payload).map_err(|e| {
                crate::error::WorkflowError::Validation(format!(
                    "payload for `{signal_name}` (step `{step_id}`) failed its declared schema: {e}"
                ))
            })?;
        }
        return Ok(payload);
    }
    // 被取消的 run 不该再挂起——直接以 Aborted 展开（原来由轮询循环里的检查
    // 负责，轮询删掉后移到这里：挂起是最后一个还能观察 flag 的边界）。
    if is_cancelled(&inner.store, &inner.run_id) {
        return Err(WorkflowCancelled.into());
    }

    let already_paused = {
        let events = inner.store.get_events(&inner.run_id)?;
        events
            .iter()
            .any(|ev| matches!(ev, WorkflowEvent::StepPaused { step_id: id, .. } if id == step_id))
    };
    if !already_paused {
        let ev = WorkflowEvent::StepPaused {
            ts: now_ms(),
            run_id: inner.run_id.clone(),
            step_id: step_id.to_string(),
            signal_name: signal_name.to_string(),
            due_at: deadline,
            reason: reason.to_string(),
            meta: meta.clone(),
        };
        inner.append(&ev)?;
        inner.publish(&ev);
    }
    // 投影 RunState 信封（fresh 与崩溃后 replay 一致：都展示 Paused + 等待说明）。
    project_run_wait(
        &inner.store,
        &inner.run_id,
        if signal_name == "__approval" {
            WaitKind::Approval {
                step_id: step_id.to_string(),
                approval_id: step_id.to_string(),
                title: reason.to_string(),
                description: None,
                meta,
            }
        } else {
            WaitKind::Signal {
                step_id: step_id.to_string(),
                signal_name: signal_name.to_string(),
                deadline,
                meta,
            }
        },
    );

    // Park: hand control back to the drive. Everything the outside world needs
    // (checkpoint, `deadline`, RunState projection) is already durable.
    Err(WorkflowParked.into())
}

/// Last delivered signal payload for `step_id`, if any.
fn find_resume(
    store: &Arc<dyn RunStore>,
    run_id: &str,
    step_id: &str,
) -> Option<serde_json::Value> {
    let events = store.get_events(run_id).ok()?;
    events.iter().find_map(|ev| match ev {
        WorkflowEvent::StepResume {
            step_id: id,
            payload,
            ..
        } if id == step_id => Some(payload.clone().unwrap_or(serde_json::Value::Null)),
        _ => None,
    })
}

/// 挂起时的 RunState 投影种类。
#[derive(Debug, Clone)]
enum WaitKind {
    Signal {
        step_id: String,
        signal_name: String,
        deadline: Option<i64>,
        meta: Option<serde_json::Value>,
    },
    Approval {
        step_id: String,
        approval_id: String,
        title: String,
        description: Option<String>,
        meta: Option<serde_json::Value>,
    },
}

impl WaitKind {
    /// 规范形（`RunState.awaiting` 的元素）。与两个镜像同源，避免三处漂移。
    fn to_awaitable(&self) -> crate::run_store::RunAwaitable {
        use crate::run_store::RunAwaitable;
        match self {
            WaitKind::Signal {
                step_id,
                signal_name,
                deadline,
                meta,
            } => RunAwaitable::Signal {
                step_id: Some(step_id.clone()),
                signal_name: signal_name.clone(),
                deadline: *deadline,
                meta: meta.clone(),
            },
            WaitKind::Approval {
                step_id,
                approval_id,
                title,
                description,
                meta,
            } => RunAwaitable::Approval {
                step_id: Some(step_id.clone()),
                approval_id: approval_id.clone(),
                title: title.clone(),
                description: description.clone(),
                meta: meta.clone(),
            },
        }
    }
}

/// 挂起时把 RunState 投影成 `Paused` + `awaiting` / `waiting_for` /
/// `pending_approval`（对齐 TanStack `RunState`）。fresh 与崩溃后 replay 都会
/// 调用，保证 observer 不需要扫事件日志就知道 run 在等什么。best-effort：失败
/// 忽略，事件日志仍为准。
///
/// **一次写三处**：`awaiting`（规范形）由 [`WaitKind::to_awaitable`] 从同一份
/// 数据派生，两个镜像分别填对应的一个、清另一个。上游
/// `run-workflow.ts:950`/`:1059` 也是三处同写。四处赋值（规范形 + 两镜像 ×2
/// 互清）都在这里，**别在别处再单独改镜像** —— 漂移了就对不上了。
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
        // 规范形先落，再填镜像。
        st.awaiting = vec![kind.to_awaitable()];
        match &kind {
            WaitKind::Signal {
                step_id,
                signal_name,
                deadline,
                meta,
            } => {
                st.waiting_for = Some(crate::run_store::WaitForState {
                    step_id: Some(step_id.clone()),
                    signal_name: signal_name.clone(),
                    deadline: *deadline,
                    meta: meta.clone(),
                });
                st.pending_approval = None;
            }
            WaitKind::Approval {
                step_id,
                approval_id,
                title,
                description,
                meta,
            } => {
                st.pending_approval = Some(crate::run_store::PendingApproval {
                    step_id: Some(step_id.clone()),
                    approval_id: approval_id.clone(),
                    title: title.clone(),
                    description: description.clone(),
                    meta: meta.clone(),
                });
                st.waiting_for = None;
            }
        }
        let _ = store.set_run_state(run_id, &st);
    }
}

/// Appends a `StepResume` for a parked run — the external side of
/// [`WorkflowCtx::approve`](crate::define::WorkflowCtx::approve). Safe to call
/// any time: nothing is parked *watching* the log (the drive already returned),
/// so this takes effect on the **next** `run_workflow` invocation, which
/// resolves the wait from the log and never re-appends its `StepPaused`.
/// Delivering the resume is the caller's job — that is the runtime driver's
/// whole reason to exist (see `aa_workflow_runtime`'s `deliver_signal`). The first
/// append per `(run_id, step_id)` wins; duplicates drop via the store's CAS
/// append. `payload` is what `approve` returns on the resumed drive.
pub fn signal_run(
    store: &dyn RunStore,
    run_id: &str,
    step_id: &str,
    payload: serde_json::Value,
) -> Result<(), WorkflowError> {
    loop {
        let n = store.get_events(run_id)?.len();
        let ev = WorkflowEvent::StepResume {
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
        WorkflowEvent::StepPaused {
            step_id,
            signal_name,
            ..
        } if signal_name == event_name => Some(step_id.clone()),
        _ => None,
    });
    match step_id {
        Some(step_id) => signal_run(store, run_id, &step_id, payload),
        None => Err(WorkflowError::SignalLost(format!(
            "no step in run `{run_id}` is paused waiting on event `{event_name}`"
        ))),
    }
}

/// 测试脚手架，供 engine 各子模块（`mod` / `run_workflow`）的测试共享。
#[cfg(test)]
pub(crate) mod testkit {
    /// 记录 step 的起止与并发峰值，用来断言执行次数与拓扑。
    #[derive(Default)]
    pub struct TestLog {
        pub started: Vec<String>,
        pub finished: Vec<String>,
        pub timeline: Vec<String>,
        pub active: usize,
        pub peaks: Vec<usize>,
        pub runs: std::collections::HashMap<String, usize>,
    }

    impl TestLog {
        pub fn note_start(&mut self, id: &str) {
            self.started.push(id.to_string());
            self.timeline.push(format!(">{id}"));
            self.active += 1;
            self.peaks.push(self.active);
            *self.runs.entry(id.to_string()).or_insert(0) += 1;
        }
        pub fn note_finish(&mut self, id: &str) {
            self.finished.push(id.to_string());
            self.timeline.push(format!("<{id}"));
            self.active -= 1;
            self.peaks.push(self.active);
        }
    }

    pub fn idx(v: &[String], s: &str) -> usize {
        v.iter().position(|x| x == s).unwrap_or(usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::define::{Backoff, BaseCtx, CreateWorkflowConfig, RetryPolicy, WorkflowDefinition, WorkflowCtx, create_workflow};
    use crate::engine::testkit::{TestLog, idx};
    use crate::run_store::InMemoryStore;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::AtomicBool;
    use tokio::try_join;

    /// 一个具名 step 产物。`step<T>` 泛型化后 handler 侧拿到的是这个类型，
    /// 而不是 `serde_json::Value`。
    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    #[serde(rename_all = "camelCase")]
    struct Probe {
        ok: bool,
        count: i64,
    }

    /// 故意**不对称**的类型：写出时把零值字段省掉，读回时却要求它存在。
    /// 用它钉住「JSON 往返不对称」的边界——首次 drive 拿到的是闭包原始值
    /// （不过往返，所以不炸），resume 才从日志反序列化，这时才炸。
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct SkippedOnZero {
        #[serde(skip_serializing_if = "is_zero")]
        n: i64,
    }

    fn is_zero(n: &i64) -> bool {
        *n == 0
    }

    #[tokio::test]
    async fn step_resolves_to_the_closure_type() {
        let store = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("w").handler(|ctx: WorkflowCtx| async move {
            // 无 turbofish、无 `from_value`：T 由闭包的返回类型推出来。
            let probe = ctx
                .step("probe", move |_sc: StepCtx| async move {
                    Ok(Probe { ok: true, count: 7 })
                })
                .await?;
            // 字段访问，不是 `["count"]` 下标。
            let doubled = probe.count * 2;
            Ok(serde_json::json!({ "doubled": doubled, "ok": probe.ok }))
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store).input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        // handler 的返回值仍是 Value（workflow 出口没泛型化），但那是出口不是 step。
        assert_eq!(out.output.unwrap()["doubled"], 14);
    }

    #[tokio::test]
    async fn typed_step_replays_back_into_the_same_type() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = WorkflowDefinition::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
                let log = log.clone();
                async move {
                    let a = ctx
                        .step("a", move |_sc: StepCtx| {
                            let log = log.clone();
                            async move {
                                log.lock().unwrap().note_start("a");
                                log.lock().unwrap().note_finish("a");
                                Ok(Probe {
                                    ok: true,
                                    count: 41,
                                })
                            }
                        })
                        .await?;
                    // +1 而不是 +true：证明 resume 拿回的是 `Probe` 不是 `Value`。
                    Ok(serde_json::json!({ "next": a.count + 1 }))
                }
            }
        });
        let first = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(first.output.unwrap()["next"], 42);
        let second = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store)
                .input(serde_json::json!({}))
                .run_id(first.run_id),
        )
        .outcome().await
        .unwrap();
        // 闭包没被重跑，但类型照样回来了。
        assert_eq!(log.lock().unwrap().runs["a"], 1);
        assert_eq!(second.output.unwrap()["next"], 42);
    }

    #[tokio::test]
    async fn replay_shape_mismatch_fails_instead_of_yielding_null() {
        let store = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("w").handler(|ctx: WorkflowCtx| async move {
            let v = ctx
                .step("skipped", move |_sc: StepCtx| async move {
                    Ok(SkippedOnZero { n: 0 })
                })
                .await?;
            // 首次：拿到的是闭包原始值 `n: 0`（没过往返）。
            assert_eq!(v.n, 0);
            Ok(serde_json::Value::Null)
        });
        let first = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(first.status, RunStatus::Finished);
        // 日志里只有 `{}`——`n` 被 skip 掉了。
        let second = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store)
                .input(serde_json::json!({}))
                .run_id(first.run_id),
        )
        .outcome().await;
        // resume 反序列化 `{}` 失败：响亮报错，而不是静默给一个 null。
        // 注意 run 级失败是 `Ok(RunOutcome { status: Errored, error: Some(..) })`，
        // 不是 `Err`——Err 留给 store/引擎层故障。
        let second = second.expect("run 级失败不是 Err");
        assert_eq!(second.status, RunStatus::Errored);
        let err = second.error.expect("应带 run 级错误").to_string();
        assert!(err.contains("does not deserialize back into"), "got: {err}");
        assert!(err.contains("missing field `n`"), "got: {err}");
    }

    /// `Serialize` 实现直接报错。用来卡住「结果写不进日志」这条路径。
    /// （不能用 `f64::NAN` 试探：serde_json 把非有限浮点写成 `null`，不报错。）
    #[derive(Deserialize, Debug)]
    struct Unserializable;

    impl Serialize for Unserializable {
        fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("this type refuses to serialize"))
        }
    }

    #[tokio::test]
    async fn unserializable_result_fails_the_step_not_a_stuck_run() {
        let store = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("w").handler(|ctx: WorkflowCtx| async move {
            ctx.step("unserializable", move |_sc: StepCtx| async move {
                // 闭包本身是成功的——失败点在**记录**这一步。
                Ok(Unserializable)
            })
            .await?;
            Ok(serde_json::Value::Null)
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store.clone()).input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        // 关键：step 走了既有的失败路径，所以 STEP_FAILED 落了日志。
        // 若当初用 `?` 提前返回，step 会永远停在 Running 且零事件。
        let events = store.get_events(&out.run_id).unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, WorkflowEvent::StepFailed { .. })),
            "no StepFailed appended"
        );
    }

    #[tokio::test]
    async fn serial_handler_runs_in_order() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = WorkflowDefinition::new("w").handler({
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
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
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
        let wf = WorkflowDefinition::new("w").handler({
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
        run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
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
        let wf = WorkflowDefinition::new("w").handler({
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
        run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
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
        let wf = WorkflowDefinition::new("w").handler({
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
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        let second = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id(out.run_id),
        )
        .outcome().await
        .unwrap();
        assert_eq!(second.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 1);
    }

    #[tokio::test]
    async fn stale_up_to_date_reruns() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let fresh_a = Arc::new(AtomicBool::new(true));
        let fresh_b = Arc::new(AtomicBool::new(true));
        let wf = WorkflowDefinition::new("w").handler({
            let log = log.clone();
            let fresh_a = fresh_a.clone();
            let fresh_b = fresh_b.clone();
            move |ctx: WorkflowCtx| {
                let log = log.clone();
                let fresh_a = fresh_a.clone();
                let fresh_b = fresh_b.clone();
                async move {
                    let opts_a =
                        StepOptions::new().up_to_date(move |_, _| fresh_a.load(Ordering::SeqCst));
                    let opts_b =
                        StepOptions::new().up_to_date(move |_, _| fresh_b.load(Ordering::SeqCst));
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
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        // mark a stale, rereun: a reruns, b stays cached
        fresh_a.store(false, Ordering::SeqCst);
        let second = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id(out.run_id),
        )
        .outcome().await
        .unwrap();
        assert_eq!(second.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 2);
        assert_eq!(l.runs["b"], 1);
    }

    #[tokio::test]
    async fn resource_gate_serializes_same_key() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = WorkflowDefinition::new("w").handler({
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
        run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
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
        let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| {
            let fail = fail.clone();
            async move {
                let fail = fail.clone();
                let retry = RetryPolicy::new(2, Backoff::Fixed { base_ms: 1 });
                ctx.step_with("a", StepOptions::new().retry(retry), move |_sc: StepCtx| {
                    let fail = fail.clone();
                    async move {
                        if fail.swap(false, Ordering::SeqCst) {
                            Err(anyhow::anyhow!("transient"))
                        } else {
                            Ok(serde_json::Value::Null)
                        }
                    }
                })
                .await
            }
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let events = store.get_events(&out.run_id).unwrap();
        let fin = events.iter().find_map(|e| match e {
            WorkflowEvent::StepFinished {
                step_id, attempts, ..
            } if step_id == "a" => Some(attempts),
            _ => None,
        });
        assert_eq!(fin.map(|a| a.len()), Some(2));
    }

    /// `StepAttempt.error` 是结构化的 `RunError`（对齐 TanStack
    /// `StepAttempt.error?: SerializedError`），不是扁平字符串。
    #[tokio::test]
    async fn failed_attempts_record_structured_error() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| async move {
            ctx.step_with(
                "a",
                StepOptions::new().retry(RetryPolicy::new(2, Backoff::Fixed { base_ms: 1 })),
                move |_sc: StepCtx| async move { Err(anyhow::anyhow!("transient")) },
            )
            .await
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);

        let events = store.get_events(&out.run_id).unwrap();
        let attempts = events
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::StepFailed { attempts, .. } => Some(attempts),
                _ => None,
            })
            .expect("应有 StepFailed");
        assert_eq!(attempts.len(), 2, "retry=2 → 两个 attempt");

        for (i, a) in attempts.iter().enumerate() {
            assert_eq!(a.attempt, i + 1);
            assert_eq!(a.result, None);
            let err = a
                .error
                .as_ref()
                .unwrap_or_else(|| panic!("attempt {} 应记下结构化错误", a.attempt));
            assert_eq!(err.message, "transient");
            assert_eq!(err.name, "Error", "anyhow 类型擦除，拿不到类名");
        }

        // 结构化错误会随事件日志持久化，replay 能读回。
        let back: Vec<WorkflowEvent> = store.get_events(&out.run_id).unwrap();
        let again = back.iter().find_map(|e| match e {
            WorkflowEvent::StepFailed { attempts, .. } => Some(attempts),
            _ => None,
        });
        assert_eq!(
            again.unwrap()[0].error.as_ref().map(|e| e.message.as_str()),
            Some("transient")
        );
    }

    #[tokio::test]
    async fn exhausted_retries_error() {
        let store = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| async move {
            let retry = RetryPolicy::new(2, Backoff::Fixed { base_ms: 1 });
            ctx.step_with(
                "a",
                StepOptions::new().retry(retry),
                move |_sc: StepCtx| async move { Err(anyhow::anyhow!("boom")) },
            )
            .await
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(out.error.unwrap().message.contains("boom"));
        let events = store.get_events(&out.run_id).unwrap();
        let failed = events.iter().find_map(|e| match e {
            WorkflowEvent::StepFailed {
                step_id, attempts, ..
            } if step_id == "a" => Some(attempts),
            _ => None,
        });
        assert_eq!(failed.map(|a| a.len()), Some(2));
    }

    #[tokio::test]
    async fn timeout_marks_step_failed() {
        let store = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("w").handler(|ctx: WorkflowCtx| async move {
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
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(out.error.unwrap().message.contains("timed out"));
    }

    #[tokio::test]
    async fn progress_and_subscribe_fan_out() {
        let store = Arc::new(InMemoryStore::new());
        let rx = store.subscribe("prog_run").unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = events.clone();
        let wf = WorkflowDefinition::new("w").handler(|ctx: WorkflowCtx| async move {
            ctx.step("a", move |sc: StepCtx| async move {
                sc.progress(0.5);
                Ok(serde_json::Value::Null)
            })
            .await
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("prog_run")
            .publish(Some(Arc::new(move |_run_id: &RunId, e: WorkflowEvent| {
                sink.lock().unwrap().push(e.clone())
            }))),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);

        // publisher saw emit-only progress + checkpoints
        let evs = events.lock().unwrap();
        assert!(
            evs.iter()
                .any(|e| matches!(e, WorkflowEvent::StepProgress { value: 0.5, .. }))
        );
        assert!(
            evs.iter()
                .any(|e| matches!(e, WorkflowEvent::RunStarted { .. }))
        );
        assert!(
            evs.iter()
                .any(|e| matches!(e, WorkflowEvent::StepFinished { .. }))
        );

        // subscriber (store log) saw the checkpoint but not emit-only events
        let mut saw_finish = false;
        let mut saw_shared = false;
        let first = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("got first event");
        saw_finish |= matches!(first, WorkflowEvent::StepFinished { .. });
        assert!(saw_finish, "first appended event should be StepFinished");
        while let Ok(e) = rx.try_recv() {
            match e {
                WorkflowEvent::StepFinished { .. } => saw_finish = true,
                WorkflowEvent::StepProgress { .. } | WorkflowEvent::StepStarted { .. } => {
                    saw_shared = true
                }
                _ => {}
            }
        }
        assert!(saw_finish);
        assert!(!saw_shared, "emit-only events must not reach the log");
    }

    #[tokio::test]
    async fn cas_conflict_detected() {
        let store = Arc::new(InMemoryStore::new());
        let ev = WorkflowEvent::StepFinished {
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
            crate::error::StoreError::Conflict {
                expected, actual, ..
            } => {
                assert_eq!(expected, 1);
                assert_eq!(actual, 2);
            }
            _ => panic!("expected Conflict"),
        }
    }

    async fn wait_until(
        store: &Arc<dyn RunStore>,
        run_id: &str,
        pred: impl Fn(&WorkflowEvent) -> bool,
    ) {
        for _ in 0..2000 {
            if store.get_events(run_id).unwrap().iter().any(|e| pred(e)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition never met for run {run_id}");
    }

    fn count_events(
        store: &Arc<dyn RunStore>,
        run_id: &str,
        pred: impl Fn(&WorkflowEvent) -> bool,
    ) -> usize {
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
        let wf = WorkflowDefinition::new("approval").handler(move |ctx: WorkflowCtx| {
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

        // Drive #1 —— 跑到挂起点就**返回**（不阻塞）。对齐上游：写 checkpoint
        // 后 `throw WorkflowPaused`，drive 直接结束。
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("r1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);
        assert!(out.error.is_none(), "挂起不是失败");
        assert!(out.output.is_none());
        assert_eq!(count_events(&store, "r1", |e| matches!(
            e,
            WorkflowEvent::RunFinished { .. } | WorkflowEvent::RunErrored { .. }
        )), 0, "挂起不得写终态事件");

        let st = store.get_run_state("r1").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Paused);
        let pa = st
            .pending_approval
            .as_ref()
            .expect("RunState 应投影 pending_approval");
        assert_eq!(pa.step_id.as_deref(), Some("release"));
        assert_eq!(pa.title, "Approve the release?");
        assert!(st.waiting_for.is_none());
        assert!(decided.lock().unwrap().is_none(), "挂起时 handler 未继续");

        // 外部投递（这是 runtime 的 `deliver_approval` 做的事）。
        signal_run(
            store.as_ref(),
            "r1",
            "release",
            serde_json::json!({ "approved": true }),
        )
        .unwrap();

        // Drive #2 —— 从日志短路 `charge`，pause 有 resume 直接放行。
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("r1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "approved": true })));
        let st = store.get_run_state("r1").unwrap().unwrap();
        assert!(
            st.waiting_for.is_none() && st.pending_approval.is_none(),
            "恢复后投影应被清除"
        );
        assert_eq!(
            decided.lock().unwrap().as_ref(),
            Some(&serde_json::json!({ "approved": true }))
        );
        assert_eq!(
            count_events(&store, "r1", |e| matches!(
                e,
                WorkflowEvent::StepPaused { step_id, .. } if step_id == "release"
            )),
            1
        );
        assert_eq!(
            count_events(&store, "r1", |e| matches!(
                e,
                WorkflowEvent::StepResume { step_id, .. } if step_id == "release"
            )),
            1
        );
        assert!(store.get_events("r1").unwrap().iter().any(|e| matches!(
            e,
            WorkflowEvent::StepFinished { step_id, .. } if step_id == "ship"
        )));
    }

    #[tokio::test]
    async fn aborted_run_replays_pause_without_reappending() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let decided = Arc::new(Mutex::new(None::<serde_json::Value>));
        let decided_in = decided.clone();
        let wf = WorkflowDefinition::new("approval").handler(move |ctx: WorkflowCtx| {
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
            run_workflow(
                RunWorkflowOptions::new(Arc::new(wf2.clone()), store2)
                    .input(serde_json::json!({}))
                .run_id("r2"),
            )
            .outcome().await
        });
        wait_until(
            &store,
            "r2",
            |e| matches!(e, WorkflowEvent::StepPaused { step_id, .. } if step_id == "release"),
        )
        .await;
        t1.abort();
        let _ = t1.await;

        // Run 2 (same run_id): succeeded checkpoint short-circuits, the pause
        // re-waits but must NOT append a second StepPaused.
        let store3 = store.clone();
        let wf3 = wf.clone();
        let t2 = tokio::spawn(async move {
            run_workflow(
                RunWorkflowOptions::new(Arc::new(wf3.clone()), store3)
                    .input(serde_json::json!({}))
                .run_id("r2"),
            )
            .outcome().await
        });
        wait_until(
            &store,
            "r2",
            |e| matches!(e, WorkflowEvent::StepPaused { step_id, .. } if step_id == "release"),
        )
        .await;
        assert_eq!(
            count_events(&store, "r2", |e| matches!(
                e,
                WorkflowEvent::StepPaused { step_id, .. } if step_id == "release"
            )),
            1,
            "replay must not re-append the pause checkpoint"
        );

        signal_run(
            store.as_ref(),
            "r2",
            "release",
            serde_json::json!({ "approved": true }),
        )
        .unwrap();
        let out = t2.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "approved": true })));
        assert_eq!(
            count_events(&store, "r2", |e| matches!(
                e,
                WorkflowEvent::StepFinished { step_id, .. } if step_id == "charge"
            )),
            1,
            "replay must short-circuit the succeeded step"
        );
        assert_eq!(
            count_events(&store, "r2", |e| matches!(
                e,
                WorkflowEvent::StepResume { step_id, .. } if step_id == "release"
            )),
            1
        );
    }

    /// sleep 挂起后**返回**，由外部（runtime 的 timer sweep）在 `due_at` 投递
    /// `StepResume` 唤醒。core 自己不投递——这是 D3 定案。
    #[tokio::test]
    async fn sleep_parks_then_external_timer_resumes() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("sleeper").handler(|ctx: WorkflowCtx| async move {
            ctx.sleep("cooldown", Duration::from_millis(250)).await?;
            ctx.step("after", move |_sc: StepCtx| async move {
                Ok(serde_json::json!({ "done": true }))
            })
            .await?;
            Ok(serde_json::Value::Null)
        });

        // Drive #1：立刻返回 `Paused`（不阻塞 250ms）。
        let started = std::time::Instant::now();
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("s1"),
        )
        .outcome().await
        .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(out.status, RunStatus::Paused);
        assert!(
            elapsed < Duration::from_millis(200),
            "挂起应立刻返回，而不是等满 250ms（实际 {elapsed:?}）"
        );
        assert_eq!(count_events(&store, "s1", |e| matches!(
            e,
            WorkflowEvent::RunFinished { .. } | WorkflowEvent::RunErrored { .. }
        )), 0, "挂起不得写终态事件");

        // sleep 的投影：status=Paused + waiting_for{signal_name:"__timer", deadline}
        let st = store.get_run_state("s1").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Paused);
        let w = st
            .waiting_for
            .as_ref()
            .expect("sleep 中应投影 waiting_for");
        assert_eq!(w.step_id.as_deref(), Some("cooldown"), "step_id 是 pause key");
        assert_eq!(w.signal_name, "__timer", "sleep 的通道是内部 __timer");
        assert!(w.deadline.is_some(), "sleep 的 due_at 应投影为 deadline");

        // 外部投递（runtime sweep 在 deadline 到点后做的事）。
        signal_run(store.as_ref(), "s1", "cooldown", serde_json::Value::Null).unwrap();

        // Drive #2：放行。
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("s1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let st = store.get_run_state("s1").unwrap().unwrap();
        assert!(
            st.waiting_for.is_none() && st.pending_approval.is_none(),
            "恢复后投影应清除"
        );

        let evs = store.get_events("s1").unwrap();
        let resume = evs.iter().find_map(|e| match e {
            WorkflowEvent::StepResume {
                step_id, payload, ..
            } if step_id == "cooldown" => payload.clone(),
            _ => None,
        });
        assert_eq!(resume, Some(serde_json::Value::Null));
        assert!(evs.iter().any(|e| matches!(
            e,
            WorkflowEvent::StepFinished { step_id, .. } if step_id == "after"
        )));
    }

    #[tokio::test]
    async fn resumed_run_replays_from_log_without_rewaiting() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("approval").handler(|ctx: WorkflowCtx| async move {
            let d = ctx.approve("gate", "Approve this?").await?;
            ctx.step("consume", move |_sc: StepCtx| async move {
                Ok(serde_json::json!({ "consumed": true }))
            })
            .await?;
            Ok(d)
        });

        // Run 1: park, signal, finish.
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("r3"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);
        signal_run(
            store.as_ref(),
            "r3",
            "gate",
            serde_json::json!({ "yes": 1 }),
        )
        .unwrap();
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("r3"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "yes": 1 })));

        // Run 2, same run_id: the StepResume is already in the log, so
        // approve resolves from the log — no new checkpoints appended.
        let out2 = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("r3"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out2.status, RunStatus::Finished);
        assert_eq!(out2.output, Some(serde_json::json!({ "yes": 1 })));
        assert_eq!(
            count_events(&store, "r3", |e| matches!(
                e,
                WorkflowEvent::StepPaused { step_id, .. } if step_id == "gate"
            )),
            1
        );
        assert_eq!(
            count_events(&store, "r3", |e| matches!(
                e,
                WorkflowEvent::StepResume { step_id, .. } if step_id == "gate"
            )),
            1
        );
    }

    #[tokio::test]
    async fn wait_for_event_fulfilled_by_signal_event() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let received = Arc::new(Mutex::new(None::<serde_json::Value>));
        let rx = received.clone();
        let wf = WorkflowDefinition::new("named-event").handler(move |ctx: WorkflowCtx| {
            let rx = rx.clone();
            async move {
                let v = ctx.wait_for_event("review", "review-approved").await?;
                *rx.lock().unwrap() = Some(v.clone());
                Ok(v)
            }
        });
        // Drive #1：跑到挂起点返回 `Paused`（不阻塞）。
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("ne1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);
        let st = store.get_run_state("ne1").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Paused);
        let w = st
            .waiting_for
            .as_ref()
            .expect("named wait 投影 waiting_for");
        assert_eq!(w.signal_name, "review-approved");
        assert_eq!(w.step_id.as_deref(), Some("review"));
        assert!(st.pending_approval.is_none(), "named event 不是 approval");

        // 外部投递 → Drive #2 放行。
        signal_event(
            store.as_ref(),
            "ne1",
            "review-approved",
            serde_json::json!({ "ok": true }),
        )
        .unwrap();
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("ne1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "ok": true })));
        assert_eq!(
            received.lock().unwrap().as_ref(),
            Some(&serde_json::json!({ "ok": true }))
        );
    }

    #[tokio::test]
    async fn wait_for_event_resolves_from_log_on_replay() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("named-event").handler(|ctx: WorkflowCtx| async move {
            let v = ctx.wait_for_event("gate", "go").await?;
            let out = v.clone();
            ctx.step("consume", move |_sc| async move { Ok(v.clone()) })
                .await?;
            Ok(out)
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("ne2"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);
        signal_event(store.as_ref(), "ne2", "go", serde_json::json!(42)).unwrap();
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("ne2"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!(42)));

        // 同名 run 重跑：StepResume 已在日志，replay 从日志取，不重新等待或加 checkpoint
        let store3 = store.clone();
        let wf3 = wf.clone();
        let out2 = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf3.clone()), store3)
                .input(serde_json::json!({}))
            .run_id("ne2"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out2.status, RunStatus::Finished);
        assert_eq!(out2.output, Some(serde_json::json!(42)));
        assert_eq!(
            count_events(&store, "ne2", |e| matches!(
                e,
                WorkflowEvent::StepPaused { signal_name, .. } if signal_name == "go"
            )),
            1
        );
    }

    /// 过去的时间戳：第一次 drive 就挂起（deadline 已过期），外部投递后放行。
    /// 「已过期」这个事实体现在 `due_at` 上，供 timer sweep 立刻认领。
    #[tokio::test]
    async fn sleep_until_past_parks_with_past_deadline() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let past = crate::engine::now_ms() - 5000;
        let wf = WorkflowDefinition::new("sleeper").handler(move |ctx: WorkflowCtx| async move {
            ctx.sleep_until("cooldown", past).await?;
            ctx.step("after", move |_sc: StepCtx| async move {
                Ok(serde_json::json!({ "done": true }))
            })
            .await?;
            Ok(serde_json::Value::Null)
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);
        let st = store.get_run_state(&out.run_id).unwrap().unwrap();
        let w = st.waiting_for.as_ref().expect("应投影 waiting_for");
        assert_eq!(
            w.deadline,
            Some(past),
            "过期的 due_at 照实投影——由 sweep 判断「已到点」"
        );

        signal_run(store.as_ref(), &out.run_id, "cooldown", serde_json::Value::Null).unwrap();
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id(out.run_id),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let evs = store.get_events(&out.run_id).unwrap();
        assert!(evs.iter().any(|e| matches!(
            e,
            WorkflowEvent::StepFinished { step_id, .. } if step_id == "after"
        )));
        assert!(evs.iter().any(|e| matches!(
            e,
            WorkflowEvent::StepResume { step_id, .. } if step_id == "cooldown"
        )));
    }

    #[tokio::test]
    async fn sleep_until_schedules_timer() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let due = crate::engine::now_ms() + 300;
        let wf = WorkflowDefinition::new("sleeper").handler(move |ctx: WorkflowCtx| async move {
            ctx.sleep_until("cooldown", due).await?;
            Ok(serde_json::Value::Null)
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("stu1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);

        let st = store.get_run_state("stu1").unwrap().unwrap();
        let w = st.waiting_for.as_ref().expect("sleep_until 应投影 waiting_for");
        assert_eq!(w.step_id.as_deref(), Some("cooldown"));
        assert_eq!(w.signal_name, "__timer");
        assert_eq!(w.deadline, Some(due), "deadline 应是调用方给的时间戳");

        signal_run(store.as_ref(), "stu1", "cooldown", serde_json::Value::Null).unwrap();
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("stu1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
    }

    #[tokio::test]
    async fn emit_reaches_publisher_but_not_log() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let wf = WorkflowDefinition::new("emitter").handler(|ctx: WorkflowCtx| async move {
            ctx.emit("ping", serde_json::json!({ "x": 1 }));
            ctx.step("a", move |_sc: StepCtx| async move {
                Ok(serde_json::Value::Null)
            })
            .await?;
            Ok(serde_json::Value::Null)
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("em1")
            .publish(Some(Arc::new(move |_run_id: &RunId, e: WorkflowEvent| {
                sink.lock().unwrap().push(e.clone())
            }))),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert!(
            seen.lock()
                .unwrap()
                .iter()
                .any(|e| matches!(e, WorkflowEvent::Custom { name, .. } if name == "ping"))
        );
        assert!(
            store
                .get_events("em1")
                .unwrap()
                .iter()
                .all(|e| !matches!(e, WorkflowEvent::Custom { .. }))
        );
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
        let wf = WorkflowDefinition::new("det").handler(|ctx: WorkflowCtx| async move {
            // 两个调用位点各自拿到稳定值；approve 强制产生一次 replay 边界。
            let t1 = ctx.now()?;
            let u1 = ctx.uuid()?;
            ctx.approve("gate", "go").await?;
            let t2 = ctx.now()?;
            let u2 = ctx.uuid()?;
            Ok(serde_json::json!({ "now": t1, "uuid": u1, "now2": t2, "uuid2": u2 }))
        });
        // Drive #1：跑到 approve 挂起就返回。
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("det1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);
        signal_run(store.as_ref(), "det1", "gate", serde_json::json!(true)).unwrap();
        // Drive #2：`__now-0` / `__uuid-0` 从日志缓存取（deterministic replay）。
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("det1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let o1 = out.output.clone().unwrap();

        // 重跑（崩溃恢复模拟）：now/uuid 全部从日志缓存值取，输出逐字节一致。
        let out2 = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("det1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out2.status, RunStatus::Finished);
        assert_eq!(
            out2.output.clone().unwrap(),
            o1,
            "replay 后 now/uuid 与首跑一致"
        );
        assert_eq!(
            count_events(&store, "det1", |e| matches!(
                e,
                WorkflowEvent::NowRecorded { .. }
            )),
            2,
            "每个调用位点只记录一次，replay 不重复 append"
        );
        assert_eq!(
            count_events(&store, "det1", |e| matches!(
                e,
                WorkflowEvent::UuidRecorded { .. }
            )),
            2
        );
        // 两对 now/uuid 都用独立的确定性 id（__now-0/__now-1、__uuid-0/__uuid-1）。
        let ids: Vec<String> = store
            .get_events("det1")
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                WorkflowEvent::NowRecorded { step_id, .. } => Some(format!("now:{step_id}")),
                WorkflowEvent::UuidRecorded { step_id, .. } => Some(format!("uuid:{step_id}")),
                _ => None,
            })
            .collect();
        assert!(
            !ids.iter()
                .any(|a| ids.iter().filter(|b| b == &a).count() > 1),
            "ids 唯一: {ids:?}"
        );
    }

    #[tokio::test]
    async fn concurrent_now_calls_stay_unique() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("conc").handler(|ctx: WorkflowCtx| async move {
            let (a, b) = {
                let (a, b) = (ctx.clone(), ctx.clone());
                tokio::join!(async move { a.now() }, async move { b.now() },)
            };
            Ok(serde_json::json!({ "a": a?, "b": b? }))
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let v = out.output.unwrap();
        assert!(
            v["a"].as_i64().is_some() && v["b"].as_i64().is_some(),
            "两个 now 都应成功"
        );
        assert_eq!(
            count_events(&store, &out.run_id, |e| matches!(
                e,
                WorkflowEvent::NowRecorded { .. }
            )),
            2,
            "并发调用各自落一个 NowRecorded，无重复 id"
        );

        // 重跑：并发 again，replay 命中 __now-0/__now-1 缓存，事件数不变。
        let store2 = store.clone();
        let wf2 = wf.clone();
        let out2 = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf2.clone()), store2)
                .input(serde_json::json!({}))
            .run_id(&out.run_id),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out2.status, RunStatus::Finished);
        assert_eq!(out2.output.clone().unwrap(), v, "并发 replay 也一致");
        assert_eq!(
            count_events(&store, &out.run_id, |e| matches!(
                e,
                WorkflowEvent::NowRecorded { .. }
            )),
            2
        );
    }

    /// cancel 一个挂起的 approval：挂起时没人在跑，所以 `Aborted` 落在
    /// RunState 上，等**下次 drive** 在第一个边界收尾（上游同理——挂起后
    /// 进程已退出，没有「立刻」响应 cancel 的实体）。
    #[tokio::test]
    async fn cancel_parked_approval_aborts() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("cancel").handler(|ctx: WorkflowCtx| async move {
            let _ = ctx.approve("release", "Approve?").await?;
            ctx.step("ship", move |_sc: StepCtx| async move {
                Ok(serde_json::Value::Null)
            })
            .await?;
            Ok(serde_json::Value::Null)
        });

        // Drive #1：挂起即返回。
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("ca1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);
        let st = store.get_run_state("ca1").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Paused);

        cancel_run(store.as_ref(), "ca1").unwrap();
        assert_eq!(
            store.get_run_state("ca1").unwrap().unwrap().status,
            RunStatus::Aborted,
            "cancel 只翻 RunState——挂起中没人执行"
        );

        // Drive #2：引擎拾取 `Aborted`，以 Aborted 收尾。
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("ca1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(
            out.status,
            RunStatus::Aborted,
            "cancel 后 run 以 Aborted 终局"
        );
        assert_eq!(out.error.unwrap().name, "Aborted");
        assert!(out.output.is_none());

        let st = store.get_run_state("ca1").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Aborted, "RunState 终态应为 Aborted");
        // 结构化错误名 + 机器可读 code，不用再去匹配 message 字符串。
        assert_eq!(
            st.error.as_ref().map(|e| e.name.as_str()),
            Some("Aborted"),
            "RunState.error 应带上结构化错误名"
        );
        assert!(st.waiting_for.is_none() && st.pending_approval.is_none());

        // 已 cancel 的 run 不能再被 drive 复活。
        let again = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("ca1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(again.status, RunStatus::Aborted, "Aborted 是锁存的终态");

        // 日志没有 Finished 终态，也不应落在 running/paused（cancel 后不再驱动）
        let evs = store.get_events("ca1").unwrap();
        let terminal_finished = evs
            .iter()
            .any(|e| matches!(e, WorkflowEvent::RunFinished { .. }));
        assert!(!terminal_finished);
        assert!(!evs.iter().any(|e| matches!(
            e,
            WorkflowEvent::StepFinished { step_id, .. } if step_id == "ship"
        )));
    }

    #[tokio::test]
    async fn step_ctx_exposes_id_attempt_and_cooperative_cancel() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let started = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new(tokio::sync::Notify::new());
        let started_in = started.clone();
        let gate_in = gate.clone();
        let wf = WorkflowDefinition::new("sig").handler(move |ctx: WorkflowCtx| {
            let started3 = started_in.clone();
            let gate3 = gate_in.clone();
            async move {
                let seen = ctx
                    .step("a", move |sc: StepCtx| async move {
                        started3.notify_one();
                        gate3.notified().await;
                        let cancelled = sc.is_cancelled();
                        let id = sc.id.clone();
                        let attempt = sc.attempt;
                        Ok(serde_json::json!({ "id": id, "attempt": attempt, "cancelled": cancelled }))
                    })
                    .await?;
                Ok(seen)
            }
        });
        let store2 = store.clone();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(
                RunWorkflowOptions::new(Arc::new(wf2.clone()), store2)
                    .input(serde_json::json!({}))
                .run_id("sig1"),
            )
            .outcome().await
        });
        started.notified().await;
        cancel_run(store.as_ref(), "sig1").unwrap();
        gate.notify_one();
        let out = task.await.unwrap().unwrap();
        assert_eq!(
            out.status,
            RunStatus::Finished,
            "协作式取消：闭包选择正常返回而非提前中止"
        );
        let v = out.output.unwrap();
        assert_eq!(v["id"], "a");
        assert_eq!(v["attempt"], 1);
        assert_eq!(v["cancelled"], true);
    }

    /// cancel 对**已挂起**的 run：没有人在跑，所以 flag 只能等下次 drive 才被
    /// 观察到——那时 run 在第一个边界以 `Aborted` 收尾。这也是上游的语义
    /// （挂起即进程退出，没人「立刻」响应 cancel）。
    #[tokio::test]
    async fn cancel_sleeping_run_aborts_and_is_recoverable() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("cancel-sleep").handler(|ctx: WorkflowCtx| async move {
            ctx.sleep("hold", Duration::from_secs(1)).await?;
            Ok(serde_json::json!({ "done": true }))
        });

        // Drive #1：挂起即返回（不等那 1 秒）。
        let started = std::time::Instant::now();
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("ca2"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);
        assert!(started.elapsed() < Duration::from_millis(500));

        // 挂起期间 cancel：只翻 RunState，没人「立刻」响应。
        cancel_run(store.as_ref(), "ca2").unwrap();
        assert_eq!(
            store.get_run_state("ca2").unwrap().unwrap().status,
            RunStatus::Aborted
        );

        // Drive #2：引擎拾取到 Aborted，在第一个边界以 Aborted 收尾。
        let out2 = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("ca2"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out2.status, RunStatus::Aborted);

        // 重复 cancel：已终局 → no-op Ok
        cancel_run(store.as_ref(), "ca2").unwrap();
        // 未知 run → RunNotFound
        let err = cancel_run(store.as_ref(), "missing").unwrap_err();
        assert!(matches!(err, crate::error::WorkflowError::RunNotFound(_)));

        // `Aborted` 是锁存终态：投递 resume 也唤不回来（上游 `resumeRun`
        // 对 aborted 直接 route 到 attach，`run-workflow.ts:275-282`）。
        signal_run(store.as_ref(), "ca2", "hold", serde_json::Value::Null).unwrap();
        let out3 = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("ca2"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out3.status, RunStatus::Aborted, "cancel 的 run 不会被唤醒");
    }

    #[tokio::test]
    async fn ctx_reports_not_cancelled_in_normal_run() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("not-cancelled").handler(|ctx: WorkflowCtx| async move {
            assert!(!ctx.is_cancelled());
            Ok(serde_json::json!({ "ok": true }))
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
    }

    #[tokio::test]
    async fn deadline_exhausted_flags_should_yield() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let seen = Arc::new(Mutex::new((None::<Option<i64>>, false, false)));
        let sink = seen.clone();
        let wf = WorkflowDefinition::new("budget").handler(move |ctx: WorkflowCtx| {
            let sink = sink.clone();
            async move {
                let (d, tr, sy) = (ctx.deadline(), ctx.time_remaining(), ctx.should_yield());
                *sink.lock().unwrap() = (Some(d), tr < 1000, sy);
                ctx.step("a", move |sc: StepCtx| {
                    let sink = sink.clone();
                    async move {
                        *sink.lock().unwrap() = (
                            Some(sc.deadline()),
                            sc.time_remaining() < 1000,
                            sc.should_yield(),
                        );
                        Ok(serde_json::json!({ "d": sc.deadline(), "tr": sc.time_remaining() }))
                    }
                })
                .await?;
                Ok(serde_json::Value::Null)
            }
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .deadline(crate::engine::now_ms() + 300)
            .publish(// 500ms 内到期 → headroom < 1000
            None),
        )
        .outcome().await
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
        let wf = WorkflowDefinition::new("yielder").handler(|ctx: WorkflowCtx| async move {
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

        // Drive #1：停在 `__yield-0`，立刻返回。
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("y1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);
        let st = store.get_run_state("y1").unwrap().unwrap();
        let w = st.waiting_for.as_ref().expect("yield 应投影 waiting_for");
        assert!(w.step_id.as_deref().unwrap().starts_with("__yield-"));
        assert_eq!(w.signal_name, "__timer");
        assert!(w.deadline.is_some(), "yield 的 target 应投影为 deadline");

        // host 重新唤起（延期预算）→ 投递 timer。
        signal_run(store.as_ref(), "y1", "__yield-0", serde_json::Value::Null).unwrap();
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("y1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert!(store.get_events("y1").unwrap().iter().any(
            |e| matches!(e, WorkflowEvent::StepFinished { step_id, .. } if step_id == "after")
        ));

        // replay：yield 的 StepResume 已在日志 → 短路径立即放行，不新增 checkpoint
        let out2 = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("y1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out2.status, RunStatus::Finished);
        assert_eq!(
            count_events(&store, "y1", |e| matches!(
                e,
                WorkflowEvent::StepPaused { .. }
            )),
            1,
            "replay 不重复 yield 的 pause"
        );
    }

    #[tokio::test]
    async fn yield_parks_until_yield_resume_at() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("yielder").handler(|ctx: WorkflowCtx| async move {
            ctx.yield_().await?;
            Ok(serde_json::json!({ "ok": true }))
        });
        let resume_at = crate::engine::now_ms() + 250;

        // Drive #1：`yield_resume_at` 原样落到 deadline 上——由外部 timer 认领，
        // 引擎自己不等（这正是 upstream `yieldResumeAt` 的用途）。
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("y2")
            .yield_resume_at(resume_at),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);
        let st = store.get_run_state("y2").unwrap().unwrap();
        assert_eq!(
            st.waiting_for.as_ref().unwrap().deadline,
            Some(resume_at),
            "yield_resume_at 应原样成为 deadline"
        );

        signal_run(store.as_ref(), "y2", "__yield-0", serde_json::Value::Null).unwrap();
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("y2"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
    }

    // ====================================================================
    // 新式 builder（create_workflow / WorkflowBuilder / WorkflowDefinition）
    // ====================================================================

    /// `default_step_retry`：workflow 级兜底 retry，step 未声明自己的
    /// `StepOptions::retry` 时生效（TanStack `defaultStepRetry`）。
    #[tokio::test]
    async fn default_step_retry_fallbacks_without_step_policy() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let attempts = Arc::new(AtomicUsize::new(0));
        let wf = create_workflow(
            CreateWorkflowConfig::new("retry-default")
                .input::<serde_json::Value>()
                .default_step_retry(RetryPolicy::new(2, Backoff::Fixed { base_ms: 1 })),
        )
        .handler(move |ctx: BaseCtx<serde_json::Value>| {
            let attempts = attempts.clone();
            async move {
                ctx.step("flaky", move |_sc: StepCtx| {
                    let attempts = attempts.clone();
                    async move {
                        if attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                            anyhow::bail!("transient failure")
                        }
                        Ok(serde_json::json!({ "ok": true }))
                    }
                })
                .await
            }
        });
        let wf: AnyWorkflowDefinition = wf.into();
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(
            out.status,
            RunStatus::Finished,
            "workflow 级 retry 应重试成功"
        );
        assert_eq!(
            count_events(&store, out.run_id.as_str(), |e| matches!(
                e,
                WorkflowEvent::StepFinished { attempts, .. } if attempts.len() == 2
            )),
            1,
            "flaky step 应留 2 次 attempt"
        );
        // step 自己的 retry 优先于 workflow 兜底。
        let store2: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let attempts2 = Arc::new(AtomicUsize::new(0));
        let wf2 = create_workflow(
            CreateWorkflowConfig::new("retry-default-overridden")
                .input::<serde_json::Value>()
                .default_step_retry(RetryPolicy::new(3, Backoff::Fixed { base_ms: 1 })),
        )
        .handler(move |ctx: BaseCtx<serde_json::Value>| {
            let attempts = attempts2.clone();
            // `retry` 放 StepOptions：1 次 attempt，workflow 兜底 3 次不生效。
            async move {
                ctx.step_with::<serde_json::Value, _, _>(
                    "no-retry",
                    StepOptions::new().retry(RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 })),
                    move |_sc: StepCtx| {
                        let attempts = attempts.clone();
                        async move {
                            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            anyhow::bail!("always fails")
                        }
                    },
                )
                .await
            }
        });
        let wf2: WorkflowDefinition = wf2.into();
        let out2 = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf2.clone()), store2.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(
            out2.status,
            RunStatus::Errored,
            "step 自己的 retry=1 → 失败"
        );
        let st = store2.get_run_state(&out2.run_id).unwrap().unwrap();
        assert!(
            st.status == RunStatus::Errored
                && count_events(&store2, &out2.run_id, |e| matches!(
                    e,
                    WorkflowEvent::StepFailed { attempts, .. } if attempts.len() == 1
                )) == 1,
            "per-step retry 覆盖 workflow 兜底"
        );
    }

    /// ctx.state 的变更在耐久边界 flush 时以 `STATE_DELTA`（emit-only）上报，
    /// op 语义对齐 RFC 6902；**不落盘**（state 由日志重放推导，持久化 delta
    /// 会在每次 invocation 重放时重复 append）。
    #[tokio::test]
    async fn state_mutations_emit_state_delta_at_boundaries() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = events.clone();
        let wf = WorkflowDefinition::new("sd")
            .initialize(|_| Ok(serde_json::json!({ "count": 0, "items": [] })))
            .handler(|mut ctx: WorkflowCtx| async move {
                ctx.state["count"] = serde_json::json!(1);
                ctx.step("a", move |_sc: StepCtx| async move {
                    Ok(serde_json::Value::Null)
                })
                .await?;
                ctx.state["items"] = serde_json::json!(["x"]);
                ctx.step("b", move |_sc: StepCtx| async move {
                    Ok(serde_json::Value::Null)
                })
                .await?;
                Ok(serde_json::Value::Null)
            });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("sd")
            .publish(Some(Arc::new(move |_run_id: &RunId, e: WorkflowEvent| {
                sink.lock().unwrap().push(e.clone())
            }))),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);

        let collected = events.lock().unwrap().clone();
        let deltas: Vec<&WorkflowEvent> = collected
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::StateDelta { .. }))
            .collect();
        assert_eq!(deltas.len(), 2, "两次耐久边界各一条 StateDelta");

        use state_diff::Operation;
        let ops1 = match deltas[0] {
            WorkflowEvent::StateDelta { delta, .. } => delta,
            _ => unreachable!(),
        };
        assert_eq!(
            ops1,
            &vec![Operation::Replace {
                path: "/count".into(),
                value: serde_json::json!(1)
            }]
        );
        let ops2 = match deltas[1] {
            WorkflowEvent::StateDelta { delta, .. } => delta,
            _ => unreachable!(),
        };
        assert_eq!(
            ops2,
            &vec![Operation::Replace {
                path: "/items".into(),
                value: serde_json::json!(["x"])
            }]
        );

        // emit-only：日志里没有 StateDelta。
        assert_eq!(
            count_events(&store, "sd", |e| matches!(
                e,
                WorkflowEvent::StateDelta { .. }
            )),
            0
        );
    }

    /// handler 末尾（最后边界之后）的 state 变更：ctx drop 时同步进 mirror，
    /// run_workflow 收尾统一 diff——尾段 STATE_DELTA **自动发射**，
    /// 无需任何显式调用（对齐上游在 handler 返回处的 flushStateDelta）。
    #[tokio::test]
    async fn tail_state_delta_emitted_at_drive_end() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&events);

        let wf = WorkflowDefinition::new("tail-auto")
            .initialize(|_| Ok(serde_json::json!({ "n": 0 })))
            .handler(|mut ctx: WorkflowCtx| async move {
                ctx.step("a", move |_sc: StepCtx| async move {
                    Ok(serde_json::Value::Null)
                })
                .await?;
                ctx.state["n"] = serde_json::json!(1);
                Ok(serde_json::Value::Null)
            });
        run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("tail:1")
            .publish(Some(Arc::new(move |_run_id: &RunId, e: WorkflowEvent| {
                sink.lock().unwrap().push(e.clone())
            }))),
        )
        .outcome().await
        .unwrap();
        let deltas: Vec<WorkflowEvent> = events
            .lock()
            .unwrap()
            .clone()
            .into_iter()
            .filter(|e| matches!(e, WorkflowEvent::StateDelta { .. }))
            .collect();
        assert_eq!(deltas.len(), 1, "尾段 STATE_DELTA 应自动发射");
        match &deltas[0] {
            WorkflowEvent::StateDelta { delta, .. } => assert_eq!(
                delta,
                &vec![state_diff::Operation::Replace {
                    path: "/n".into(),
                    value: serde_json::json!(1)
                }]
            ),
            _ => unreachable!(),
        }
    }

    // ── await 信封：awaiting 规范形 + meta + payload schema ────────────

    /// 挂起时 `awaiting`（规范形）与两个镜像**由同一份数据派生**，三处不许漂移。
    /// 这是对齐上游 `run-workflow.ts:950`/`:1059` 的三处同写。
    #[tokio::test]
    async fn project_run_wait_writes_awaiting_and_both_mirrors() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("aw").handler(|ctx: WorkflowCtx| async move {
            ctx.wait_for_event_with(
                "hold",
                "go",
                crate::define::WaitForEventOptions {
                    deadline: Some(1_800_000_000_000),
                    meta: Some(serde_json::json!({ "ui": "warn" })),
                    schema: None,
                },
            )
            .await?;
            Ok(serde_json::json!({}))
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("aw1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);

        let st = store.get_run_state("aw1").unwrap().unwrap();
        // 规范形：一个元素，带 type 判别式
        assert_eq!(st.awaiting.len(), 1, "awaiting 应恰好一个元素");
        let crate::run_store::RunAwaitable::Signal {
            step_id,
            signal_name,
            deadline,
            meta,
        } = &st.awaiting[0]
        else {
            panic!("awaiting[0] 应是 Signal，实际 {:?}", st.awaiting[0]);
        };
        assert_eq!(step_id.as_deref(), Some("hold"));
        assert_eq!(signal_name, "go");
        assert_eq!(*deadline, Some(1_800_000_000_000));
        assert_eq!(meta.as_ref().unwrap()["ui"], "warn");

        // 镜像与规范形一致 —— 同一份信息两种看法
        let w = st.waiting_for.as_ref().expect("waiting_for 镜像");
        assert_eq!(w.step_id.as_deref(), Some("hold"));
        assert_eq!(w.signal_name, "go");
        assert_eq!(w.deadline, Some(1_800_000_000_000));
        assert_eq!(w.meta.as_ref().unwrap()["ui"], "warn");
        assert!(st.pending_approval.is_none(), "signal 等待不该有 approval 镜像");

        // meta 同时落进 StepPaused checkpoint（观察者只读日志也能拿到）
        let evs = store.get_events("aw1").unwrap();
        let paused = evs
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::StepPaused { meta, .. } => Some(meta.clone()),
                _ => None,
            })
            .expect("应有 StepPaused");
        assert_eq!(paused.as_ref().unwrap()["ui"], "warn");
    }

    /// approval 走另一条分支：`awaiting[0]` 应是 Approval 判别式，
    /// `pending_approval` 镜像填上、`waiting_for` 清空。
    #[tokio::test]
    async fn approval_wait_projects_approval_awaitable() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("ap").handler(|ctx: WorkflowCtx| async move {
            ctx.approve_with(
                "gate",
                "放行这笔？",
                crate::define::ApproveOptions {
                    deadline: None,
                    meta: Some(serde_json::json!({ "amount": 9000 })),
                    schema: None,
                },
            )
            .await?;
            Ok(serde_json::json!({}))
        });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("ap1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);

        let st = store.get_run_state("ap1").unwrap().unwrap();
        assert!(matches!(
            st.awaiting[0],
            crate::run_store::RunAwaitable::Approval { .. }
        ));
        assert!(st.waiting_for.is_none(), "approval 不该有 signal 镜像");
        let a = st.pending_approval.as_ref().expect("approval 镜像");
        assert_eq!(a.approval_id, "gate");
        assert_eq!(a.title, "放行这笔？");
        assert_eq!(a.meta.as_ref().unwrap()["amount"], 9000);
    }

    #[derive(serde::Deserialize)]
    struct Decision {
        approved: bool,
    }

    /// 声明了 schema 的等待：投递**形状不对**的 payload → run 报错，
    /// 而不是把脏数据交给 workflow。
    #[tokio::test]
    async fn declared_payload_schema_rejects_bad_payload() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("sch").handler(|ctx: WorkflowCtx| async move {
            let v = ctx
                .wait_for_event_with(
                    "hold",
                    "go",
                    crate::define::WaitForEventOptions {
                        deadline: None,
                        meta: None,
                        schema: Some(crate::define::PayloadSchema::of::<Decision>()),
                    },
                )
                .await?;
            // 校验过了，这行才安全。
            let d: Decision = serde_json::from_value(v)?;
            Ok(serde_json::json!({ "approved": d.approved }))
        });
        // 先挂起
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("sc1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Paused);

        // 投递一个缺 `approved` 字段的 payload
        signal_run(store.as_ref(), "sc1", "hold", serde_json::json!({ "ok": 1 })).unwrap();
        let out2 = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("sc1"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out2.status, RunStatus::Errored, "坏 payload 应让 run 报错");
    }

    /// 同一机制的正向：形状对就正常恢复。
    #[tokio::test]
    async fn declared_payload_schema_accepts_good_payload() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("sch2").handler(|ctx: WorkflowCtx| async move {
            let v = ctx
                .wait_for_event_with(
                    "hold",
                    "go",
                    crate::define::WaitForEventOptions {
                        deadline: None,
                        meta: None,
                        schema: Some(crate::define::PayloadSchema::of::<Decision>()),
                    },
                )
                .await?;
            let d: Decision = serde_json::from_value(v)?;
            Ok(serde_json::json!({ "approved": d.approved }))
        });
        run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("sc2"),
        )
        .outcome().await
        .unwrap();
        signal_run(
            store.as_ref(),
            "sc2",
            "hold",
            serde_json::json!({ "approved": true }),
        )
        .unwrap();
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("sc2"),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output.unwrap()["approved"], true);
    }

    /// 旧的两参数 `wait_for_event` / `approve` 仍是无选项版本 —— 投影里
    /// `deadline`/`meta` 为 None，`awaiting` 照样有（保证规范形非空）。
    #[tokio::test]
    async fn plain_wait_for_event_has_empty_options() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("plain").handler(|ctx: WorkflowCtx| async move {
            ctx.wait_for_event("hold", "go").await?;
            Ok(serde_json::json!({}))
        });
        run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("pl1"),
        )
        .outcome().await
        .unwrap();
        let st = store.get_run_state("pl1").unwrap().unwrap();
        let w = st.waiting_for.as_ref().unwrap();
        assert_eq!(w.deadline, None, "无选项时不该有 deadline");
        assert_eq!(w.meta, None, "无选项时不该有 meta");
        assert_eq!(st.awaiting.len(), 1, "规范形照样要写");
    }
}
