use serde::{Deserialize, Serialize};

use std::collections::HashMap;

use crate::error::{RunError, RunErrorCode};

/// Overall lifecycle status of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Running,
    Finished,
    Errored,
    Aborted,
    /// The run is parked at a durable wait point (`SignalAwaited`), e.g. an
    /// approval or a sleep; it resumes when a `SignalResolved` arrives.
    Paused,
}

/// Per-step lifecycle status, derived from the event log via
/// [`fold_step_states`]. This is a *projection* of the log, never stored
/// independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StepStatus {
    #[default]
    Pending,
    Running,
    Success,
    Failed,
    /// Derived from a `SignalAwaited` checkpoint: the step is parked at a durable
    /// wait (approval / sleep) until a matching `SignalResolved` arrives.
    Paused,
}

/// A single execution attempt of one step (used for retry bookkeeping).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepAttempt {
    /// 1-based attempt number.
    pub attempt: usize,
    pub started_at: i64,
    pub finished_at: i64,
    pub result: Option<serde_json::Value>,
    /// 结构化错误（对齐 TanStack `StepAttempt.error?: SerializedError`，
    /// `types.ts:261`）。
    pub error: Option<RunError>,
}

/// `STEP_FINISHED` 的可选元数据槽（对齐上游事件上的 `meta`）。目前只承载
/// 一种事实：checkpoint 从源 run 导入（`reexecute_from` 的 INVALIDATE 侧）。
/// 引擎自己产生的 checkpoint 恒为 `None`——序列化时整个省略，旧日志反序
/// 列化为 `None`，日志形状前后兼容。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StepMeta {
    /// 导入溯源（上游 `meta.importedFrom`）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported_from: Option<ImportedFrom>,
}

/// 一次 checkpoint 导入的出处：源 run 与该事件在**源 run 持久化日志**里的
/// 下标（`RUN_STARTED` 不落盘、不占下标——与上游同约定）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImportedFrom {
    pub run_id: String,
    pub index: usize,
}

/// Append-only persisted events. Each event is immutable; the log is the
/// engine's source of truth and doubling as the resume checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkflowEvent {
    /// Observability only (not persisted): marks the start of a run.
    RunStarted {
        ts: i64,
        run_id: String,
        /// 客户端关联用的线程标识（上游 `threadId`，`types.ts:76`）。**只挂在
        /// `RUN_STARTED` 上**，别的事件不带——它不参与 resume，也不落进
        /// `RunState`，纯粹给订阅方把一次 drive 归到某个会话/请求上。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread_id: Option<String>,
    },
    /// Checkpoint (persisted): final state of a completed run.
    RunFinished {
        ts: i64,
        run_id: String,
        output: Option<serde_json::Value>,
    },
    /// Checkpoint (persisted): the run errored (a step failed terminally).
    RunErrored {
        ts: i64,
        run_id: String,
        error: RunError,
        /// 机器可读的终局原因（TanStack `RUN_ERRORED.code`）。用它分支，别去
        /// 匹配 `error.message`。
        code: RunErrorCode,
    },
    /// Observability only (not persisted): a step began executing.
    StepStarted {
        ts: i64,
        run_id: String,
        step_id: String,
    },
    /// Checkpoint (persisted): a step finished successfully; the run may be
    /// resumed from here without re-execution.
    StepFinished {
        ts: i64,
        run_id: String,
        step_id: String,
        result: Option<serde_json::Value>,
        attempts: Vec<StepAttempt>,
        /// 元数据槽（上游 `meta`）；`reexecute_from` 导入的 checkpoint 在
        /// 这里带 [`ImportedFrom`] 溯源，引擎自己产生的恒为 `None`。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        meta: Option<StepMeta>,
    },
    /// Checkpoint (persisted): a step failed terminally.
    StepFailed {
        ts: i64,
        run_id: String,
        step_id: String,
        error: RunError,
        attempts: Vec<StepAttempt>,
    },
    /// Checkpoint (persisted): the run parked on a **named signal** wait
    /// (`ctx.wait_for_event` / `ctx.sleep*`). Aligns with upstream
    /// `SIGNAL_AWAITED` (`types.ts`, 字段逐字对应)。
    ///
    /// `name` is the channel the run is parked on — `"__timer"` for sleeps,
    /// a user event name for named waits. `deadline` is `Some` (wall-clock
    /// ms) for sleep-like waits so a timer host could auto-deliver.
    SignalAwaited {
        ts: i64,
        run_id: String,
        step_id: String,
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deadline: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        meta: Option<serde_json::Value>,
    },
    /// Checkpoint (persisted): the run parked on an **approval** wait
    /// (`ctx.approve`). Aligns with upstream `APPROVAL_REQUESTED`.
    ///
    /// `approval_id` is the deterministic pause key (== the approval id the
    /// host delivers a decision for); `title` is what approvers see.
    ApprovalRequested {
        ts: i64,
        run_id: String,
        step_id: String,
        approval_id: String,
        title: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        meta: Option<serde_json::Value>,
    },
    /// Checkpoint (persisted): a signal resolved the matching
    /// [`SignalAwaited`](WorkflowEvent::SignalAwaited). Aligns with upstream
    /// `SIGNAL_RESOLVED`.
    ///
    /// `signal_id` is the host-supplied idempotency token — upstream's rule:
    /// same `signal_id` at the same `step_id` is a no-op (idempotent retry),
    /// a different one is a lost race. `payload` is what the
    /// `wait_for_event` call returns.
    SignalResolved {
        ts: i64,
        run_id: String,
        step_id: String,
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signal_id: Option<String>,
        payload: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        meta: Option<serde_json::Value>,
    },
    /// Checkpoint (persisted): an approval decision resolved the matching
    /// [`ApprovalRequested`](WorkflowEvent::ApprovalRequested). Aligns with
    /// upstream `APPROVAL_RESOLVED`.
    ///
    /// 上游这里**没有** payload 字段——审批是结构化决定（`approved` +
    /// `feedback`），任意 payload 只属于 [`SignalResolved`]。见
    /// `docs/tanstack-alignment.md` 的事件分类对照。
    ApprovalResolved {
        ts: i64,
        run_id: String,
        step_id: String,
        approval_id: String,
        approved: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        feedback: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        meta: Option<serde_json::Value>,
    },
    /// Observability only (not persisted): 0.0..=1.0 progress signal.
    StepProgress {
        ts: i64,
        run_id: String,
        step_id: String,
        value: f64,
    },
    /// Observability only (not persisted): `ctx.emit` fan-out. Reaches only
    /// the publisher, never the log — so it never becomes part of replay
    /// (mirrors TanStack's `CUSTOM` event).
    Custom {
        ts: i64,
        run_id: String,
        name: String,
        value: serde_json::Value,
    },
    /// Checkpoint (persisted): `ctx.now()` recorded a wall-clock timestamp.
    /// On replay the cached value is served so a run sees the same clock
    /// across resumes (TanStack `NOW_RECORDED`).
    NowRecorded {
        ts: i64,
        run_id: String,
        step_id: String,
        value: i64,
    },
    /// Checkpoint (persisted): `ctx.uuid()` recorded a generated id. On
    /// replay the same id is served (TanStack `UUID_RECORDED`).
    UuidRecorded {
        ts: i64,
        run_id: String,
        step_id: String,
        value: String,
    },
    /// Observability only (not persisted): RFC 6902 JSON Patch describing
    /// `ctx.state` changes since the last durable boundary (TanStack
    /// `STATE_DELTA`).
    ///
    /// **emit-only** 的理由（上游注释原话）：state 由日志重放推导，持久化
    /// delta 会在每次 invocation 重放时重复 append，或者需要重放时跳过。
    /// delta 语义见 [`crate::engine::state_diff`]。
    StateDelta {
        ts: i64,
        run_id: String,
        delta: Vec<crate::engine::state_diff::Operation>,
    },
    /// Checkpoint (persisted): 未捕获失败触发了补偿——原始失败随 `error`
    /// 持久化，已登记的撤销 handler 将逆序执行。崩溃后恢复的驱动据此得知
    /// "补偿进行中"。
    CompensationStarted {
        ts: i64,
        run_id: String,
        error: RunError,
    },
    /// Checkpoint (persisted): 一个已登记的撤销 handler 成功完成——重启后
    /// 该补偿不再重复执行（幂等键 = 补偿 id）。
    CompensationCompleted {
        ts: i64,
        run_id: String,
        compensation_id: String,
    },
    /// Checkpoint (persisted): 一个撤销 handler 耗尽重试——补偿链就地停止
    /// （后续登记的补偿保持未执行，留给人工处置）。
    CompensationFailed {
        ts: i64,
        run_id: String,
        compensation_id: String,
        error: RunError,
    },
    /// Checkpoint (persisted)：耐久重试（#19 `retry.durable`）——步骤失败后
    /// 登记下次重试的绝对截止时间，run 挂起（`__timer` + deadline），到期的
    /// sweep/重驱以此恢复。重放规则：本事件后该步骤视同未完成（attempts 随
    /// 事件携带，耗尽前的 STEP_FAILED 不落日志）。
    StepRetryScheduled {
        ts: i64,
        run_id: String,
        step_id: String,
        /// 本次失败是第几次 attempt（重放时据此续算）。
        attempt: usize,
        /// 下次重试的绝对截止时间（UTC ms）。
        next_attempt_at: i64,
        error: RunError,
        attempts: Vec<StepAttempt>,
    },
}

impl WorkflowEvent {
    pub fn step_id(&self) -> Option<&str> {
        match self {
            WorkflowEvent::RunStarted { .. }
            | WorkflowEvent::RunFinished { .. }
            | WorkflowEvent::RunErrored { .. }
            | WorkflowEvent::Custom { .. }
            | WorkflowEvent::StateDelta { .. }
            | WorkflowEvent::CompensationStarted { .. }
            | WorkflowEvent::CompensationCompleted { .. }
            | WorkflowEvent::CompensationFailed { .. } => None,
            WorkflowEvent::StepRetryScheduled { step_id, .. }
            | WorkflowEvent::StepStarted { step_id, .. }
            | WorkflowEvent::StepFinished { step_id, .. }
            | WorkflowEvent::StepFailed { step_id, .. }
            | WorkflowEvent::SignalAwaited { step_id, .. }
            | WorkflowEvent::ApprovalRequested { step_id, .. }
            | WorkflowEvent::SignalResolved { step_id, .. }
            | WorkflowEvent::ApprovalResolved { step_id, .. }
            | WorkflowEvent::StepProgress { step_id, .. }
            | WorkflowEvent::NowRecorded { step_id, .. }
            | WorkflowEvent::UuidRecorded { step_id, .. } => Some(step_id),
        }
    }

    /// 事件发生时间（每个变体都带 `ts`，对齐 TS 侧 `event.ts`）。
    pub fn ts(&self) -> i64 {
        match self {
            WorkflowEvent::RunStarted { ts, .. }
            | WorkflowEvent::RunFinished { ts, .. }
            | WorkflowEvent::RunErrored { ts, .. }
            | WorkflowEvent::StepStarted { ts, .. }
            | WorkflowEvent::StepFinished { ts, .. }
            | WorkflowEvent::StepFailed { ts, .. }
            | WorkflowEvent::SignalAwaited { ts, .. }
            | WorkflowEvent::ApprovalRequested { ts, .. }
            | WorkflowEvent::SignalResolved { ts, .. }
            | WorkflowEvent::ApprovalResolved { ts, .. }
            | WorkflowEvent::StepProgress { ts, .. }
            | WorkflowEvent::Custom { ts, .. }
            | WorkflowEvent::NowRecorded { ts, .. }
            | WorkflowEvent::UuidRecorded { ts, .. }
            | WorkflowEvent::StateDelta { ts, .. }
            | WorkflowEvent::CompensationStarted { ts, .. }
            | WorkflowEvent::CompensationCompleted { ts, .. }
            | WorkflowEvent::CompensationFailed { ts, .. }
            | WorkflowEvent::StepRetryScheduled { ts, .. } => *ts,
        }
    }

    /// 对齐 TS 侧 `event.type`）。
    pub fn type_name(&self) -> &'static str {
        let name = match self {
            WorkflowEvent::RunStarted { .. } => "RUN_STARTED",
            WorkflowEvent::RunFinished { .. } => "RUN_FINISHED",
            WorkflowEvent::RunErrored { .. } => "RUN_ERRORED",
            WorkflowEvent::StepStarted { .. } => "STEP_STARTED",
            WorkflowEvent::StepFinished { .. } => "STEP_FINISHED",
            WorkflowEvent::StepFailed { .. } => "STEP_FAILED",
            WorkflowEvent::SignalAwaited { .. } => "SIGNAL_AWAITED",
            WorkflowEvent::ApprovalRequested { .. } => "APPROVAL_REQUESTED",
            WorkflowEvent::SignalResolved { .. } => "SIGNAL_RESOLVED",
            WorkflowEvent::ApprovalResolved { .. } => "APPROVAL_RESOLVED",
            WorkflowEvent::StepProgress { .. } => "STEP_PROGRESS",
            WorkflowEvent::Custom { .. } => "CUSTOM",
            WorkflowEvent::NowRecorded { .. } => "NOW_RECORDED",
            WorkflowEvent::UuidRecorded { .. } => "UUID_RECORDED",
            WorkflowEvent::StateDelta { .. } => "STATE_DELTA",
            WorkflowEvent::CompensationStarted { .. } => "COMPENSATION_STARTED",
            WorkflowEvent::CompensationCompleted { .. } => "COMPENSATION_COMPLETED",
            WorkflowEvent::CompensationFailed { .. } => "COMPENSATION_FAILED",
            WorkflowEvent::StepRetryScheduled { .. } => "STEP_RETRY_SCHEDULED",
        };
        debug_assert_eq!(
            serde_json::to_value(self).unwrap()["type"],
            serde_json::Value::String(name.to_string()),
            "type_name 必须与 serde tag 保持一致"
        );
        name
    }
}

/// Derived per-step state reconstructed from an event log.
#[derive(Debug, Clone, Default)]
pub struct StepState {
    pub status: StepStatus,
    pub result: Option<serde_json::Value>,
    /// 与 [`StepAttempt::error`] 同构（都是 `RunError`）。
    pub error: Option<RunError>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
}

/// Replays an event log and folds it into per-step states.
///
/// This is the resume mechanism: the log's terminal checkpoints
/// (`StepFinished`/`StepFailed`) fully determine which steps still need to
/// run. No application code is re-executed here.
pub fn fold_step_states(events: &[WorkflowEvent]) -> HashMap<String, StepState> {
    let mut states: HashMap<String, StepState> = HashMap::new();
    for ev in events {
        match ev {
            WorkflowEvent::StepStarted { step_id, ts, .. } => {
                let st = states.entry(step_id.clone()).or_default();
                st.status = StepStatus::Running;
                st.started_at = Some(*ts);
            }
            WorkflowEvent::StepFinished {
                step_id,
                result,
                ts,
                ..
            } => {
                let st = states.entry(step_id.clone()).or_default();
                st.status = StepStatus::Success;
                st.result = result.clone();
                st.error = None;
                st.finished_at = Some(*ts);
            }
            WorkflowEvent::StepFailed {
                step_id, error, ts, ..
            } => {
                let st = states.entry(step_id.clone()).or_default();
                st.status = StepStatus::Failed;
                st.error = Some(error.clone());
                st.finished_at = Some(*ts);
            }
            WorkflowEvent::SignalAwaited {
                step_id, ts, ..
            } => {
                let st = states.entry(step_id.clone()).or_default();
                // 泊车不降级已完成/已失败的步骤：interim-goal 的泊车
                // checkpoint 落在 StepFinished **之后**（跑完才停），若允许
                // 覆盖，重驱时该步骤会被当"未执行"再次真跑。
                if st.status != StepStatus::Success && st.status != StepStatus::Failed {
                    st.status = StepStatus::Paused;
                }
                st.started_at = st.started_at.or(Some(*ts));
                if let WorkflowEvent::SignalAwaited { deadline: Some(d), .. } = ev {
                    st.finished_at = Some(*d);
                }
            }
            WorkflowEvent::ApprovalRequested { step_id, ts, .. } => {
                let st = states.entry(step_id.clone()).or_default();
                if st.status != StepStatus::Success && st.status != StepStatus::Failed {
                    st.status = StepStatus::Paused;
                }
                st.started_at = st.started_at.or(Some(*ts));
            }
            WorkflowEvent::SignalResolved {
                step_id, payload, ..
            } => {
                // A delivered signal exposes its payload as the paused step's
                // derived result; the step stays `Paused` in the projection.
                let st = states.entry(step_id.clone()).or_default();
                st.result = Some(payload.clone());
            }
            WorkflowEvent::ApprovalResolved {
                step_id,
                approved,
                feedback,
                ..
            } => {
                // 审批决定是结构化的（对齐上游 `APPROVAL_RESOLVED`：无 payload）。
                // 投影成 `{approved, feedback}` 供观察者与 schema 校验使用。
                let st = states.entry(step_id.clone()).or_default();
                st.result = Some(serde_json::json!({
                    "approved": approved,
                    "feedback": feedback,
                }));
            }
            _ => {}
        }
    }
    states
}
