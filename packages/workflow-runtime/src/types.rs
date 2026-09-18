//! runtime 层的数据类型。
//!
//! 对齐 TanStack `aa-workflow-runtime/src/types.ts`。命名与字段保持逐字对应
//! （`camelCase` → `snake_case`），JSON 形状见各类型的 `serde` 属性。
//!
//! 只放**数据结构**；契约（trait）在各自模块里。

use serde::{Deserialize, Serialize};

use aa_workflow_core::{RunState, RunStatus, WorkflowEvent};

/// 类型别名，对齐上游（那边全是 `string` 的具名别名，用于文档与自解释）。
pub type WorkflowId = String;
pub type WorkflowVersion = String;
pub type RunId = String;
pub type ScheduleId = String;
pub type ScheduleBucketId = String;

/// lease 持有者标识（对齐 `types.ts:19`）。
///
/// serverless 下每次调用都是新进程，用这个标识「谁在驱这个 run」——
/// 例如 `http:start` / `cron:sweep` / `worker:3`。
pub type LeaseOwner = String;

/// runtime 的 run 状态 = core 的 [`RunStatus`] **加一个 `queued`**
/// （对齐 `types.ts:26`：`WorkflowExecutionStatus = RunStatus | 'queued'`）。
///
/// `queued` 是 runtime 层特有的：「已登记、等待被认领执行」。core 不知道它——
/// core 只在被 drive 时看 `Running`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkflowExecutionStatus {
    Queued,
    Running,
    Paused,
    Finished,
    Errored,
    Aborted,
}

impl From<RunStatus> for WorkflowExecutionStatus {
    fn from(s: RunStatus) -> Self {
        match s {
            RunStatus::Running => Self::Running,
            RunStatus::Paused => Self::Paused,
            RunStatus::Finished => Self::Finished,
            RunStatus::Errored => Self::Errored,
            RunStatus::Aborted => Self::Aborted,
        }
    }
}

impl WorkflowExecutionStatus {
    /// 终态（对齐上游 `isTerminal`：finished / errored / aborted）。
    /// 终态 run 不可再被 claim。
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            WorkflowExecutionStatus::Finished
                | WorkflowExecutionStatus::Errored
                | WorkflowExecutionStatus::Aborted
        )
    }
}

// ============================================================
// 事件存储（基础层的读路径形状）
// ============================================================

/// 带存储元数据的事件（对齐 `types.ts:50`）。
///
/// 与裸 [`WorkflowEvent`] 的差别就是外面的信封：`event_index` 用于游标分页与
/// 按位查询，`created_at` 用于 timeline 排序，`event_type` / `step_id` 冗余出来
/// 供 DB 建索引（不用解析 JSON 体）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredWorkflowEvent {
    pub run_id: RunId,
    pub event_index: u64,
    /// 冗余自 `event`，对齐上游 `WorkflowEvent['type']`。
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    pub event: WorkflowEvent,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveRunStateArgs {
    pub state: RunState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadEventsArgs {
    pub run_id: RunId,
    /// 从该索引（含）开始读；`None` = 从头。对齐 `types.ts:76`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_index: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppendEventsArgs {
    pub run_id: RunId,
    /// CAS 护栏：日志当前长度必须等于它，否则报冲突。
    pub expected_next_index: u64,
    pub events: Vec<WorkflowEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppendEventsResult {
    /// append 之后的下一索引，供调用方续写。
    pub next_index: u64,
}

// ============================================================
// 执行记录
// ============================================================

/// 一个 run 的执行记录（对齐 `types.ts:33` 的 `WorkflowExecution`）。
///
/// 比 core 的 [`RunState`] 宽的地方：`status` 多 [`Queued`](WorkflowExecutionStatus::Queued)
/// 态、`lease` / `wake_at` 是 runtime 关注的信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowExecution {
    pub run_id: RunId,
    pub workflow_id: WorkflowId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_version: Option<WorkflowVersion>,
    pub status: WorkflowExecutionStatus,
    pub input: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<aa_workflow_core::RunError>,
    /// 挂起等待外部 signal 的投影。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub waiting_for: Option<aa_workflow_core::WaitForState>,
    /// 挂起等待审批的投影。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_approval: Option<aa_workflow_core::PendingApproval>,
    /// 下次该被唤醒的时间（timer 的投影），供 sweep 建时间索引。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wake_at: Option<i64>,
    /// 当前 lease；无 lease 为 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease: Option<WorkflowLease>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// 一次执行认领（对齐 `types.ts:28` 的 `WorkflowLease`）。
///
/// `heartbeat` 会续 `expires_at`——lease 不是一次 claim 就完事（上游
/// `runtime-model.md:156`：runtime renews every third of `leaseMs`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowLease {
    pub owner: LeaseOwner,
    pub expires_at: i64,
}

/// run 摘要（对齐 `types.ts:268` 的 `RunSummary`），供 `list_runs` 返回。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSummary {
    pub run_id: RunId,
    pub workflow_id: WorkflowId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_version: Option<WorkflowVersion>,
    pub status: WorkflowExecutionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub waiting_for: Option<aa_workflow_core::WaitForState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_approval: Option<aa_workflow_core::PendingApproval>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wake_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// 一个 run 的时间线：执行记录 + 全部事件（对齐 `types.ts:282`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunTimeline {
    pub run: WorkflowExecution,
    pub events: Vec<StoredWorkflowEvent>,
}

/// 一次 run 的完整加载结果（对齐 `types.ts:59` 的 `LoadedExecution`）。
///
/// 只有 `run` + `events` 两个字段——state 可从 `events` 重放得出
/// （core 的 `fold_step_states`），存两份会有漂移风险。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadedExecution {
    pub run: WorkflowExecution,
    pub events: Vec<StoredWorkflowEvent>,
}

// ============================================================
// 三组扩展方法的参数与结果类型
// ============================================================

// ── run 生命周期 ────────────────────────────────────────────

/// 对齐 `types.ts:64`。`input` 在 TS 是 `unknown`，我们是 `Value`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateRunArgs {
    pub run_id: RunId,
    pub workflow_id: WorkflowId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_version: Option<WorkflowVersion>,
    pub input: serde_json::Value,
    pub now: i64,
}

/// 幂等创建的两种结果（对齐 `types.ts:72`）。`Existing` 表示该 run_id 已存在，
/// 未新建——调用方据此避免重复启动。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum CreateRunResult {
    Created { run: WorkflowExecution },
    Existing { run: WorkflowExecution },
}

/// 对齐 `types.ts:115`。
///
/// 上游还带 `awaiting?`（`RunState['awaiting']`）。**我们没有**——`awaiting`
/// 是上游为未来 fan-out 预留的数组投影，本仓刻意不做。这里只带我们真有的
/// 两个挂起投影。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkRunPausedArgs {
    pub run_id: RunId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub waiting_for: Option<aa_workflow_core::WaitForState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_approval: Option<aa_workflow_core::PendingApproval>,
    /// 下次该被唤醒的时间（timer 投影），供 sweep 建索引。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wake_at: Option<i64>,
    pub now: i64,
}

/// 对齐 `types.ts:124`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkRunFinishedArgs {
    pub run_id: RunId,
    pub output: serde_json::Value,
    pub now: i64,
}

/// 对齐 `types.ts:130`。`error` / `code` 对应我们的
/// [`RunError`](aa_workflow_core::RunError) / [`RunErrorCode`](aa_workflow_core::RunErrorCode)。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkRunErroredArgs {
    pub run_id: RunId,
    pub error: aa_workflow_core::RunError,
    pub code: aa_workflow_core::RunErrorCode,
    pub now: i64,
}

// ── lease ───────────────────────────────────────────────────

/// 对齐 `types.ts:91`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimRunArgs {
    pub run_id: RunId,
    pub lease_owner: LeaseOwner,
    /// lease 时长（ms）。runtime 会按它的 1/3 续租。
    pub lease_ms: i64,
    pub now: i64,
}

/// 认领的三态结果（对齐 `types.ts:98`）。
///
/// `NotClaimable` 与 `NotFound` 分开是必要的：前者说明 run 存在但暂时不该被
/// 驱（已被别的 worker 持有、或状态不允许），后者说明根本不存在。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ClaimRunResult {
    Claimed { run: WorkflowExecution },
    NotFound,
    NotClaimable { run: WorkflowExecution },
}

/// 对齐 `types.ts:103`。字段与 [`ClaimRunArgs`] 相同。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatRunLeaseArgs {
    pub run_id: RunId,
    pub lease_owner: LeaseOwner,
    pub lease_ms: i64,
    pub now: i64,
}

/// 对齐 `types.ts:110`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseRunLeaseArgs {
    pub run_id: RunId,
    pub lease_owner: LeaseOwner,
}

/// 对齐 `types.ts:249`。扫陈旧 run（持有者崩溃、lease 过期）并认领。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimStaleRunsArgs {
    pub now: i64,
    pub limit: usize,
    pub lease_owner: LeaseOwner,
    pub lease_ms: i64,
}

/// 一次 stale 认领的结果（对齐 `types.ts:256`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunClaim {
    pub run: WorkflowExecution,
    pub lease: WorkflowLease,
}

// ── timer ───────────────────────────────────────────────────

/// 登记一个 timer（对齐 `types.ts:137`）。
///
/// `signal_id` 是幂等键：同一 `signal_id` 重复登记应无副作用。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleTimerArgs {
    pub run_id: RunId,
    pub workflow_id: WorkflowId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_version: Option<WorkflowVersion>,
    pub wake_at: i64,
    pub signal_id: String,
    pub now: i64,
}

/// 对齐 `types.ts:146`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimDueTimersArgs {
    pub now: i64,
    pub limit: usize,
    pub lease_owner: LeaseOwner,
    pub lease_ms: i64,
}

/// 一个到期的 timer（对齐 `types.ts:153`）。字段与 [`ScheduleTimerArgs`] 相同
/// （去掉 `now`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimerWakeup {
    pub run_id: RunId,
    pub workflow_id: WorkflowId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_version: Option<WorkflowVersion>,
    pub wake_at: i64,
    pub signal_id: String,
}

// ── 投递（signal / approval）───────────────────────────────

/// 外部事件的投递（对齐 core `types.ts:492` 的 `SignalDelivery`）。
///
/// `signal_id` 是幂等令牌：同一 `signalId` 重复投递 = no-op 重试；不同
/// `signalId` = 竞态丢失。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalDelivery {
    pub signal_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    /// 等待中的信号名（与 `ctx.wait_for_event(name)` 传入的一致）。
    pub name: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliverSignalArgs {
    pub run_id: RunId,
    pub delivery: SignalDelivery,
    pub now: i64,
}

/// 审批决定（对齐 core `types.ts:312` 的 `ApprovalResult`）。
///
/// 上游定义在 core；我们的 core 没有审批投递入口，故放在 runtime。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalResult {
    pub approved: bool,
    pub approval_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feedback: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliverApprovalArgs {
    pub run_id: RunId,
    pub approval: ApprovalResult,
    pub now: i64,
}

/// 投递的四态结果（对齐上游 `DeliverSignalResult` / `DeliverApprovalResult`）。
///
/// `Duplicate` = 同一 `signalId` 的重复投递（幂等 no-op）；
/// `NotWaiting` = run 存在但没在等这个信号 / 审批。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DeliverSignalResult {
    NotFound,
    Duplicate { run: WorkflowExecution },
    NotWaiting { run: WorkflowExecution },
    Delivered { run: WorkflowExecution },
}

/// 见 [`DeliverSignalResult`]——两者四态相同。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DeliverApprovalResult {
    NotFound,
    Duplicate { run: WorkflowExecution },
    NotWaiting { run: WorkflowExecution },
    Delivered { run: WorkflowExecution },
}

// ── 查询 ────────────────────────────────────────────────────

/// 对齐 `types.ts:261`。`cursor` 是不透明分页游标（in-memory 实现用 offset）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListRunsArgs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<WorkflowExecutionStatus>,
    pub limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

// ── schedule ────────────────────────────────────────────────

/// 重叠策略（对齐 `types.ts:185`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowOverlapPolicy {
    Skip,
    Allow,
    BufferOne,
    CancelPrevious,
    TerminatePrevious,
}

/// schedule 规格（对齐 `types.ts:192`）：cron 表达式或固定间隔。
///
/// `rename_all = "kebab-case"` 管**变体名**（`kind: "interval"`）；
/// `rename_all_fields = "camelCase"` 管**变体字段**（`everyMs`，对齐上游的
/// camelCase 字段名）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum WorkflowScheduleSpec {
    Cron {
        expression: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        timezone: Option<String>,
    },
    Interval {
        every_ms: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        timezone: Option<String>,
    },
}

/// 注册期声明的 schedule（对齐 `types.ts:204` 的 `WorkflowScheduleDefinition`）。
///
/// 挂在 [`WorkflowRegistration`](crate::WorkflowRegistration) 上；由
/// [`materialize_workflow_schedules`](crate::schedule_materializer::materialize_workflow_schedules)
/// 换算成 [`UpsertScheduleArgs`] 后登记进 store。
///
/// 与上游的差异：`input` 在 TS 可以是**函数**（延迟求值，每轮物化时调用一次），
/// Rust 是纯值——需要动态 input 的宿主可在调用物化前自行构造定义。
#[derive(Debug, Clone)]
pub struct WorkflowScheduleDefinition {
    /// 缺省时由物化器按 `{workflowId}:{index}` 推导（对齐上游 `getScheduleId`）。
    pub id: Option<ScheduleId>,
    pub schedule: WorkflowScheduleSpec,
    /// 缺省 `Skip`（对齐上游 `overlapPolicy ?? 'skip'`）。
    pub overlap_policy: Option<WorkflowOverlapPolicy>,
    pub input: Option<serde_json::Value>,
    /// `Some(false)` = 登记为停用（对齐上游 `enabled === false` 的分支）。
    pub enabled: Option<bool>,
}

/// 对齐 `types.ts:212`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertScheduleArgs {
    pub schedule_id: ScheduleId,
    pub workflow_id: WorkflowId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_version: Option<WorkflowVersion>,
    pub schedule: WorkflowScheduleSpec,
    pub overlap_policy: WorkflowOverlapPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    /// 下次触发时间——由 host 计算（core 不管 cron 表达式解析）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_fire_at: Option<i64>,
    pub enabled: bool,
    pub now: i64,
}

/// 一个到期的 schedule 桶（对齐 `types.ts:231`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleBucket {
    pub schedule_id: ScheduleId,
    pub bucket_id: ScheduleBucketId,
    pub workflow_id: WorkflowId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_version: Option<WorkflowVersion>,
    /// 桶推导出的 runId：`{workflowId}:{scheduleId}:{bucketId}`。
    pub run_id: RunId,
    pub fire_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    pub overlap_policy: WorkflowOverlapPolicy,
}

/// 对齐 `types.ts:224`。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimDueScheduleBucketsArgs {
    pub now: i64,
    pub limit: usize,
    pub lease_owner: LeaseOwner,
    pub lease_ms: i64,
}

/// 对齐 `types.ts:241` 附近（`MarkScheduleBucketStartedArgs`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkScheduleBucketStartedArgs {
    pub schedule_id: ScheduleId,
    pub bucket_id: ScheduleBucketId,
    pub run_id: RunId,
}
