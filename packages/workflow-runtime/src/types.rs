//! runtime 层的数据类型。
//!
//! 对齐 TanStack `workflow-runtime/src/types.ts`。命名与字段保持逐字对应
//! （`camelCase` → `snake_case`），JSON 形状见各类型的 `serde` 属性。
//!
//! 只放**数据结构**；契约（trait）在各自模块里。

use serde::{Deserialize, Serialize};

use workflow_core::{RunState, RunStatus, WorkflowEvent};

/// 类型别名，对齐上游（那边全是 `string` 的具名别名，用于文档与自解释）。
pub type WorkflowId = String;
pub type WorkflowVersion = String;
pub type RunId = String;

/// lease 持有者标识（对齐 `types.ts:19`）。
///
/// serverless 下每次调用都是新进程，用这个标识「谁在驱这个 run」——
/// 例如 `http:start` / `cron:sweep` / `worker:3`。
pub type LeaseOwner = String;

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
// 执行记录（扩展层的查询形状）
// ============================================================

/// 一个 run 的执行记录（对齐 `types.ts` 的 `WorkflowExecution`）。
///
/// 比 core 的 [`RunState`] 多出 `lease`、`wake_at` 这些 runtime 关注的信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowExecution {
    pub run_id: RunId,
    pub workflow_id: WorkflowId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_version: Option<WorkflowVersion>,
    pub status: RunStatus,
    /// 当前 lease；无 lease 为 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease: Option<WorkflowLease>,
    /// 下次该被唤醒的时间（timer 的投影），供 sweep 建时间索引。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wake_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// 一次执行认领（对齐上游 `WorkflowLease`）。
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
    pub status: RunStatus,
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
/// 只有 `run` + `events` 两个字段——我最初多加了 `state: RunState`，但上游没有，
/// 且 `state` 可以从 `events` 重放得出（core 的 `fold_step_states` 就是干这个
/// 的），存两份会有漂移风险。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadedExecution {
    pub run: WorkflowExecution,
    pub events: Vec<StoredWorkflowEvent>,
}

// ============================================================
// 三组扩展方法的参数与结果类型
//
// 对齐上游 types.ts。这里先立 run 生命周期 / lease / timer 三组——它们是
// 「无进程常驻」形态的刚需（判据见 docs/runtime-design.md 的 D5）。
// `schedule*` 三件套与 `list_runs` / `get_run_timeline` 暂不立：前者只在需要
// cron 时才要，后者依赖尚未定义的查询语义。
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
/// 是上游为未来 fan-out 预留的数组投影，本仓刻意不做（见 README「已知事项」
/// 与 D5）。这里只带我们真有的三个挂起投影。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkRunPausedArgs {
    pub run_id: RunId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub waiting_for: Option<workflow_core::WaitForState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_approval: Option<workflow_core::PendingApproval>,
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
/// [`RunError`](workflow_core::RunError) / [`RunErrorCode`](workflow_core::RunErrorCode)。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkRunErroredArgs {
    pub run_id: RunId,
    pub error: workflow_core::RunError,
    pub code: workflow_core::RunErrorCode,
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
/// 驱（例如已被别的 worker 持有，或状态不允许），后者说明根本不存在。
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
