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

/// 一次 run 的完整加载结果（对齐 `types.ts` 的 `LoadedExecution`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadedExecution {
    pub run: WorkflowExecution,
    pub state: RunState,
    pub events: Vec<StoredWorkflowEvent>,
}
