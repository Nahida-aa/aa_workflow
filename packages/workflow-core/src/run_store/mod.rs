//! Run 的持久化契约与内置实现。
//!
//! - 契约本身（[`RunStore`] + [`RunState`] 信封）在本文件；
//! - 各实现按后端分文件（[`InMemoryStore`] 在 `in_memory`）。

use std::sync::mpsc::Receiver;

use serde::{Deserialize, Serialize};

use crate::error::StoreError;
use crate::event::{RunEvent, RunStatus};

mod in_memory;
pub use in_memory::InMemoryStore;

/// 挂起等待中的外部信号（对齐 TanStack `RunState.waitingFor`）。sleep 的
/// `due_at` 就是这里的 `deadline` —— host 可用它做时间索引的唤醒调度。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitForState {
    pub step_id: String,
    pub signal_name: String,
    pub deadline: Option<i64>,
}

/// 挂起中的审批（对齐 TanStack `RunState.pendingApproval`）。我们的
/// `approve` 用 key 作 `approval_id`；`title` 即挂起时给的理由。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingApproval {
    pub step_id: String,
    pub approval_id: String,
    pub title: String,
    pub description: Option<String>,
}

/// Minimal, durable metadata for a run. The heavy state lives in the event
/// log; this is just the envelope the launcher needs to locate runs.
/// `waiting_for` / `pending_approval` 是挂起态的一等投影（派生自事件日志，
/// 恢复时清除）——观察者无需扫日志就能告诉 run 在等什么。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_version: Option<String>,
    pub status: RunStatus,
    pub input: serde_json::Value,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    /// 挂起等待外部 signal / sleep 到期（sleep 有 deadline）。
    #[serde(default)]
    pub waiting_for: Option<WaitForState>,
    /// 挂起等待审批。
    #[serde(default)]
    pub pending_approval: Option<PendingApproval>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// The store contract, modelled after TanStack Workflow's two-surface design:
///
/// 1. **Metadata surface** — `get/set/delete` a small [`RunState`] envelope.
/// 2. **Append-only event-log surface** — `append_event` with a CAS
///    `expected_next_index`, `get_events` for resume, and `subscribe` for live
///    fan-out.
///
/// The engine never mutates events; state is always derived by replaying the
/// log (see [`crate::event::fold_step_states`]).
pub trait RunStore: Send + Sync {
    fn get_run_state(&self, run_id: &str) -> Result<Option<RunState>, StoreError>;
    fn set_run_state(&self, run_id: &str, state: &RunState) -> Result<(), StoreError>;
    fn delete_run(&self, run_id: &str) -> Result<(), StoreError>;

    /// Appends an event at `expected_next_index`. Implementations MUST reject
    /// the write with [`StoreError::Conflict`] if the current log length
    /// differs — this is the optimistic-concurrency guard that lets a
    /// coordinator serialize concurrent writers.
    fn append_event(
        &self,
        run_id: &str,
        expected_next_index: usize,
        event: &RunEvent,
    ) -> Result<(), StoreError>;
    fn get_events(&self, run_id: &str) -> Result<Vec<RunEvent>, StoreError>;

    /// Cuts the log at `step_id`'s **latest terminal checkpoint** (inclusive):
    /// that checkpoint and every later event are dropped, the prefix is kept.
    /// Used by `continue_from` so a replayed handler re-executes the step's
    /// suffix from scratch. A step with no terminal checkpoint leaves the log
    /// untouched (there is nothing to cut — resume would re-run it anyway).
    /// Stores that cannot truncate must reject this with
    /// [`StoreError::Io`] rather than silently no-oping.
    fn truncate_runs(&self, run_id: &str, step_id: &str) -> Result<(), StoreError>;

    /// Live subscription: a receiver that sees every future event appended to
    /// this run's log. `None` if the store does not support subscriptions.
    fn subscribe(&self, run_id: &str) -> Option<Receiver<RunEvent>> {
        let _ = run_id;
        None
    }
}
