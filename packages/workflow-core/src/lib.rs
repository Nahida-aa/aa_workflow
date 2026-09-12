//! Async **code-as-DAG** workflow engine, modelled after TanStack Workflow's
//! store/event contract but with no determinism requirement and no server
//! daemon. There is no declared graph: the orchestration is ordinary async
//! code (branching = `if/else`, parallelism = `tokio::try_join!`, order =
//! lexical `.await`), and only durable side effects go through
//! [`define::WorkflowCtx::step`].
//!
//! ## TanStack Workflow 对照
//!
//! 采纳其命名与 store/event 契约（`tag="type"`、CAS append、step checkpoint），
//! 但 authoring 面对齐它的 handler 形态（`Promise.all` ↔ `tokio::try_join!`，
//! `awaiting[]` ↔ 事件推导的 step 状态），不做显式 DAG。重放由
//! [`engine::run_workflow`] 驱动 handler 完成：`ctx.step` 在 recheck 存在时
//! 短路到缓存结果、失败 checkpoint 直接 rethrow（失败即终局，重试靠
//! `continue_from` / 新 run）。完整论证见 `docs/tanstack-alignment.md`。
//!
//! ## Persistence model
//!
//! The engine's source of truth is an **append-only event log** behind the
//! [`store::RunStore`] trait (CAS append + optional subscribe for
//! fan-out). Per-step status is *derived* by replaying the log
//! ([`event::fold_step_states`]); resume short-circuits succeeded steps so
//! code is re-executed only for what still needs to run. `continue_from`
//! truncates the log at a step's latest terminal checkpoint (store layer)
//! before re-driving the handler.

pub mod define;
pub mod engine;
pub mod error;
pub mod event;
pub mod resource;
pub mod store;

pub use define::{
    Backoff, RetryPolicy, StepCtx, StepOptions, TypedCtx, TypedWorkflow, Workflow, WorkflowCtx,
};
pub use engine::{
    cancel_run, run_workflow, run_workflow_sync, signal_event, signal_run, RunOptions, RunOutcome,
};
pub use error::{StoreError, WorkflowError};
pub use event::{fold_step_states, RunEvent, RunStatus, StepAttempt, StepState, StepStatus};
pub use resource::{Gate, GateGuard, ResourceKey};
pub use store::{InMemoryStore, PendingApproval, RunState, RunStore, WaitForState};