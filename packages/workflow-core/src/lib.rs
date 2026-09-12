//! Async **code-as-DAG** workflow engine, modelled after TanStack Workflow's
//! core-engine layer: headless replay plus a store/event contract. No
//! scheduler, no host adapters, no managed control plane.
//!
//! There is no declared graph: the orchestration is ordinary async code
//! (branching = `if/else`, parallelism = `tokio::try_join!`, order = lexical
//! `.await`), and only durable side effects go through
//! [`define::WorkflowCtx::step`].
//!
//! Like TanStack, there is **no server daemon requirement** — a drive is one
//! `run_workflow` call that runs the run to its next pause or completion. What
//! is *not* ported is the `@tanstack/workflow-runtime` layer (leases, sweep,
//! timer indexes, schedules).
//!
//! ## 确定性契约
//!
//! The handler **must** reach the same primitives in the same order on every
//! replay — same contract as TanStack (`docs/concepts/replay-and-resume.md`).
//! `SystemTime::now()` / `Uuid::new_v4()` / bare I/O outside `ctx.step` are
//! violations; use [`define::BaseCtx::now`] / [`define::BaseCtx::uuid`] /
//! `ctx.step`. The engine neither detects nor enforces this (nor does
//! TanStack); violations surface as replay drift or checkpoint mismatches.
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
//! [`run_store::RunStore`] trait (CAS append + optional subscribe for
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
pub mod run_store;

pub use define::{
    Backoff, BaseCtx, CreateWorkflowConfig, Middleware, RetryPolicy, StepCtx, StepOptions,
    Workflow, WorkflowBuilder, WorkflowCtx, WorkflowDefinition, create_workflow,
};
pub use engine::{
    RunOptions, RunOutcome, cancel_run, run_workflow, run_workflow_sync, select_workflow_version,
    signal_event, signal_run,
};
pub use error::{RunError, RunErrorCode, StoreError, WorkflowError};
pub use event::{RunEvent, RunStatus, StepAttempt, StepState, StepStatus, fold_step_states};
pub use resource::{Gate, GateGuard, ResourceKey};
pub use run_store::{
    DeleteReason, InMemoryStore, PendingApproval, RunState, RunStore, WaitForState,
};
