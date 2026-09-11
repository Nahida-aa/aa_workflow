//! File-artifact DAG workflow engine, modelled after TanStack Workflow's
//! contract but with no determinism requirement and no server daemon: step
//! closures are plain sync functions writing artifacts to disk, and the
//! schedule is simply the explicit `needs` graph.
//!
//! ## TanStack Workflow 对照
//!
//! 我们采纳其命名与 store/event 契约（含 `STEP_*` 事件、`tag="type"`、CAS append），
//! 但保留显式 DAG 而非 handler 代码重放，并提供 TanStack 官方没有的
//! `continue_from` / `target_step` / `up_to_date` / resource 门。完整论证（含
//! "用户层面能否 hack 出 continue_from" 和领域取舍）见
//! `docs/tanstack-alignment.md`。
//!
//! ## Persistence model
//!
//! The engine's source of truth is an **append-only event log** behind the
//! [`store::RunStore`] trait (with CAS append + optional subscribe for
//! fan-out). Per-step status is *derived* by replaying the log
//! ([`event::fold_step_states`]); resume therefore never re-executes code,
//! because the graph is static. Context files (e.g. LocalDub's `ctx.json`)
//! are a deterministic projection of the log, not a second authoritative
//! book.

pub mod define;
pub mod error;
pub mod event;
pub mod resource;
pub mod scheduler;
pub mod store;

pub use define::{Backoff, StepSpec, RetryPolicy, Workflow};
pub use error::{StoreError, WorkflowError};
pub use event::{fold_step_states, StepAttempt, StepState, StepStatus, RunEvent, RunStatus};
pub use resource::{Gate, GateGuard, ResourceKey};
pub use scheduler::{run_workflow, StepContext, RunOptions, RunOutcome};
pub use store::{InMemoryStore, RunState, RunStore};