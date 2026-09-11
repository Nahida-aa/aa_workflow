//! File-artifact DAG workflow engine, modelled after TanStack Workflow's
//! contract but with no determinism requirement and no server daemon: node
//! closures are plain sync functions writing artifacts to disk, and the
//! schedule is simply the explicit `needs` graph.
//!
//! ## Persistence model
//!
//! The engine's source of truth is an **append-only event log** behind the
//! [`store::RunStore`] trait (with CAS append + optional subscribe for
//! fan-out). Per-node status is *derived* by replaying the log
//! ([`event::fold_node_states`]); resume therefore never re-executes code,
//! because the graph is static. Context files (e.g. LocalDub's `ctx.json`)
//! are a deterministic projection of the log, not a second authoritative
//! book.

pub mod define;
pub mod error;
pub mod event;
pub mod resource;
pub mod scheduler;
pub mod store;

pub use define::{Backoff, NodeSpec, RetryPolicy, Workflow};
pub use error::{StoreError, WorkflowError};
pub use event::{fold_node_states, NodeAttempt, NodeState, NodeStatus, RunEvent, RunStatus};
pub use resource::{Gate, GateGuard, ResourceKey};
pub use scheduler::{run, RunCtx, RunOptions, RunOutcome};
pub use store::{InMemoryStore, RunState, RunStore};