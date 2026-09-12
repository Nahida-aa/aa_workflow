use std::fmt;

/// Errors produced by a [`crate::run_store::RunStore`] implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// `append_event` was called with a stale expected index (CAS violation).
    Conflict {
        run_id: String,
        expected: usize,
        actual: usize,
    },
    /// Backing-store I/O or serialization failure.
    Io(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Conflict {
                run_id,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "append CAS conflict for run {run_id}: expected index {expected}, found {actual}"
                )
            }
            StoreError::Io(msg) => write!(f, "store io error: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

/// Errors produced by the engine.
#[derive(Debug)]
pub enum WorkflowError {
    /// The workflow graph is invalid (duplicate ids, unknown `needs`, cycle).
    Validation(String),
    /// A workflow step failed after exhausting its retry policy.
    Step { step_id: String, error: String },
    /// The finalize step failed.
    Finalize(String),
    /// Transport/persistence error while talking to the store.
    Store(StoreError),
    /// No `RunState` exists for the requested run id.
    RunNotFound(String),
    /// A delivered signal could not be matched to a parked wait point (from
    /// [`crate::engine::signal_event`]: nothing was paused on that event name).
    SignalLost(String),
    /// An internal invariant was violated (deadlock guard, channel closed).
    Internal(String),
}

impl fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkflowError::Validation(msg) => write!(f, "workflow validation failed: {msg}"),
            WorkflowError::Step { step_id, error } => {
                write!(f, "step `{step_id}` failed: {error}")
            }
            WorkflowError::Finalize(msg) => write!(f, "finalize failed: {msg}"),
            WorkflowError::Store(e) => write!(f, "store error: {e}"),
            WorkflowError::RunNotFound(id) => write!(f, "run `{id}` not found"),
            WorkflowError::SignalLost(msg) => write!(f, "signal lost: {msg}"),
            WorkflowError::Internal(msg) => write!(f, "engine internal error: {msg}"),
        }
    }
}

impl std::error::Error for WorkflowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WorkflowError::Store(e) => Some(e),
            _ => None,
        }
    }
}

impl From<StoreError> for WorkflowError {
    fn from(e: StoreError) -> Self {
        WorkflowError::Store(e)
    }
}
