use serde::{Deserialize, Serialize};

use std::collections::HashMap;

/// Overall lifecycle status of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Running,
    Finished,
    Errored,
    Aborted,
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
}

/// A single execution attempt of one step (used for retry bookkeeping).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepAttempt {
    /// 1-based attempt number.
    pub attempt: usize,
    pub started_at: i64,
    pub finished_at: i64,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
}

/// Append-only persisted events. Each event is immutable; the log is the
/// engine's source of truth and doubling as the resume checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunEvent {
    /// Observability only (not persisted): marks the start of a run.
    RunStarted { ts: i64, run_id: String },
    /// Checkpoint (persisted): final state of a completed run.
    RunFinished { ts: i64, run_id: String, output: Option<serde_json::Value> },
    /// Checkpoint (persisted): the run errored (a step failed terminally).
    RunErrored { ts: i64, run_id: String, error: String },
    /// Observability only (not persisted): a step began executing.
    StepStarted { ts: i64, run_id: String, step_id: String },
    /// Checkpoint (persisted): a step finished successfully; the run may be
    /// resumed from here without re-execution.
    StepFinished {
        ts: i64,
        run_id: String,
        step_id: String,
        result: Option<serde_json::Value>,
        attempts: Vec<StepAttempt>,
    },
    /// Checkpoint (persisted): a step failed terminally.
    StepFailed {
        ts: i64,
        run_id: String,
        step_id: String,
        error: String,
        attempts: Vec<StepAttempt>,
    },
    /// Observability only (not persisted): 0.0..=1.0 progress signal.
    StepProgress { ts: i64, run_id: String, step_id: String, value: f64 },
}

impl RunEvent {
    pub fn step_id(&self) -> Option<&str> {
        match self {
            RunEvent::RunStarted { .. } | RunEvent::RunFinished { .. } | RunEvent::RunErrored { .. } => None,
            RunEvent::StepStarted { step_id, .. }
            | RunEvent::StepFinished { step_id, .. }
            | RunEvent::StepFailed { step_id, .. }
            | RunEvent::StepProgress { step_id, .. } => Some(step_id),
        }
    }
}

/// Derived per-step state reconstructed from an event log.
#[derive(Debug, Clone, Default)]
pub struct StepState {
    pub status: StepStatus,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
}

/// Replays an event log and folds it into per-step states.
///
/// This is the resume mechanism: the log's terminal checkpoints
/// (`StepFinished`/`StepFailed`) fully determine which steps still need to
/// run. No application code is re-executed here.
pub fn fold_step_states(events: &[RunEvent]) -> HashMap<String, StepState> {
    let mut states: HashMap<String, StepState> = HashMap::new();
    for ev in events {
        match ev {
            RunEvent::StepStarted { step_id, ts, .. } => {
                let st = states.entry(step_id.clone()).or_default();
                st.status = StepStatus::Running;
                st.started_at = Some(*ts);
            }
            RunEvent::StepFinished { step_id, result, ts, .. } => {
                let st = states.entry(step_id.clone()).or_default();
                st.status = StepStatus::Success;
                st.result = result.clone();
                st.error = None;
                st.finished_at = Some(*ts);
            }
            RunEvent::StepFailed { step_id, error, ts, .. } => {
                let st = states.entry(step_id.clone()).or_default();
                st.status = StepStatus::Failed;
                st.error = Some(error.clone());
                st.finished_at = Some(*ts);
            }
            _ => {}
        }
    }
    states
}