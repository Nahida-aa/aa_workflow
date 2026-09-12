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
    /// The run is parked at a durable wait point (`StepPaused`), e.g. an
    /// approval or a sleep; it resumes when a `StepResume` arrives.
    Paused,
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
    /// Derived from a `StepPaused` checkpoint: the step is parked at a durable
    /// wait (approval / sleep) until a matching `StepResume` arrives.
    Paused,
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
    /// Checkpoint (persisted): the run parked at a durable wait point
    /// (`ctx.approve` / `ctx.sleep` / `ctx.sleep_until` / `ctx.wait_for_event`).
    /// `step_id` is the deterministic pause key (== the signal id
    /// [`signal_run`](crate::engine::signal_run) / [`signal_event`](crate::engine::signal_event)
    /// targets); `signal_name` is the channel the run is parked on
    /// (`"__timer"` for sleeps, `"__approval"` for approvals, a user event
    /// name for named waits). `due_at` is `Some` (wall-clock ms) for sleep-like
    /// waits so a timer host could auto-deliver, `None` for waits that need an
    /// external decision.
    StepPaused {
        ts: i64,
        run_id: String,
        step_id: String,
        signal_name: String,
        due_at: Option<i64>,
        reason: String,
    },
    /// Checkpoint (persisted): a signal was delivered for a paused run
    /// (see [`signal_run`](crate::engine::signal_run)). Resolves the pending
    /// `StepPaused` with the same `step_id`; `payload` is what the
    /// `approve`/`sleep` call returns.
    StepResume {
        ts: i64,
        run_id: String,
        step_id: String,
        payload: Option<serde_json::Value>,
    },
    /// Observability only (not persisted): 0.0..=1.0 progress signal.
    StepProgress { ts: i64, run_id: String, step_id: String, value: f64 },
    /// Observability only (not persisted): `ctx.emit` fan-out. Reaches only
    /// the publisher, never the log — so it never becomes part of replay
    /// (mirrors TanStack's `CUSTOM` event).
    Custom { ts: i64, run_id: String, name: String, value: serde_json::Value },
    /// Checkpoint (persisted): `ctx.now()` recorded a wall-clock timestamp.
    /// On replay the cached value is served so a run sees the same clock
    /// across resumes (TanStack `NOW_RECORDED`).
    NowRecorded { ts: i64, run_id: String, step_id: String, value: i64 },
    /// Checkpoint (persisted): `ctx.uuid()` recorded a generated id. On
    /// replay the same id is served (TanStack `UUID_RECORDED`).
    UuidRecorded { ts: i64, run_id: String, step_id: String, value: String },
}

impl RunEvent {
    pub fn step_id(&self) -> Option<&str> {
        match self {
            RunEvent::RunStarted { .. }
            | RunEvent::RunFinished { .. }
            | RunEvent::RunErrored { .. }
            | RunEvent::Custom { .. } => None,
            RunEvent::StepStarted { step_id, .. }
            | RunEvent::StepFinished { step_id, .. }
            | RunEvent::StepFailed { step_id, .. }
            | RunEvent::StepPaused { step_id, .. }
            | RunEvent::StepResume { step_id, .. }
            | RunEvent::StepProgress { step_id, .. }
            | RunEvent::NowRecorded { step_id, .. }
            | RunEvent::UuidRecorded { step_id, .. } => Some(step_id),
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
            RunEvent::StepPaused { step_id, ts, due_at, .. } => {
                let st = states.entry(step_id.clone()).or_default();
                st.status = StepStatus::Paused;
                st.started_at = st.started_at.or(Some(*ts));
                st.finished_at = due_at.or(st.finished_at);
            }
            RunEvent::StepResume { step_id, payload, .. } => {
                // A delivered signal exposes its payload as the paused step's
                // derived result; the step stays `Paused` in the projection.
                let st = states.entry(step_id.clone()).or_default();
                st.result = payload.clone();
            }
            _ => {}
        }
    }
    states
}