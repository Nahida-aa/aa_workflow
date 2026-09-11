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

/// Per-node lifecycle status, derived from the event log via
/// [`fold_node_states`]. This is a *projection* of the log, never stored
/// independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    #[default]
    Pending,
    Running,
    Success,
    Failed,
}

/// A single execution attempt of one node (used for retry bookkeeping).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAttempt {
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
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunEvent {
    /// Observability only (not persisted): marks the start of a run.
    RunStarted { ts: i64, run_id: String },
    /// Checkpoint (persisted): final state of a completed run.
    RunFinished { ts: i64, run_id: String, output: Option<serde_json::Value> },
    /// Checkpoint (persisted): the run errored (a node failed terminally).
    RunErrored { ts: i64, run_id: String, error: String },
    /// Observability only (not persisted): a node began executing.
    NodeStarted { ts: i64, run_id: String, node_id: String },
    /// Checkpoint (persisted): a node finished successfully; the run may be
    /// resumed from here without re-execution.
    NodeFinished {
        ts: i64,
        run_id: String,
        node_id: String,
        result: Option<serde_json::Value>,
        attempts: Vec<NodeAttempt>,
    },
    /// Checkpoint (persisted): a node failed terminally.
    NodeFailed {
        ts: i64,
        run_id: String,
        node_id: String,
        error: String,
        attempts: Vec<NodeAttempt>,
    },
    /// Observability only (not persisted): 0.0..=1.0 progress signal.
    NodeProgress { ts: i64, run_id: String, node_id: String, value: f64 },
}

impl RunEvent {
    pub fn node_id(&self) -> Option<&str> {
        match self {
            RunEvent::RunStarted { .. } | RunEvent::RunFinished { .. } | RunEvent::RunErrored { .. } => None,
            RunEvent::NodeStarted { node_id, .. }
            | RunEvent::NodeFinished { node_id, .. }
            | RunEvent::NodeFailed { node_id, .. }
            | RunEvent::NodeProgress { node_id, .. } => Some(node_id),
        }
    }
}

/// Derived per-node state reconstructed from an event log.
#[derive(Debug, Clone, Default)]
pub struct NodeState {
    pub status: NodeStatus,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
}

/// Replays an event log and folds it into per-node states.
///
/// This is the resume mechanism: the log's terminal checkpoints
/// (`NodeFinished`/`NodeFailed`) fully determine which nodes still need to
/// run. No application code is re-executed here.
pub fn fold_node_states(events: &[RunEvent]) -> HashMap<String, NodeState> {
    let mut states: HashMap<String, NodeState> = HashMap::new();
    for ev in events {
        match ev {
            RunEvent::NodeStarted { node_id, ts, .. } => {
                let st = states.entry(node_id.clone()).or_default();
                st.status = NodeStatus::Running;
                st.started_at = Some(*ts);
            }
            RunEvent::NodeFinished { node_id, result, ts, .. } => {
                let st = states.entry(node_id.clone()).or_default();
                st.status = NodeStatus::Success;
                st.result = result.clone();
                st.error = None;
                st.finished_at = Some(*ts);
            }
            RunEvent::NodeFailed { node_id, error, ts, .. } => {
                let st = states.entry(node_id.clone()).or_default();
                st.status = NodeStatus::Failed;
                st.error = Some(error.clone());
                st.finished_at = Some(*ts);
            }
            _ => {}
        }
    }
    states
}