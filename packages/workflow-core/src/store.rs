use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::StoreError;
use crate::event::{RunEvent, RunStatus};

/// Minimal, durable metadata for a run. The heavy state lives in the event
/// log; this is just the envelope the launcher needs to locate runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_version: Option<String>,
    pub status: RunStatus,
    pub input: serde_json::Value,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
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

    /// Live subscription: a receiver that sees every future event appended to
    /// this run's log. `None` if the store does not support subscriptions.
    fn subscribe(&self, run_id: &str) -> Option<Receiver<RunEvent>> {
        let _ = run_id;
        None
    }
}

#[derive(Default)]
struct Inner {
    runs: HashMap<String, RunState>,
    logs: HashMap<String, Vec<RunEvent>>,
    subs: HashMap<String, Vec<Sender<RunEvent>>>,
}

/// In-memory [`RunStore`] for phase 0. Provides the full contract surface
/// (CAS append + subscribe) so the engine can be exercised with property
/// tests before a real filesystem store is written.
#[derive(Default)]
pub struct InMemoryStore {
    inner: Mutex<Inner>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RunStore for InMemoryStore {
    fn get_run_state(&self, run_id: &str) -> Result<Option<RunState>, StoreError> {
        Ok(self.inner.lock().map_err(|e| StoreError::Io(e.to_string()))?.runs.get(run_id).cloned())
    }

    fn set_run_state(&self, run_id: &str, state: &RunState) -> Result<(), StoreError> {
        self.inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?
            .runs
            .insert(run_id.to_string(), state.clone());
        Ok(())
    }

    fn delete_run(&self, run_id: &str) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().map_err(|e| StoreError::Io(e.to_string()))?;
        inner.runs.remove(run_id);
        inner.logs.remove(run_id);
        inner.subs.remove(run_id);
        Ok(())
    }

    fn append_event(
        &self,
        run_id: &str,
        expected_next_index: usize,
        event: &RunEvent,
    ) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().map_err(|e| StoreError::Io(e.to_string()))?;
        let log = inner.logs.entry(run_id.to_string()).or_default();
        let actual = log.len();
        if actual != expected_next_index {
            return Err(StoreError::Conflict {
                run_id: run_id.to_string(),
                expected: expected_next_index,
                actual,
            });
        }
        log.push(event.clone());
        if let Some(senders) = inner.subs.get_mut(run_id) {
            senders.retain(|s| s.send(event.clone()).is_ok());
        }
        Ok(())
    }

    fn get_events(&self, run_id: &str) -> Result<Vec<RunEvent>, StoreError> {
        Ok(self
            .inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?
            .logs
            .get(run_id)
            .cloned()
            .unwrap_or_default())
    }

    fn subscribe(&self, run_id: &str) -> Option<Receiver<RunEvent>> {
        let (tx, rx) = mpsc::channel();
        let mut inner = self.inner.lock().ok()?;
        inner.subs.entry(run_id.to_string()).or_default().push(tx);
        Some(rx)
    }
}