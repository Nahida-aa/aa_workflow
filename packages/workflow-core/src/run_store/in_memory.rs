//! 内存 [`RunStore`] 实现：契约的最小完整实现，用来在接入真实持久化后端之前
//! 跑通引擎与测试。
//!
//! 三个 map 分别对应契约的两个面：`runs` 是元数据面，`logs` 是 append-only
//! 事件日志面，`subs` 只服务 `subscribe` fan-out。并发靠一把 `Mutex` 加
//! `append_event` 的 CAS 索引；不做跨进程协调。

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::mpsc::{self, Receiver, Sender};

use crate::error::StoreError;
use crate::event::RunEvent;

use super::{DeleteReason, RunState, RunStore};

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
        Ok(self
            .inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?
            .runs
            .get(run_id)
            .cloned())
    }

    fn set_run_state(&self, run_id: &str, state: &RunState) -> Result<(), StoreError> {
        self.inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?
            .runs
            .insert(run_id.to_string(), state.clone());
        Ok(())
    }

    fn delete_run(&self, run_id: &str, _reason: DeleteReason) -> Result<(), StoreError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?;
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
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?;
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

    fn truncate_runs(&self, run_id: &str, step_id: &str) -> Result<(), StoreError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StoreError::Io(e.to_string()))?;
        let Some(log) = inner.logs.get_mut(run_id) else {
            return Ok(());
        };
        // Latest terminal checkpoint for step_id (success or failure).
        let cut = log.iter().rposition(|ev| match ev {
            RunEvent::StepFinished { step_id: id, .. }
            | RunEvent::StepFailed { step_id: id, .. } => id == step_id,
            _ => false,
        });
        if let Some(i) = cut {
            log.truncate(i); // drop event i and everything after it
        }
        Ok(())
    }

    fn subscribe(&self, run_id: &str) -> Option<Receiver<RunEvent>> {
        let (tx, rx) = mpsc::channel();
        let mut inner = self.inner.lock().ok()?;
        inner.subs.entry(run_id.to_string()).or_default().push(tx);
        Some(rx)
    }
}
