//! 一个独立的落盘 [`RunStore`] 实现，供 example 层使用，同时充当"store 契约
//! 可插拔"的活例。
//!
//! 布局（按 run_id 隔离，多个 run 可共存于同一 base 目录）：
//!
//! - `<base>/<run_id>/run.json` — [`RunState`] 信封（元数据面）
//! - `<base>/<run_id>/events.jsonl` — append-only 事件日志（契约面，CAS append）
//!
//! 语义对齐 [`workflow_core::store::InMemoryStore`]（CAS 冲突报
//! [`StoreError::Conflict`]、`subscribe` 用 `std::sync::mpsc` 扇出），只是落盘。
//! 与 LocalDub 的 `FsRunStore` 是同一思想的独立最小实现（彼方不回依赖这里，
//! 语义上可以此为准）。

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Mutex;

use workflow_core::{RunEvent, RunState, RunStore, StoreError};

fn map_io(e: std::io::Error) -> StoreError {
    StoreError::Io(e.to_string())
}

#[derive(Debug)]
pub struct FileRunStore {
    base: PathBuf,
    // 串行化读-改-写 (append / truncate)；单进程内引擎只有调度该 run 的任务写，
    // 此锁是防御性的，真正的并发一致性由 expected_next_index CAS 保证。
    lock: Mutex<()>,
    subs: Mutex<HashMap<String, Vec<Sender<RunEvent>>>>,
}

impl FileRunStore {
    pub fn new(base: impl AsRef<Path>) -> Self {
        Self {
            base: base.as_ref().to_path_buf(),
            lock: Mutex::new(()),
            subs: Mutex::new(HashMap::new()),
        }
    }

    fn run_dir(&self, run_id: &str) -> PathBuf {
        self.base.join(run_id)
    }
    fn run_state_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("run.json")
    }
    fn events_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("events.jsonl")
    }

    fn atomic_write(path: &Path, contents: &str) -> Result<(), StoreError> {
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, contents).map_err(map_io)?;
        fs::rename(&tmp, path).map_err(map_io)?;
        Ok(())
    }

    /// 读 events.jsonl；文件缺失视为空日志。
    fn read_events(&self, run_id: &str) -> Result<Vec<RunEvent>, StoreError> {
        let raw = match fs::read_to_string(self.events_path(run_id)) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(map_io(e)),
        };
        let mut events = Vec::new();
        for line in raw.lines() {
            if line.trim().is_empty() {
                continue;
            }
            events.push(
                serde_json::from_str(line)
                    .map_err(|e| StoreError::Io(format!("events.jsonl 解析失败: {e}")))?,
            );
        }
        Ok(events)
    }

    fn broadcast(&self, run_id: &str, ev: &RunEvent) {
        if let Ok(mut subs) = self.subs.lock() {
            if let Some(senders) = subs.get_mut(run_id) {
                senders.retain(|s| s.send(ev.clone()).is_ok());
            }
        }
    }
}

impl RunStore for FileRunStore {
    fn get_run_state(&self, run_id: &str) -> Result<Option<RunState>, StoreError> {
        match fs::read_to_string(self.run_state_path(run_id)) {
            Ok(raw) => serde_json::from_str(&raw)
                .map(Some)
                .map_err(|e| StoreError::Io(format!("run.json 解析失败: {e}"))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(map_io(e)),
        }
    }

    fn set_run_state(&self, run_id: &str, state: &RunState) -> Result<(), StoreError> {
        let raw = serde_json::to_string_pretty(state)
            .map_err(|e| StoreError::Io(format!("run.json 序列化失败: {e}")))?;
        fs::create_dir_all(self.run_dir(run_id)).map_err(map_io)?;
        Self::atomic_write(&self.run_state_path(run_id), &raw)
    }

    fn delete_run(&self, run_id: &str) -> Result<(), StoreError> {
        let _guard = self.lock.lock().map_err(|e| StoreError::Io(e.to_string()))?;
        match fs::remove_dir_all(self.run_dir(run_id)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(map_io(e)),
        }
        if let Ok(mut subs) = self.subs.lock() {
            subs.remove(run_id);
        }
        Ok(())
    }

    fn append_event(
        &self,
        run_id: &str,
        expected_next_index: usize,
        event: &RunEvent,
    ) -> Result<(), StoreError> {
        let _guard = self.lock.lock().map_err(|e| StoreError::Io(e.to_string()))?;
        let events = self.read_events(run_id)?;
        let actual = events.len();
        if actual != expected_next_index {
            return Err(StoreError::Conflict {
                run_id: run_id.to_string(),
                expected: expected_next_index,
                actual,
            });
        }
        let line = serde_json::to_string(event)
            .map_err(|e| StoreError::Io(format!("event 序列化失败: {e}")))?;
        let mut out = match fs::read_to_string(self.events_path(run_id)) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(map_io(e)),
        };
        if !out.is_empty() && !out.ends_with('\n') {
            out.push('\n');
        }
        out.push_str(&line);
        out.push('\n');
        fs::create_dir_all(self.run_dir(run_id)).map_err(map_io)?;
        Self::atomic_write(&self.events_path(run_id), &out)?;
        drop(_guard);
        self.broadcast(run_id, event);
        Ok(())
    }

    fn get_events(&self, run_id: &str) -> Result<Vec<RunEvent>, StoreError> {
        self.read_events(run_id)
    }

    fn truncate_runs(&self, run_id: &str, step_id: &str) -> Result<(), StoreError> {
        let _guard = self.lock.lock().map_err(|e| StoreError::Io(e.to_string()))?;
        let events = self.read_events(run_id)?;
        // 对齐 InMemoryStore：裁到 step_id 最新终态 checkpoint（含）之前的保留。
        let Some(cut) = events
            .iter()
            .rposition(|ev| match ev {
                RunEvent::StepFinished { step_id: id, .. }
                | RunEvent::StepFailed { step_id: id, .. } => id == step_id,
                _ => false,
            })
        else {
            return Ok(());
        };
        let mut out = String::new();
        for ev in &events[..cut] {
            out.push_str(
                &serde_json::to_string(ev)
                    .map_err(|e| StoreError::Io(format!("event 序列化失败: {e}")))?,
            );
            out.push('\n');
        }
        Self::atomic_write(&self.events_path(run_id), &out)
    }

    fn subscribe(&self, run_id: &str) -> Option<Receiver<RunEvent>> {
        let (tx, rx) = mpsc::channel();
        self.subs
            .lock()
            .ok()?
            .entry(run_id.to_string())
            .or_default()
            .push(tx);
        Some(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use workflow_core::RunStatus;

    fn temp_base(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "wf_examples_store_{name}_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn st(run_id: &str) -> RunState {
        RunState {
            run_id: run_id.into(),
            workflow_id: "w".into(),
            workflow_version: None,
            status: RunStatus::Running,
            input: serde_json::json!({"x": 1}),
            output: None,
            error: None,
            waiting_for: None,
            pending_approval: None,
            created_at: 1,
            updated_at: 1,
        }
    }

    fn finished(run_id: &str, step: &str) -> RunEvent {
        RunEvent::StepFinished {
            ts: 1,
            run_id: run_id.into(),
            step_id: step.into(),
            result: None,
            attempts: vec![],
        }
    }

    #[test]
    fn roundtrip_and_cas() {
        let base = temp_base("rt");
        let store = FileRunStore::new(&base);
        assert_eq!(store.get_events("r1").unwrap().len(), 0);
        assert!(store.get_run_state("r1").unwrap().is_none());

        store.set_run_state("r1", &st("r1")).unwrap();
        let reloaded = store.get_run_state("r1").unwrap().unwrap();
        assert_eq!(reloaded.run_id, "r1");

        store.append_event("r1", 0, &finished("r1", "a")).unwrap();
        let err = store.append_event("r1", 0, &finished("r1", "b")).unwrap_err();
        assert!(matches!(
            err,
            StoreError::Conflict { expected: 0, actual: 1, .. }
        ));

        // 新实例重开同一 base 应看到之前的日志（模拟进程重启）
        let reopened = FileRunStore::new(&base);
        let evs = reopened.get_events("r1").unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].step_id(), Some("a"));

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn truncate_keeps_prefix_through_terminal() {
        let base = temp_base("trunc");
        let store = FileRunStore::new(&base);
        for (idx, step) in ["a", "b", "c"].iter().enumerate() {
            store.append_event("r1", idx, &finished("r1", step)).unwrap();
        }
        store.truncate_runs("r1", "b").unwrap();
        let evs = store.get_events("r1").unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].step_id(), Some("a"));

        // 无终态 checkpoint → 不动
        store.truncate_runs("r1", "zzz").unwrap();
        assert_eq!(store.get_events("r1").unwrap().len(), 1);

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn subscribe_fans_out_append_only() {
        let base = temp_base("sub");
        let store = FileRunStore::new(&base);
        let rx = store.subscribe("r1").expect("subscribe 应支持");
        store
            .append_event("r1", 0, &finished("r1", "a"))
            .unwrap();
        store
            .append_event("r1", 1, &finished("r1", "b"))
            .unwrap();
        let got: Vec<_> = rx.try_iter().collect();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].step_id(), Some("a"));
        assert_eq!(got[1].step_id(), Some("b"));

        let _ = fs::remove_dir_all(&base);
    }
}