use std::collections::HashMap;
use std::sync::Mutex;
use workflow_core::{DeleteReason, RunState, WorkflowEvent};

use crate::types::*;
use crate::{WorkflowExecutionStore, WorkflowRunStoreAdapterStore};

/// 测试用最小 execution store：只够验证适配器与端到端链路。
///
/// 覆盖 18 个方法（基础层 6 + 扩展层 12），但扩展部分**只做最简实现**——
/// 例如 `claim_run` 总是成功（单进程无并发）、`claim_stale_runs` 永远返回空。
/// 真要做成可用的 `InMemoryExecutionStore` 需要更完整的语义，那是后续的事。
#[derive(Default)]
pub struct MemStore {
    runs: Mutex<HashMap<String, RunState>>,
    executions: Mutex<HashMap<String, WorkflowExecution>>,
    logs: Mutex<HashMap<String, Vec<WorkflowEvent>>>,
    wakeups: Mutex<Vec<TimerWakeup>>,
}

impl WorkflowRunStoreAdapterStore for MemStore {
    fn load_run_state(&self, run_id: &RunId) -> anyhow::Result<Option<RunState>> {
        Ok(self.runs.lock().unwrap().get(run_id).cloned())
    }

    fn save_run_state(&self, args: SaveRunStateArgs) -> anyhow::Result<()> {
        self.runs
            .lock()
            .unwrap()
            .insert(args.state.run_id.clone(), args.state);
        Ok(())
    }

    fn delete_run(&self, run_id: &RunId, _reason: DeleteReason) -> anyhow::Result<()> {
        self.runs.lock().unwrap().remove(run_id);
        self.executions.lock().unwrap().remove(run_id);
        self.logs.lock().unwrap().remove(run_id);
        Ok(())
    }

    fn append_events(&self, args: AppendEventsArgs) -> anyhow::Result<AppendEventsResult> {
        let mut logs = self.logs.lock().unwrap();
        let log = logs.entry(args.run_id.clone()).or_default();
        let actual = log.len() as u64;
        if actual != args.expected_next_index {
            return Err(anyhow::Error::new(workflow_core::StoreError::Conflict {
                run_id: args.run_id,
                expected: args.expected_next_index as usize,
                actual: actual as usize,
            }));
        }
        log.extend(args.events);
        Ok(AppendEventsResult {
            next_index: log.len() as u64,
        })
    }

    fn read_events(&self, args: ReadEventsArgs) -> anyhow::Result<Vec<StoredWorkflowEvent>> {
        let logs = self.logs.lock().unwrap();
        let log = logs.get(&args.run_id).cloned().unwrap_or_default();
        let from = args.from_index.unwrap_or(0) as usize;
        Ok(log
            .into_iter()
            .enumerate()
            .skip(from)
            .map(|(i, event)| StoredWorkflowEvent {
                run_id: args.run_id.clone(),
                event_index: i as u64,
                event_type: "TEST".into(),
                step_id: None,
                event,
                created_at: 0,
            })
            .collect())
    }
}

impl WorkflowExecutionStore for MemStore {
    fn create_run(&self, args: CreateRunArgs) -> anyhow::Result<CreateRunResult> {
        let mut execs = self.executions.lock().unwrap();
        if let Some(existing) = execs.get(&args.run_id) {
            return Ok(CreateRunResult::Existing {
                run: existing.clone(),
            });
        }
        let run = WorkflowExecution {
            run_id: args.run_id.clone(),
            workflow_id: args.workflow_id,
            workflow_version: args.workflow_version,
            status: workflow_core::RunStatus::Running,
            lease: None,
            wake_at: None,
            created_at: args.now,
            updated_at: args.now,
        };
        execs.insert(args.run_id, run.clone());
        Ok(CreateRunResult::Created { run })
    }

    fn load_run(&self, run_id: &RunId) -> anyhow::Result<Option<WorkflowExecution>> {
        Ok(self.executions.lock().unwrap().get(run_id).cloned())
    }

    fn load_execution(&self, run_id: &RunId) -> anyhow::Result<Option<LoadedExecution>> {
        let Some(run) = self.load_run(run_id)? else {
            return Ok(None);
        };
        let events = self.read_events(ReadEventsArgs {
            run_id: run_id.clone(),
            from_index: None,
        })?;
        Ok(Some(LoadedExecution { run, events }))
    }

    fn mark_run_paused(&self, args: MarkRunPausedArgs) -> anyhow::Result<()> {
        if let Some(e) = self.executions.lock().unwrap().get_mut(&args.run_id) {
            e.status = workflow_core::RunStatus::Paused;
            e.wake_at = args.wake_at;
            e.updated_at = args.now;
        }
        Ok(())
    }

    fn mark_run_finished(&self, args: MarkRunFinishedArgs) -> anyhow::Result<()> {
        if let Some(e) = self.executions.lock().unwrap().get_mut(&args.run_id) {
            e.status = workflow_core::RunStatus::Finished;
            e.wake_at = None;
            e.updated_at = args.now;
        }
        Ok(())
    }

    fn mark_run_errored(&self, args: MarkRunErroredArgs) -> anyhow::Result<()> {
        if let Some(e) = self.executions.lock().unwrap().get_mut(&args.run_id) {
            e.status = workflow_core::RunStatus::Errored;
            e.wake_at = None;
            e.updated_at = args.now;
        }
        Ok(())
    }

    /// 单进程测试实现：总是认领成功（无并发场景）。
    fn claim_run(&self, args: ClaimRunArgs) -> anyhow::Result<ClaimRunResult> {
        let mut execs = self.executions.lock().unwrap();
        let Some(run) = execs.get_mut(&args.run_id) else {
            return Ok(ClaimRunResult::NotFound);
        };
        run.lease = Some(WorkflowLease {
            owner: args.lease_owner,
            expires_at: args.now + args.lease_ms,
        });
        run.updated_at = args.now;
        Ok(ClaimRunResult::Claimed { run: run.clone() })
    }

    fn heartbeat_run_lease(&self, args: HeartbeatRunLeaseArgs) -> anyhow::Result<()> {
        if let Some(e) = self.executions.lock().unwrap().get_mut(&args.run_id)
            && e.lease.as_ref().map(|l| &l.owner) == Some(&args.lease_owner)
        {
            e.lease = Some(WorkflowLease {
                owner: args.lease_owner,
                expires_at: args.now + args.lease_ms,
            });
        }
        Ok(())
    }

    fn release_run_lease(&self, args: ReleaseRunLeaseArgs) -> anyhow::Result<()> {
        if let Some(e) = self.executions.lock().unwrap().get_mut(&args.run_id)
            && e.lease.as_ref().map(|l| &l.owner) == Some(&args.lease_owner)
        {
            e.lease = None;
        }
        Ok(())
    }

    /// 测试实现暂不扫描陈旧 run。
    fn claim_stale_runs(&self, _args: ClaimStaleRunsArgs) -> anyhow::Result<Vec<RunClaim>> {
        Ok(vec![])
    }

    fn schedule_timer(&self, args: ScheduleTimerArgs) -> anyhow::Result<()> {
        let mut ws = self.wakeups.lock().unwrap();
        // signal_id 幂等：重复登记同一 signal_id 无副作用。
        if ws.iter().any(|w| w.signal_id == args.signal_id) {
            return Ok(());
        }
        ws.push(TimerWakeup {
            run_id: args.run_id,
            workflow_id: args.workflow_id,
            workflow_version: args.workflow_version,
            wake_at: args.wake_at,
            signal_id: args.signal_id,
        });
        Ok(())
    }

    /// 认领到期 timer 并从表里移除（同样不做 lease）。
    fn claim_due_timers(&self, args: ClaimDueTimersArgs) -> anyhow::Result<Vec<TimerWakeup>> {
        let mut ws = self.wakeups.lock().unwrap();
        let mut due = Vec::new();
        let mut keep = Vec::new();
        for w in ws.drain(..) {
            if w.wake_at <= args.now && due.len() < args.limit {
                due.push(w);
            } else {
                keep.push(w);
            }
        }
        *ws = keep;
        Ok(due)
    }
}
