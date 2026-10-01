//! [`InMemoryExecutionStore`]：全部 19 个方法的内存实现。
//!
//! 对齐 TanStack `workflow-runtime/src/in-memory-store.ts`（674 行）。
//! **测试与演示用**——进程一退数据全没（上游文档原话：Use the in-memory store
//! for tests and demos only）。生产部署需要 DB 后端。
//!
//! # 从 TS 移植时消失的东西
//!
//! 上游是工厂函数 + 闭包状态 + `updateRun(updater)` 统一读-改-写，外加一个
//! **8 个函数的 `clone*` 家族**——JS 对象是引用共享，每个返回值都要深拷贝，
//! 否则外部改动会穿透内部状态。Rust 的所有权 + `Mutex` 让这件事**编译期就不
//! 可能**，clone 家族整体消失，读路径就是一个 `.lock().get().cloned()`。
//!
//! # 从 TS 移植时保住的语义要点
//!
//! - `create_run` → `Queued`（不是 `Running`）；`claim_run` 成功才 `Running`；
//!   `deliver_signal` / `deliver_approval` 成功后**回到 `Queued`**（待再认领）
//! - `claim_run` 三态：不存在 / 终态或 lease 被别人持有 → 不可认领
//! - `can_claim` 三条件：无 lease / 是自己的 / 已过期
//! - `claim_due_timers` **不移除** timer，挂 lease 防重复投递；投递成功后由
//!   `deliver_signal` 删除
//! - `mark_run_paused` / `mark_run_finished` / `mark_run_errored` 都清 lease
//!   ——挂起 / 终态不让出前白占
//! - `subscribe_events` **先补发** `from_index` 起的存量，再注册增量
//! - `delete_run` 连带清 timer 与 signal 投递记录
//! - `save_run_state` 会用 `execution_from_run_state` **重建** execution
//!   （两份状态的同步点；`wakeAt` 从 `waitingFor.signal_name == "__timer"`
//!   的 deadline 推导）

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use aa_workflow_core::{DeleteReason, RunState, WorkflowEvent};

use crate::types::*;
use crate::{WorkflowExecutionStore, WorkflowRunStoreAdapterStore};

type Subscriber = Arc<dyn Fn(&WorkflowEvent, u64) + Send + Sync>;

/// 内部共享状态。拆出 `Inner` 是为了让 `subscribe_events` 返回的取消闭包能
/// 持有 `Arc<Inner>`（`&self` 拿不到 `Arc<Self>`）。
#[derive(Default)]
struct Inner {
    runs: Mutex<HashMap<RunId, WorkflowExecution>>,
    run_states: Mutex<HashMap<RunId, RunState>>,
    logs: Mutex<HashMap<RunId, Vec<StoredWorkflowEvent>>>,
    timers: Mutex<HashMap<String, TimerRecord>>,
    signal_deliveries: Mutex<HashSet<String>>,
    schedules: Mutex<HashMap<ScheduleId, ScheduleRecord>>,
    schedule_buckets: Mutex<HashMap<String, ScheduleBucketRecord>>,
    subscribers: Mutex<HashMap<RunId, Vec<Subscriber>>>,
}

/// 内部 timer 记录 = [`TimerWakeup`] + 投递 lease（对齐上游 `TimerRecord`）。
#[derive(Debug, Clone)]
struct TimerRecord {
    timer: TimerWakeup,
    lease: Option<WorkflowLease>,
}

/// 内部 schedule 定义。
#[derive(Debug, Clone)]
struct ScheduleRecord {
    schedule_id: ScheduleId,
    workflow_id: WorkflowId,
    workflow_version: Option<WorkflowVersion>,
    next_fire_at: Option<i64>,
    input: Option<serde_json::Value>,
    overlap_policy: WorkflowOverlapPolicy,
    enabled: bool,
}

/// 内部 schedule 桶 = [`ScheduleBucket`] + 状态 + lease。
#[derive(Debug, Clone)]
struct ScheduleBucketRecord {
    bucket: ScheduleBucket,
    status: BucketStatus,
    lease: Option<WorkflowLease>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketStatus {
    Claimed,
    Started,
}

/// 全内存的执行存储（对齐上游 `inMemoryWorkflowExecutionStore`）。
///
/// **测试与演示用**，进程一退数据全没。生产需要 DB 后端实现同一套契约。
#[derive(Clone, Default)]
pub struct InMemoryExecutionStore {
    inner: Arc<Inner>,
}

/// 构造一个全内存执行存储（对齐上游 `inMemoryWorkflowExecutionStore()`）。
pub fn in_memory_execution_store() -> InMemoryExecutionStore {
    InMemoryExecutionStore::default()
}

/// `can_claim` 三条件（对齐上游同名函数）：无 lease / 是自己的 / 已过期。
fn can_claim(existing: Option<&WorkflowLease>, owner: &LeaseOwner, now: i64) -> bool {
    match existing {
        None => true,
        Some(l) => l.owner == *owner || l.expires_at <= now,
    }
}

/// 构造 lease（对齐上游 `lease()`）。
fn new_lease(owner: LeaseOwner, lease_ms: i64, now: i64) -> WorkflowLease {
    WorkflowLease {
        owner,
        expires_at: now + lease_ms,
    }
}

/// `"{run_id}:{signal_id}"`（对齐上游 `timerKey` / `signalKey`——两者同形）。
fn signal_key(run_id: &RunId, signal_id: &str) -> String {
    format!("{run_id}:{signal_id}")
}

/// `"{schedule_id}:{bucket_id}"`（对齐上游 `scheduleBucketKey`）。
fn schedule_bucket_key(schedule_id: &ScheduleId, bucket_id: &ScheduleBucketId) -> String {
    format!("{schedule_id}:{bucket_id}")
}

/// 从事件的 `step_id` 字段提取（对齐上游 `getStepId`）。
fn step_id_of(event: &WorkflowEvent) -> Option<String> {
    event.step_id().map(str::to_string)
}

/// 把事件装进索引信封（对齐上游 `storeEvent`）。
fn store_event(run_id: &RunId, event_index: u64, event: WorkflowEvent) -> StoredWorkflowEvent {
    StoredWorkflowEvent {
        run_id: run_id.clone(),
        event_index,
        event_type: event.type_name().to_string(),
        step_id: step_id_of(&event),
        created_at: event.ts(),
        event,
    }
}

/// 从 core 信封重建 execution（对齐上游 `executionFromRunState`）。
///
/// 这是两份状态的同步点。`wake_at` 从 timer 等待推导：
/// `waitingFor.signalName == "__timer"` 时取其 `deadline`。
fn execution_from_run_state(state: &RunState, lease: Option<&WorkflowLease>) -> WorkflowExecution {
    let wake_at = state
        .waiting_for
        .as_ref()
        .filter(|w| w.signal_name == "__timer")
        .and_then(|w| w.deadline);
    WorkflowExecution {
        run_id: state.run_id.clone(),
        workflow_id: state.workflow_id.clone(),
        workflow_version: state.workflow_version.clone(),
        status: state.status.into(),
        input: state.input.clone(),
        output: state.output.clone(),
        error: state.error.clone(),
        waiting_for: state.waiting_for.clone(),
        pending_approval: state.pending_approval.clone(),
        wake_at,
        lease: lease.cloned(),
        created_at: state.created_at,
        updated_at: state.updated_at,
    }
}

fn to_run_summary(run: &WorkflowExecution) -> RunSummary {
    RunSummary {
        run_id: run.run_id.clone(),
        workflow_id: run.workflow_id.clone(),
        workflow_version: run.workflow_version.clone(),
        status: run.status,
        waiting_for: run.waiting_for.clone(),
        pending_approval: run.pending_approval.clone(),
        wake_at: run.wake_at,
        created_at: run.created_at,
        updated_at: run.updated_at,
    }
}

/// run 是否在等这个信号（对齐上游 `signalAwaitableMatches` + `isRunWaitingForSignal`）。
///
/// 信号名相等，且投递的 `step_id` 与等待点兼容（任一未指定，或相等）。
/// 上游还会检查 `run.awaiting[]`——本仓没有 awaiting，见 `MarkRunPausedArgs` 文档。
fn is_run_waiting_for_signal(run: &WorkflowExecution, delivery: &SignalDelivery) -> bool {
    match &run.waiting_for {
        Some(w) => {
            w.signal_name == delivery.name
                && (delivery.step_id.is_none()
                    || w.step_id.is_none()
                    || w.step_id == delivery.step_id)
        }
        None => false,
    }
}

/// run 是否在等这个审批（对齐上游 `isRunWaitingForApproval`，去掉 awaiting 分支）。
fn is_run_waiting_for_approval(run: &WorkflowExecution, approval: &ApprovalResult) -> bool {
    run.pending_approval
        .as_ref()
        .map(|p| p.approval_id == approval.approval_id)
        .unwrap_or(false)
}

impl InMemoryExecutionStore {
    fn set_run(&self, run: WorkflowExecution) {
        self.inner
            .runs
            .lock()
            .unwrap()
            .insert(run.run_id.clone(), run);
    }

    fn get_run(&self, run_id: &str) -> Option<WorkflowExecution> {
        self.inner.runs.lock().unwrap().get(run_id).cloned()
    }

    /// 读-改-写统一入口（对齐上游 `updateRun`）。run 不存在则返回 `None`。
    fn update_run(
        &self,
        run_id: &RunId,
        updater: impl FnOnce(WorkflowExecution) -> WorkflowExecution,
    ) -> Option<WorkflowExecution> {
        let mut runs = self.inner.runs.lock().unwrap();
        let existing = runs.get(run_id)?;
        let next = updater(existing.clone());
        runs.insert(run_id.clone(), next.clone());
        Some(next)
    }

    fn publish(&self, run_id: &RunId, event: &WorkflowEvent, index: u64) {
        if let Some(subs) = self.inner.subscribers.lock().unwrap().get(run_id) {
            for sub in subs {
                sub(event, index);
            }
        }
    }
}

impl InMemoryExecutionStore {
    /// 当前挂在 `run_id` 上的订阅回调数。仅供测试断言退订是否真的发生
    /// ——适配器的 `subscribe` 把 `Receiver` 桥接成回调后，「订阅有没有被
    /// 撤掉」只能从这里观察。
    ///
    /// `#[cfg(test)]`：这是**测试脚手架**，不是产品能力，不该出现在公开 API
    /// 面上（`workflow_runtime` 没有 `public_api` 快照测试兜着，得自己拦住）。
    #[cfg(test)]
    pub fn debug_subscriber_count(&self, run_id: &str) -> usize {
        self.inner
            .subscribers
            .lock()
            .unwrap()
            .get(run_id)
            .map_or(0, |s| s.len())
    }
}

impl WorkflowRunStoreAdapterStore for InMemoryExecutionStore {
    fn load_run_state(&self, run_id: &str) -> anyhow::Result<Option<RunState>> {
        Ok(self.inner.run_states.lock().unwrap().get(run_id).cloned())
    }

    fn save_run_state(&self, args: SaveRunStateArgs) -> anyhow::Result<()> {
        let state = args.state;
        self.inner
            .run_states
            .lock()
            .unwrap()
            .insert(state.run_id.clone(), state.clone());
        let lease = self
            .inner
            .runs
            .lock()
            .unwrap()
            .get(&state.run_id)
            .and_then(|r| r.lease.clone());
        self.set_run(execution_from_run_state(&state, lease.as_ref()));
        Ok(())
    }

    fn delete_run(&self, run_id: &str, _reason: DeleteReason) -> anyhow::Result<()> {
        self.inner.runs.lock().unwrap().remove(run_id);
        self.inner.run_states.lock().unwrap().remove(run_id);
        self.inner.logs.lock().unwrap().remove(run_id);
        self.inner.subscribers.lock().unwrap().remove(run_id);
        // 连带清理该 run 的 timer 与 signal 投递记录（对齐上游 deleteRun）。
        self.inner
            .timers
            .lock()
            .unwrap()
            .retain(|_, t| t.timer.run_id != *run_id);
        let prefix = format!("{run_id}:");
        self.inner
            .signal_deliveries
            .lock()
            .unwrap()
            .retain(|k| !k.starts_with(&prefix));
        Ok(())
    }

    fn append_events(&self, args: AppendEventsArgs) -> anyhow::Result<AppendEventsResult> {
        let mut stored_events = Vec::new();
        {
            let mut logs = self.inner.logs.lock().unwrap();
            let log = logs.entry(args.run_id.clone()).or_default();
            let actual = log.len() as u64;
            if actual != args.expected_next_index {
                return Err(anyhow::Error::new(aa_workflow_core::StoreError::Conflict {
                    run_id: args.run_id,
                    expected: args.expected_next_index as usize,
                    actual: actual as usize,
                }));
            }
            for event in args.events {
                let index = log.len() as u64;
                let stored = store_event(&args.run_id, index, event);
                log.push(stored.clone());
                stored_events.push(stored);
            }
        }
        // 锁外发布：订阅者回调可能再进 store（读状态等），持锁发布会死锁。
        for stored in &stored_events {
            self.publish(&args.run_id, &stored.event, stored.event_index);
        }
        let next_index = stored_events
            .last()
            .map(|s| s.event_index + 1)
            .unwrap_or(args.expected_next_index as u64);
        Ok(AppendEventsResult { next_index })
    }

    fn read_events(&self, args: ReadEventsArgs) -> anyhow::Result<Vec<StoredWorkflowEvent>> {
        let logs = self.inner.logs.lock().unwrap();
        let log = logs.get(&args.run_id).cloned().unwrap_or_default();
        let from = args.from_index.unwrap_or(0) as usize;
        Ok(log.into_iter().skip(from).collect())
    }

    /// 先补发 `from_index` 起的存量事件，再注册增量订阅（对齐上游语义）。
    fn subscribe_events(
        &self,
        run_id: &str,
        from_index: u64,
        on_event: Box<dyn Fn(&WorkflowEvent, u64) + Send + Sync>,
    ) -> Option<Box<dyn Fn() + Send + Sync>> {
        // 补发存量。
        {
            let logs = self.inner.logs.lock().unwrap();
            let log = logs.get(run_id).cloned().unwrap_or_default();
            for stored in log.into_iter().skip(from_index as usize) {
                on_event(&stored.event, stored.event_index);
            }
        }
        // 注册增量。
        let sub: Subscriber = Arc::new(on_event);
        {
            let mut subs = self.inner.subscribers.lock().unwrap();
            subs.entry(run_id.to_string())
                .or_default()
                .push(sub.clone());
        }
        let inner = Arc::clone(&self.inner);
        let run_id = run_id.to_string();
        Some(Box::new(move || {
            if let Some(list) = inner.subscribers.lock().unwrap().get_mut(&run_id) {
                list.retain(|s| !Arc::ptr_eq(s, &sub));
            }
        }))
    }
}

impl WorkflowExecutionStore for InMemoryExecutionStore {
    fn create_run(&self, args: CreateRunArgs) -> anyhow::Result<CreateRunResult> {
        if let Some(existing) = self.get_run(&args.run_id) {
            return Ok(CreateRunResult::Existing { run: existing });
        }
        let run = WorkflowExecution {
            run_id: args.run_id.clone(),
            workflow_id: args.workflow_id,
            workflow_version: args.workflow_version,
            // 注意是 Queued 不是 Running——入队未跑，claim 成功才 Running。
            status: WorkflowExecutionStatus::Queued,
            input: args.input,
            output: None,
            error: None,
            waiting_for: None,
            pending_approval: None,
            wake_at: None,
            lease: None,
            created_at: args.now,
            updated_at: args.now,
        };
        self.set_run(run.clone());
        Ok(CreateRunResult::Created { run })
    }

    fn load_run(&self, run_id: &str) -> anyhow::Result<Option<WorkflowExecution>> {
        Ok(self.get_run(run_id))
    }

    fn load_execution(&self, run_id: &str) -> anyhow::Result<Option<LoadedExecution>> {
        let Some(run) = self.get_run(run_id) else {
            return Ok(None);
        };
        let events = self.read_events(ReadEventsArgs {
            run_id: run_id.to_string(),
            from_index: None,
        })?;
        Ok(Some(LoadedExecution { run, events }))
    }

    fn mark_run_paused(&self, args: MarkRunPausedArgs) -> anyhow::Result<()> {
        self.update_run(&args.run_id, |mut run| {
            run.status = WorkflowExecutionStatus::Paused;
            run.waiting_for = args.waiting_for;
            run.pending_approval = args.pending_approval;
            run.wake_at = args.wake_at;
            run.lease = None; // 挂起即让出 lease
            run.updated_at = args.now;
            run
        });
        Ok(())
    }

    fn mark_run_finished(&self, args: MarkRunFinishedArgs) -> anyhow::Result<()> {
        self.update_run(&args.run_id, |mut run| {
            run.status = WorkflowExecutionStatus::Finished;
            run.output = Some(args.output);
            run.waiting_for = None;
            run.pending_approval = None;
            run.wake_at = None;
            run.lease = None;
            run.updated_at = args.now;
            run
        });
        Ok(())
    }

    fn mark_run_errored(&self, args: MarkRunErroredArgs) -> anyhow::Result<()> {
        // 上游 `void args.code`——execution 不存 code（code 只进 RUN_ERRORED 事件）。
        self.update_run(&args.run_id, |mut run| {
            run.status = WorkflowExecutionStatus::Errored;
            run.error = Some(args.error);
            run.waiting_for = None;
            run.pending_approval = None;
            run.wake_at = None;
            run.lease = None;
            run.updated_at = args.now;
            run
        });
        Ok(())
    }

    fn claim_run(&self, args: ClaimRunArgs) -> anyhow::Result<ClaimRunResult> {
        let Some(existing) = self.get_run(&args.run_id) else {
            return Ok(ClaimRunResult::NotFound);
        };
        if existing.status.is_terminal() {
            return Ok(ClaimRunResult::NotClaimable { run: existing });
        }
        if !can_claim(existing.lease.as_ref(), &args.lease_owner, args.now) {
            return Ok(ClaimRunResult::NotClaimable { run: existing });
        }
        let claimed = self.update_run(&args.run_id, |mut run| {
            run.status = WorkflowExecutionStatus::Running;
            run.lease = Some(new_lease(args.lease_owner, args.lease_ms, args.now));
            run.updated_at = args.now;
            run
        });
        Ok(ClaimRunResult::Claimed {
            run: claimed.expect("update_run 在 get_run 之后必然存在"),
        })
    }

    fn heartbeat_run_lease(&self, args: HeartbeatRunLeaseArgs) -> anyhow::Result<()> {
        self.update_run(&args.run_id, |mut run| {
            if run.lease.as_ref().map(|l| &l.owner) == Some(&args.lease_owner) {
                run.lease = Some(new_lease(args.lease_owner, args.lease_ms, args.now));
                run.updated_at = args.now;
            }
            run
        });
        Ok(())
    }

    fn release_run_lease(&self, args: ReleaseRunLeaseArgs) -> anyhow::Result<()> {
        self.update_run(&args.run_id, |mut run| {
            if run.lease.as_ref().map(|l| &l.owner) == Some(&args.lease_owner) {
                run.lease = None;
            }
            run
        });
        Ok(())
    }

    /// 只扫 `Running` 且 lease 已过期的 run（对齐上游）。
    fn claim_stale_runs(&self, args: ClaimStaleRunsArgs) -> anyhow::Result<Vec<RunClaim>> {
        let mut claims = Vec::new();
        let candidates: Vec<WorkflowExecution> = {
            let runs = self.inner.runs.lock().unwrap();
            runs.values()
                .filter(|r| {
                    r.status == WorkflowExecutionStatus::Running
                        && r.lease.as_ref().map(|l| l.expires_at) <= Some(args.now)
                        && r.lease.is_some()
                })
                .cloned()
                .collect()
        };
        for run in candidates {
            if claims.len() >= args.limit {
                break;
            }
            let next_lease = new_lease(args.lease_owner.clone(), args.lease_ms, args.now);
            if let Some(claimed) = self.update_run(&run.run_id, |mut current| {
                current.lease = Some(next_lease.clone());
                current.updated_at = args.now;
                current
            }) {
                claims.push(RunClaim {
                    run: claimed,
                    lease: next_lease,
                });
            }
        }
        Ok(claims)
    }

    fn schedule_timer(&self, args: ScheduleTimerArgs) -> anyhow::Result<()> {
        self.inner.timers.lock().unwrap().insert(
            signal_key(&args.run_id, &args.signal_id),
            TimerRecord {
                timer: TimerWakeup {
                    run_id: args.run_id.clone(),
                    workflow_id: args.workflow_id,
                    workflow_version: args.workflow_version,
                    wake_at: args.wake_at,
                    signal_id: args.signal_id,
                },
                lease: None,
            },
        );
        self.update_run(&args.run_id, |mut run| {
            run.wake_at = Some(args.wake_at);
            run.updated_at = args.now;
            run
        });
        Ok(())
    }

    /// 认领 = 给 timer 挂 lease，**不删**。删除发生在 `deliver_signal`。
    fn claim_due_timers(&self, args: ClaimDueTimersArgs) -> anyhow::Result<Vec<TimerWakeup>> {
        let mut timers = self.inner.timers.lock().unwrap();
        let mut due = Vec::new();
        for (key, record) in timers.iter_mut() {
            if due.len() >= args.limit {
                break;
            }
            if record.timer.wake_at > args.now {
                continue;
            }
            if !can_claim(record.lease.as_ref(), &args.lease_owner, args.now) {
                continue;
            }
            record.lease = Some(new_lease(args.lease_owner.clone(), args.lease_ms, args.now));
            due.push(record.timer.clone());
            let _ = key;
        }
        Ok(due)
    }

    fn deliver_signal(&self, args: DeliverSignalArgs) -> anyhow::Result<DeliverSignalResult> {
        let Some(run) = self.get_run(&args.run_id) else {
            return Ok(DeliverSignalResult::NotFound);
        };
        let key = signal_key(&args.run_id, &args.delivery.signal_id);
        if self.inner.signal_deliveries.lock().unwrap().contains(&key) {
            return Ok(DeliverSignalResult::Duplicate { run });
        }
        if !is_run_waiting_for_signal(&run, &args.delivery) {
            return Ok(DeliverSignalResult::NotWaiting { run });
        }
        self.inner
            .signal_deliveries
            .lock()
            .unwrap()
            .insert(key.clone());
        // 信号已投，对应的 timer 不再需要。
        self.inner
            .timers
            .lock()
            .unwrap()
            .remove(&signal_key(&args.run_id, &args.delivery.signal_id));
        let updated = self.update_run(&args.run_id, |mut current| {
            // 回到 Queued：待认领再驱一次，处理 payload。
            current.status = WorkflowExecutionStatus::Queued;
            current.waiting_for = None;
            current.pending_approval = None;
            current.wake_at = None;
            current.updated_at = args.now;
            current
        });
        Ok(DeliverSignalResult::Delivered {
            run: updated.expect("get_run 之后必然存在"),
        })
    }

    fn deliver_approval(&self, args: DeliverApprovalArgs) -> anyhow::Result<DeliverApprovalResult> {
        let Some(run) = self.get_run(&args.run_id) else {
            return Ok(DeliverApprovalResult::NotFound);
        };
        let key = signal_key(
            &args.run_id,
            &format!("approval:{}", args.approval.approval_id),
        );
        if self.inner.signal_deliveries.lock().unwrap().contains(&key) {
            return Ok(DeliverApprovalResult::Duplicate { run });
        }
        if !is_run_waiting_for_approval(&run, &args.approval) {
            return Ok(DeliverApprovalResult::NotWaiting { run });
        }
        self.inner.signal_deliveries.lock().unwrap().insert(key);
        let updated = self.update_run(&args.run_id, |mut current| {
            current.status = WorkflowExecutionStatus::Queued;
            current.waiting_for = None;
            current.pending_approval = None;
            current.wake_at = None;
            current.updated_at = args.now;
            current
        });
        Ok(DeliverApprovalResult::Delivered {
            run: updated.expect("get_run 之后必然存在"),
        })
    }

    fn upsert_schedule(&self, args: UpsertScheduleArgs) -> anyhow::Result<()> {
        self.inner.schedules.lock().unwrap().insert(
            args.schedule_id.clone(),
            ScheduleRecord {
                schedule_id: args.schedule_id,
                workflow_id: args.workflow_id,
                workflow_version: args.workflow_version,
                next_fire_at: args.next_fire_at,
                input: args.input,
                overlap_policy: args.overlap_policy,
                enabled: args.enabled,
            },
        );
        Ok(())
    }

    /// 桶按 `nextFireAt` 生成 bucketId（同一天重复 fire 由桶状态去重）。
    fn claim_due_schedule_buckets(
        &self,
        args: ClaimDueScheduleBucketsArgs,
    ) -> anyhow::Result<Vec<ScheduleBucket>> {
        let mut due = Vec::new();
        let schedules: Vec<ScheduleRecord> = {
            let s = self.inner.schedules.lock().unwrap();
            s.values().cloned().collect()
        };
        for schedule in schedules {
            if due.len() >= args.limit {
                break;
            }
            let Some(next_fire_at) = schedule.next_fire_at else {
                continue;
            };
            if !schedule.enabled || next_fire_at > args.now {
                continue;
            }
            let bucket_id: ScheduleBucketId = next_fire_at.to_string();
            let key = schedule_bucket_key(&schedule.schedule_id, &bucket_id);
            {
                let buckets = self.inner.schedule_buckets.lock().unwrap();
                match buckets.get(&key) {
                    Some(b) if b.status == BucketStatus::Started => continue,
                    Some(b) if !can_claim(b.lease.as_ref(), &args.lease_owner, args.now) => {
                        continue;
                    }
                    _ => {}
                }
            }
            let bucket = ScheduleBucket {
                schedule_id: schedule.schedule_id.clone(),
                bucket_id: bucket_id.clone(),
                workflow_id: schedule.workflow_id.clone(),
                workflow_version: schedule.workflow_version.clone(),
                run_id: format!(
                    "{}:{}:{}",
                    schedule.workflow_id, schedule.schedule_id, bucket_id
                ),
                fire_at: next_fire_at,
                input: schedule.input.clone(),
                overlap_policy: schedule.overlap_policy,
            };
            self.inner.schedule_buckets.lock().unwrap().insert(
                key,
                ScheduleBucketRecord {
                    bucket: bucket.clone(),
                    status: BucketStatus::Claimed,
                    lease: Some(new_lease(args.lease_owner.clone(), args.lease_ms, args.now)),
                },
            );
            due.push(bucket);
        }
        Ok(due)
    }

    fn mark_schedule_bucket_started(
        &self,
        args: MarkScheduleBucketStartedArgs,
    ) -> anyhow::Result<()> {
        let key = schedule_bucket_key(&args.schedule_id, &args.bucket_id);
        if let Some(b) = self.inner.schedule_buckets.lock().unwrap().get_mut(&key) {
            b.bucket.run_id = args.run_id;
            b.status = BucketStatus::Started;
        }
        Ok(())
    }

    /// 按 `updated_at` 倒序；cursor 是 in-memory 的 offset（不透明字符串）。
    fn list_runs(&self, args: ListRunsArgs) -> anyhow::Result<Vec<RunSummary>> {
        let runs = self.inner.runs.lock().unwrap();
        let start: usize = args
            .cursor
            .as_deref()
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        let mut all: Vec<&WorkflowExecution> = runs.values().collect();
        all.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        Ok(all
            .into_iter()
            .filter(|r| {
                args.workflow_id
                    .as_ref()
                    .map(|w| &r.workflow_id == w)
                    .unwrap_or(true)
            })
            .filter(|r| args.status.map(|s| r.status == s).unwrap_or(true))
            .skip(start)
            .take(args.limit)
            .map(to_run_summary)
            .collect())
    }

    fn get_run_timeline(&self, run_id: &str) -> anyhow::Result<Option<RunTimeline>> {
        let Some(run) = self.get_run(run_id) else {
            return Ok(None);
        };
        let events = self.read_events(ReadEventsArgs {
            run_id: run_id.to_string(),
            from_index: None,
        })?;
        Ok(Some(RunTimeline { run, events }))
    }
}

#[cfg(test)]
mod in_memory_tests {
    //! 移植上游 in-memory store 的关键语义（lease / 三态 claim / 四态投递 /
    //! catch-up 订阅 / timer 幂等）。
    use super::*;
    use aa_workflow_core::{PendingApproval, WaitForState};

    fn waiting(signal: &str) -> Option<WaitForState> {
        Some(WaitForState {
            step_id: None,
            signal_name: signal.into(),
            deadline: None,
            meta: None,
        })
    }

    fn paused_args(run_id: &str, signal: &str, now: i64) -> MarkRunPausedArgs {
        MarkRunPausedArgs {
            run_id: run_id.into(),
            waiting_for: waiting(signal),
            pending_approval: None,
            wake_at: None,
            now,
        }
    }

    fn created(run_id: &str, now: i64) -> CreateRunArgs {
        CreateRunArgs {
            run_id: run_id.into(),
            workflow_id: "wf".into(),
            workflow_version: None,
            input: serde_json::json!({}),
            now,
        }
    }

    /// create 幂等 + 初始态是 `Queued`（不是 `Running`——runtime 层特有）。
    #[test]
    fn create_run_is_idempotent_and_starts_queued() {
        let s = in_memory_execution_store();
        let first = s.create_run(created("r1", 0)).unwrap();
        let second = s.create_run(created("r1", 5)).unwrap();

        assert!(matches!(first, CreateRunResult::Created { .. }));
        assert!(matches!(second, CreateRunResult::Existing { .. }));
        match first {
            CreateRunResult::Created { run } => {
                assert_eq!(run.status, WorkflowExecutionStatus::Queued);
                assert_eq!(run.created_at, 0);
            }
            _ => unreachable!(),
        }
    }

    /// claim 三态：NotFound / 终态不可认领 / 被他人持有不可认领。
    #[test]
    fn claim_run_three_states() {
        let s = in_memory_execution_store();
        // NotFound
        assert!(matches!(
            s.claim_run(ClaimRunArgs {
                run_id: "nope".into(),
                lease_owner: "w1".into(),
                lease_ms: 100,
                now: 0,
            })
            .unwrap(),
            ClaimRunResult::NotFound
        ));

        // 终态 → NotClaimable
        s.create_run(created("fin", 0)).unwrap();
        s.mark_run_finished(MarkRunFinishedArgs {
            run_id: "fin".into(),
            output: serde_json::json!({}),
            now: 1,
        })
        .unwrap();
        assert!(matches!(
            s.claim_run(ClaimRunArgs {
                run_id: "fin".into(),
                lease_owner: "w1".into(),
                lease_ms: 100,
                now: 2,
            })
            .unwrap(),
            ClaimRunResult::NotClaimable { .. }
        ));

        // 被他人持有（lease 未过期）→ NotClaimable；过期后可被别人认领
        s.create_run(created("r", 0)).unwrap();
        s.claim_run(ClaimRunArgs {
            run_id: "r".into(),
            lease_owner: "w1".into(),
            lease_ms: 100,
            now: 0,
        })
        .unwrap();
        assert!(matches!(
            s.claim_run(ClaimRunArgs {
                run_id: "r".into(),
                lease_owner: "w2".into(),
                lease_ms: 100,
                now: 50,
            })
            .unwrap(),
            ClaimRunResult::NotClaimable { .. }
        ));
        match s
            .claim_run(ClaimRunArgs {
                run_id: "r".into(),
                lease_owner: "w2".into(),
                lease_ms: 100,
                now: 101, // w1 的 lease 已过期
            })
            .unwrap()
        {
            ClaimRunResult::Claimed { run } => {
                assert_eq!(run.status, WorkflowExecutionStatus::Running);
                assert_eq!(run.lease.as_ref().unwrap().owner, "w2");
            }
            _ => panic!("lease 过期后应可被别人认领"),
        }
    }

    /// heartbeat 只为持有者续租；release 只释放自己的。
    #[test]
    fn heartbeat_and_release_are_owner_scoped() {
        let s = in_memory_execution_store();
        s.create_run(created("r", 0)).unwrap();
        s.claim_run(ClaimRunArgs {
            run_id: "r".into(),
            lease_owner: "w1".into(),
            lease_ms: 100,
            now: 0,
        })
        .unwrap();

        // 别人 heartbeat 不生效
        s.heartbeat_run_lease(HeartbeatRunLeaseArgs {
            run_id: "r".into(),
            lease_owner: "w2".into(),
            lease_ms: 100,
            now: 10,
        })
        .unwrap();
        let run = s.load_run("r").unwrap().unwrap();
        assert_eq!(run.lease.as_ref().unwrap().expires_at, 100);

        // 持有者 heartbeat 续租
        s.heartbeat_run_lease(HeartbeatRunLeaseArgs {
            run_id: "r".into(),
            lease_owner: "w1".into(),
            lease_ms: 100,
            now: 30,
        })
        .unwrap();
        let run = s.load_run("r").unwrap().unwrap();
        assert_eq!(run.lease.as_ref().unwrap().expires_at, 130);

        // 别人 release 不生效；持有者 release 生效
        s.release_run_lease(ReleaseRunLeaseArgs {
            run_id: "r".into(),
            lease_owner: "w2".into(),
        })
        .unwrap();
        assert!(s.load_run("r").unwrap().unwrap().lease.is_some());
        s.release_run_lease(ReleaseRunLeaseArgs {
            run_id: "r".into(),
            lease_owner: "w1".into(),
        })
        .unwrap();
        assert!(s.load_run("r").unwrap().unwrap().lease.is_none());
    }

    /// mark_run_paused 挂起并让出 lease；mark_run_finished 清投影。
    #[test]
    fn mark_paused_releases_lease_and_projects_waits() {
        let s = in_memory_execution_store();
        s.create_run(created("r", 0)).unwrap();
        s.claim_run(ClaimRunArgs {
            run_id: "r".into(),
            lease_owner: "w1".into(),
            lease_ms: 100,
            now: 0,
        })
        .unwrap();

        s.mark_run_paused(paused_args("r", "payment", 1)).unwrap();
        let run = s.load_run("r").unwrap().unwrap();
        assert_eq!(run.status, WorkflowExecutionStatus::Paused);
        assert!(run.lease.is_none(), "挂起即让出 lease");
        assert_eq!(run.waiting_for.as_ref().unwrap().signal_name, "payment");

        s.mark_run_finished(MarkRunFinishedArgs {
            run_id: "r".into(),
            output: serde_json::json!({ "ok": true }),
            now: 2,
        })
        .unwrap();
        let run = s.load_run("r").unwrap().unwrap();
        assert_eq!(run.status, WorkflowExecutionStatus::Finished);
        assert!(run.waiting_for.is_none());
        assert_eq!(run.output, Some(serde_json::json!({ "ok": true })));
    }

    /// 投递四态 + 成功后 run 回 `Queued` + 对应 timer 被删。
    #[test]
    fn deliver_signal_four_states_and_timer_cleanup() {
        let s = in_memory_execution_store();
        s.create_run(created("r", 0)).unwrap();

        let delivery = SignalDelivery {
            signal_id: "pay-1".into(),
            step_id: None,
            name: "payment".into(),
            payload: serde_json::json!({ "id": "p1" }),
        };

        // NotWaiting：run 没在等
        assert!(matches!(
            s.deliver_signal(DeliverSignalArgs {
                run_id: "r".into(),
                delivery: delivery.clone(),
                now: 1,
            })
            .unwrap(),
            DeliverSignalResult::NotWaiting { .. }
        ));

        // 挂起等待 + 登记 timer
        s.mark_run_paused(paused_args("r", "payment", 1)).unwrap();
        s.schedule_timer(ScheduleTimerArgs {
            run_id: "r".into(),
            workflow_id: "wf".into(),
            workflow_version: None,
            wake_at: 999,
            signal_id: "pay-1".into(),
            now: 1,
        })
        .unwrap();

        // Delivered：run 回 Queued，投影清除
        match s
            .deliver_signal(DeliverSignalArgs {
                run_id: "r".into(),
                delivery: delivery.clone(),
                now: 2,
            })
            .unwrap()
        {
            DeliverSignalResult::Delivered { run } => {
                assert_eq!(run.status, WorkflowExecutionStatus::Queued);
                assert!(run.waiting_for.is_none());
                assert!(run.wake_at.is_none(), "信号已投，timer 投影清除");
            }
            _ => panic!("应 Delivered"),
        }

        // Duplicate：同 signalId 幂等
        assert!(matches!(
            s.deliver_signal(DeliverSignalArgs {
                run_id: "r".into(),
                delivery: delivery.clone(),
                now: 3,
            })
            .unwrap(),
            DeliverSignalResult::Duplicate { .. }
        ));

        // timer 已被 deliver_signal 删除
        assert!(
            s.claim_due_timers(ClaimDueTimersArgs {
                now: 1000,
                limit: 10,
                lease_owner: "sweep".into(),
                lease_ms: 100,
            })
            .unwrap()
            .is_empty()
        );
    }

    /// 审批投递走 `pending_approval` 投影。
    #[test]
    fn deliver_approval_by_approval_id() {
        let s = in_memory_execution_store();
        s.create_run(created("r", 0)).unwrap();
        s.mark_run_paused(MarkRunPausedArgs {
            run_id: "r".into(),
            waiting_for: None,
            pending_approval: Some(PendingApproval {
                step_id: None,
                approval_id: "a-1".into(),
                title: "Approve?".into(),
                description: None,
                meta: None,
            }),
            wake_at: None,
            now: 0,
        })
        .unwrap();

        let result = s
            .deliver_approval(DeliverApprovalArgs {
                run_id: "r".into(),
                approval: ApprovalResult {
                    approved: true,
                    approval_id: "a-1".into(),
                    feedback: None,
                },
                now: 1,
            })
            .unwrap();
        assert!(matches!(result, DeliverApprovalResult::Delivered { .. }));
        let run = s.load_run("r").unwrap().unwrap();
        assert_eq!(run.status, WorkflowExecutionStatus::Queued);
        assert!(run.pending_approval.is_none());
    }

    /// timer 认领不移除（挂 lease 防重复），事件投递后由 deliver_signal 删。
    #[test]
    fn claim_due_timers_lease_not_remove() {
        let s = in_memory_execution_store();
        s.create_run(created("r", 0)).unwrap();
        s.schedule_timer(ScheduleTimerArgs {
            run_id: "r".into(),
            workflow_id: "wf".into(),
            workflow_version: None,
            wake_at: 10,
            signal_id: "t-1".into(),
            now: 0,
        })
        .unwrap();

        // sweep1 认领（挂 lease）
        let due = s
            .claim_due_timers(ClaimDueTimersArgs {
                now: 10,
                limit: 10,
                lease_owner: "sweep1".into(),
                lease_ms: 100,
            })
            .unwrap();
        assert_eq!(due.len(), 1);

        // sweep2 在 lease 未过期时认领不到（防重复投递）
        assert!(
            s.claim_due_timers(ClaimDueTimersArgs {
                now: 50,
                limit: 10,
                lease_owner: "sweep2".into(),
                lease_ms: 100,
            })
            .unwrap()
            .is_empty()
        );

        // sweep1 的 lease 过期后可再认领
        let due = s
            .claim_due_timers(ClaimDueTimersArgs {
                now: 200,
                limit: 10,
                lease_owner: "sweep1".into(),
                lease_ms: 100,
            })
            .unwrap();
        assert_eq!(due.len(), 1);
    }

    /// stale 扫描：只捡 `Running` 且 lease 过期的。
    #[test]
    fn claim_stale_runs_picks_up_expired_running() {
        let s = in_memory_execution_store();
        // running + lease 过期 → 捡
        s.create_run(created("stale", 0)).unwrap();
        s.claim_run(ClaimRunArgs {
            run_id: "stale".into(),
            lease_owner: "dead-worker".into(),
            lease_ms: 50,
            now: 0,
        })
        .unwrap();
        // paused → 不捡
        s.create_run(created("paused", 0)).unwrap();
        s.mark_run_paused(paused_args("paused", "x", 0)).unwrap();
        // queued 且无 lease → 不捡
        s.create_run(created("fresh", 0)).unwrap();

        let claims = s
            .claim_stale_runs(ClaimStaleRunsArgs {
                now: 100,
                limit: 10,
                lease_owner: "recovery".into(),
                lease_ms: 100,
            })
            .unwrap();
        assert_eq!(claims.len(), 1, "只应捡 stale 那一个");
        assert_eq!(claims[0].run.run_id, "stale");
        assert_eq!(claims[0].lease.owner, "recovery");
    }

    /// 订阅：先补发 `from_index` 起的存量，再收增量；退订后不再收。
    #[test]
    fn subscribe_replays_catch_up_then_live() {
        let s = in_memory_execution_store();
        s.create_run(created("r", 0)).unwrap();
        s.append_events(AppendEventsArgs {
            run_id: "r".into(),
            expected_next_index: 0,
            events: vec![
                WorkflowEvent::RunStarted {
                    ts: 0,
                    run_id: "r".into(),
                    thread_id: None,
                },
                WorkflowEvent::RunStarted {
                    ts: 1,
                    run_id: "r".into(),
                    thread_id: None,
                },
            ],
        })
        .unwrap();

        let received: Arc<Mutex<Vec<(String, u64)>>> = Default::default();
        let rx = {
            let received = received.clone();
            s.subscribe_events(
                "r",
                1, // 从 index 1 起
                Box::new(move |ev, idx| {
                    received.lock().unwrap().push((ev.type_name().into(), idx));
                }),
            )
            .unwrap()
        };

        // 补发：只有 index 1（skip 了 0）
        assert_eq!(
            received.lock().unwrap().clone(),
            vec![("RUN_STARTED".into(), 1)]
        );

        // 增量
        s.append_events(AppendEventsArgs {
            run_id: "r".into(),
            expected_next_index: 2,
            events: vec![WorkflowEvent::RunStarted {
                ts: 2,
                run_id: "r".into(),
                thread_id: None,
            }],
        })
        .unwrap();
        assert_eq!(
            received.lock().unwrap().last().unwrap().1,
            2,
            "应收到新 append 的事件"
        );

        // 退订
        rx();
        s.append_events(AppendEventsArgs {
            run_id: "r".into(),
            expected_next_index: 3,
            events: vec![WorkflowEvent::RunStarted {
                ts: 3,
                run_id: "r".into(),
                thread_id: None,
            }],
        })
        .unwrap();
        assert_eq!(received.lock().unwrap().len(), 2, "退订后不再收");
    }

    /// list_runs：按 updated_at 倒序 + 过滤 + cursor 分页。
    #[test]
    fn list_runs_sorts_filters_and_paginates() {
        let s = in_memory_execution_store();
        for (id, ts) in [("a", 10), ("b", 30), ("c", 20)] {
            s.create_run(CreateRunArgs {
                run_id: id.into(),
                workflow_id: "wf".into(),
                workflow_version: None,
                input: serde_json::json!({}),
                now: ts,
            })
            .unwrap();
        }
        let all = s
            .list_runs(ListRunsArgs {
                workflow_id: None,
                status: None,
                limit: 10,
                cursor: None,
            })
            .unwrap();
        let ids: Vec<&str> = all.iter().map(|r| r.run_id.as_str()).collect();
        assert_eq!(ids, vec!["b", "c", "a"], "按 updated_at 倒序");

        let page = s
            .list_runs(ListRunsArgs {
                workflow_id: None,
                status: None,
                limit: 1,
                cursor: Some("1".into()),
            })
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].run_id, "c");
    }

    /// delete_run 连带清 timer 与 signal 投递记录。
    #[test]
    fn delete_run_cleans_timers_and_deliveries() {
        let s = in_memory_execution_store();
        s.create_run(created("r", 0)).unwrap();
        s.mark_run_paused(paused_args("r", "payment", 0)).unwrap();
        s.schedule_timer(ScheduleTimerArgs {
            run_id: "r".into(),
            workflow_id: "wf".into(),
            workflow_version: None,
            wake_at: 10,
            signal_id: "pay-1".into(),
            now: 0,
        })
        .unwrap();
        // 先投一次（留下 signal_deliveries 记录）
        s.deliver_signal(DeliverSignalArgs {
            run_id: "r".into(),
            delivery: SignalDelivery {
                signal_id: "pay-1".into(),
                step_id: None,
                name: "payment".into(),
                payload: serde_json::json!({}),
            },
            now: 1,
        })
        .unwrap();

        s.delete_run("r", DeleteReason::Finished).unwrap();
        assert!(s.load_run("r").unwrap().is_none());
        assert!(
            s.claim_due_timers(ClaimDueTimersArgs {
                now: 1000,
                limit: 10,
                lease_owner: "sweep".into(),
                lease_ms: 100,
            })
            .unwrap()
            .is_empty()
        );
        // 删除后 signal 投递记录也清了：重新 start 同 run_id 可再投同名信号
        s.create_run(created("r", 2)).unwrap();
        s.mark_run_paused(paused_args("r", "payment", 2)).unwrap();
        let result = s
            .deliver_signal(DeliverSignalArgs {
                run_id: "r".into(),
                delivery: SignalDelivery {
                    signal_id: "pay-1".into(),
                    step_id: None,
                    name: "payment".into(),
                    payload: serde_json::json!({}),
            },
            now: 3,
        })
        .unwrap();
        assert!(
            matches!(result, DeliverSignalResult::Delivered { .. }),
            "删除 run 应连带清投递记录，否则新 run 收不到同名信号"
        );
    }
}

/// 把 [`store_contract::run_store_contract`] 跑在 in-memory 实现上。
///
/// 对齐上游 `in-memory-store.test.ts`——那边整个文件就 8 行，纯粹把契约套件
/// 跑在内存实现上，没有自己的用例。
#[cfg(test)]
mod contract {
    use super::*;
    use crate::store_contract::run_store_contract;

    #[test]
    fn in_memory_satisfies_execution_store_contract() {
        run_store_contract("in-memory", || {
            Arc::new(InMemoryExecutionStore::default())
        });
    }
}
