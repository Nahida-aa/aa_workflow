//! `FileExecutionStore`：文件落盘的 [`WorkflowExecutionStore`] 实现。
//!
//! 语义逐方法对齐 `workflow-runtime` 的 `InMemoryExecutionStore`
//! （`in_memory_store.rs`），落盘手法（CAS 追加 / 原子写）照 `examples/shared` 的
//! `FileRunStore`。
//!
//! 并发边界见 crate 文档：**单进程正确**（进程内 `Mutex` 串行化读-改-写），
//! 多 worker 请用 `workflow-store-sqlx-postgres`。

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use aa_workflow_core::{DeleteReason, RunState, StoreError};
use aa_workflow_runtime::run_store_adapter::{WorkflowExecutionStore, WorkflowRunStoreAdapterStore};
use aa_workflow_runtime::types::*;

// ============================================================
// 落盘的小工具
// ============================================================

fn map_io(e: std::io::Error) -> StoreError {
    StoreError::Io(e.to_string())
}

/// 读一个 JSON 文件；**文件不存在**返回 `None`（不是错误）。
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>, StoreError> {
    match fs::read_to_string(path) {
        Ok(raw) => serde_json::from_str(&raw)
            .map(Some)
            .map_err(|e| StoreError::Io(format!("{} 解析失败: {e}", path.display()))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(map_io(e)),
    }
}

/// 原子写：先写 `.tmp` 再 rename（同目录 rename 在同一文件系统上是原子的）。
fn write_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), StoreError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(map_io)?;
    }
    let body = serde_json::to_string_pretty(value)
        .map_err(|e| StoreError::Io(format!("序列化失败: {e}")))?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, body).map_err(map_io)?;
    fs::rename(&tmp, path).map_err(map_io)?;
    Ok(())
}

/// lease 是否可认领（对齐 `in_memory_store.rs` 的 `can_claim`）：
/// 无 lease / 是自己的 / 已过期。
fn can_claim(existing: Option<&WorkflowLease>, owner: &LeaseOwner, now: i64) -> bool {
    match existing {
        None => true,
        Some(l) => l.owner == *owner || l.expires_at <= now,
    }
}

fn new_lease(owner: LeaseOwner, lease_ms: i64, now: i64) -> WorkflowLease {
    WorkflowLease {
        owner,
        expires_at: now + lease_ms,
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

/// 从 core 的 `RunState` 投影出 runtime 的 `WorkflowExecution`
/// （对齐 `execution_from_run_state`）。
///
/// `wake_at` 由 timer 等待推导：`waiting_for.signal_name == "__timer"` 时取 `deadline`。
/// `lease` 不在这里——它由调用方传（因为 lease 是 runtime 独占的概念，
/// core 侧的 `save_run_state` 不该覆盖它）。
fn execution_from_run_state(state: &RunState, lease: Option<WorkflowLease>) -> WorkflowExecution {
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
        lease,
        created_at: state.created_at,
        updated_at: state.updated_at,
    }
}

/// run 是否在等这个信号（对齐 `is_run_waiting_for_signal`）。
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

fn is_run_waiting_for_approval(run: &WorkflowExecution, approval: &ApprovalResult) -> bool {
    run.pending_approval
        .as_ref()
        .map(|p| p.approval_id == approval.approval_id)
        .unwrap_or(false)
}

// ============================================================
// 落盘的中间结构
// ============================================================

/// timer 记录（含 lease）。**认领是挂 lease，不是删除**——所以 lease 要落盘。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TimerRecord {
    timer: TimerWakeup,
    lease_owner: Option<LeaseOwner>,
    lease_expires_at: Option<i64>,
}

impl TimerRecord {
    fn lease(&self) -> Option<WorkflowLease> {
        match (&self.lease_owner, self.lease_expires_at) {
            (Some(owner), Some(expires_at)) => Some(WorkflowLease {
                owner: owner.clone(),
                expires_at,
            }),
            _ => None,
        }
    }
}

/// schedule 桶记录（含 lease / status），对齐 `ScheduleBucketRecord`。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BucketRecord {
    bucket: ScheduleBucket,
    /// `claimed` / `started`
    status: String,
    lease_owner: Option<LeaseOwner>,
    lease_expires_at: Option<i64>,
}

impl BucketRecord {
    fn lease(&self) -> Option<WorkflowLease> {
        match (&self.lease_owner, self.lease_expires_at) {
            (Some(owner), Some(expires_at)) => Some(WorkflowLease {
                owner: owner.clone(),
                expires_at,
            }),
            _ => None,
        }
    }
}

/// `schedule` 定义（落盘只需这些字段）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ScheduleRecord {
    schedule_id: ScheduleId,
    workflow_id: WorkflowId,
    workflow_version: Option<WorkflowVersion>,
    next_fire_at: Option<i64>,
    input: Option<serde_json::Value>,
    overlap_policy: WorkflowOverlapPolicy,
    enabled: bool,
}

// ============================================================
// store
// ============================================================

/// 文件落盘的执行存储。
///
/// 克隆**不共享锁**（每个实例一把新锁）——契约套件要求「每条用例独立 store」，
/// 而 `Mutex` 是进程内的；要共享请用 `Arc<FileExecutionStore>`。
#[derive(Debug)]
pub struct FileExecutionStore {
    base: PathBuf,
    /// 串行化读-改-写。**只保证单进程正确**（见 crate 文档）。
    lock: Mutex<()>,
}

impl FileExecutionStore {
    pub fn new(base: impl AsRef<Path>) -> Self {
        Self {
            base: base.as_ref().to_path_buf(),
            lock: Mutex::new(()),
        }
    }

    fn run_dir(&self, run_id: &str) -> PathBuf {
        self.base.join(run_id)
    }
    fn run_state_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("run.json")
    }
    fn exec_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("exec.json")
    }
    fn events_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("events.jsonl")
    }
    fn timers_path(&self) -> PathBuf {
        self.base.join("timers.json")
    }
    fn deliveries_path(&self) -> PathBuf {
        self.base.join("deliveries.json")
    }
    fn schedules_path(&self) -> PathBuf {
        self.base.join("schedules.json")
    }
    fn buckets_path(&self) -> PathBuf {
        self.base.join("schedule_buckets.json")
    }

    fn block(&self) -> std::sync::MutexGuard<'_, ()> {
        self.lock.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 读执行记录。
    fn read_exec(&self, run_id: &str) -> Result<Option<WorkflowExecution>, StoreError> {
        read_json(&self.exec_path(run_id))
    }

    /// 写执行记录。
    fn write_exec(&self, run: &WorkflowExecution) -> Result<(), StoreError> {
        write_json(&self.exec_path(&run.run_id), run)
    }

    /// 读-改-写执行记录（不存在则 no-op，对齐 `update_run` 的语义）。
    fn update_exec(
        &self,
        run_id: &str,
        f: impl FnOnce(WorkflowExecution) -> WorkflowExecution,
    ) -> Result<Option<WorkflowExecution>, StoreError> {
        let Some(existing) = self.read_exec(run_id)? else {
            return Ok(None);
        };
        let next = f(existing);
        self.write_exec(&next)?;
        Ok(Some(next))
    }

    /// 读事件日志；文件缺失视为空日志。
    fn read_events_raw(&self, run_id: &str) -> Result<Vec<StoredWorkflowEvent>, StoreError> {
        let raw = match fs::read_to_string(self.events_path(run_id)) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(map_io(e)),
        };
        let mut out = Vec::new();
        for line in raw.lines() {
            if line.trim().is_empty() {
                continue;
            }
            out.push(
                serde_json::from_str(line)
                    .map_err(|e| StoreError::Io(format!("events.jsonl 解析失败: {e}")))?,
            );
        }
        Ok(out)
    }

    fn read_deliveries(&self) -> Result<Vec<String>, StoreError> {
        Ok(read_json(&self.deliveries_path())?.unwrap_or_default())
    }

    fn read_timers(&self) -> Result<HashMap<String, TimerRecord>, StoreError> {
        Ok(read_json(&self.timers_path())?.unwrap_or_default())
    }

    fn read_buckets(&self) -> Result<HashMap<String, BucketRecord>, StoreError> {
        Ok(read_json(&self.buckets_path())?.unwrap_or_default())
    }

    fn read_schedules(&self) -> Result<HashMap<String, ScheduleRecord>, StoreError> {
        Ok(read_json(&self.schedules_path())?.unwrap_or_default())
    }

    /// `"{run_id}:{signal_id}"`（与内存实现的 `signal_key` 同形）。
    fn signal_key(run_id: &str, signal_id: &str) -> String {
        format!("{run_id}:{signal_id}")
    }

    fn bucket_key(schedule_id: &str, bucket_id: &str) -> String {
        format!("{schedule_id}:{bucket_id}")
    }
}

// ============================================================
// 基础面：WorkflowRunStoreAdapterStore
// ============================================================

impl WorkflowRunStoreAdapterStore for FileExecutionStore {
    fn load_run_state(&self, run_id: &str) -> anyhow::Result<Option<RunState>> {
        Ok(read_json(&self.run_state_path(run_id))?)
    }

    fn save_run_state(&self, args: SaveRunStateArgs) -> anyhow::Result<()> {
        let _g = self.block();
        let state = &args.state;
        write_json(&self.run_state_path(&state.run_id), state)?;

        // 同步投影到 exec.json（对齐 `execution_from_run_state`），
        // **保留已有 lease**——lease 由 claim/heartbeat/release 独占管理，
        // core 侧的 save 不该碰它（与 sqlx 版同一处理）。
        let lease = self.read_exec(&state.run_id)?.and_then(|e| e.lease);
        let exec = execution_from_run_state(state, lease);
        self.write_exec(&exec)?;
        Ok(())
    }

    fn delete_run(&self, run_id: &str, _reason: DeleteReason) -> anyhow::Result<()> {
        let _g = self.block();
        // 连带清理该 run 的 timer 与投递记录（对齐内存实现 / sqlx 版）。
        let mut timers = self.read_timers()?;
        timers.retain(|_, t| t.timer.run_id != run_id);
        write_json(&self.timers_path(), &timers)?;

        let prefix = format!("{run_id}:");
        let mut deliveries = self.read_deliveries()?;
        deliveries.retain(|k| !k.starts_with(&prefix));
        write_json(&self.deliveries_path(), &deliveries)?;

        let dir = self.run_dir(run_id);
        match fs::remove_dir_all(&dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(map_io(e).into()),
        }
    }

    fn append_events(&self, args: AppendEventsArgs) -> anyhow::Result<AppendEventsResult> {
        let _g = self.block();
        let existing = self.read_events_raw(&args.run_id)?;
        let actual = existing.len() as u64;
        if actual != args.expected_next_index {
            return Err(anyhow::Error::new(StoreError::Conflict {
                run_id: args.run_id.clone(),
                expected: args.expected_next_index as usize,
                actual: actual as usize,
            }));
        }

        // 逐条追加（索引连续），整批一次原子写。
        let mut all = existing;
        let mut next_index = args.expected_next_index;
        for event in args.events {
            all.push(StoredWorkflowEvent {
                run_id: args.run_id.clone(),
                event_index: next_index,
                event_type: event.type_name().to_string(),
                step_id: event.step_id().map(str::to_string),
                created_at: event.ts(),
                event,
            });
            next_index += 1;
        }

        let mut body = String::new();
        for ev in &all {
            body.push_str(
                &serde_json::to_string(ev)
                    .map_err(|e| StoreError::Io(format!("事件序列化失败: {e}")))?,
            );
            body.push('\n');
        }
        let path = self.events_path(&args.run_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(map_io)?;
        }
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, body).map_err(map_io)?;
        fs::rename(&tmp, &path).map_err(map_io)?;

        Ok(AppendEventsResult { next_index })
    }

    fn read_events(&self, args: ReadEventsArgs) -> anyhow::Result<Vec<StoredWorkflowEvent>> {
        let _g = self.block();
        let from = args.from_index.unwrap_or(0);
        Ok(self
            .read_events_raw(&args.run_id)?
            .into_iter()
            .filter(|e| e.event_index >= from)
            .collect())
    }

    // subscribe_events：文件介质做实时推送意义不大（要轮询），用 trait 的默认
    // 实现返回 None。与 -sqlx-postgres 一致。
}

// ============================================================
// 扩展面：WorkflowExecutionStore
// ============================================================

impl WorkflowExecutionStore for FileExecutionStore {
    fn create_run(&self, args: CreateRunArgs) -> anyhow::Result<CreateRunResult> {
        let _g = self.block();
        // 幂等：已存在则原样返回，**不覆盖**已有 input（契约断言这点）。
        if let Some(existing) = self.read_exec(&args.run_id)? {
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
        self.write_exec(&run)?;
        Ok(CreateRunResult::Created { run })
    }

    fn load_run(&self, run_id: &str) -> anyhow::Result<Option<WorkflowExecution>> {
        Ok(self.read_exec(run_id)?)
    }

    fn load_execution(&self, run_id: &str) -> anyhow::Result<Option<LoadedExecution>> {
        let Some(run) = self.read_exec(run_id)? else {
            return Ok(None);
        };
        let events = self.read_events(ReadEventsArgs {
            run_id: run_id.to_string(),
            from_index: None,
        })?;
        Ok(Some(LoadedExecution { run, events }))
    }

    fn mark_run_paused(&self, args: MarkRunPausedArgs) -> anyhow::Result<()> {
        let _g = self.block();
        self.update_exec(&args.run_id, |mut run| {
            run.status = WorkflowExecutionStatus::Paused;
            run.waiting_for = args.waiting_for;
            run.pending_approval = args.pending_approval;
            run.wake_at = args.wake_at;
            run.lease = None; // 挂起即让出 lease
            run.updated_at = args.now;
            run
        })?;
        Ok(())
    }

    fn mark_run_finished(&self, args: MarkRunFinishedArgs) -> anyhow::Result<()> {
        let _g = self.block();
        self.update_exec(&args.run_id, |mut run| {
            run.status = WorkflowExecutionStatus::Finished;
            run.output = Some(args.output);
            run.waiting_for = None;
            run.pending_approval = None;
            run.wake_at = None;
            run.lease = None;
            run.updated_at = args.now;
            run
        })?;
        Ok(())
    }

    fn mark_run_errored(&self, args: MarkRunErroredArgs) -> anyhow::Result<()> {
        let _g = self.block();
        // `args.code` 不进 execution（只进 RUN_ERRORED 事件），与内存实现一致。
        self.update_exec(&args.run_id, |mut run| {
            run.status = WorkflowExecutionStatus::Errored;
            run.error = Some(args.error);
            run.waiting_for = None;
            run.pending_approval = None;
            run.wake_at = None;
            run.lease = None;
            run.updated_at = args.now;
            run
        })?;
        Ok(())
    }

    fn claim_run(&self, args: ClaimRunArgs) -> anyhow::Result<ClaimRunResult> {
        let _g = self.block();
        let Some(existing) = self.read_exec(&args.run_id)? else {
            return Ok(ClaimRunResult::NotFound);
        };
        if existing.status.is_terminal()
            || !can_claim(existing.lease.as_ref(), &args.lease_owner, args.now)
        {
            return Ok(ClaimRunResult::NotClaimable { run: existing });
        }
        let lease = new_lease(args.lease_owner, args.lease_ms, args.now);
        let claimed = self.update_exec(&args.run_id, |mut run| {
            run.status = WorkflowExecutionStatus::Running;
            run.lease = Some(lease);
            run.updated_at = args.now;
            run
        })?;
        Ok(ClaimRunResult::Claimed {
            run: claimed.expect("read_exec 之后必然存在"),
        })
    }

    fn heartbeat_run_lease(&self, args: HeartbeatRunLeaseArgs) -> anyhow::Result<()> {
        let _g = self.block();
        // 非持有者续租是**静默 no-op**（对齐上游；契约测试明确断言这点）。
        self.update_exec(&args.run_id, |mut run| {
            if run.lease.as_ref().map(|l| &l.owner) == Some(&args.lease_owner) {
                run.lease = Some(new_lease(args.lease_owner, args.lease_ms, args.now));
                run.updated_at = args.now;
            }
            run
        })?;
        Ok(())
    }

    fn release_run_lease(&self, args: ReleaseRunLeaseArgs) -> anyhow::Result<()> {
        let _g = self.block();
        self.update_exec(&args.run_id, |mut run| {
            if run.lease.as_ref().map(|l| &l.owner) == Some(&args.lease_owner) {
                run.lease = None;
            }
            run
        })?;
        Ok(())
    }

    /// 只扫 `Running` 且 lease 已过期的 run。
    fn claim_stale_runs(
        &self,
        args: ClaimStaleRunsArgs,
    ) -> anyhow::Result<Vec<RunClaim>> {
        let _g = self.block();
        let mut candidates: Vec<WorkflowExecution> = Vec::new();
        let dirs = match fs::read_dir(&self.base) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(map_io(e).into()),
        };
        for entry in dirs.flatten() {
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let run_id = entry.file_name().to_string_lossy().to_string();
            if let Some(run) = self.read_exec(&run_id)? {
                let stale = run.status == WorkflowExecutionStatus::Running
                    && run
                        .lease
                        .as_ref()
                        .map(|l| l.expires_at <= args.now)
                        .unwrap_or(false);
                if stale {
                    candidates.push(run);
                }
            }
        }
        // 稳定顺序（目录遍历顺序不保证），便于测试与使用。
        candidates.sort_by_key(|r| r.updated_at);

        let mut claims = Vec::new();
        for run in candidates {
            if claims.len() >= args.limit {
                break;
            }
            let lease = new_lease(args.lease_owner.clone(), args.lease_ms, args.now);
            if let Some(claimed) = self.update_exec(&run.run_id, |mut current| {
                current.lease = Some(lease.clone());
                current.updated_at = args.now;
                current
            })? {
                claims.push(RunClaim {
                    run: claimed,
                    lease,
                });
            }
        }
        Ok(claims)
    }

    fn schedule_timer(&self, args: ScheduleTimerArgs) -> anyhow::Result<()> {
        let _g = self.block();
        let mut timers = self.read_timers()?;
        // 幂等：同 (run_id, signal_id) 覆盖（并清掉旧 lease）。
        timers.insert(
            Self::signal_key(&args.run_id, &args.signal_id),
            TimerRecord {
                timer: TimerWakeup {
                    run_id: args.run_id.clone(),
                    workflow_id: args.workflow_id,
                    workflow_version: args.workflow_version,
                    wake_at: args.wake_at,
                    signal_id: args.signal_id,
                },
                lease_owner: None,
                lease_expires_at: None,
            },
        );
        write_json(&self.timers_path(), &timers)?;
        self.update_exec(&args.run_id, |mut run| {
            run.wake_at = Some(args.wake_at);
            run.updated_at = args.now;
            run
        })?;
        Ok(())
    }

    /// 认领 = 给 timer 挂 lease，**不删**。删除发生在 `deliver_signal`。
    fn claim_due_timers(
        &self,
        args: ClaimDueTimersArgs,
    ) -> anyhow::Result<Vec<TimerWakeup>> {
        let _g = self.block();
        let mut timers = self.read_timers()?;
        let mut due = Vec::new();
        // 稳定顺序：按 wake_at 升序，避免目录/HashMap 顺序影响结果。
        let mut keys: Vec<String> = timers.keys().cloned().collect();
        keys.sort_by_key(|k| timers[k].timer.wake_at);

        for key in keys {
            if due.len() >= args.limit {
                break;
            }
            let Some(record) = timers.get(&key) else {
                continue;
            };
            if record.timer.wake_at > args.now {
                continue;
            }
            if !can_claim(record.lease().as_ref(), &args.lease_owner, args.now) {
                continue;
            }
            let record = timers.get_mut(&key).expect("刚查过");
            record.lease_owner = Some(args.lease_owner.clone());
            record.lease_expires_at = Some(args.now + args.lease_ms);
            due.push(record.timer.clone());
        }
        write_json(&self.timers_path(), &timers)?;
        Ok(due)
    }

    fn deliver_signal(
        &self,
        args: DeliverSignalArgs,
    ) -> anyhow::Result<DeliverSignalResult> {
        let _g = self.block();
        let Some(run) = self.read_exec(&args.run_id)? else {
            return Ok(DeliverSignalResult::NotFound);
        };
        let key = Self::signal_key(&args.run_id, &args.delivery.signal_id);
        let deliveries = self.read_deliveries()?;
        if deliveries.contains(&key) {
            return Ok(DeliverSignalResult::Duplicate { run });
        }
        if !is_run_waiting_for_signal(&run, &args.delivery) {
            return Ok(DeliverSignalResult::NotWaiting { run });
        }

        let mut deliveries = deliveries;
        deliveries.push(key);
        write_json(&self.deliveries_path(), &deliveries)?;

        // 信号已投，对应 timer 不再需要。
        let mut timers = self.read_timers()?;
        timers.remove(&Self::signal_key(&args.run_id, &args.delivery.signal_id));
        write_json(&self.timers_path(), &timers)?;

        let updated = self.update_exec(&args.run_id, |mut current| {
            // 回到 Queued：待认领再驱一次，处理 payload。
            current.status = WorkflowExecutionStatus::Queued;
            current.waiting_for = None;
            current.pending_approval = None;
            current.wake_at = None;
            current.updated_at = args.now;
            current
        })?;
        Ok(DeliverSignalResult::Delivered {
            run: updated.expect("read_exec 之后必然存在"),
        })
    }

    fn deliver_approval(
        &self,
        args: DeliverApprovalArgs,
    ) -> anyhow::Result<DeliverApprovalResult> {
        let _g = self.block();
        let Some(run) = self.read_exec(&args.run_id)? else {
            return Ok(DeliverApprovalResult::NotFound);
        };
        // approval 的幂等键与 signal 共用一套，用 `approval:` 前缀区分。
        let key = Self::signal_key(
            &args.run_id,
            &format!("approval:{}", args.approval.approval_id),
        );
        let deliveries = self.read_deliveries()?;
        if deliveries.contains(&key) {
            return Ok(DeliverApprovalResult::Duplicate { run });
        }
        if !is_run_waiting_for_approval(&run, &args.approval) {
            return Ok(DeliverApprovalResult::NotWaiting { run });
        }

        let mut deliveries = deliveries;
        deliveries.push(key);
        write_json(&self.deliveries_path(), &deliveries)?;

        let updated = self.update_exec(&args.run_id, |mut current| {
            current.status = WorkflowExecutionStatus::Queued;
            current.waiting_for = None;
            current.pending_approval = None;
            current.wake_at = None;
            current.updated_at = args.now;
            current
        })?;
        Ok(DeliverApprovalResult::Delivered {
            run: updated.expect("read_exec 之后必然存在"),
        })
    }

    fn upsert_schedule(&self, args: UpsertScheduleArgs) -> anyhow::Result<()> {
        let _g = self.block();
        let mut schedules = self.read_schedules()?;
        schedules.insert(
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
        write_json(&self.schedules_path(), &schedules)?;
        Ok(())
    }

    /// 桶按 `next_fire_at` 生成 `bucket_id`（同一天重复 fire 由桶状态去重）。
    fn claim_due_schedule_buckets(
        &self,
        args: ClaimDueScheduleBucketsArgs,
    ) -> anyhow::Result<Vec<ScheduleBucket>> {
        let _g = self.block();
        let schedules = self.read_schedules()?;
        let mut buckets = self.read_buckets()?;
        let mut due = Vec::new();

        let mut ids: Vec<String> = schedules.keys().cloned().collect();
        ids.sort();

        for id in ids {
            if due.len() >= args.limit {
                break;
            }
            let Some(schedule) = schedules.get(&id) else {
                continue;
            };
            let Some(next_fire_at) = schedule.next_fire_at else {
                continue;
            };
            if !schedule.enabled || next_fire_at > args.now {
                continue;
            }
            let bucket_id = next_fire_at.to_string();
            let key = Self::bucket_key(&schedule.schedule_id, &bucket_id);

            if let Some(existing) = buckets.get(&key) {
                if existing.status == "started" {
                    continue;
                }
                if !can_claim(existing.lease().as_ref(), &args.lease_owner, args.now) {
                    continue;
                }
            }

            let bucket = ScheduleBucket {
                schedule_id: schedule.schedule_id.clone(),
                bucket_id: bucket_id.clone(),
                workflow_id: schedule.workflow_id.clone(),
                workflow_version: schedule.workflow_version.clone(),
                // run_id 由三者推导，天然幂等（对齐内存实现 / 上游）。
                run_id: format!(
                    "{}:{}:{}",
                    schedule.workflow_id, schedule.schedule_id, bucket_id
                ),
                fire_at: next_fire_at,
                input: schedule.input.clone(),
                overlap_policy: schedule.overlap_policy,
            };
            buckets.insert(
                key,
                BucketRecord {
                    bucket: bucket.clone(),
                    status: "claimed".to_string(),
                    lease_owner: Some(args.lease_owner.clone()),
                    lease_expires_at: Some(args.now + args.lease_ms),
                },
            );
            due.push(bucket);
        }
        write_json(&self.buckets_path(), &buckets)?;
        Ok(due)
    }

    fn mark_schedule_bucket_started(
        &self,
        args: MarkScheduleBucketStartedArgs,
    ) -> anyhow::Result<()> {
        let _g = self.block();
        let mut buckets = self.read_buckets()?;
        let key = Self::bucket_key(&args.schedule_id, &args.bucket_id);
        if let Some(b) = buckets.get_mut(&key) {
            b.bucket.run_id = args.run_id;
            b.status = "started".to_string();
        }
        write_json(&self.buckets_path(), &buckets)?;
        Ok(())
    }

    /// 按 `updated_at` 倒序；cursor 用同一字段做键集分页。
    fn list_runs(&self, args: ListRunsArgs) -> anyhow::Result<Vec<RunSummary>> {
        let _g = self.block();
        let cursor: Option<i64> = args.cursor.as_deref().and_then(|c| c.parse().ok());

        let dirs = match fs::read_dir(&self.base) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(map_io(e).into()),
        };
        let mut runs: Vec<WorkflowExecution> = Vec::new();
        for entry in dirs.flatten() {
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let run_id = entry.file_name().to_string_lossy().to_string();
            if let Some(run) = self.read_exec(&run_id)? {
                runs.push(run);
            }
        }
        runs.sort_by_key(|r| std::cmp::Reverse(r.updated_at));

        Ok(runs
            .into_iter()
            .filter(|r| {
                args.workflow_id
                    .as_ref()
                    .map(|w| &r.workflow_id == w)
                    .unwrap_or(true)
            })
            .filter(|r| args.status.map(|s| r.status == s).unwrap_or(true))
            .filter(|r| cursor.map(|c| r.updated_at < c).unwrap_or(true))
            .take(args.limit)
            .map(|r| to_run_summary(&r))
            .collect())
    }

    fn get_run_timeline(&self, run_id: &str) -> anyhow::Result<Option<RunTimeline>> {
        let Some(run) = self.read_exec(run_id)? else {
            return Ok(None);
        };
        let events = self.read_events(ReadEventsArgs {
            run_id: run_id.to_string(),
            from_index: None,
        })?;
        Ok(Some(RunTimeline { run, events }))
    }
}
