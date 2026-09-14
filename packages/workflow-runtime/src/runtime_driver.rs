//! 执行所有权层的**驱动器**：认领 → 驱动 → 心跳 → 收尾，外加 sweep。
//!
//! 对齐 TanStack `workflow-runtime/src/runtime-driver.ts`（1214 行）。
//!
//! # 与上游的一处行为差异（源自 core）
//!
//! 上游 core 的 `ctx.sleep` 挂起后**立即返回**（drive 结束、结果为 paused），
//! timer 由 sweep 稍后投递、再驱动一次。我们的 core 在挂起点**自轮询**
//! （25ms，见 `engine::exec_pause`），同一次 drive 内自动恢复——因此：
//!
//! - 本 driver 的 `deliver_signal` / `deliver_timer` 仍按上游做四态 bookkeeping
//!   并追加 `StepResume`，然后**尝试认领并驱动**；若原 drive 仍存活（持有
//!   lease），认领会得到 `NotClaimable`——而存活的 drive 会在 25ms 内拾取
//!   `StepResume` 继续跑。两种形态殊途同归：信号最终被处理。
//! - serverless（drive 已死）时认领成功，由本次 drive 完成剩余工作。
//!
//! # 与上游的另两处差异
//!
//! - **telemetry**：core 无 OTel 集成，省略（上游每个 store 调用包一个 span）。
//! - **schedule materializer**：cron 表达式解析（上游
//!   `schedule-materializer.ts`，272 行）暂未移植——`next_fire_at` 由 host
//!   计算后传入 `upsert_schedule`。
//! - **workflow 注册表**：上游用异步 `load()` 闭包（为 JS 代码分割）；Rust
//!   直接持有构建好的 [`Workflow`] 值。
//!
//! # 已知约束：abort 驱动任务会泄漏心跳
//!
//! 心跳是 `tokio::spawn` 出的独立任务，`stop()` 只在 drive 正常收尾时调用。
//! 如果调用方 **abort** 一个正在驱动的任务，心跳不会被杀——它会一直续租，
//! 该 run 永远不会被 stale 扫描恢复。上游 TS 在 serverless 上没有这个问题
//! （进程死 = 一切皆死）。约束：**不要 abort 驱动任务**；要停就用
//! `cancel_run` 让 drive 以 `Aborted` 收尾（心跳照常释放）。

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::{WorkflowExecutionStore, create_run_store_adapter};
use workflow_core::{RunOptions, Workflow, WorkflowEvent, run_workflow};

use crate::types::*;

/// 默认 lease 时长（对齐上游 `DEFAULT_LEASE_MS`）。
pub const DEFAULT_LEASE_MS: i64 = 30_000;
/// 默认 sweep 每类上限（对齐上游 `DEFAULT_SWEEP_LIMIT`）。
pub const DEFAULT_SWEEP_LIMIT: usize = 25;
/// 默认 `min_yield_remaining_ms`（对齐上游）。
pub const DEFAULT_MIN_YIELD_REMAINING_MS: u64 = 1_000;

// ============================================================
// 注册表与配置
// ============================================================

/// 一个 workflow 的注册项（对齐上游 `WorkflowRegistration`）。
///
/// 上游用异步 `load()` 闭包（JS 代码分割）；Rust 直接持有构建好的
/// [`Workflow`] 值。上游还带 `schedules`（注册期声明 cron）——随 materializer
/// 一起暂缓。
#[derive(Clone)]
pub struct WorkflowRegistration {
    pub workflow: Workflow,
    /// resume 时按持久化版本路由（对齐上游 `previousVersions`）。
    pub previous_versions: Vec<Workflow>,
    /// 覆盖 workflow 自身的 version（对齐上游 `version?`）。
    pub version_override: Option<String>,
    /// 注册期声明的 schedule（对齐上游 `schedules?`）。
    ///
    /// 由 [`materialize_workflow_schedules`](crate::schedule_materializer::materialize_workflow_schedules)
    /// 换算成 store 里的 schedule 记录——那一步之前它们只是声明。
    pub schedules: Vec<WorkflowScheduleDefinition>,
}

/// runtime 配置（对齐上游 `WorkflowRuntimeConfig`）。
///
/// 上游的 `telemetry` 项省略（core 无 OTel 集成）。
#[derive(Clone)]
pub struct WorkflowRuntimeConfig {
    pub store: Arc<dyn WorkflowExecutionStore>,
    /// 按 `workflow_id` 注册的 workflow。
    pub workflows: HashMap<WorkflowId, WorkflowRegistration>,
    /// lease 默认时长；单次调用可用 `lease_ms` 覆盖。
    pub default_lease_ms: Option<i64>,
    /// 全局事件 fan-out（best-effort，不参与耐久执行）。
    pub publish: Option<Arc<dyn Fn(&RunId, &WorkflowEvent) + Send + Sync>>,
}

impl WorkflowRuntimeConfig {
    pub fn new(
        store: Arc<dyn WorkflowExecutionStore>,
        workflows: HashMap<WorkflowId, WorkflowRegistration>,
    ) -> Self {
        Self {
            store,
            workflows,
            default_lease_ms: None,
            publish: None,
        }
    }
}

// ============================================================
// 调用参数与结果
// ============================================================

/// 单次驱动的结果类别（对齐上游 `WorkflowRuntimeRunResult['kind']`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RunResultKind {
    /// 跑完了。
    Completed,
    /// 停在挂起点。
    Paused,
    /// 出错或被中止。
    Errored,
    /// 仍在跑（本次 drive 没有驱到终态，或被别的 worker 持有）。
    Running,
    /// run 存在但不可认领（被别人持有 / 非可跑状态）。
    NotClaimable,
    /// run 不存在。
    NotFound,
    /// 同 `signal_id` 的重复投递（幂等 no-op）。
    Duplicate,
    /// run 没在等这个信号 / 审批。
    NotWaiting,
}

/// 单次驱动 / 投递的结果（对齐上游 `WorkflowRuntimeRunResult`）。
#[derive(Debug, Clone)]
pub struct RunResult {
    pub kind: RunResultKind,
    pub run_id: RunId,
    pub workflow_id: Option<WorkflowId>,
    pub run: Option<WorkflowExecution>,
    /// 本次 drive 观测到的事件（`include_events = false` 时为空）。
    pub events: Vec<WorkflowEvent>,
    /// 观测到的事件总数（含被 `max_events` 截断的）。
    pub event_count: usize,
    /// `true` = 事件列表因 `max_events` 被截断。
    pub events_truncated: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub struct WorkflowRuntimeStartRunArgs {
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub input: serde_json::Value,
    /// 覆盖时钟（测试用）；缺省取当前时间。
    pub now: Option<i64>,
    pub deadline: Option<i64>,
    /// 本次 drive 的最大时长（与 [`Self::deadline`] 取较小者）。
    pub max_duration_ms: Option<i64>,
    pub min_yield_remaining_ms: Option<u64>,
    pub lease_owner: Option<LeaseOwner>,
    pub lease_ms: Option<i64>,
    /// 默认 `true`：把 drive 观测到的事件带回结果。
    pub include_events: Option<bool>,
    /// 结果里最多保留多少事件（总数仍计入 `event_count`）。
    pub max_events: Option<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct WorkflowRuntimeDeliverSignalArgs {
    pub run_id: RunId,
    /// 幂等令牌：同一 `signal_id` 重复投递 = no-op。
    pub signal_id: String,
    /// 等待点绑定的 step（可选；未指定则任意等待同名信号的 pause 均可匹配）。
    pub step_id: Option<String>,
    /// 信号名（与 `ctx.wait_for_event(name)` 一致）。
    pub name: String,
    pub payload: serde_json::Value,
    pub now: Option<i64>,
    pub lease_owner: Option<LeaseOwner>,
    pub lease_ms: Option<i64>,
    pub deadline: Option<i64>,
    pub max_duration_ms: Option<i64>,
    pub min_yield_remaining_ms: Option<u64>,
    pub include_events: Option<bool>,
    pub max_events: Option<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct WorkflowRuntimeDeliverApprovalArgs {
    pub run_id: RunId,
    pub approval: ApprovalResult,
    pub now: Option<i64>,
    pub lease_owner: Option<LeaseOwner>,
    pub lease_ms: Option<i64>,
    pub deadline: Option<i64>,
    pub max_duration_ms: Option<i64>,
    pub min_yield_remaining_ms: Option<u64>,
    pub include_events: Option<bool>,
    pub max_events: Option<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct WorkflowRuntimeSweepArgs {
    pub now: Option<i64>,
    pub deadline: Option<i64>,
    pub max_duration_ms: Option<i64>,
    pub min_yield_remaining_ms: Option<u64>,
    pub lease_owner: Option<LeaseOwner>,
    pub lease_ms: Option<i64>,
    pub max_recovered_runs: Option<usize>,
    pub max_scheduled_runs: Option<usize>,
    pub max_timers: Option<usize>,
    /// 一次性设置三类上限（单项显式值优先）。
    pub limit: Option<usize>,
    pub include_events: Option<bool>,
    pub max_events: Option<usize>,
}

/// sweep 摘要里的分类计数。
pub type KindCounts = BTreeMap<RunResultKind, usize>;

#[derive(Debug, Clone, Default)]
pub struct SweepSummary {
    pub recovered: KindCounts,
    pub scheduled: KindCounts,
    pub timers: KindCounts,
    pub event_count: usize,
    pub returned_event_count: usize,
}

#[derive(Debug, Clone, Default)]
pub struct WorkflowRuntimeSweepResult {
    pub recovered: Vec<RunResult>,
    pub scheduled: Vec<RunResult>,
    pub timers: Vec<RunResult>,
    pub summary: SweepSummary,
    /// true = 因 deadline / 上限而提前停止。
    pub deadline_reached: bool,
    /// true = 可能还有活没干完，值得再 sweep 一次。
    pub remaining_may_exist: bool,
}

// ============================================================
// runtime
// ============================================================

/// workflow runtime：认领 → 驱动 → 心跳 → 收尾 + sweep。
///
/// 对齐上游 `defineWorkflowRuntime` 返回的 `WorkflowRuntimeDefinition`。
pub struct WorkflowRuntime {
    config: WorkflowRuntimeConfig,
}

/// 构造 runtime（对齐上游 `defineWorkflowRuntime`）。
pub fn define_workflow_runtime(config: WorkflowRuntimeConfig) -> WorkflowRuntime {
    WorkflowRuntime { config }
}

static LEASE_OWNER_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 生成一次性 lease owner（对齐上游 `createLeaseOwner`：前缀 + 时间 + 随机）。
/// 随机部分这里用进程内计数器替代。
fn create_lease_owner(prefix: &str) -> LeaseOwner {
    let n = LEASE_OWNER_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = now_ms();
    format!("{prefix}:{now}:{n}")
}

fn normalize_lease_ms(v: i64) -> anyhow::Result<i64> {
    if v <= 0 {
        return Err(anyhow::anyhow!(
            "Workflow runtime leaseMs must be a positive finite number."
        ));
    }
    Ok(v)
}

fn resolve_lease_ms(config: &WorkflowRuntimeConfig, lease_ms: Option<i64>) -> anyhow::Result<i64> {
    normalize_lease_ms(
        lease_ms
            .or(config.default_lease_ms)
            .unwrap_or(DEFAULT_LEASE_MS),
    )
}

/// `deadline` 与 `max_duration_ms`（从 `started_at` 起算）取较小者。
fn resolve_runtime_deadline(
    deadline: Option<i64>,
    max_duration_ms: Option<i64>,
    started_at: i64,
) -> anyhow::Result<Option<i64>> {
    let duration_deadline = max_duration_ms.map(|d| started_at + d);
    Ok(match (deadline, duration_deadline) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    })
}

fn should_stop_for_deadline(deadline: Option<i64>, min_yield_remaining_ms: u64) -> bool {
    deadline
        .map(|d| d - now_ms() <= min_yield_remaining_ms as i64)
        .unwrap_or(false)
}

fn count_kinds(results: &[RunResult]) -> KindCounts {
    let mut map = BTreeMap::new();
    for r in results {
        *map.entry(r.kind).or_default() += 1;
    }
    map
}

impl WorkflowRuntime {
    /// 按 id 加载 workflow：注册项的 version 覆盖自身、合并 previous_versions
    /// （对齐上游 `loadWorkflow`）。
    fn load_workflow(&self, workflow_id: &WorkflowId) -> anyhow::Result<Workflow> {
        let registration = self
            .config
            .workflows
            .get(workflow_id)
            .ok_or_else(|| anyhow::anyhow!("Workflow \"{workflow_id}\" is not registered."))?;
        let mut workflow = registration.workflow.clone();
        if registration.version_override.is_some() || !registration.previous_versions.is_empty() {
            workflow.version = registration
                .version_override
                .clone()
                .or_else(|| workflow.version.clone());
            let mut prevs = workflow.previous_versions.clone();
            prevs.extend(registration.previous_versions.iter().cloned());
            workflow.previous_versions = prevs;
        }
        Ok(workflow)
    }

    fn resolve_lease(
        &self,
        lease_owner: Option<LeaseOwner>,
        lease_ms: Option<i64>,
        prefix: &str,
    ) -> anyhow::Result<(LeaseOwner, i64)> {
        let owner = lease_owner.unwrap_or_else(|| create_lease_owner(prefix));
        let ms = resolve_lease_ms(&self.config, lease_ms)?;
        Ok((owner, ms))
    }

    /// 启动一个 run：幂等创建 → 认领 → 驱动到下一个 pause / 终态。
    pub async fn start_run(&self, args: WorkflowRuntimeStartRunArgs) -> anyhow::Result<RunResult> {
        let started_at = now_ms();
        let now = args.now.unwrap_or_else(now_ms);
        let deadline = resolve_runtime_deadline(args.deadline, args.max_duration_ms, started_at)?;
        let (lease_owner, lease_ms) = self.resolve_lease(
            args.lease_owner,
            args.lease_ms,
            &format!("runtime:{}", args.run_id),
        )?;
        let workflow = self.load_workflow(&args.workflow_id)?;
        let workflow_version = workflow.version.clone();

        let created = self.config.store.create_run(CreateRunArgs {
            run_id: args.run_id.clone(),
            workflow_id: args.workflow_id.clone(),
            workflow_version: workflow_version.clone(),
            input: args.input.clone(),
            now,
        })?;

        // 已存在且不在 Queued（在跑 / 挂起 / 终态）→ 不重复驱动。
        if let CreateRunResult::Existing { run } = &created
            && run.status != WorkflowExecutionStatus::Queued
        {
            return Ok(RunResult {
                kind: RunResultKind::NotClaimable,
                run_id: args.run_id,
                workflow_id: Some(args.workflow_id),
                run: Some(run.clone()),
                events: vec![],
                event_count: 0,
                events_truncated: None,
            });
        }

        self.drive_claimed_run(DriveArgs {
            workflow: &workflow,
            workflow_id: &args.workflow_id,
            run_id: &args.run_id,
            input: Some(args.input),
            lease_owner,
            lease_ms,
            deadline,
            min_yield_remaining_ms: args.min_yield_remaining_ms,
            yield_resume_at: Some(now + 1),
            include_events: args.include_events,
            max_events: args.max_events,
        })
        .await
    }

    /// 投递外部信号：四态 bookkeeping → 追加 `StepResume` → 尝试驱动。
    pub async fn deliver_signal(
        &self,
        args: WorkflowRuntimeDeliverSignalArgs,
    ) -> anyhow::Result<RunResult> {
        let started_at = now_ms();
        let now = args.now.unwrap_or_else(now_ms);
        let deadline = resolve_runtime_deadline(args.deadline, args.max_duration_ms, started_at)?;
        let (lease_owner, lease_ms) = self.resolve_lease(
            args.lease_owner,
            args.lease_ms,
            &format!("signal:{}", args.run_id),
        )?;

        let delivered = self.config.store.deliver_signal(DeliverSignalArgs {
            run_id: args.run_id.clone(),
            delivery: SignalDelivery {
                signal_id: args.signal_id.clone(),
                step_id: args.step_id.clone(),
                name: args.name.clone(),
                payload: args.payload.clone(),
            },
            now,
        })?;

        // 桥给 core：signal_event 按 pause 点的 signal_name 追加 StepResume。
        let run_store = create_run_store_adapter(Arc::clone(&self.config.store));
        match &delivered {
            DeliverSignalResult::Delivered { run } => {
                let workflow_id = run.workflow_id.clone();
                workflow_core::signal_event(
                    run_store.as_ref(),
                    &args.run_id,
                    &args.name,
                    args.payload.clone(),
                )?;
                let workflow = self.load_workflow(&workflow_id)?;
                return self
                    .drive_claimed_run(DriveArgs {
                        workflow: &workflow,
                        workflow_id: &workflow_id,
                        run_id: &args.run_id,
                        input: None,
                        lease_owner,
                        lease_ms,
                        deadline,
                        min_yield_remaining_ms: args.min_yield_remaining_ms,
                        yield_resume_at: Some(now + 1),
                        include_events: args.include_events,
                        max_events: args.max_events,
                    })
                    .await;
            }
            DeliverSignalResult::NotFound => {
                return Ok(RunResult {
                    kind: RunResultKind::NotFound,
                    run_id: args.run_id,
                    workflow_id: None,
                    run: None,
                    events: vec![],
                    event_count: 0,
                    events_truncated: None,
                });
            }
            DeliverSignalResult::Duplicate { run } | DeliverSignalResult::NotWaiting { run } => {
                let kind = match delivered {
                    DeliverSignalResult::Duplicate { .. } => RunResultKind::Duplicate,
                    _ => RunResultKind::NotWaiting,
                };
                return Ok(RunResult {
                    kind,
                    run_id: args.run_id,
                    workflow_id: Some(run.workflow_id.clone()),
                    run: Some(run.clone()),
                    events: vec![],
                    event_count: 0,
                    events_truncated: None,
                });
            }
        }
    }

    /// 投递审批决定：四态 bookkeeping → 追加 `StepResume`（pause key =
    /// approval_id）→ 尝试驱动。
    pub async fn deliver_approval(
        &self,
        args: WorkflowRuntimeDeliverApprovalArgs,
    ) -> anyhow::Result<RunResult> {
        let started_at = now_ms();
        let now = args.now.unwrap_or_else(now_ms);
        let deadline = resolve_runtime_deadline(args.deadline, args.max_duration_ms, started_at)?;
        let (lease_owner, lease_ms) = self.resolve_lease(
            args.lease_owner,
            args.lease_ms,
            &format!("approval:{}", args.run_id),
        )?;

        let delivered = self.config.store.deliver_approval(DeliverApprovalArgs {
            run_id: args.run_id.clone(),
            approval: args.approval.clone(),
            now,
        })?;

        let workflow_id = match &delivered {
            DeliverApprovalResult::Delivered { run } => run.workflow_id.clone(),
            DeliverApprovalResult::NotFound => {
                return Ok(RunResult {
                    kind: RunResultKind::NotFound,
                    run_id: args.run_id,
                    workflow_id: None,
                    run: None,
                    events: vec![],
                    event_count: 0,
                    events_truncated: None,
                });
            }
            DeliverApprovalResult::Duplicate { run }
            | DeliverApprovalResult::NotWaiting { run } => {
                let kind = match delivered {
                    DeliverApprovalResult::Duplicate { .. } => RunResultKind::Duplicate,
                    _ => RunResultKind::NotWaiting,
                };
                return Ok(RunResult {
                    kind,
                    run_id: args.run_id,
                    workflow_id: Some(run.workflow_id.clone()),
                    run: Some(run.clone()),
                    events: vec![],
                    event_count: 0,
                    events_truncated: None,
                });
            }
        };

        let run_store = create_run_store_adapter(Arc::clone(&self.config.store));
        // 我们的 core：approve 的 pause key 即 approval_id，payload 是 approve
        // 调用的返回值（决定内容）。
        workflow_core::signal_run(
            run_store.as_ref(),
            &args.run_id,
            &args.approval.approval_id,
            serde_json::to_value(&args.approval)?,
        )?;

        let workflow = self.load_workflow(&workflow_id)?;
        self.drive_claimed_run(DriveArgs {
            workflow: &workflow,
            workflow_id: &workflow_id,
            run_id: &args.run_id,
            input: None,
            lease_owner,
            lease_ms,
            deadline,
            min_yield_remaining_ms: args.min_yield_remaining_ms,
            yield_resume_at: Some(now + 1),
            include_events: args.include_events,
            max_events: args.max_events,
        })
        .await
    }

    /// 有界 sweep：恢复陈旧 run → 启动到期 schedule → 投递到期 timer。
    pub async fn sweep(
        &self,
        args: WorkflowRuntimeSweepArgs,
    ) -> anyhow::Result<WorkflowRuntimeSweepResult> {
        let started_at = now_ms();
        let now = args.now.unwrap_or_else(now_ms);
        let deadline = resolve_runtime_deadline(args.deadline, args.max_duration_ms, started_at)?;
        let min_yield = args
            .min_yield_remaining_ms
            .unwrap_or(DEFAULT_MIN_YIELD_REMAINING_MS);
        let max_recovered = args
            .max_recovered_runs
            .or(args.limit)
            .unwrap_or(DEFAULT_SWEEP_LIMIT);
        let max_scheduled = args
            .max_scheduled_runs
            .or(args.limit)
            .unwrap_or(DEFAULT_SWEEP_LIMIT);
        let max_timers = args
            .max_timers
            .or(args.limit)
            .unwrap_or(DEFAULT_SWEEP_LIMIT);
        let (lease_owner, lease_ms) =
            self.resolve_lease(args.lease_owner, args.lease_ms, &format!("sweep:{now}"))?;

        let mut recovered = Vec::new();
        let mut scheduled = Vec::new();
        let mut timers = Vec::new();
        let mut deadline_reached = false;

        // ── 恢复陈旧 run ──
        while recovered.len() < max_recovered {
            if should_stop_for_deadline(deadline, min_yield) {
                deadline_reached = true;
                break;
            }
            let claims = self.config.store.claim_stale_runs(ClaimStaleRunsArgs {
                now,
                limit: 1,
                lease_owner: lease_owner.clone(),
                lease_ms,
            })?;
            let Some(claim) = claims.into_iter().next() else {
                break;
            };
            let workflow = self.load_workflow(&claim.run.workflow_id)?;
            recovered.push(
                self.drive_claimed_run(DriveArgs {
                    workflow: &workflow,
                    workflow_id: &claim.run.workflow_id,
                    run_id: &claim.run.run_id,
                    input: Some(claim.run.input.clone()),
                    lease_owner: lease_owner.clone(),
                    lease_ms,
                    deadline,
                    min_yield_remaining_ms: args.min_yield_remaining_ms,
                    yield_resume_at: Some(now + 1),
                    include_events: args.include_events,
                    max_events: args.max_events,
                })
                .await?,
            );
        }

        // ── 启动到期 schedule ──
        while scheduled.len() < max_scheduled {
            if should_stop_for_deadline(deadline, min_yield) {
                deadline_reached = true;
                break;
            }
            let buckets =
                self.config
                    .store
                    .claim_due_schedule_buckets(ClaimDueScheduleBucketsArgs {
                        now,
                        limit: 1,
                        lease_owner: lease_owner.clone(),
                        lease_ms,
                    })?;
            let Some(bucket) = buckets.into_iter().next() else {
                break;
            };
            let result = self
                .start_run(WorkflowRuntimeStartRunArgs {
                    workflow_id: bucket.workflow_id.clone(),
                    run_id: bucket.run_id.clone(),
                    input: bucket.input.clone().unwrap_or(serde_json::Value::Null),
                    now: Some(now),
                    deadline,
                    max_duration_ms: None,
                    min_yield_remaining_ms: args.min_yield_remaining_ms,
                    lease_owner: Some(lease_owner.clone()),
                    lease_ms: Some(lease_ms),
                    include_events: args.include_events,
                    max_events: args.max_events,
                })
                .await?;
            if !matches!(
                result.kind,
                RunResultKind::NotClaimable | RunResultKind::NotFound
            ) {
                self.config
                    .store
                    .mark_schedule_bucket_started(MarkScheduleBucketStartedArgs {
                        schedule_id: bucket.schedule_id.clone(),
                        bucket_id: bucket.bucket_id.clone(),
                        run_id: bucket.run_id.clone(),
                    })?;
            }
            scheduled.push(result);
        }

        // ── 投递到期 timer ──
        while timers.len() < max_timers {
            if should_stop_for_deadline(deadline, min_yield) {
                deadline_reached = true;
                break;
            }
            let due = self.config.store.claim_due_timers(ClaimDueTimersArgs {
                now,
                limit: 1,
                lease_owner: lease_owner.clone(),
                lease_ms,
            })?;
            let Some(timer) = due.into_iter().next() else {
                break;
            };
            // timer 投递 = 以 `__timer` 信号名走投递路径（payload 空）。
            let result = self
                .deliver_signal(WorkflowRuntimeDeliverSignalArgs {
                    run_id: timer.run_id.clone(),
                    signal_id: timer.signal_id.clone(),
                    step_id: None,
                    name: "__timer".into(),
                    payload: serde_json::Value::Null,
                    now: Some(now),
                    lease_owner: Some(lease_owner.clone()),
                    lease_ms: Some(lease_ms),
                    deadline,
                    max_duration_ms: None,
                    min_yield_remaining_ms: args.min_yield_remaining_ms,
                    include_events: args.include_events,
                    max_events: args.max_events,
                })
                .await?;
            timers.push(result);
        }

        let summary = SweepSummary {
            recovered: count_kinds(&recovered),
            scheduled: count_kinds(&scheduled),
            timers: count_kinds(&timers),
            event_count: recovered
                .iter()
                .chain(&scheduled)
                .chain(&timers)
                .map(|r| r.event_count)
                .sum(),
            returned_event_count: recovered
                .iter()
                .chain(&scheduled)
                .chain(&timers)
                .map(|r| r.events.len())
                .sum(),
        };
        let remaining_may_exist = deadline_reached
            || recovered.len() >= max_recovered
            || scheduled.len() >= max_scheduled
            || timers.len() >= max_timers;

        Ok(WorkflowRuntimeSweepResult {
            recovered,
            scheduled,
            timers,
            summary,
            deadline_reached,
            remaining_may_exist,
        })
    }

    /// 认领 → 心跳 → 驱动 → 释放 → 分类结果。
    ///
    /// 驱动用 core 的 [`run_workflow`](workflow_core::run_workflow)（经
    /// [`create_run_store_adapter`] 降格），心跳任务每 `lease_ms / 3` 续租一次。
    #[allow(clippy::too_many_arguments)]
    async fn drive_claimed_run(&self, args: DriveArgs<'_>) -> anyhow::Result<RunResult> {
        let claim = self.config.store.claim_run(ClaimRunArgs {
            run_id: args.run_id.to_string(),
            lease_owner: args.lease_owner.clone(),
            lease_ms: args.lease_ms,
            now: now_ms(),
        })?;
        match claim {
            ClaimRunResult::NotFound => {
                return Ok(RunResult {
                    kind: RunResultKind::NotFound,
                    run_id: args.run_id.to_string(),
                    workflow_id: Some(args.workflow_id.to_string()),
                    run: None,
                    events: vec![],
                    event_count: 0,
                    events_truncated: None,
                });
            }
            ClaimRunResult::NotClaimable { run } => {
                return Ok(RunResult {
                    kind: RunResultKind::NotClaimable,
                    run_id: args.run_id.to_string(),
                    workflow_id: Some(args.workflow_id.to_string()),
                    run: Some(run),
                    events: vec![],
                    event_count: 0,
                    events_truncated: None,
                });
            }
            ClaimRunResult::Claimed { .. } => {}
        }

        // 事件收集器：计数全量，保留受 max_events 限制。
        let collected: Arc<Mutex<Vec<WorkflowEvent>>> = Default::default();
        let total: Arc<AtomicUsize> = Default::default();
        let include_events = args.include_events.unwrap_or(true);
        let max_events = args.max_events;
        let run_id = args.run_id.to_string();
        let runtime_publish = self.config.publish.clone();
        let publisher: Arc<dyn Fn(&WorkflowEvent) + Send + Sync> = {
            let collected = collected.clone();
            let total = total.clone();
            let run_id = run_id.clone();
            Arc::new(move |event: &WorkflowEvent| {
                let count = total.fetch_add(1, Ordering::Relaxed);
                let keep = include_events && max_events.map(|m| count < m).unwrap_or(true);
                if keep {
                    collected.lock().unwrap().push(event.clone());
                }
                if let Some(publish) = &runtime_publish {
                    publish(&run_id, event);
                }
            })
        };

        // 心跳：lease_ms / 3 周期续租（对齐上游 startLeaseHeartbeat）。
        let heartbeat = start_lease_heartbeat(
            Arc::clone(&self.config.store),
            args.run_id.to_string(),
            args.lease_owner.clone(),
            args.lease_ms,
        );

        // 驱动（core）。signal / approval 的恢复在进入 drive 前已经以
        // StepResume 落盘，重放时由 exec_pause 拾取。
        let mut opts = RunOptions::new(args.input.clone().unwrap_or(serde_json::Value::Null))
            .run_id(args.run_id.clone())
            .min_yield_remaining(
                args.min_yield_remaining_ms
                    .unwrap_or(DEFAULT_MIN_YIELD_REMAINING_MS),
            );
        if let Some(deadline) = args.deadline {
            opts = opts.deadline(deadline);
        }
        if let Some(at) = args.yield_resume_at {
            opts = opts.yield_resume_at(at);
        }
        let drive_result = run_workflow(
            args.workflow,
            run_store_for_core(&self.config),
            &opts,
            Some(publisher),
        )
        .await;

        // 心跳停止 + 释放 lease（无论 drive 成败）。
        let heartbeat_error = heartbeat.stop().await;

        // drive 收尾：run 若停在 `__timer` 上，把它的 deadline 登记成 timer 记录。
        // **必须在这里做**——core 的挂起是「写到挂起点就返回」，它自己不登记
        // 定时器（D3）；不登记的话这个 run 永远等不到唤醒。放在释放 lease 之前，
        // 与上游 `runtime-driver.ts:756`（`syncTimerFromRunState` 在 finally 的
        // releaseRunLease 之前）同序。
        //
        // 失败不致命：timer 登记不上，sweep 下一轮还会从 `waiting_for` 恢复，
        // 但这一轮先如实报错。
        let sync_result = sync_timer_from_run_state(&self.config, args.run_id, args.workflow_id);

        self.config.store.release_run_lease(ReleaseRunLeaseArgs {
            run_id: args.run_id.to_string(),
            lease_owner: args.lease_owner.clone(),
        })?;
        heartbeat_error?;
        sync_result?;

        // drive 出错（step 终局失败等）→ Errored 结果。run_workflow 的错误
        // 详情已经落在事件日志（StepFailed / RunErrored）里，快照里也有。
        if drive_result.is_err() {
            return Ok(RunResult {
                kind: RunResultKind::Errored,
                run_id: args.run_id.to_string(),
                workflow_id: Some(args.workflow_id.to_string()),
                run: None,
                events: vec![],
                event_count: total.load(Ordering::Relaxed),
                events_truncated: None,
            });
        }

        let events = collected.lock().unwrap().clone();
        let event_count = total.load(Ordering::Relaxed);
        let snapshot = self.config.store.load_run(&args.run_id.to_string())?;
        let kind = classify_run(snapshot.as_ref(), event_count);
        Ok(RunResult {
            kind,
            run_id: args.run_id.to_string(),
            workflow_id: Some(args.workflow_id.to_string()),
            run: snapshot,
            events,
            event_count,
            events_truncated: max_events.map(|m| event_count > m),
        })
    }
}

/// [`drive_claimed_run`] 的参数包（避免超长参数列表）。
struct DriveArgs<'a> {
    workflow: &'a Workflow,
    workflow_id: &'a WorkflowId,
    run_id: &'a RunId,
    /// 全新启动时的 input；resume 时为 `None`（input 已在 store 里）。
    input: Option<serde_json::Value>,
    lease_owner: LeaseOwner,
    lease_ms: i64,
    deadline: Option<i64>,
    min_yield_remaining_ms: Option<u64>,
    yield_resume_at: Option<i64>,
    include_events: Option<bool>,
    max_events: Option<usize>,
}

/// 每次调用建一个新 adapter（轻量 struct + Arc，与上游每次 drive 建一次一致）。
fn run_store_for_core(config: &WorkflowRuntimeConfig) -> Arc<dyn workflow_core::RunStore> {
    create_run_store_adapter(Arc::clone(&config.store))
}

/// drive 收尾后，若 run 正挂在 `__timer` 上，把它的 deadline 登记成一条 timer
/// 记录，供后续 `sweep` 认领投递。
///
/// 对齐上游 `syncTimerFromRunState`（`runtime-driver.ts:815`）：
///
/// - 只在 `waiting_for.signal_name == "__timer"` **且** `deadline` 存在时登记；
/// - `wake_at` 就是 core 投影出来的 `deadline`（D3 之后它是**绝对**时间戳，
///   与上游 `sleepUntil` 同形）；
/// - `signal_id` 形如 `timer:{run_id}:{step_id}:{deadline}`，**幂等键**——同一次
///   挂起重复登记无副作用，正好抵消「每次 drive 收尾都跑一遍」。
fn sync_timer_from_run_state(
    config: &WorkflowRuntimeConfig,
    run_id: &str,
    workflow_id: &str,
) -> anyhow::Result<()> {
    let Some(state) = config.store.load_run_state(run_id)? else {
        return Ok(());
    };
    let Some(w) = state.waiting_for else {
        return Ok(());
    };
    if w.signal_name != "__timer" {
        return Ok(());
    }
    let Some(deadline) = w.deadline else {
        return Ok(());
    };
    let step_id = w.step_id.unwrap_or_default();

    config.store.schedule_timer(ScheduleTimerArgs {
        run_id: run_id.to_string(),
        workflow_id: workflow_id.to_string(),
        workflow_version: state.workflow_version.clone(),
        wake_at: deadline,
        signal_id: format!("timer:{run_id}:{step_id}:{deadline}"),
        now: now_ms(),
    })
}

/// 心跳任务：每 `lease_ms / 3` 续租，直到 stop。错误在 `stop().await` 时回传。
fn start_lease_heartbeat(
    store: Arc<dyn WorkflowExecutionStore>,
    run_id: RunId,
    lease_owner: LeaseOwner,
    lease_ms: i64,
) -> HeartbeatHandle {
    let interval_ms = (lease_ms / 3).max(1);
    let stop = Arc::new(AtomicBool::new(false));
    // Notify 让 stop() 能立刻打断睡眠——否则 stop().await 要等满一个心跳周期
    // （lease 30s → 间隔 10s），sweep 会被拖住 10 秒。
    let notify = Arc::new(tokio::sync::Notify::new());
    let stop_flag = Arc::clone(&stop);
    let notify_for_task = Arc::clone(&notify);
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(interval_ms as u64)) => {}
                _ = notify_for_task.notified() => {}
            }
            if stop_flag.load(Ordering::Relaxed) {
                return Ok(());
            }
            store.heartbeat_run_lease(HeartbeatRunLeaseArgs {
                run_id: run_id.clone(),
                lease_owner: lease_owner.clone(),
                lease_ms,
                now: now_ms(),
            })?;
        }
    });
    HeartbeatHandle {
        stop,
        notify,
        handle,
    }
}

struct HeartbeatHandle {
    stop: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl HeartbeatHandle {
    async fn stop(self) -> anyhow::Result<()> {
        self.stop.store(true, Ordering::Relaxed);
        self.notify.notify_one();
        self.handle
            .await
            .map_err(|e| anyhow::anyhow!("heartbeat task panicked: {e}"))?
    }
}

fn classify_run(run: Option<&WorkflowExecution>, event_count: usize) -> RunResultKind {
    match run.map(|r| r.status) {
        Some(WorkflowExecutionStatus::Finished) => RunResultKind::Completed,
        Some(WorkflowExecutionStatus::Paused) => RunResultKind::Paused,
        Some(WorkflowExecutionStatus::Errored) | Some(WorkflowExecutionStatus::Aborted) => {
            RunResultKind::Errored
        }
        Some(WorkflowExecutionStatus::Running) | Some(WorkflowExecutionStatus::Queued) => {
            RunResultKind::Running
        }
        None if event_count > 0 => RunResultKind::Running,
        None => RunResultKind::NotFound,
    }
}

#[cfg(test)]
mod driver_tests {
    //! 驱动器端到端：真实 workflow + 真实 InMemoryExecutionStore。
    use super::*;
    use crate::WorkflowRunStoreAdapterStore;
    use crate::in_memory_store::InMemoryExecutionStore;
    use workflow_core::{
        CreateWorkflowConfig, RunState, RunStatus, WaitForState, WorkflowCtx, create_workflow,
    };

    /// 双柄：`mem` 供测试直读内部，`store` 喂给 runtime（trait 对象）。
    struct Fixture {
        rt: Arc<WorkflowRuntime>,
        mem: Arc<InMemoryExecutionStore>,
    }

    fn runtime_with(workflow_id: &str, workflow: Workflow) -> Fixture {
        let mem: Arc<InMemoryExecutionStore> = Arc::new(InMemoryExecutionStore::default());
        let store: Arc<dyn WorkflowExecutionStore> = mem.clone();
        let mut workflows = HashMap::new();
        workflows.insert(
            workflow_id.to_string(),
            WorkflowRegistration {
                workflow,
                previous_versions: vec![],
                version_override: None,
                schedules: vec![],
            },
        );
        Fixture {
            rt: Arc::new(define_workflow_runtime(WorkflowRuntimeConfig::new(
                store, workflows,
            ))),
            mem,
        }
    }

    fn simple_workflow() -> Workflow {
        create_workflow(CreateWorkflowConfig::new("simple").input::<serde_json::Value>())
            .handler(|ctx: WorkflowCtx| async move {
                let a = ctx
                    .step("a", move |_sc: workflow_core::StepCtx| async move {
                        Ok(serde_json::json!({ "v": 1 }))
                    })
                    .await?;
                Ok(serde_json::json!({ "got": a }))
            })
            .into_workflow()
    }

    fn waiting_workflow() -> Workflow {
        create_workflow(CreateWorkflowConfig::new("waiter").input::<serde_json::Value>())
            .handler(|ctx: WorkflowCtx| async move {
                let payment = ctx.wait_for_event("payment", "payment").await?;
                let _ = payment;
                ctx.step("ship", move |_sc: workflow_core::StepCtx| async move {
                    Ok(serde_json::json!({ "shipped": true }))
                })
                .await?;
                Ok(serde_json::json!({ "shipped": true }))
            })
            .into_workflow()
    }

    fn sleeping_workflow() -> Workflow {
        create_workflow(CreateWorkflowConfig::new("sleeper").input::<serde_json::Value>())
            .handler(|ctx: WorkflowCtx| async move {
                // 先落一个 checkpoint，证明恢复时重放短路。
                ctx.step(
                    "before-sleep",
                    move |_sc: workflow_core::StepCtx| async move { Ok(serde_json::Value::Null) },
                )
                .await?;
                ctx.sleep("hold", std::time::Duration::from_secs(60))
                    .await?;
                ctx.step(
                    "after-sleep",
                    move |_sc: workflow_core::StepCtx| async move {
                        Ok(serde_json::json!({ "woke": true }))
                    },
                )
                .await?;
                Ok(serde_json::json!({ "woke": true }))
            })
            .into_workflow()
    }

    /// 短 sleep（80ms），用于端到端验证 sweep 认领 timer。
    fn short_sleep_workflow() -> Workflow {
        create_workflow(CreateWorkflowConfig::new("sleeper").input::<serde_json::Value>())
            .handler(|ctx: WorkflowCtx| async move {
                ctx.sleep("hold", std::time::Duration::from_millis(80))
                    .await?;
                ctx.step(
                    "after",
                    move |_sc: workflow_core::StepCtx| async move {
                        Ok(serde_json::json!({ "after": true }))
                    },
                )
                .await?;
                Ok(serde_json::json!({ "woke": true, "after": true }))
            })
            .into_workflow()
    }

    fn long_step_workflow() -> Workflow {        create_workflow(CreateWorkflowConfig::new("long").input::<serde_json::Value>())
            .handler(|ctx: WorkflowCtx| async move {
                ctx.step("slow", move |_sc: workflow_core::StepCtx| async move {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    Ok(serde_json::Value::Null)
                })
                .await?;
                Ok(serde_json::Value::Null)
            })
            .into_workflow()
    }

    /// 轮询直到 run 进入指定状态（超时 panic）。
    async fn wait_for_status(
        mem: &InMemoryExecutionStore,
        run_id: &str,
        want: WorkflowExecutionStatus,
    ) -> WorkflowExecution {
        for _ in 0..500 {
            if let Some(run) = mem.load_run(run_id).unwrap()
                && run.status == want
            {
                return run;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("run {run_id} 迟迟未进入 {want:?}");
    }

    #[tokio::test]
    async fn start_run_completes_basic_workflow() {
        let fx = runtime_with("simple", simple_workflow());
        let out = fx
            .rt
            .start_run(WorkflowRuntimeStartRunArgs {
                workflow_id: "simple".into(),
                run_id: "r1".into(),
                input: serde_json::json!({}),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(out.kind, RunResultKind::Completed);
        assert_eq!(
            out.run.as_ref().unwrap().status,
            WorkflowExecutionStatus::Finished
        );
        assert!(out.event_count > 0, "应收集到事件");
        assert!(!out.events.is_empty());
    }

    /// 已存在且不在 Queued 的 run 不重复驱动。
    #[tokio::test]
    async fn start_run_existing_finished_is_not_claimable() {
        let fx = runtime_with("simple", simple_workflow());
        let args = WorkflowRuntimeStartRunArgs {
            workflow_id: "simple".into(),
            run_id: "r1".into(),
            input: serde_json::json!({}),
            ..Default::default()
        };
        fx.rt.start_run(args.clone()).await.unwrap();
        let second = fx.rt.start_run(args).await.unwrap();
        assert_eq!(second.kind, RunResultKind::NotClaimable);
    }

    /// 信号投递：`deliver_signal` 自己认领 + 驱动到完成。
    ///
    /// D3 之后挂起会**释放** lease（drive 已返回），所以 deliver 能直接 claim，
    /// 不用再等一个存活的自轮询 drive 来拾取 StepResume。
    #[tokio::test]
    async fn deliver_signal_resumes_paused_run() {
        let fx = runtime_with("waiter", waiting_workflow());

        // start_run 跑到挂起点就返回，run 落在 Paused。
        let started = fx
            .rt
            .start_run(WorkflowRuntimeStartRunArgs {
                workflow_id: "waiter".into(),
                run_id: "r1".into(),
                input: serde_json::json!({}),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(started.kind, RunResultKind::Paused);
        let run = fx.mem.load_run("r1").unwrap().unwrap();
        assert_eq!(run.status, WorkflowExecutionStatus::Paused);
        assert_eq!(
            run.waiting_for.as_ref().map(|w| w.signal_name.as_str()),
            Some("payment")
        );

        // deliver_signal 认领 + 驱动到完成。
        let result = fx
            .rt
            .deliver_signal(WorkflowRuntimeDeliverSignalArgs {
                run_id: "r1".into(),
                signal_id: "pay-1".into(),
                name: "payment".into(),
                payload: serde_json::json!({ "paymentId": "p1" }),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result.kind, RunResultKind::Completed);

        let finished = wait_for_status(&fx.mem, "r1", WorkflowExecutionStatus::Finished).await;
        assert_eq!(
            finished.output,
            Some(serde_json::json!({ "shipped": true }))
        );
    }

    /// **两次 sleep 的端到端**：每次挂起 → sweep 认领 timer 投递 → 再挂起 → …
    /// 直到 Finished。验证「每次 drive 收尾都重新登记 timer」是累积有效的
    /// （第二次 sleep 的 timer 也必须被登记）。
    #[tokio::test]
    async fn sweep_chains_across_two_sleeps() {
        let fx = runtime_with(
            "sleeper",
            create_workflow(CreateWorkflowConfig::new("sleeper").input::<serde_json::Value>())
                .handler(|ctx: WorkflowCtx| async move {
                    ctx.sleep("s1", std::time::Duration::from_millis(40))
                        .await?;
                    ctx_step_once(&ctx, "mid").await?;
                    ctx.sleep("s2", std::time::Duration::from_millis(40))
                        .await?;
                    Ok(serde_json::json!({ "done": true }))
                })
                .into_workflow(),
        );

        let started = fx
            .rt
            .start_run(WorkflowRuntimeStartRunArgs {
                workflow_id: "sleeper".into(),
                run_id: "chain:1".into(),
                input: serde_json::json!({}),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(started.kind, RunResultKind::Paused);

        // 反复 sleep 到点 + sweep，最多 4 轮。
        let mut finished = false;
        for _ in 0..4 {
            tokio::time::sleep(std::time::Duration::from_millis(60)).await;
            fx.rt
                .sweep(WorkflowRuntimeSweepArgs {
                    max_timers: Some(5),
                    ..Default::default()
                })
                .await
                .unwrap();
            if fx.mem.load_run("chain:1").unwrap().unwrap().status
                == WorkflowExecutionStatus::Finished
            {
                finished = true;
                break;
            }
        }
        assert!(finished, "两次 sleep 应都能被 sweep 唤醒");
        assert_eq!(
            fx.mem.load_run("chain:1").unwrap().unwrap().output,
            Some(serde_json::json!({ "done": true }))
        );
    }

    /// timer 登记是**幂等**的：同一挂起点被 drive 收尾重复同步，不能堆出多条
    /// timer（否则 sweep 会重复投递同一信号）。`signal_id` 里编了
    /// `run_id:step_id:deadline`，正好当幂等键。
    ///
    /// 这里用「投一个名字对不上的信号」逼出第二次 drive 收尾：`deliver_signal`
    /// 会 claim + drive（收尾再次同步 timer），但 `NotWaiting` 不会唤醒 run，
    /// 正好隔离出「只测同步幂等」。
    #[tokio::test]
    async fn timer_registration_is_idempotent() {
        let fx = runtime_with("sleeper", sleeping_workflow());

        // 起一次，停在 sleep（60s，测试期间不会到期）。
        fx.rt
            .start_run(WorkflowRuntimeStartRunArgs {
                workflow_id: "sleeper".into(),
                run_id: "idem:1".into(),
                input: serde_json::json!({}),
                ..Default::default()
            })
            .await
            .unwrap();

        let run = fx.mem.load_run("idem:1").unwrap().unwrap();
        assert_eq!(run.status, WorkflowExecutionStatus::Paused);
        let deadline = run.waiting_for.as_ref().unwrap().deadline.unwrap();
        let signal_id = format!("timer:idem:1:hold:{deadline}");

        // 第一次 drive 收尾已经登记过一条（用很短的 lease，认领完就过期，
        // 免得挡住后面的探测）。
        let after_first = fx
            .mem
            .claim_due_timers(ClaimDueTimersArgs {
                now: deadline + 1,
                limit: 10,
                lease_owner: "probe1".into(),
                lease_ms: 1,
            })
            .unwrap();
        assert_eq!(after_first.len(), 1, "第一次 drive 收尾应登记一条 timer");
        assert_eq!(after_first[0].signal_id, signal_id);

        // 投一个名字对不上的信号 → 逼出第二次 drive 收尾（run 不被唤醒）。
        let delivered = fx
            .rt
            .deliver_signal(WorkflowRuntimeDeliverSignalArgs {
                run_id: "idem:1".into(),
                signal_id: "noise".into(),
                name: "unrelated".into(),
                payload: serde_json::Value::Null,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            delivered.kind,
            RunResultKind::NotWaiting,
            "投的不是它在等的信号"
        );

        // 认领应**仍然只有一条**：第二次同步是同键覆盖，不是新增；且非 timer 的
        // 投递不该把它删掉。
        let after_second = fx
            .mem
            .claim_due_timers(ClaimDueTimersArgs {
                now: deadline + 2,
                limit: 10,
                lease_owner: "probe2".into(),
                lease_ms: 1,
            })
            .unwrap();
        assert_eq!(
            after_second.len(),
            1,
            "第二次 drive 收尾不该堆出第二条 timer，也不该把原有的删掉"
        );
        assert_eq!(after_second[0].signal_id, signal_id);
    }

    async fn ctx_step_once(ctx: &WorkflowCtx, id: &str) -> anyhow::Result<()> {
        let id = id.to_string();
        ctx.step(&id, move |_sc: workflow_core::StepCtx| async move {
            Ok(serde_json::Value::Null)
        })
        .await?;
        Ok(())
    }

    /// 模拟「上一个 worker 刚 claim 就死了」：create + claim（短 lease），不驱动。
    async fn setup_dead_run(fx: &Fixture, run_id: &str) {
        fx.mem
            .create_run(CreateRunArgs {
                run_id: run_id.into(),
                workflow_id: "simple".into(),
                workflow_version: None,
                input: serde_json::json!({}),
                now: 0,
            })
            .unwrap();
        fx.mem
            .claim_run(ClaimRunArgs {
                run_id: run_id.into(),
                lease_owner: "dead-worker".into(),
                lease_ms: 50, // 50ms 后过期
                now: 0,
            })
            .unwrap();
    }

    /// sweep 恢复「持有者已死」的 run：lease 过期后 claim_stale_runs 捡起并
    /// 重放完成。
    #[tokio::test]
    async fn sweep_recovers_stale_run() {
        let fx = runtime_with("simple", simple_workflow());
        setup_dead_run(&fx, "dead:1").await;
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        let sweep = fx
            .rt
            .sweep(WorkflowRuntimeSweepArgs::default())
            .await
            .unwrap();
        assert_eq!(sweep.recovered.len(), 1);
        assert_eq!(sweep.recovered[0].kind, RunResultKind::Completed);
        assert!(!sweep.deadline_reached);
        assert!(!sweep.remaining_may_exist);
    }

    /// **端到端**：真跑一个含 `sleep` 的 workflow，只靠 sweep 收尾。
    ///
    /// 上面那个测试手工构造了 Paused 状态与 timer 记录，绕过了 driver 的
    /// 「drive 后登记 timer」这一步——所以它**测不出**那一步缺失。这个测试从
    /// `start_run` 开始，不碰任何 store 原语。
    #[tokio::test]
    async fn start_run_then_sweep_completes_a_sleeping_workflow() {
        let fx = runtime_with("sleeper", short_sleep_workflow());

        // 短 sleep（80ms）——start_run 挂起即返回，run 停在 Paused。
        let started = fx
            .rt
            .start_run(WorkflowRuntimeStartRunArgs {
                workflow_id: "sleeper".into(),
                run_id: "sleep:e2e".into(),
                input: serde_json::json!({}),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(started.kind, RunResultKind::Paused);

        let run = fx.mem.load_run("sleep:e2e").unwrap().unwrap();
        assert_eq!(run.status, WorkflowExecutionStatus::Paused);
        let w = run.waiting_for.as_ref().expect("应投影 waiting_for");
        assert_eq!(w.signal_name, "__timer", "sleep 走 __timer 通道");
        assert!(w.deadline.is_some(), "sleep 应投影 deadline");

        // 等 timer 到期，然后 sweep——**不手工 schedule_timer**：driver 应该在
        // drive 收尾时自己登记。
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        let sweep = fx
            .rt
            .sweep(WorkflowRuntimeSweepArgs {
                max_timers: Some(5),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(
            sweep.timers.len(),
            1,
            "drive 收尾时应登记 timer，sweep 才有得投递"
        );
        let run = fx.mem.load_run("sleep:e2e").unwrap().unwrap();
        assert_eq!(run.status, WorkflowExecutionStatus::Finished);
        assert_eq!(
            run.output,
            Some(serde_json::json!({ "woke": true, "after": true }))
        );
    }

    /// sweep 投递到期 timer：run 挂在 `__timer` 上、持有者已死 → 投递 →
    /// 重放完成。这是 serverless 形态的核心路径。
    ///
    /// 「持有者已死」用**直接构造状态**模拟（create + 存 Paused 信封 + 落
    /// StepPaused 事件 + 登记 timer），不通过 abort 真实 drive——因为 abort
    /// 父任务不会杀死其内部 spawn 的心跳任务，心跳会永远续租（上游 serverless
    /// 无此问题：进程死 = 一切皆死）。该约束记录在 driver 模块文档。
    #[tokio::test]
    async fn sweep_delivers_due_timer_and_completes() {
        let fx = runtime_with("sleeper", sleeping_workflow());
        let now = now_ms();

        // 直接构造「死进程遗留」的挂起状态：
        // 1. execution（runtime 视角：Paused + __timer 等待）
        // 2. RunState 信封（core 视角）
        // 3. 事件日志：before-sleep 完成 + sleep 挂起（StepPaused）
        // 4. 到期 timer
        fx.mem
            .create_run(CreateRunArgs {
                run_id: "sleep:1".into(),
                workflow_id: "sleeper".into(),
                workflow_version: None,
                input: serde_json::json!({}),
                now,
            })
            .unwrap();
        fx.mem
            .save_run_state(SaveRunStateArgs {
                state: RunState {
                    run_id: "sleep:1".into(),
                    workflow_id: "sleeper".into(),
                    workflow_version: None,
                    status: RunStatus::Paused,
                    input: serde_json::json!({}),
                    output: None,
                    error: None,
                    waiting_for: Some(WaitForState {
                        step_id: Some("hold".into()),
                        signal_name: "__timer".into(),
                        deadline: Some(now + 30),
                    }),
                    pending_approval: None,
                    created_at: now,
                    updated_at: now,
                },
            })
            .unwrap();
        fx.mem
            .append_events(AppendEventsArgs {
                run_id: "sleep:1".into(),
                expected_next_index: 0,
                events: vec![
                    WorkflowEvent::RunStarted {
                        ts: now,
                        run_id: "sleep:1".into(),
                    },
                    WorkflowEvent::StepFinished {
                        ts: now,
                        run_id: "sleep:1".into(),
                        step_id: "before-sleep".into(),
                        result: None,
                        attempts: vec![],
                    },
                    WorkflowEvent::StepPaused {
                        ts: now,
                        run_id: "sleep:1".into(),
                        step_id: "hold".into(),
                        signal_name: "__timer".into(),
                        due_at: Some(now + 30),
                        reason: "sleep".into(),
                    },
                ],
            })
            .unwrap();
        fx.mem
            .schedule_timer(ScheduleTimerArgs {
                run_id: "sleep:1".into(),
                workflow_id: "sleeper".into(),
                workflow_version: None,
                wake_at: now + 30,
                signal_id: "timer:sleep:1".into(),
                now,
            })
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(40)).await; // timer 到期

        let sweep = fx
            .rt
            .sweep(WorkflowRuntimeSweepArgs {
                max_timers: Some(5),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(sweep.timers.len(), 1, "应投递一个到期 timer");

        // 恢复后 run 完成（重放短路 before-sleep，sleep 因 StepResume 立即过）。
        let run = fx.mem.load_run("sleep:1").unwrap().unwrap();
        assert_eq!(run.status, WorkflowExecutionStatus::Finished);
        let before_runs = fx
            .mem
            .read_events(ReadEventsArgs {
                run_id: "sleep:1".into(),
                from_index: None,
            })
            .unwrap()
            .into_iter()
            .filter(|e| {
                matches!(
                    e.event,
                    WorkflowEvent::StepFinished { ref step_id, .. } if step_id == "before-sleep"
                )
            })
            .count();
        assert_eq!(before_runs, 1, "before-sleep 应被重放短路，只跑一次");
    }

    /// 心跳维持 lease：长 step 期间别的 sweep 抢不走。
    #[tokio::test]
    async fn heartbeat_keeps_lease_alive_during_long_drive() {
        let fx = runtime_with("long", long_step_workflow());
        let rt = Arc::clone(&fx.rt);
        let drive = tokio::spawn(async move {
            rt.start_run(WorkflowRuntimeStartRunArgs {
                workflow_id: "long".into(),
                run_id: "long:1".into(),
                input: serde_json::json!({}),
                lease_ms: Some(300), // 心跳每 100ms 续
                ..Default::default()
            })
            .await
        });

        // drive 中途（~350ms，lease 已被心跳续到 600ms）sweep：不应捡到。
        tokio::time::sleep(std::time::Duration::from_millis(350)).await;
        let sweep = fx
            .rt
            .sweep(WorkflowRuntimeSweepArgs {
                max_recovered_runs: Some(5),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(
            sweep.recovered.is_empty(),
            "心跳应维持 lease，stale 扫描不该抢走正在跑的 run"
        );

        let out = drive.await.unwrap().unwrap();
        assert_eq!(out.kind, RunResultKind::Completed);
    }

    /// 没活干的 sweep：全空 + remaining_may_exist = false。
    #[tokio::test]
    async fn sweep_with_nothing_to_do_is_empty() {
        let fx = runtime_with("simple", simple_workflow());
        let sweep = fx
            .rt
            .sweep(WorkflowRuntimeSweepArgs::default())
            .await
            .unwrap();
        assert!(sweep.recovered.is_empty());
        assert!(sweep.scheduled.is_empty());
        assert!(sweep.timers.is_empty());
        assert!(!sweep.remaining_may_exist);
        assert!(!sweep.deadline_reached);
    }
}
