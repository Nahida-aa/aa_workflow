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
//! - **publisher 签名**：上游 `WorkflowRuntimeEventPublisher`（`types.ts:21`）可
//!   async（`void | Promise<void>`），我们是同步 `Fn`——runtime 的 fan-out 闭包
//!   里还塞着事件收集计数，改 async 会把收集也拖成异步。需要落盘走 core 的
//!   `async_publish`。合并语义（去重 + 都调 + 配置级在前）已对齐上游
//!   `combinePublishers`（`:1139-1161`）。
//! - **threadId**：已接（`startRun` / `deliverSignal` / `deliverApproval` 三个
//!   args 各带一个，透传进 `RUN_STARTED`）。注意 `sweep` **不带**——上游
//!   `WorkflowRuntimeSweepArgs`（`types.ts:459-474`）就没这个字段。
//! - **lease 属主前缀**：解析收敛在 `drive_claimed_run`（上游 `:679-681`
//!   `args.leaseOwner ?? createLeaseOwner(\`runtime:${runId}\`)`）。只有 sweep
//!   自带 `sweep:{now}`（上游 `:416`，那是批次标识）。
//! - **schedule materializer**：cron 表达式解析（上游
//!   `schedule-materializer.ts`，272 行）暂未移植——`next_fire_at` 由 host
//!   计算后传入 `upsert_schedule`。
//! - **workflow 注册表**：上游用异步 `load()` 闭包（为 JS 代码分割）；Rust
//!   直接持有构建好的 [`WorkflowDefinition`] 值。
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
use aa_workflow_core::{
    AnyWorkflowDefinition,RunWorkflowOptions, WorkflowDefinition, WorkflowEvent, run_workflow};

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
/// workflow loader（对齐上游 `WorkflowLoader<TWorkflow>`，types.ts:345）。
///
/// 上游是**异步**闭包（JS 代码分割 + 模块形态归一化：`default`/`workflow`
/// 包装，见 `WorkflowLoaderResult`）；Rust 无此需求，收敛为同步闭包直接返回
/// 构建好的 [`WorkflowDefinition`]——需要异步取数的场景在使用者闭包内自行预取。
/// 上游 `loadWorkflow` 不 memoize（JS 模块缓存吸收成本）——这里同样不缓存：
/// loader 应廉价；昂贵加载在使用者侧闭包内自行记忆化。
pub type WorkflowLoader = Arc<dyn Fn() -> AnyWorkflowDefinition + Send + Sync>;

/// 单个 workflow 的注册项（对齐上游 `WorkflowRegistration<TWorkflow>`，
/// types.ts:349）。
#[derive(Clone)]
pub struct WorkflowRegistration {
    /// 构建本 workflow 的 loader——每次 load 重新调用（不做 memoize）。
    pub load: WorkflowLoader,
    /// 覆盖 workflow 自身的 version（对齐上游 `version?`）。
    pub version: Option<String>,
    /// 历史版本的 loader（对齐上游 `previousVersions?: Record<版本, loader>`）。
    /// key 目前只是声明——`loadWorkflow` 只合并 values 进
    /// `workflow.previous_versions`，供引擎按持久化版本路由。
    pub previous_versions: HashMap<String, WorkflowLoader>,
    /// 注册期声明的 schedule（对齐上游 `schedules?`）。
    ///
    /// 由 [`materialize_workflow_schedules`](crate::schedule_materializer::materialize_workflow_schedules)
    /// 换算成 store 里的 schedule 记录——那一步之前它们只是声明。
    pub schedules: Vec<WorkflowScheduleDefinition>,
}

/// `TWorkflows` 的最低能力：按 id 查注册项（对齐上游 `Record<string,
/// WorkflowRegistration>` 的静态查找）。擦除形态（HashMap）天然满足；
/// 使用者自己的注册表类型实现它即可接入 runtime——未来「按 workflow id
/// 校验 input 的类型安全」也挂在这个 trait 上扩展。
pub trait WorkflowRegistry {
    fn get(&self, workflow_id: &WorkflowId) -> Option<&WorkflowRegistration>;
}

impl WorkflowRegistry for HashMap<WorkflowId, WorkflowRegistration> {
    fn get(&self, workflow_id: &WorkflowId) -> Option<&WorkflowRegistration> {
        HashMap::get(self, workflow_id)
    }
}

/// runtime 配置（对齐上游 `WorkflowRuntimeConfig<TWorkflows>`，types.ts:360）。
///
/// 上游的 `telemetry` 项省略（core 无 OTel 集成）。`TWorkflows` 默认擦除为
/// `HashMap<WorkflowId, WorkflowRegistration>`——上游靠 `const TWorkflows` 把
/// 事件 fan-out 回调（上游 `WorkflowRuntimeEventPublisher`，`types.ts:21`）。
///
/// 上游签名是 `(runId, event) => void | Promise<void>`，可以 async；我们是**同步**
/// `Fn`——runtime 的 fan-out 闭包里还塞着事件收集器（`include_events` /
/// `max_events` 计数），改成 async 会把收集也拖成异步，没好处。需要落盘 / 发网络
/// 走 core 的 `RunWorkflowOptions::async_publish`。
///
/// 事件按 `&WorkflowEvent` **借**给回调，不强制调用方 clone（上游按值）。
pub type WorkflowRuntimeEventPublisher = Arc<dyn Fn(&str, &WorkflowEvent) + Send + Sync>;

/// 注册表类型带在定义上；Rust 无 keyof，先做形状对齐（start_run 的 id 仍按
/// 注册表运行期查找），按 id 校验 input 的类型安全留待后续设计。
#[derive(Clone)]
pub struct WorkflowRuntimeConfig<TWorkflows = HashMap<WorkflowId, WorkflowRegistration>> {
    pub store: Arc<dyn WorkflowExecutionStore>,
    /// 按 `workflow_id` 注册的 workflow。
    pub workflows: TWorkflows,
    /// lease 默认时长；单次调用可用 `lease_ms` 覆盖。
    pub default_lease_ms: Option<i64>,
    /// 全局事件 fan-out（best-effort，不参与耐久执行）。
    pub publish: Option<WorkflowRuntimeEventPublisher>,
}

impl<TWorkflows> WorkflowRuntimeConfig<TWorkflows> {
    pub fn new(
        store: Arc<dyn WorkflowExecutionStore>,
        workflows: TWorkflows,
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
pub enum WorkflowRuntimeRunResultKind {
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
pub struct WorkflowRuntimeRunResult {
    pub kind: WorkflowRuntimeRunResultKind,
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

#[derive(Clone, Default)]
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
    /// 客户端关联标识（上游 `threadId`）。透传到 `RUN_STARTED` 事件；不落 store。
    pub thread_id: Option<String>,
    /// 默认 `true`：把 drive 观测到的事件带回结果。
    pub include_events: Option<bool>,
    /// 结果里最多保留多少事件（总数仍计入 `event_count`）。
    pub max_events: Option<usize>,
    /// 单次调用级 fan-out。与 `config.publish` **合并**（两个都调），见
    /// [`combine_publishers`]。
    pub publish: Option<WorkflowRuntimeEventPublisher>,
}

#[derive(Clone, Default)]
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
    /// 客户端关联标识（上游 `threadId`）。透传到 `RUN_STARTED` 事件；不落 store。
    pub thread_id: Option<String>,
    pub deadline: Option<i64>,
    pub max_duration_ms: Option<i64>,
    pub min_yield_remaining_ms: Option<u64>,
    pub include_events: Option<bool>,
    pub max_events: Option<usize>,
    /// 单次调用级 fan-out。与 `config.publish` **合并**（两个都调），见
    /// [`combine_publishers`]。
    pub publish: Option<WorkflowRuntimeEventPublisher>,
}

#[derive(Clone, Default)]
pub struct WorkflowRuntimeDeliverApprovalArgs {
    pub run_id: RunId,
    pub approval: ApprovalResult,
    pub now: Option<i64>,
    pub lease_owner: Option<LeaseOwner>,
    pub lease_ms: Option<i64>,
    /// 客户端关联标识（上游 `threadId`）。透传到 `RUN_STARTED` 事件；不落 store。
    pub thread_id: Option<String>,
    pub deadline: Option<i64>,
    pub max_duration_ms: Option<i64>,
    pub min_yield_remaining_ms: Option<u64>,
    pub include_events: Option<bool>,
    pub max_events: Option<usize>,
    /// 单次调用级 fan-out。与 `config.publish` **合并**（两个都调），见
    /// [`combine_publishers`]。
    pub publish: Option<WorkflowRuntimeEventPublisher>,
}

#[derive(Clone, Default)]
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
    /// 单次调用级 fan-out。与 `config.publish` **合并**（两个都调），见
    /// [`combine_publishers`]。
    pub publish: Option<WorkflowRuntimeEventPublisher>,
}

/// sweep 摘要里的分类计数。
pub type WorkflowRuntimeRunKindCounts = BTreeMap<WorkflowRuntimeRunResultKind, usize>;

#[derive(Debug, Clone, Default)]
pub struct WorkflowRuntimeSweepSummary {
    pub recovered: WorkflowRuntimeRunKindCounts,
    pub scheduled: WorkflowRuntimeRunKindCounts,
    pub timers: WorkflowRuntimeRunKindCounts,
    pub event_count: usize,
    pub returned_event_count: usize,
}

#[derive(Debug, Clone, Default)]
pub struct WorkflowRuntimeSweepResult {
    pub recovered: Vec<WorkflowRuntimeRunResult>,
    pub scheduled: Vec<WorkflowRuntimeRunResult>,
    pub timers: Vec<WorkflowRuntimeRunResult>,
    pub summary: WorkflowRuntimeSweepSummary,
    /// true = 因 deadline / 上限而提前停止。
    pub deadline_reached: bool,
    /// true = 可能还有活没干完，值得再 sweep 一次。
    pub remaining_may_exist: bool,
}

// ============================================================
// runtime
// ============================================================

/// workflow runtime definition：认领 → 驱动 → 心跳 → 收尾 + sweep。
///
/// 对齐上游 `WorkflowRuntimeDefinition<TWorkflows>`（types.ts:371）——继承
/// config（Rust 用组合：`definition.config`）+ 驱动方法。执行进度状态全在
/// store；definition 只是把 config（含**有状态的 store 柄**）与驱动方法绑在
/// 一个值上，跨进程「读」同一 base = 重建一个 definition 指向它。
pub struct WorkflowRuntimeDefinition<
    TWorkflows = HashMap<WorkflowId, WorkflowRegistration>,
> {
    pub config: WorkflowRuntimeConfig<TWorkflows>,
}

/// 构造 runtime（对齐上游 `defineWorkflowRuntime`，define-runtime.ts:11）。
pub fn define_workflow_runtime<TWorkflows>(
    config: WorkflowRuntimeConfig<TWorkflows>,
) -> WorkflowRuntimeDefinition<TWorkflows> {
    WorkflowRuntimeDefinition { config }
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
            "WorkflowDefinition runtime leaseMs must be a positive finite number."
        ));
    }
    Ok(v)
}

/// 归一 `min_yield_remaining_ms`（上游 `normalizeMinYieldRemainingMs`，
/// `runtime-driver.ts:1035-1043`）。上游在这里对非有限值 / 负数 `throw`；我们
/// 参数是 `u64`，负数在类型层面构造不出来，所以只剩「缺省填默认」。
/// 放**入口**做（上游在 `startRun` 顶部 `:73`），别拖到 drive 里——那样
/// `create_run` 已经在 store 落过一遍，"已归一" 也没写进类型。
fn normalize_min_yield_remaining_ms(value: Option<u64>) -> u64 {
    value.unwrap_or(DEFAULT_MIN_YIELD_REMAINING_MS)
}

/// 把 config 级和单次调用级的 publisher 合并（上游 `combinePublishers`，
/// `runtime-driver.ts:1139-1161`）。语义照抄三点：
/// - 两个都没有 ⇒ `None`（不造空闭包，省一次调用）；
/// - **同一个** publisher 同时配在两处只调一次（上游用 `Set` 按引用去重，
///   我们用 `Arc::ptr_eq`）；
/// - 配置级在前、单次级在后，两个都调。
///
/// best-effort 不变：publisher 抛错**不会**掀掉 run。这里不必自己兜——回调最终
/// 是在 core 的 publisher 闭包里被调的，而 core 用 `catch_unwind` 逐个兜住
/// （`run_workflow.rs:611`，注释直接引上游 *"A misbehaving publisher must not
/// break the run"*）。
fn combine_publishers(
    configured: Option<WorkflowRuntimeEventPublisher>,
    requested: Option<WorkflowRuntimeEventPublisher>,
) -> Option<WorkflowRuntimeEventPublisher> {
    match (configured, requested) {
        (None, None) => None,
        (Some(c), None) => Some(c),
        (None, Some(r)) => Some(r),
        (Some(c), Some(r)) => {
            if Arc::ptr_eq(&c, &r) {
                return Some(c);
            }
            Some(Arc::new(move |run_id: &str, event: &WorkflowEvent| {
                c(run_id, event);
                r(run_id, event);
            }))
        }
    }
}

fn resolve_lease_ms<TWorkflows>(
    config: &WorkflowRuntimeConfig<TWorkflows>,
    lease_ms: Option<i64>,
) -> anyhow::Result<i64> {
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

fn count_kinds(results: &[WorkflowRuntimeRunResult]) -> WorkflowRuntimeRunKindCounts {
    let mut map = BTreeMap::new();
    for r in results {
        *map.entry(r.kind).or_default() += 1;
    }
    map
}

impl<TWorkflows: WorkflowRegistry> WorkflowRuntimeDefinition<TWorkflows> {
    /// 按 id 加载 workflow：调 loader 构建本体、合并 previous_versions 的
    /// loader 产物、应用 version 覆盖（对齐上游 `loadWorkflow`，
    /// runtime-driver.ts:856——上游每次都重新 `await load()`，这里同样不缓存）。
    fn load_workflow(&self, workflow_id: &WorkflowId) -> anyhow::Result<AnyWorkflowDefinition> {
        let registration = self
            .config
            .workflows
            .get(workflow_id)
            .ok_or_else(|| anyhow::anyhow!("WorkflowDefinition \"{workflow_id}\" is not registered."))?;
        let loaded = (registration.load)();
        let mut prevs = loaded.previous_versions.clone();
        for load_previous in registration.previous_versions.values() {
            prevs.push((load_previous)());
        }
        if registration.version.is_some() || !prevs.is_empty() {
            // 版本路由的罕见路径：拿一份可改的字段载体，把注册项声明的 version /
            // previous_versions 覆盖上去。`AnyWorkflowDefinition` 是 Arc 包着的，
            // 只能读，所以这里显式取底层再改。
            let mut workflow: WorkflowDefinition = (*loaded).clone();
            workflow.version = registration
                .version
                .clone()
                .or_else(|| workflow.version.clone());
            workflow.previous_versions = prevs;
            return Ok(workflow.into());
        }
        Ok(loaded)
    }

    /// 启动一个 run：幂等创建 → 认领 → 驱动到下一个 pause / 终态。
    pub async fn start_run(&self, args: WorkflowRuntimeStartRunArgs) -> anyhow::Result<WorkflowRuntimeRunResult> {
        let started_at = now_ms();
        let now = args.now.unwrap_or_else(now_ms);
        let deadline = resolve_runtime_deadline(args.deadline, args.max_duration_ms, started_at)?;
        let min_yield = normalize_min_yield_remaining_ms(args.min_yield_remaining_ms);
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
            return Ok(WorkflowRuntimeRunResult {
                kind: WorkflowRuntimeRunResultKind::NotClaimable,
                run_id: args.run_id,
                workflow_id: Some(args.workflow_id),
                run: Some(run.clone()),
                events: vec![],
                event_count: 0,
                events_truncated: None,
            });
        }

        self.drive_claimed_run(DriveArgs {
            thread_id: args.thread_id.clone(),
            publish: args.publish.clone(),
            workflow: &workflow,
            workflow_id: &args.workflow_id,
            run_id: &args.run_id,
            input: Some(args.input),
            lease_owner: args.lease_owner.clone(),
            lease_ms: args.lease_ms,
            now,
            deadline,
            min_yield_remaining_ms: min_yield,
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
    ) -> anyhow::Result<WorkflowRuntimeRunResult> {
        let started_at = now_ms();
        let now = args.now.unwrap_or_else(now_ms);
        let deadline = resolve_runtime_deadline(args.deadline, args.max_duration_ms, started_at)?;
        let min_yield = normalize_min_yield_remaining_ms(args.min_yield_remaining_ms);

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
                aa_workflow_core::signal_event(
                    run_store.as_ref(),
                    &args.run_id,
                    &args.name,
                    args.payload.clone(),
                )?;
                let workflow = self.load_workflow(&workflow_id)?;
                return self
                    .drive_claimed_run(DriveArgs {
            thread_id: args.thread_id.clone(),
            publish: args.publish.clone(),
                        workflow: &workflow,
                        workflow_id: &workflow_id,
                        run_id: &args.run_id,
                        // resume 用**持久化在 run 记录里的 input**，不是 None：
                        // core 每次 drive 都用 `initialize(input)` 重建 state，
                        // 给 Null 会让强类型 input（`.input::<T>()`）反序列化失败。
                        // 上游同此：`runtime-driver.ts:481` 传 `claim.run.input`。
                        input: Some(run.input.clone()),
                        lease_owner: args.lease_owner.clone(),
                        lease_ms: args.lease_ms,
                        now,
                        deadline,
                        min_yield_remaining_ms: min_yield,
                        yield_resume_at: Some(now + 1),
                        include_events: args.include_events,
                        max_events: args.max_events,
                    })
                    .await;
            }
            DeliverSignalResult::NotFound => {
                return Ok(WorkflowRuntimeRunResult {
                    kind: WorkflowRuntimeRunResultKind::NotFound,
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
                    DeliverSignalResult::Duplicate { .. } => WorkflowRuntimeRunResultKind::Duplicate,
                    _ => WorkflowRuntimeRunResultKind::NotWaiting,
                };
                return Ok(WorkflowRuntimeRunResult {
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
    ) -> anyhow::Result<WorkflowRuntimeRunResult> {
        let started_at = now_ms();
        let now = args.now.unwrap_or_else(now_ms);
        let deadline = resolve_runtime_deadline(args.deadline, args.max_duration_ms, started_at)?;
        let min_yield = normalize_min_yield_remaining_ms(args.min_yield_remaining_ms);

        let delivered = self.config.store.deliver_approval(DeliverApprovalArgs {
            run_id: args.run_id.clone(),
            approval: args.approval.clone(),
            now,
        })?;

        let workflow_id = match &delivered {
            DeliverApprovalResult::Delivered { run } => run.workflow_id.clone(),
            DeliverApprovalResult::NotFound => {
                return Ok(WorkflowRuntimeRunResult {
                    kind: WorkflowRuntimeRunResultKind::NotFound,
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
                    DeliverApprovalResult::Duplicate { .. } => WorkflowRuntimeRunResultKind::Duplicate,
                    _ => WorkflowRuntimeRunResultKind::NotWaiting,
                };
                return Ok(WorkflowRuntimeRunResult {
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
        aa_workflow_core::signal_run(
            run_store.as_ref(),
            &args.run_id,
            &args.approval.approval_id,
            serde_json::to_value(&args.approval)?,
        )?;

        let workflow = self.load_workflow(&workflow_id)?;
        // 同 `deliver_signal`：resume 必须带持久化的 input。
        let input = self
            .config
            .store
            .load_run(&args.run_id)?
            .map(|r| r.input)
            .unwrap_or(serde_json::Value::Null);
        self.drive_claimed_run(DriveArgs {
            thread_id: args.thread_id.clone(),
            publish: args.publish.clone(),
            workflow: &workflow,
            workflow_id: &workflow_id,
            run_id: &args.run_id,
            input: Some(input),
            lease_owner: args.lease_owner.clone(),
            lease_ms: args.lease_ms,
            now,
            deadline,
            min_yield_remaining_ms: min_yield,
            yield_resume_at: Some(now + 1),
            include_events: args.include_events,
            max_events: args.max_events,
        })
        .await
    }

    /// 有界 sweep：恢复陈旧 run → 启动到期 schedule → 投递到期 timer。
    pub async fn sweep(        &self,
        args: WorkflowRuntimeSweepArgs,
    ) -> anyhow::Result<WorkflowRuntimeSweepResult> {
        let started_at = now_ms();
        let now = args.now.unwrap_or_else(now_ms);
        let deadline = resolve_runtime_deadline(args.deadline, args.max_duration_ms, started_at)?;
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
        // sweep 是唯一自带属主前缀的入口：`sweep:{now}` 标识这一批扫描（上游
        // `runtime-driver.ts:416`）。其余入口都不生成，交给 `drive_claimed_run`
        // 兜底成 `runtime:{run_id}`（上游 `:680`）。
        let lease_owner = args
            .lease_owner
            .clone()
            .unwrap_or_else(|| create_lease_owner(&format!("sweep:{now}")));
        // store 的 claim 调用（`claim_stale_runs` / `claim_due_schedule_buckets` /
        // `claim_due_timers`）直接要 leaseMs，所以 sweep 自己解析一份（上游 `:418`）。
        let lease_ms = resolve_lease_ms(&self.config, args.lease_ms)?;
        let min_yield = normalize_min_yield_remaining_ms(args.min_yield_remaining_ms);

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
                    lease_owner: Some(lease_owner.clone()),
                    lease_ms: Some(lease_ms),
                    thread_id: None,
                    publish: None,
                    now,
                    deadline,
                    min_yield_remaining_ms: min_yield,
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
                    thread_id: None,
                    publish: args.publish.clone(),
                    include_events: args.include_events,
                    max_events: args.max_events,
                })
                .await?;
            if !matches!(
                result.kind,
                WorkflowRuntimeRunResultKind::NotClaimable | WorkflowRuntimeRunResultKind::NotFound
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
                    thread_id: None,
                    publish: args.publish.clone(),
                    deadline,
                    max_duration_ms: None,
                    min_yield_remaining_ms: args.min_yield_remaining_ms,
                    include_events: args.include_events,
                    max_events: args.max_events,
                })
                .await?;
            timers.push(result);
        }

        let summary = WorkflowRuntimeSweepSummary {
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
    /// 驱动用 core 的 [`run_workflow`](aa_workflow_core::run_workflow)（经
    /// [`create_run_store_adapter`] 降格），心跳任务每 `lease_ms / 3` 续租一次。
    #[allow(clippy::too_many_arguments)]
    async fn drive_claimed_run(&self, args: DriveArgs<'_>) -> anyhow::Result<WorkflowRuntimeRunResult> {
        // 租约在**这里**解析，不在各个入口（上游 `runtime-driver.ts:679-681`：
        // `args.leaseOwner ?? createLeaseOwner(\`runtime:${runId}\`)` + `resolveLeaseMs`）。
        // 之前我们让每个入口自己解析，凭空多出 `signal:` / `approval:` 两个上游
        // 没有的属主前缀——属主前缀是 lease 的身份，入口层不该发明它。
        let lease_owner = args
            .lease_owner
            .clone()
            .unwrap_or_else(|| create_lease_owner(&format!("runtime:{}", args.run_id)));
        let lease_ms = resolve_lease_ms(&self.config, args.lease_ms)?;
        let claim = self.config.store.claim_run(ClaimRunArgs {
            run_id: args.run_id.to_string(),
            lease_owner: lease_owner.clone(),
            lease_ms,
            now: now_ms(),
        })?;
        match claim {
            ClaimRunResult::NotFound => {
                return Ok(WorkflowRuntimeRunResult {
                    kind: WorkflowRuntimeRunResultKind::NotFound,
                    run_id: args.run_id.to_string(),
                    workflow_id: Some(args.workflow_id.to_string()),
                    run: None,
                    events: vec![],
                    event_count: 0,
                    events_truncated: None,
                });
            }
            ClaimRunResult::NotClaimable { run } => {
                return Ok(WorkflowRuntimeRunResult {
                    kind: WorkflowRuntimeRunResultKind::NotClaimable,
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
        // core 现在直接把 run_id 交给 publish（对齐上游 `(runId, event)`），
        // 所以这里不必再自己 capture 一份——闭包首参就是它。
        // 合并在汇合点做：config 级在前、单次级在后，同一个只调一次。
        let runtime_publish = combine_publishers(self.config.publish.clone(), args.publish.clone());
        let publish: Arc<dyn Fn(&str, WorkflowEvent) + Send + Sync> = {
            let collected = collected.clone();
            let total = total.clone();
            Arc::new(move |run_id: &str, event: WorkflowEvent| {
                let count = total.fetch_add(1, Ordering::Relaxed);
                let keep = include_events && max_events.map(|m| count < m).unwrap_or(true);
                if keep {
                    collected.lock().unwrap().push(event.clone());
                }
                if let Some(publish) = &runtime_publish {
                    publish(run_id, &event);
                }
            })
        };

        // 心跳：lease_ms / 3 周期续租（对齐上游 startLeaseHeartbeat）。
        let heartbeat = start_lease_heartbeat(
            Arc::clone(&self.config.store),
            args.run_id.to_string(),
            lease_owner.clone(),
            lease_ms,
        );

        // 驱动（core）。signal / approval 的恢复在进入 drive 前已经以
        // StepResume 落盘，重放时由 exec_pause 拾取。
        let mut opts = RunWorkflowOptions::new(
            Arc::new(args.workflow.clone()),
            run_store_for_core(&self.config),
        )
        .input(args.input.clone().unwrap_or(serde_json::Value::Null))
        .run_id(args.run_id.clone())
        .min_yield_remaining(args.min_yield_remaining_ms);
        if let Some(deadline) = args.deadline {
            opts = opts.deadline(deadline);
        }
        if let Some(at) = args.yield_resume_at {
            opts = opts.yield_resume_at(at);
        }
        // start_run / deliver_signal / deliver_approval 三个入口都汇到这里，
        // 所以 threadId 只要在这一处转发就够（上游三个 args 各带一个，
        // runtime-driver.ts:115/182/248 同样都往 driveClaimedRun 传）。
        opts = opts.thread_id(args.thread_id.clone());
        opts = opts.publish(Some(publish));
        let drive_result = run_workflow(opts).outcome().await;

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
        let sync_result =
            sync_timer_from_run_state(&self.config, args.run_id, args.workflow_id, args.now);

        self.config.store.release_run_lease(ReleaseRunLeaseArgs {
            run_id: args.run_id.to_string(),
            lease_owner: lease_owner.clone(),
        })?;
        heartbeat_error?;
        sync_result?;

        // drive 出错（step 终局失败等）→ Errored 结果。run_workflow 的错误
        // 详情已经落在事件日志（StepFailed / RunErrored）里，快照里也有。
        if drive_result.is_err() {
            return Ok(WorkflowRuntimeRunResult {
                kind: WorkflowRuntimeRunResultKind::Errored,
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
        Ok(WorkflowRuntimeRunResult {
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
    workflow: &'a WorkflowDefinition,
    workflow_id: &'a WorkflowId,
    run_id: &'a RunId,
    /// 全新启动时的 input；resume 时为 `None`（input 已在 store 里）。
    input: Option<serde_json::Value>,
    /// 租约属主。`None` = 由 [`Self::drive_claimed_run`] 按上游
    /// `runtime:{run_id}` 前缀兜底生成（`runtime-driver.ts:680`）。**只有 sweep
    /// 会预先生成** `sweep:{now}`（上游 `:416`）——那是批次标识，不是 per-run 的。
    lease_owner: Option<LeaseOwner>,
    /// `None` = 走 `config.default_lease_ms` → `DEFAULT_LEASE_MS`。
    lease_ms: Option<i64>,
    /// 客户端关联标识，透传进 `RUN_STARTED`（上游 `DriveOptions.threadId`）。
    /// sweep 传 `None`——上游 `sweep` 不带 threadId（`types.ts:459-474`）。
    thread_id: Option<String>,
    /// **单次调用级** fan-out；与 `config.publish` 的合并发生在
    /// [`Self::drive_claimed_run`]（四个入口的唯一汇合点，避免每处重复调
    /// [`combine_publishers`]）。
    publish: Option<WorkflowRuntimeEventPublisher>,
    /// 本次 drive 的「现在」，入口算一次一路传下去。**不要**在下游重读墙钟：
    /// 上游把 `args.now` 一路传到 `syncTimerFromRunState`
    /// （`runtime-driver.ts:761`）就是这个原因。
    now: i64,
    deadline: Option<i64>,
    /// 已归一（入口套了 [`normalize_min_yield_remaining_ms`]），所以不是
    /// `Option`。上游类型是 `Option<number>` 但同样在 `startRun` 顶部就归一了
    /// （`:73`）——归一过还用 `Option` 表示，等于把「已校验」只写在注释里。
    min_yield_remaining_ms: u64,
    yield_resume_at: Option<i64>,
    include_events: Option<bool>,
    max_events: Option<usize>,
}

/// 每次调用建一个新 adapter（轻量 struct + Arc，与上游每次 drive 建一次一致）。
fn run_store_for_core<TWorkflows>(
    config: &WorkflowRuntimeConfig<TWorkflows>,
) -> Arc<dyn aa_workflow_core::RunStore> {
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
/// `now` 由调用方传入（上游 `runtime-driver.ts:761` 把 `args.now` 一路传下来），
/// **不要**在这里重读墙钟：timer 的 deadline 判定要和这次 drive 用同一个「现在」，
/// 否则同一次 drive 里两处时间会差几毫秒。
fn sync_timer_from_run_state<TWorkflows>(
    config: &WorkflowRuntimeConfig<TWorkflows>,
    run_id: &str,
    workflow_id: &str,
    now: i64,
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
        now,
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

fn classify_run(run: Option<&WorkflowExecution>, event_count: usize) -> WorkflowRuntimeRunResultKind {
    match run.map(|r| r.status) {
        Some(WorkflowExecutionStatus::Finished) => WorkflowRuntimeRunResultKind::Completed,
        Some(WorkflowExecutionStatus::Paused) => WorkflowRuntimeRunResultKind::Paused,
        Some(WorkflowExecutionStatus::Errored) | Some(WorkflowExecutionStatus::Aborted) => {
            WorkflowRuntimeRunResultKind::Errored
        }
        Some(WorkflowExecutionStatus::Running) | Some(WorkflowExecutionStatus::Queued) => {
            WorkflowRuntimeRunResultKind::Running
        }
        None if event_count > 0 => WorkflowRuntimeRunResultKind::Running,
        None => WorkflowRuntimeRunResultKind::NotFound,
    }
}

#[cfg(test)]
mod driver_tests {
    //! 驱动器端到端：真实 workflow + 真实 InMemoryExecutionStore。
    use super::*;
    use crate::WorkflowRunStoreAdapterStore;
    use crate::in_memory_store::InMemoryExecutionStore;
    use aa_workflow_core::{CreateWorkflowConfig, RunState, RunStatus, WaitForState, WorkflowCtx, create_workflow};

    /// 双柄：`mem` 供测试直读内部，`store` 喂给 runtime（trait 对象）。
    struct Fixture {
        rt: Arc<WorkflowRuntimeDefinition>,
        mem: Arc<InMemoryExecutionStore>,
    }

    fn runtime_with(workflow_id: &str, workflow: WorkflowDefinition) -> Fixture {
        let mem: Arc<InMemoryExecutionStore> = Arc::new(InMemoryExecutionStore::default());
        let store: Arc<dyn WorkflowExecutionStore> = mem.clone();
        let mut workflows = HashMap::new();
        workflows.insert(
            workflow_id.to_string(),
            WorkflowRegistration {
                load: Arc::new(move || workflow.clone().into()),
                previous_versions: HashMap::new(),
                version: None,
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

    /// `combine_publishers` 的三条语义（上游 `combinePublishers`，
    /// `runtime-driver.ts:1139-1161`）：两个都没有 ⇒ `None`；同一个 publisher
    /// 配在两处**只调一次**（上游 `Set` 去重）；两个不同 ⇒ 都调，配置级在前。
    #[test]
    fn combine_publishers_semantics() {
        assert!(combine_publishers(None, None).is_none(), "两个都没有不该造空闭包");

        let only_cfg = combine_publishers(Some(Arc::new(|_: &str, _: &WorkflowEvent| {})), None);
        assert!(only_cfg.is_some(), "只有 config 级时应原样返回");

        let only_req = combine_publishers(None, Some(Arc::new(|_: &str, _: &WorkflowEvent| {})));
        assert!(only_req.is_some(), "只有单次级时应原样返回");

        // 同一个 Arc 配在两处 ⇒ 只调一次
        let calls = Arc::new(std::sync::Mutex::new(0usize));
        let c1 = calls.clone();
        let shared: WorkflowRuntimeEventPublisher =
            Arc::new(move |_: &str, _: &WorkflowEvent| {
                *c1.lock().unwrap() += 1;
            });
        let combined = combine_publishers(Some(shared.clone()), Some(shared.clone()))
            .expect("同一 publisher 仍应产出回调");
        combined("r", &WorkflowEvent::RunFinished {
            ts: 0,
            run_id: "r".into(),
            output: None,
        });
        assert_eq!(*calls.lock().unwrap(), 1, "同一 publisher 只该调一次");

        // 两个不同 ⇒ 都调，且配置级在前
        let order = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));
        let o1 = order.clone();
        let cfg: WorkflowRuntimeEventPublisher = Arc::new(move |_: &str, _: &WorkflowEvent| {
            o1.lock().unwrap().push("cfg");
        });
        let o2 = order.clone();
        let req: WorkflowRuntimeEventPublisher = Arc::new(move |_: &str, _: &WorkflowEvent| {
            o2.lock().unwrap().push("req");
        });
        let combined = combine_publishers(Some(cfg), Some(req)).unwrap();
        combined("r", &WorkflowEvent::RunFinished {
            ts: 0,
            run_id: "r".into(),
            output: None,
        });
        assert_eq!(*order.lock().unwrap(), vec!["cfg", "req"], "配置级应在前");
    }

    /// 端到端：config 级与单次调用级**都**收到事件（上游 `combinePublishers` 的
    /// 存在意义就是让单次调用不必牺牲全局订阅）。
    #[tokio::test]
    async fn config_and_per_call_publishers_both_fire() {
        let mem: Arc<InMemoryExecutionStore> = Arc::new(InMemoryExecutionStore::default());
        let store: Arc<dyn WorkflowExecutionStore> = mem.clone();
        let mut workflows = HashMap::new();
        workflows.insert(
            "simple".to_string(),
            WorkflowRegistration {
                load: Arc::new(move || simple_workflow().into()),
                previous_versions: HashMap::new(),
                version: None,
                schedules: vec![],
            },
        );
        let cfg_hits = Arc::new(std::sync::Mutex::new(0usize));
        let req_hits = Arc::new(std::sync::Mutex::new(0usize));
        let c = cfg_hits.clone();
        let config_publish: WorkflowRuntimeEventPublisher =
            Arc::new(move |_: &str, _: &WorkflowEvent| {
                *c.lock().unwrap() += 1;
            });
        let r = req_hits.clone();
        let per_call_publish: WorkflowRuntimeEventPublisher =
            Arc::new(move |_: &str, _: &WorkflowEvent| {
                *r.lock().unwrap() += 1;
            });

        let mut cfg = WorkflowRuntimeConfig::new(store, workflows);
        cfg.publish = Some(config_publish);
        let rt = define_workflow_runtime(cfg);
        rt.start_run(WorkflowRuntimeStartRunArgs {
            workflow_id: "simple".into(),
            run_id: "both".into(),
            input: serde_json::json!({}),
            publish: Some(per_call_publish),
            ..Default::default()
        })
        .await
        .unwrap();

        assert!(*cfg_hits.lock().unwrap() > 0, "config 级 publisher 应收到事件");
        assert!(*req_hits.lock().unwrap() > 0, "单次调用级 publisher 应收到事件");
    }

    fn simple_workflow() -> WorkflowDefinition {
        create_workflow(CreateWorkflowConfig::new("simple").input::<serde_json::Value>())
            .handler(|ctx: WorkflowCtx| async move {
                let a = ctx
                    .step("a", move |_sc: aa_workflow_core::StepCtx| async move {
                        Ok(serde_json::json!({ "v": 1 }))
                    })
                    .await?;
                Ok(serde_json::json!({ "got": a }))
            })
            .into()
    }

    fn waiting_workflow() -> WorkflowDefinition {
        create_workflow(CreateWorkflowConfig::new("waiter").input::<serde_json::Value>())
            .handler(|ctx: WorkflowCtx| async move {
                let payment = ctx.wait_for_event("payment", "payment").await?;
                let _ = payment;
                ctx.step("ship", move |_sc: aa_workflow_core::StepCtx| async move {
                    Ok(serde_json::json!({ "shipped": true }))
                })
                .await?;
                Ok(serde_json::json!({ "shipped": true }))
            })
            .into()
    }

    fn sleeping_workflow() -> WorkflowDefinition {
        create_workflow(CreateWorkflowConfig::new("sleeper").input::<serde_json::Value>())
            .handler(|ctx: WorkflowCtx| async move {
                // 先落一个 checkpoint，证明恢复时重放短路。
                ctx.step(
                    "before-sleep",
                    move |_sc: aa_workflow_core::StepCtx| async move { Ok(serde_json::Value::Null) },
                )
                .await?;
                ctx.sleep("hold", std::time::Duration::from_secs(60))
                    .await?;
                ctx.step(
                    "after-sleep",
                    move |_sc: aa_workflow_core::StepCtx| async move {
                        Ok(serde_json::json!({ "woke": true }))
                    },
                )
                .await?;
                Ok(serde_json::json!({ "woke": true }))
            })
            .into()
    }

    /// 短 sleep（80ms），用于端到端验证 sweep 认领 timer。
    fn short_sleep_workflow() -> WorkflowDefinition {
        create_workflow(CreateWorkflowConfig::new("sleeper").input::<serde_json::Value>())
            .handler(|ctx: WorkflowCtx| async move {
                ctx.sleep("hold", std::time::Duration::from_millis(80))
                    .await?;
                ctx.step(
                    "after",
                    move |_sc: aa_workflow_core::StepCtx| async move {
                        Ok(serde_json::json!({ "after": true }))
                    },
                )
                .await?;
                Ok(serde_json::json!({ "woke": true, "after": true }))
            })
            .into()
    }

    fn long_step_workflow() -> WorkflowDefinition {        create_workflow(CreateWorkflowConfig::new("long").input::<serde_json::Value>())
            .handler(|ctx: WorkflowCtx| async move {
                ctx.step("slow", move |_sc: aa_workflow_core::StepCtx| async move {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    Ok(serde_json::Value::Null)
                })
                .await?;
                Ok(serde_json::Value::Null)
            })
            .into()
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

        assert_eq!(out.kind, WorkflowRuntimeRunResultKind::Completed);
        assert_eq!(
            out.run.as_ref().unwrap().status,
            WorkflowExecutionStatus::Finished
        );
        assert!(out.event_count > 0, "应收集到事件");
        assert!(!out.events.is_empty());
    }

    /// `thread_id` 从 args 一路透到 `RUN_STARTED`（上游
    /// `runtime-driver.ts:115` → `run-workflow.ts:242`）。
    ///
    /// 上一条 core 测试已经钉住 emit 本身；这条钉的是**runtime 层的接线**——
    /// 三个 args 各带一个 `threadId`（`types.ts:402/418/433`），
    /// `drive_claimed_run` 单点转发（`:115/182/248`）。中间任何一环漏掉都只会在
    /// 这里现形。
    #[tokio::test]
    async fn start_run_threads_id_into_run_started() {
        let fx = runtime_with("simple", simple_workflow());
        let out = fx
            .rt
            .start_run(WorkflowRuntimeStartRunArgs {
                workflow_id: "simple".into(),
                run_id: "rt-thread".into(),
                input: serde_json::json!({}),
                thread_id: Some("from-args".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        let started = out
            .events
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::RunStarted { thread_id, .. } => Some(thread_id.clone()),
                _ => None,
            })
            .expect("事件流里应有 RUN_STARTED");
        assert_eq!(started, Some("from-args".into()));
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
        assert_eq!(second.kind, WorkflowRuntimeRunResultKind::NotClaimable);
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
        assert_eq!(started.kind, WorkflowRuntimeRunResultKind::Paused);
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
        assert_eq!(result.kind, WorkflowRuntimeRunResultKind::Completed);

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
                .into(),
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
        assert_eq!(started.kind, WorkflowRuntimeRunResultKind::Paused);

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
            WorkflowRuntimeRunResultKind::NotWaiting,
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
        ctx.step(&id, move |_sc: aa_workflow_core::StepCtx| async move {
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

    /// 属主前缀只在 `drive_claimed_run` 里兜底成 `runtime:{run_id}`（上游
    /// `runtime-driver.ts:680`）；入口层**不发明**前缀。之前我们让每个入口自己
    /// 解析 lease，凭空多出 `signal:` / `approval:` 两个上游没有的前缀。
    ///
    /// 观测方式：让 workflow 在**执行途中**读自己的 lease。不能在 drive 之后读
    /// ——`release_run_lease` 会把 lease 清掉（`in_memory_store.rs:471-478`）；
    /// 也不能靠 `can_claim` 的同属主规则绕（那样测的是 claim 成不成功，不是
    /// 前缀本身，而且 run 得先被占住，很容易写成空测试——我第一版就是这么废掉的）。
    #[tokio::test]
    async fn start_run_lease_owner_is_runtime_prefixed() {
        let mem: Arc<InMemoryExecutionStore> = Arc::new(InMemoryExecutionStore::default());
        let seen: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
        let s1 = seen.clone();
        let m1 = mem.clone();
        let wf = WorkflowDefinition::new("peek").handler(move |ctx: WorkflowCtx| {
            let s1 = s1.clone();
            let m1 = m1.clone();
            async move {
                ctx.step("peek", move |_sc: aa_workflow_core::StepCtx| {
                    let s1 = s1.clone();
                    let m1 = m1.clone();
                    async move {
                        // 此刻 drive 正在跑，lease 还在。
                        let owner = m1
                            .load_run("peek:1")
                            .unwrap()
                            .and_then(|r| r.lease.map(|l| l.owner));
                        *s1.lock().unwrap() = owner;
                        Ok(serde_json::Value::Null)
                    }
                })
                .await?;
                Ok(serde_json::Value::Null)
            }
        });
        let store: Arc<dyn WorkflowExecutionStore> = mem.clone();
        let mut workflows = HashMap::new();
        workflows.insert(
            "peek".to_string(),
            WorkflowRegistration {
                load: Arc::new(move || wf.clone().into()),
                previous_versions: HashMap::new(),
                version: None,
                schedules: vec![],
            },
        );
        let rt = define_workflow_runtime(WorkflowRuntimeConfig::new(store, workflows));
        rt.start_run(WorkflowRuntimeStartRunArgs {
            workflow_id: "peek".into(),
            run_id: "peek:1".into(),
            input: serde_json::json!({}),
            now: Some(0),
            lease_owner: None,
            lease_ms: None,
            ..Default::default()
        })
        .await
        .unwrap();

        let owner = seen.lock().unwrap().clone().expect("step 内应读到 lease");
        assert!(
            owner.starts_with("runtime:"),
            "兜底属主前缀应是 runtime:（上游 :680），实际：{owner}"
        );
        assert!(
            !owner.starts_with("signal:") && !owner.starts_with("approval:"),
            "入口层不该发明前缀（那是我们本地自造的），实际：{owner}"
        );
    }

    /// sweep 的属主前缀是 `sweep:{now}`（上游 `:416`），不是 `runtime:{run_id}`。
    /// 同样在 workflow 执行途中读 lease。
    #[tokio::test]
    async fn sweep_lease_owner_is_sweep_prefixed() {
        let mem: Arc<InMemoryExecutionStore> = Arc::new(InMemoryExecutionStore::default());
        let seen: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
        let s1 = seen.clone();
        let m1 = mem.clone();
        let wf = WorkflowDefinition::new("peek").handler(move |ctx: WorkflowCtx| {
            let s1 = s1.clone();
            let m1 = m1.clone();
            async move {
                ctx.step("peek", move |_sc: aa_workflow_core::StepCtx| {
                    let s1 = s1.clone();
                    let m1 = m1.clone();
                    async move {
                        let owner = m1
                            .load_run("peek:2")
                            .unwrap()
                            .and_then(|r| r.lease.map(|l| l.owner));
                        *s1.lock().unwrap() = owner;
                        Ok(serde_json::Value::Null)
                    }
                })
                .await?;
                Ok(serde_json::Value::Null)
            }
        });
        let store: Arc<dyn WorkflowExecutionStore> = mem.clone();
        let mut workflows = HashMap::new();
        workflows.insert(
            "peek".to_string(),
            WorkflowRegistration {
                load: Arc::new(move || wf.clone().into()),
                previous_versions: HashMap::new(),
                version: None,
                schedules: vec![],
            },
        );
        let rt = define_workflow_runtime(WorkflowRuntimeConfig::new(store, workflows));
        // 造一个"上一个 worker 死了"的 run：lease 过期 → sweep 会捡起并驱动。
        mem.create_run(CreateRunArgs {
            run_id: "peek:2".into(),
            workflow_id: "peek".into(),
            workflow_version: None,
            input: serde_json::json!({}),
            now: 0,
        })
        .unwrap();
        mem.claim_run(ClaimRunArgs {
            run_id: "peek:2".into(),
            lease_owner: "dead-worker".into(),
            lease_ms: 50,
            now: 0,
        })
        .unwrap();
        rt.sweep(WorkflowRuntimeSweepArgs { now: Some(100), ..Default::default() })
            .await
            .unwrap();

        let owner = seen.lock().unwrap().clone().expect("step 内应读到 lease");
        assert!(
            owner.starts_with("sweep:"),
            "sweep 的属主前缀应是 sweep:（上游 :416），实际：{owner}"
        );
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
        assert_eq!(sweep.recovered[0].kind, WorkflowRuntimeRunResultKind::Completed);
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
        assert_eq!(started.kind, WorkflowRuntimeRunResultKind::Paused);

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
                    awaiting: vec![],
                    waiting_for: Some(WaitForState {
                        step_id: Some("hold".into()),
                        signal_name: "__timer".into(),
                        deadline: Some(now + 30),
                        meta: None,
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
                        thread_id: None,
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
                        meta: None,
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
        assert_eq!(out.kind, WorkflowRuntimeRunResultKind::Completed);
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
