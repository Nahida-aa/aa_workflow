//! Workflow 的**执行所有权层**：lease、sweep、timer、schedule。
//!
//! 对齐 TanStack `@tanstack/workflow-runtime`。定位见
//! `docs/runtime-design.md`（决策记录）与 core 的 `run_store` 模块文档
//! （两个 store 契约的对应关系）。
//!
//! # 为什么 core 里没有这一层
//!
//! core 刻意只做 replay + 耐久原语 + 一个「存住就行」的 [`RunStore`](workflow_core::RunStore) 契约
//! （上游原话：*The core engine is intentionally not a scheduler, queue,
//! database adapter, or deployment adapter.*）。「谁来跑、什么时候跑、崩溃了
//! 谁接手」是这一层的职责。
//!
//! # 两种部署形态，同一套抽象
//!
//! | 形态 | 代表 | lease | 有界 sweep | timer 投递 |
//! | ---- | ---- | ----- | ---------- | ---------- |
//! | 常驻进程 | LocalDub 的 server queue | 进程内锁够用 | 不必需 | 引擎自轮询够用 |
//! | 无进程常驻 | serverless / 多 worker | **必需** | **必需** | **必需**（无人常驻） |
//!
//! 抽象按**较强的那一侧**设计：有界 sweep 与 lease 即使常驻形态用不上，
//! 也是接口形状的一部分——省掉它们等于把 serverless 形态排除在外。
//!
//! # 当前状态
//!
//! 基础契约（[`WorkflowRunStoreAdapterStore`]）与类型已立，[`WorkflowExecutionStore`]
//! 的扩展方法**尚未定义**——它们依赖 `docs/runtime-design.md` 的 D2（lease
//! 位置）与 D4（sweep 边界）敲定。
//!
//! # 层次
//!
//! ```text
//! workflow_core::RunStore          引擎 replay 用（6 个方法）
//!   ↕ 形状相近但不同（见 run_store_adapter 模块文档）
//! WorkflowRunStoreAdapterStore     本 crate 的存储基础（6 个方法）
//!   └── WorkflowExecutionStore     扩展：lease / timer / schedule / 查询
//! ```

pub mod run_store_adapter;
pub mod types;

pub use run_store_adapter::WorkflowRunStoreAdapterStore;
pub use types::{
    AppendEventsArgs, AppendEventsResult, LeaseOwner, LoadedExecution, ReadEventsArgs, RunId,
    RunSummary, RunTimeline, SaveRunStateArgs, StoredWorkflowEvent, WorkflowExecution, WorkflowId,
    WorkflowLease, WorkflowVersion,
};

/// 运行时的执行存储契约。
///
/// **继承 [`WorkflowRunStoreAdapterStore`]**，与上游结构一致
/// （`WorkflowExecutionStore extends WorkflowRunStoreAdapterStore`）。
///
/// # 层次关系
///
/// ```text
/// workflow_core::RunStore          引擎 replay 用
///   ↕ 形状相近但不同 —— 见 run_store_adapter 模块文档
/// WorkflowRunStoreAdapterStore     本 crate 的存储基础（元数据 + 事件日志）
///   └── WorkflowExecutionStore     本 trait：lease / timer / schedule / 查询
/// ```
///
/// # 为什么需要比基础层更多
///
/// 基础层的 6 个方法只够**读写**：run 元数据信封 + append-only 事件日志。
/// serverless / 多 worker 还需要（上游 `runtime-model.md:72`）：
///
/// - **原子认领与 lease** —— 每次调用都是新进程，必须防止两个 worker 同时
///   驱同一个 run；且 lease 要**续租**（`heartbeat`），不是一次 claim 就完事
/// - **timer 索引** —— 无进程常驻时，到期的 sleep 只能由外部 sweep 认领投递
/// - **run 状态转移作为原子操作** —— 不是「改 `RunState` 再 save」
/// - **schedule 定义与分桶**
/// - **list / timeline** —— 无进程内状态可查，全部走 store
///
/// # 关于上游那三个名字
///
/// 上游 core 侧叫 `RunStore`，runtime 侧另起了一套 `…AdapterStore` +
/// `WorkflowExecutionStore`，中间还有一行 `export type WorkflowRunStoreAdapter
/// = RunStore`。**那层重复是迁移的产物**（core 用旧形状、runtime 用新形状，
/// `createRunStoreAdapter` 负责降格）。详见 [`WorkflowRunStoreAdapterStore`] 的文档与
/// `docs/runtime-design.md` 的 D1。
///
/// 我们照上游立 [`WorkflowRunStoreAdapterStore`]，名字也**逐字沿用**——尽管它
/// 读起来拗口（「Store 的 Adapter 的 Store」），且 `Adapter` 那半截在我们这里
/// 没有对应物。理由是本项目一贯的选择：命名对齐上游（`RunStore` / `RunState` /
/// `WorkflowEvent` / `WorkflowExecutionStore` 全是照搬）。名字难读是一次性成本，
/// 对不上上游是持续成本——每次读上游代码都要在脑子里做映射。
///
/// 另：那个真别名（`WorkflowRunStoreAdapter = RunStore`）**不设**——它是 TS 的
/// `type` 别名，Rust 里无廉价对应，且它的唯一用途是标注适配器输出类型，而我们
/// 不需要适配器：supertrait 关系已经让本 trait 的实现天然可用于 core 的
/// `run_workflow`。
///
/// # 方法集（未实现，待 D2 / D4 敲定）
///
/// 上游的扩展方法分六组，实现前先按组确认范围：
///
/// | 组 | 方法 |
/// | --- | --- |
/// | run 生命周期 | createRun / loadRun / loadExecution / markRunPaused / markRunFinished / markRunErrored |
/// | lease | claimRun / heartbeatRunLease / releaseRunLease / claimStaleRuns |
/// | timer | scheduleTimer / claimDueTimers |
/// | 投递 | deliverSignal / deliverApproval |
/// | schedule | upsertSchedule / claimDueScheduleBuckets / markScheduleBucketStarted |
/// | 查询 | listRuns / getRunTimeline |
///
/// 注意：**不要照抄全集**。哪些是当前形态真正需要的，由
/// `docs/runtime-design.md` 的 D5 按形态判断——例如 `schedule*` 三件套只在
/// 需要 cron 时才要。
pub trait WorkflowExecutionStore: WorkflowRunStoreAdapterStore {}
