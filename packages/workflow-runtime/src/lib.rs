//! Workflow 的**执行所有权层**：lease、sweep、timer、schedule。
//!
//! 对齐 TanStack `@tanstack/workflow-runtime`。定位见
//! `docs/runtime-design.md`（决策记录）与 core 的 `run_store` 模块文档
//! （两个 store 契约的对应关系）。
//!
//! # 为什么 core 里没有这一层
//!
//! core 刻意只做 replay + 耐久原语 + 一个「存住就行」的 [`RunStore`] 契约
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
//! 只有 trait 骨架 + 数据结构，**尚无实现**。`docs/runtime-design.md` 的
//! D2（lease 位置）与 D4（sweep 边界）待敲定后再填方法。

use workflow_core::RunStore;

/// 运行时的执行存储契约。
///
/// **继承 [`RunStore`]**，不设中间层。上游在 core 的 `RunStore` 与
/// `WorkflowExecutionStore` 之间还有一个 `WorkflowRunStoreAdapterStore`，
/// 那是 TS 的 `export type` 别名产物（`WorkflowRunStoreAdapter = RunStore`），
/// 与我们无关——详见 `RunStore` 的文档注释。
///
/// # 为什么比 `RunStore` 宽
///
/// `RunStore` 的 6 个方法只够 replay：读写 run 元数据 + append-only 事件日志。
/// serverless / 多 worker 还需要（上游 `runtime-model.md:72`）：
///
/// - **原子认领与 lease** —— 每次调用都是新进程，必须防止两个 worker 同时
///   驱同一个 run；且 lease 要**续租**（`heartbeat`），不是一次 claim 就完事
/// - **timer 索引** —— 无进程常驻时，到期的 sleep 只能由外部 sweep 认领投递
/// - **run 状态转移作为原子操作** —— 不是「改 `RunState` 再 save」
/// - **schedule 定义与分桶**
/// - **list / timeline** —— 无进程内状态可查，全部走 store
///
/// # 方法集（未实现，待 D2 / D4 敲定）
///
/// 上游的 19 个方法分五组，实现前先按组确认范围：
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
pub trait WorkflowExecutionStore: RunStore {}
