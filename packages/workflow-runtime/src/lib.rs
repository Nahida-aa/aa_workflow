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
/// **继承 [`RunStore`]**，不设中间层、也不需要适配器。上游那两个额外名字
/// （`WorkflowRunStoreAdapter` / `WorkflowRunStoreAdapterStore`）的来龙去脉
/// 见下节。
///
/// # 与上游的关系（三个名字，容易看错）
///
/// 上游那边有**三个**相关的名字，其中两个只差一个 `Store` 后缀：
///
/// | 上游 | 是什么 | 我们的对应 |
/// | ---- | ------ | ---------- |
/// | `RunStore`（core） | 6 个方法，[`RunStore`] | ✅ 就是本 trait 的父 trait |
/// | `WorkflowRunStoreAdapter = RunStore` | 一行 `export type` 真别名 | ❌ 不需要（其唯一用途是标注适配器输出类型） |
/// | `WorkflowRunStoreAdapterStore` | 独立 interface，**不是** `RunStore` 的重复 | ❌ 不设，见下 |
/// | `WorkflowExecutionStore extends …AdapterStore` | +19 个方法 | ✅ 本 trait |
///
/// `WorkflowRunStoreAdapterStore` 与 `RunStore` 在事件读写上有**真实差别**
/// （批量 `appendEvents`、返回带 `eventIndex` / `createdAt` 的
/// `StoredWorkflowEvent`），不是命名差异。它存在是因为**上游正处于迁移中**：
/// core 还在用旧的 `RunStore`，而 runtime 已用新的——`createRunStoreAdapter`
/// 就是把新的降格成旧的喂给 core 的过渡层。
///
/// 我们不设它的理由：
///
/// 1. 那是**迁移期的成本**，我们不在迁移中；
/// 2. 那两处新语义（事件索引、批量 append）**driver 根本不用**——查
///    `runtime-driver.ts`，它只调 lease / timer / schedule / run 生命周期
///    那几组方法；新增语义只有 store 内部实现与 `subscribe` 回调用到；
/// 3. **没有适配器可写**：适配器的唯一职责是「新 → 旧」降格，我们没有两代
///    形状，supertrait 已经免费做到了它做的事（core 的 `run_workflow` 要
///    `RunStore`，而本 trait 就是）。
///
/// 代价：将来做 Postgres / D1 这类后端时，事件索引与分页会变成刚需，那时要改
/// 本 trait。接受——现在还没有第二个 store 实现，为假想的第三个实现设计形状是
/// 过度设计。
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
pub trait WorkflowExecutionStore: RunStore {}
