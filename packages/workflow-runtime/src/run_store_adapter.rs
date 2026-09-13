//! 执行存储的**基础契约**：run 元数据 + 事件日志。
//!
//! 对齐 TanStack `WorkflowRunStoreAdapterStore`
//! （`workflow-runtime/src/types.ts:290`）。`WorkflowExecutionStore` 继承它。
//!
//! # 与 core 的 [`RunStore`] 是什么关系
//!
//! 两者**形状相近但不是同一个**。core 的 `RunStore` 是引擎 replay 需要的最小
//! 面；本 trait 是 runtime 需要的面，在事件读写上有真实差别：
//!
//! | core [`RunStore`] | 本 trait | 差别 |
//! | ----------------- | -------- | ---- |
//! | `get_run_state` | [`load_run_state`](WorkflowRunStoreAdapterStore::load_run_state) | 仅命名 |
//! | `set_run_state` | [`save_run_state`](WorkflowRunStoreAdapterStore::save_run_state) | 命名 + 参数打包 |
//! | `delete_run(id, reason)` | [`delete_run`](WorkflowRunStoreAdapterStore::delete_run) | ✅ 相同 |
//! | `append_event`（单条） | [`append_events`](WorkflowRunStoreAdapterStore::append_events)（批量，返回 next_index） | **语义差别** |
//! | `get_events` → 裸数组 | [`read_events`](WorkflowRunStoreAdapterStore::read_events) → 带索引信封 | **语义差别**（含 `from_index` 游标） |
//! | `subscribe` | [`subscribe_events`](WorkflowRunStoreAdapterStore::subscribe_events) | 同签名 |
//!
//! 上游之所以在 core 之上另立这一层，是因为它**正处于迁移中**：core 还在用
//! `RunStore`，runtime 已改用这一套，`run-store-adapter.ts` 的
//! `createRunStoreAdapter` 负责把新的降格成旧的喂给 core
//! （`runtime-driver.ts:723`）。
//!
//! # 我们为什么照做
//!
//! 尽管我们不处于迁移中，这一层仍然要立，因为：
//!
//! 1. `WorkflowExecutionStore` **依赖它**——扩展层的查询方法
//!    （`list_runs` / `get_run_timeline`）返回的就是 [`StoredWorkflowEvent`]；
//! 2. 事件索引与游标是**跨进程 / DB store 的基础**：`InMemoryStore` 里
//!    `event_index` 等于数组下标，但 Postgres store 要靠它分页与按位查询。
//!    先立形状，实现可以晚。
//!
//! [`RunStore`]: workflow_core::RunStore

use workflow_core::{DeleteReason, RunState, WorkflowEvent};

use crate::types::{
    AppendEventsArgs, AppendEventsResult, ReadEventsArgs, RunId, SaveRunStateArgs,
    StoredWorkflowEvent,
};

/// 执行存储的基础契约。对齐上游 `WorkflowRunStoreAdapterStore`
/// （`workflow-runtime/src/types.ts:290`，六个方法）。
///
/// # 名字照搬上游，尽管拗口
///
/// 这个名字读起来是「Store 的 Adapter 的 Store」，且 `Adapter` 那半截在我们
/// 这里**没有对应物**（我们不做迁移，不写那个转换函数，见下）。之所以逐字
/// 沿用，是因为本项目一贯的命名策略是对齐上游：名字难读是一次性成本，对不上
/// 上游是持续成本——每次读上游代码都要在脑子里做映射。
///
/// 它的**定位**才是要紧的：这一层是「runtime 需要的存储基础面」，与 core 的
/// [`RunStore`](workflow_core::RunStore) 形状相近但**不同**（见模块文档的对照
/// 表），差别集中在事件读写（批量 + 索引信封 + 游标）。
///
/// # 实现者要做什么
///
/// 实现本 trait + 扩展层的 `WorkflowExecutionStore` 即可被 runtime 驱动。
///
/// ⚠️ **若还想喂给 core 的 `run_workflow`，必须另外 `impl RunStore`**：那个
/// 函数要 `Arc<dyn RunStore>`，而本 trait 的继承链
/// （`WorkflowExecutionStore: WorkflowRunStoreAdapterStore`）**与 `RunStore`
/// 无关**。两套是平行的方法（`load_run_state` vs `get_run_state`、
/// `append_events` vs `append_event`），没有自动转换。实测：
/// `dyn WorkflowExecutionStore` 传给要 `&dyn RunStore` 的函数报
/// `E0308: mismatched types`。
///
/// 上游用 `createRunStoreAdapter` 在两者间做形状转换；我们省掉的是**那个转换
/// 函数**（因为可以两套都直接实现），**不是实现工作量**。
pub trait WorkflowRunStoreAdapterStore: Send + Sync {
    /// 读 run 元数据信封。`None` = 该 run 不存在。
    fn load_run_state(&self, run_id: &RunId) -> anyhow::Result<Option<RunState>>;

    /// 写 run 元数据信封。
    fn save_run_state(&self, args: SaveRunStateArgs) -> anyhow::Result<()>;

    /// 删除一个 run（含其事件日志）。
    fn delete_run(&self, run_id: &RunId, reason: DeleteReason) -> anyhow::Result<()>;

    /// 在 `expected_next_index` 处**批量**追加事件，返回追加后的下一索引。
    ///
    /// CAS 护栏：当前日志长度必须等于 `expected_next_index`，否则失败（见
    /// [`StoreError::Conflict`](workflow_core::StoreError::Conflict)）。批量是
    /// 上游的语义——一次调用可以提交多个事件，要么全成要么全不成。
    fn append_events(&self, args: AppendEventsArgs) -> anyhow::Result<AppendEventsResult>;

    /// 读事件，按 append 顺序；带索引信封。
    ///
    /// `args.from_index` 给出游标（含该索引），`None` 表示从头。
    fn read_events(&self, args: ReadEventsArgs) -> anyhow::Result<Vec<StoredWorkflowEvent>>;

    /// 订阅新事件的推送。返回取消订阅的句柄；不支持的实现返回 `None`，
    /// 调用方退化为轮询 [`read_events`](WorkflowRunStoreAdapterStore::read_events)。
    ///
    /// 与 core 的 `RunStore::subscribe` 同签名（对齐上游 `subscribeEvents?`）。
    fn subscribe_events(
        &self,
        run_id: &RunId,
        from_index: u64,
        _on_event: Box<dyn Fn(&WorkflowEvent, u64) + Send + Sync>,
    ) -> Option<Box<dyn Fn() + Send + Sync>> {
        let _ = (run_id, from_index);
        None
    }
}

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
/// `type` 别名，Rust 里无廉价对应，且它的唯一用途是标注适配器输出类型。
///
/// ⚠️ **但「不需要适配器」不等于「适配是免费的」**：core 的 `run_workflow`
/// 入参是 `Arc<dyn RunStore>`，而本 trait 的继承链是
/// `WorkflowExecutionStore: WorkflowRunStoreAdapterStore`——**与 `RunStore` 无
/// 关**。想让同一个类型既能被 runtime 驱动、又能喂给 core，必须**两套都实现**
/// （`RunStore` 的 6 个方法 + 这里的 6 个 + 扩展方法）。
///
/// 上游用 `createRunStoreAdapter` 做形状转换，我们省掉的是**那个转换函数**，
/// 不是**实现工作量**。实测证据：`dyn WorkflowExecutionStore` 传给要
/// `&dyn RunStore` 的函数会报 `E0308: mismatched types`。
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
