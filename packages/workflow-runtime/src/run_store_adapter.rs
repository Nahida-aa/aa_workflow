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
//! # 为什么适配必须是具体类型（`RunStoreAdapter`）
//!
//! 自然的想法是「给 `dyn WorkflowExecutionStore` 直接 `impl RunStore`，省掉
//! 那个 struct」。**已实测，这条路走不通**：
//!
//! ```ignore
//! impl RunStore for dyn WorkflowExecutionStore { /* 6 个方法 */ }   // ✅ 编译通过
//!
//! fn coerce(x: Arc<dyn WorkflowExecutionStore>) -> Arc<dyn RunStore> { x }  // ❌ E0308
//! fn coerce_ref(x: &dyn WorkflowExecutionStore) -> &dyn RunStore { x }      // ❌ E0308
//! RunStore::get_events(x, "r1")                                            // ✅ 编译通过
//! ```
//!
//! 三点结论：
//!
//! 1. `impl LocalTrait for dyn LocalTrait` **合法**（孤儿规则允许），这点容易
//!    误判为不合法；
//! 2. 但它**不产生 coercion**——trait 对象的类型转换只认 **supertrait 关系**
//!    （`trait B: A` 时 `dyn B → dyn A`），手动 `impl` 不参与；
//! 3. 它唯一的作用是让方法能被 UFCS 显式调用（`RunStore::get_events(x, ..)`），
//!    而 core 的 `run_workflow` 要的是 `Arc<dyn RunStore>` 这个**类型**——
//!    「能调方法」不等于「能当参数传」。
//!
//! 所以必须有一个具体类型承载 `impl RunStore`，转换发生在 `Arc::new` 那一刻：
//!
//! ```ignore
//! let store: Arc<dyn RunStore> = Arc::new(RunStoreAdapter { inner: exec });
//! //                             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//! //                             把「实现了 RunStore 的具体类型」装进 dyn，
//! //                             这是 trait 对象转换的合法路径
//! ```
//!
//! # 与 TS 对比：这一层不是 Rust 特有的开销
//!
//! 上游同样必须手写转发（`createRunStoreAdapter` 里逐方法包装）。两边结构
//! 完全对应：
//!
//! | 环节 | TS | Rust |
//! | ---- | -- | ---- |
//! | 转换函数作者 | 库 | 库 |
//! | 实现者负担 | 一套方法 | 一套方法 |
//! | 转换时刻 | 一次对象分配 | 一次 `Arc` 分配 |
//! | 每调用跳转 | 2 层（core → adapter → store） | 2 层（同） |
//! | 参数包装 | `{ events: [event] }` | `vec![event.clone()]` |
//!
//! **打平，不是 Rust 更贵。** 而且 TS 的结构化类型意味着「只要方法名写对就
//! 通过」，Rust 的 `impl` 强制实现全部 6 个——真要论差异，那是编译期保证的
//! 收益，不是开销。
//!
//! [`RunStore`]: workflow_core::RunStore

use std::sync::Arc;
use std::sync::mpsc::Receiver;

use workflow_core::{DeleteReason, RunState, RunStore, StoreError, WorkflowEvent};

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
/// /// 它的**定位**才是要紧的：这一层是「runtime 需要的存储基础面」，与 core 的
/// [`RunStore`] 形状相近但**不同**（见模块文档的对照
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
    /// [`StoreError::Conflict`]）。批量是
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

// ============================================================
// 供 core 使用的降格适配器
// ============================================================

/// 内部载体：给 `dyn WorkflowExecutionStore` 挂 `impl RunStore` 的具体类型。
///
/// Rust 不允许直接给 `dyn Trait A` 实现 `Trait B`，需要一个具体类型承载。
/// **不是公共 API**——调用方拿到的永远是 [`create_run_store_adapter`] 返回的
/// `Arc<dyn RunStore>`，不需要知道背后是谁。
struct RunStoreAdapter {
    inner: Arc<dyn WorkflowExecutionStore>,
}

/// 把 [`WorkflowExecutionStore`] 降格成 core 的 [`RunStore`]，供 `run_workflow`
/// 使用。
///
/// 对齐上游 `run-store-adapter.ts` 的 `createRunStoreAdapter`——**它是必需的**，
/// 因为 core 的 `run_workflow` 入参是 `Arc<dyn RunStore>`（具体 trait 对象），
/// 而 [`WorkflowExecutionStore`] 的继承链
/// （`WorkflowExecutionStore: WorkflowRunStoreAdapterStore`）**与 `RunStore`
/// 无关**。实测把 `&dyn WorkflowExecutionStore` 传给要 `&dyn RunStore` 的函数会
/// 报 `E0308: mismatched types`。
///
/// # 用法
///
/// ```ignore
/// let store: Arc<dyn WorkflowExecutionStore> = Arc::new(MyStore::new());
/// let outcome = run_workflow(&wf, create_run_store_adapter(store), &opts, None).await?;
/// ```
///
/// # 为什么不让实现者自己 `impl RunStore`
///
/// 那要求每个实现者写两套几乎相同的方法（`get_run_state` vs `load_run_state`
/// 等六对），纯重复，且有漂移风险（改了一套忘另一套，表现为 runtime 与 core
/// 看到的状态不一致）。本适配器把这件事收进库里做一次。
///
/// # 有损吗
///
/// 不。适配器丢弃的只有 `from_index` 游标与 [`StoredWorkflowEvent`] 索引信封
/// ——**而 `RunStore` 本来就没有这两样**（它读的是裸事件数组）。所以是「投影到
/// 更小的面」，不是信息损失。
///
/// # 代价
///
/// 每次调用多一次 `dyn` 间接 + 参数转换（`&str` → `&String`、单条包成单元素
/// 数组等）。对本地低频率调用可忽略。
pub fn create_run_store_adapter(store: Arc<dyn WorkflowExecutionStore>) -> Arc<dyn RunStore> {
    Arc::new(RunStoreAdapter { inner: store })
}

impl RunStore for RunStoreAdapter {
    fn get_run_state(&self, run_id: &str) -> Result<Option<RunState>, StoreError> {
        self.inner
            .load_run_state(&run_id.to_string())
            .map_err(to_store_error)
    }

    fn set_run_state(&self, _run_id: &str, state: &RunState) -> Result<(), StoreError> {
        // 上游同样忽略 runId：状态信封里自带 `run_id`。
        self.inner
            .save_run_state(SaveRunStateArgs {
                state: state.clone(),
            })
            .map_err(to_store_error)
    }

    fn delete_run(&self, run_id: &str, reason: DeleteReason) -> Result<(), StoreError> {
        self.inner
            .delete_run(&run_id.to_string(), reason)
            .map_err(to_store_error)
    }

    fn append_event(
        &self,
        run_id: &str,
        expected_next_index: usize,
        event: &WorkflowEvent,
    ) -> Result<(), StoreError> {
        self.inner
            .append_events(AppendEventsArgs {
                run_id: run_id.to_string(),
                expected_next_index: expected_next_index as u64,
                events: vec![event.clone()],
            })
            .map(|_| ())
            .map_err(to_store_error)
    }

    fn get_events(&self, run_id: &str) -> Result<Vec<WorkflowEvent>, StoreError> {
        self.inner
            .read_events(ReadEventsArgs {
                run_id: run_id.to_string(),
                from_index: None,
            })
            .map(|stored| stored.into_iter().map(|e| e.event).collect())
            .map_err(to_store_error)
    }

    fn truncate_runs(&self, _run_id: &str, _step_id: &str) -> Result<(), StoreError> {
        // 基础层没有对应方法（上游的 `createRunStoreAdapter` 也没有实现
        // `truncateRuns` —— 那不在 `WorkflowRunStoreAdapterStore` 上）。
        // core 的 `continue_from` 走这里，故对 runtime store 暂不支持。
        Err(StoreError::Io(
            "truncate_runs 不在 WorkflowRunStoreAdapterStore 契约里；\
             经适配器暴露给 core 的 store 暂不支持 continue_from"
                .into(),
        ))
    }

    fn subscribe(&self, run_id: &str) -> Option<Receiver<WorkflowEvent>> {
        // 基础层的 `subscribe_events` 是回调式（`Box<dyn Fn>`），core 要的是
        // `mpsc::Receiver`，两者形状不同（上游的 TS 版本也是回调式，因为它没有
        // Rust 的通道）。要弥合需另起一个转发线程，暂不做——core 的订阅只用于
        // fan-out 观测，不影响正确性。
        let _ = run_id;
        None
    }
}

/// `anyhow::Error` → [`StoreError`]：若原本就是 `StoreError` 则原样取出，
/// 否则包装为 `Io`。这样 CAS 冲突（`StoreError::Conflict`）经适配器后仍是
/// `Conflict`，core 的 rebase-retry 循环才会正常工作。
fn to_store_error(e: anyhow::Error) -> StoreError {
    match e.downcast::<StoreError>() {
        Ok(store_err) => store_err,
        Err(other) => StoreError::Io(other.to_string()),
    }
}
#[cfg(test)]
mod adapter_tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// 最小 `WorkflowExecutionStore` 实现：只为验证适配器。
    #[derive(Default)]
    struct MemStore {
        runs: Mutex<HashMap<String, RunState>>,
        logs: Mutex<HashMap<String, Vec<WorkflowEvent>>>,
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
            self.logs.lock().unwrap().remove(run_id);
            Ok(())
        }

        fn append_events(&self, args: AppendEventsArgs) -> anyhow::Result<AppendEventsResult> {
            let mut logs = self.logs.lock().unwrap();
            let log = logs.entry(args.run_id.clone()).or_default();
            let actual = log.len() as u64;
            if actual != args.expected_next_index {
                // 关键：以 StoreError::Conflict 形式报错，供适配器的
                // downcast 保留语义。
                return Err(anyhow::Error::new(StoreError::Conflict {
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

    impl WorkflowExecutionStore for MemStore {}

    fn state(run_id: &str) -> RunState {
        RunState {
            run_id: run_id.into(),
            workflow_id: "w".into(),
            workflow_version: None,
            status: workflow_core::RunStatus::Running,
            input: serde_json::Value::Null,
            output: None,
            error: None,
            waiting_for: None,
            pending_approval: None,
            created_at: 0,
            updated_at: 0,
        }
    }

    fn event(run_id: &str) -> WorkflowEvent {
        WorkflowEvent::RunStarted {
            ts: 0,
            run_id: run_id.into(),
        }
    }

    fn adapted() -> (Arc<dyn RunStore>, Arc<MemStore>) {
        let mem = Arc::new(MemStore::default());
        let exec: Arc<dyn WorkflowExecutionStore> = mem.clone();
        (create_run_store_adapter(exec), mem)
    }

    #[test]
    fn get_run_state_projects_through() {
        let (store, mem) = adapted();
        assert!(store.get_run_state("r1").unwrap().is_none());
        mem.save_run_state(SaveRunStateArgs { state: state("r1") })
            .unwrap();
        assert_eq!(
            store.get_run_state("r1").unwrap().map(|s| s.run_id),
            Some("r1".into())
        );
    }

    #[test]
    fn set_run_state_reaches_inner() {
        let (store, mem) = adapted();
        store.set_run_state("r1", &state("r1")).unwrap();
        assert!(mem.load_run_state(&"r1".to_string()).unwrap().is_some());
    }

    #[test]
    fn append_event_wraps_single_into_batch() {
        let (store, mem) = adapted();
        store.append_event("r1", 0, &event("r1")).unwrap();
        let stored = mem
            .read_events(ReadEventsArgs {
                run_id: "r1".into(),
                from_index: None,
            })
            .unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].event_index, 0);
    }

    #[test]
    fn get_events_strips_index_envelope() {
        let (store, _mem) = adapted();
        store.append_event("r1", 0, &event("r1")).unwrap();
        store.append_event("r1", 1, &event("r1")).unwrap();
        let events = store.get_events("r1").unwrap();
        assert_eq!(events.len(), 2);
    }

    /// 关键回归：CAS 冲突经适配器后**仍是 `StoreError::Conflict`**，
    /// core 的 rebase-retry 循环才不会把它当普通 IO 错误。
    #[test]
    fn cas_conflict_survives_adaptation() {
        let (store, _mem) = adapted();
        store.append_event("r1", 0, &event("r1")).unwrap();
        let err = store.append_event("r1", 0, &event("r1")).unwrap_err();
        assert!(
            matches!(err, StoreError::Conflict { .. }),
            "应保留 Conflict，实际为 {err:?}"
        );
    }

    /// `truncate_runs` 不在基础层契约里，适配器如实报错而非静默 no-op。
    #[test]
    fn truncate_runs_reports_unsupported() {
        let (store, _mem) = adapted();
        assert!(store.truncate_runs("r1", "a").is_err());
    }

    #[test]
    fn subscribe_returns_none() {
        let (store, _mem) = adapted();
        assert!(store.subscribe("r1").is_none());
    }
}
#[cfg(test)]
mod e2e_tests {
    //! 端到端验证适配器：用 `WorkflowExecutionStore` 实现驱动 core 的引擎。
    use super::*;
    use crate::types::{AppendEventsArgs, AppendEventsResult, ReadEventsArgs, SaveRunStateArgs};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use workflow_core::{
        BaseCtx, CreateWorkflowConfig, RunOptions, RunStatus, StepCtx, Workflow, create_workflow,
    };

    #[derive(Default)]
    struct MemStore {
        runs: Mutex<HashMap<String, RunState>>,
        logs: Mutex<HashMap<String, Vec<WorkflowEvent>>>,
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
            self.logs.lock().unwrap().remove(run_id);
            Ok(())
        }
        fn append_events(&self, args: AppendEventsArgs) -> anyhow::Result<AppendEventsResult> {
            let mut logs = self.logs.lock().unwrap();
            let log = logs.entry(args.run_id.clone()).or_default();
            let actual = log.len() as u64;
            if actual != args.expected_next_index {
                return Err(anyhow::Error::new(StoreError::Conflict {
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

    impl WorkflowExecutionStore for MemStore {}

    /// 用适配器把一个 `WorkflowExecutionStore` 实现接进 core，跑通一次真实
    /// workflow：**这是「不需要实现者手写两套方法」的实证。**
    #[tokio::test]
    async fn core_runs_against_adapted_store() {
        let wf: Workflow =
            create_workflow(CreateWorkflowConfig::new("e2e").input::<serde_json::Value>())
                .handler(|ctx: BaseCtx<serde_json::Value>| async move {
                    let a = ctx
                        .step("a", |_sc: StepCtx| async move {
                            Ok(serde_json::json!({ "v": 1 }))
                        })
                        .await?;
                    Ok(serde_json::json!({ "got": a }))
                })
                .into_workflow();

        let mem = Arc::new(MemStore::default());
        let exec: Arc<dyn WorkflowExecutionStore> = mem.clone();
        let store = create_run_store_adapter(exec);

        let out = workflow_core::run_workflow(
            &wf,
            store,
            &RunOptions::new(serde_json::json!({})).run_id("e2e:1"),
            None,
        )
        .await
        .unwrap();

        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "got": { "v": 1 } })));
        // 状态与事件都落在内层 store 里。
        let st = mem.load_run_state(&"e2e:1".to_string()).unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Finished);
        let events = mem
            .read_events(ReadEventsArgs {
                run_id: "e2e:1".into(),
                from_index: None,
            })
            .unwrap();
        assert!(!events.is_empty(), "事件应写进内层 store");
    }
}
