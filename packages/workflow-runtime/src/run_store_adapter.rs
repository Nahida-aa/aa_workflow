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
//! | `get_run_state` | [`load_run_state`](RunStoreAdapterStore::load_run_state) | 仅命名 |
//! | `set_run_state` | [`save_run_state`](RunStoreAdapterStore::save_run_state) | 命名 + 参数打包 |
//! | `delete_run(id, reason)` | [`delete_run`](RunStoreAdapterStore::delete_run) | ✅ 相同 |
//! | `append_event`（单条） | [`append_events`](RunStoreAdapterStore::append_events)（批量，返回 next_index） | **语义差别** |
//! | `get_events` → 裸数组 | [`read_events`](RunStoreAdapterStore::read_events) → 带索引信封 | **语义差别**（含 `from_index` 游标） |
//! | `subscribe` | [`subscribe_events`](RunStoreAdapterStore::subscribe_events) | 同签名 |
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
/// 实现者实现本 trait + 扩展层的 `WorkflowExecutionStore` 即可被 runtime 驱动；
/// 同时因为 `WorkflowExecutionStore` 继承它、而 core 的 `run_workflow` 只要求
/// `RunStore`，实现者若也实现 `RunStore` 就能两处通用——**适配在 Rust 里由
/// supertrait 免费完成，不需要上游那个 `createRunStoreAdapter`**。
pub trait RunStoreAdapterStore: Send + Sync {
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
    /// 调用方退化为轮询 [`read_events`](RunStoreAdapterStore::read_events)。
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
