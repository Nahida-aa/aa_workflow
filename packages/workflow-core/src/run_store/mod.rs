//! Run 的持久化契约与内置实现。
//!
//! - 契约本身（[`RunStore`] + [`RunState`] 信封）在本文件；
//! - 各实现按后端分文件（[`InMemoryStore`] 在 `in_memory`）。

use std::sync::mpsc::Receiver;

use serde::{Deserialize, Serialize};

use crate::error::{RunError, StoreError};
use crate::event::{RunStatus, WorkflowEvent};

mod in_memory;
pub use in_memory::InMemoryStore;

/// 挂起等待中的外部信号（对齐 TanStack `RunState.waitingFor`）。sleep 的
/// `due_at` 就是这里的 `deadline` —— host 可用它做时间索引的唤醒调度。
///
/// `step_id` 在 TS 侧是可选的（`types.ts:555`）：signal 也可以只按
/// `signal_name` 投递，不必绑定某个 step。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitForState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    pub signal_name: String,
    pub deadline: Option<i64>,
}

/// 挂起中的审批（对齐 TanStack `RunState.pendingApproval`）。我们的
/// `approve` 用 key 作 `approval_id`；`title` 即挂起时给的理由。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingApproval {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    pub approval_id: String,
    pub title: String,
    pub description: Option<String>,
}

/// Minimal, durable metadata for a run. The heavy state lives in the event
/// log; this is just the envelope the launcher needs to locate runs.
/// `waiting_for` / `pending_approval` 是挂起态的一等投影（派生自事件日志，
/// 恢复时清除）——观察者无需扫日志就能告诉 run 在等什么。
///
/// ## 谁写、谁存
///
/// **内容由引擎写，介质由 store 定**。引擎在每次 drive 的收尾、以及挂起时
/// （[`crate::engine`] 的 pause 投影）调用 [`RunStore::set_run_state`] 更新信封；
/// store 只负责把它放哪儿——内存 map、JSON 文件、数据库行都行。TanStack 同样
/// 如此：`run-workflow.ts` 有 7 处调 `setRunState`，挂起投影那处的注释就是
/// “Persist waitingFor on the run state so out-of-process workers can
/// discover the pending wake”。
///
/// 注意别把 `RunState` 和某个具体 store 的文件布局混为一谈：本 crate 不假定
/// 介质，也没有任何代码知道 `run.json` 这种文件名（那是示例层 `FileRunStore`
/// 的事）。
///
/// ⚠️ **`RunState` 不是 `ctx.state`，两者毫无关系**（仅名字相似，极易混淆）。
///
/// | | `RunState`（本类型） | `ctx.state: TState` |
/// | --- | --- | --- |
/// | 是什么 | run 的**持久化元数据信封** | workflow 的**业务状态** |
/// | 谁消费 | **store**（路由 / 恢复 / 审计） | **handler**（`ctx.state.count += 1`） |
/// | 怎么声明 | 固定结构 | `.state::<T>()` + `.initialize(...)` |
/// | 存哪 | **持久化**（本类型就是被存的那个） | **不持久化**，每次 resume 重建 |
///
/// **本类型里刻意不放 `ctx.state`。** 上游 `types.ts:534-536` 原话：
/// “Persisted run metadata. **State is intentionally NOT stored here** — it is
/// reconstructed from `initialize(input)` + log replay on every resume.”
///
/// 由此推得三条，别搞反：
///
/// - store 的表里**不该有业务 state 的列**（`workflow_runs` /
///   `workflow_run_states` 存的都是 `RunState` 这一族）；
/// - `ctx.state` 的变更走 `STATE_DELTA`，那是 **emit-only、不进日志、不参与
///   replay** 的可观测事件——**不是** `ctx.state` 的持久化形态。`ctx.state`
///   在整条链上没有任何持久化形态；
/// - `ctx.state` 靠 replay 重建，所以它依赖 handler 的确定性：step 闭包内的
///   修改会随闭包短路而丢失，并行分支经 `ctx.clone()` 各持快照互不可见。
///   跨分支传数据要走 **step 的返回值**（durable 结果），不要走 state。
///
/// `In` / `Out` 默认擦除为 [`serde_json::Value`]，因为 [`RunStore`] 的契约面
/// 必须能装下任意 workflow 的 input/output（store 是 `dyn`，无法带泛型）。
/// 想要具体类型的调用方用 [`RunState::into_typed`] 窄化。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState<In = serde_json::Value, Out = serde_json::Value> {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_version: Option<String>,
    pub status: RunStatus,
    pub input: In,
    pub output: Option<Out>,
    pub error: Option<RunError>,
    /// 挂起等待外部 signal / sleep 到期（sleep 有 deadline）。
    #[serde(default)]
    pub waiting_for: Option<WaitForState>,
    /// 挂起等待审批。
    #[serde(default)]
    pub pending_approval: Option<PendingApproval>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// 从 store 的擦除形态（`RunState<Value, Value>`）窄化成具体类型。
///
/// 对应 TanStack 的 `RunState<TInput, TOutput>`（`types.ts:540`）。差别值得写清：
/// 他们的 `RunStore` 边界同样是 `RunState<unknown, unknown>`，消费者用
/// `WorkflowInput<typeof wf>` / `WorkflowOutput<typeof wf>` 重新参数化后
/// **cast** 过去——纯类型断言，无运行时成本也无校验。Rust 没有 `unknown`，
/// 必须真的反序列化，所以这里会失败并返回 `Err`。
impl RunState<serde_json::Value, serde_json::Value> {
    pub fn into_typed<In, Out>(self) -> anyhow::Result<RunState<In, Out>>
    where
        In: serde::de::DeserializeOwned,
        Out: serde::de::DeserializeOwned,
    {
        Ok(RunState {
            run_id: self.run_id,
            workflow_id: self.workflow_id,
            workflow_version: self.workflow_version,
            status: self.status,
            input: serde_json::from_value(self.input)?,
            output: self.output.map(serde_json::from_value).transpose()?,
            error: self.error,
            waiting_for: self.waiting_for,
            pending_approval: self.pending_approval,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

/// 删除 run 的原因（对齐 TanStack `DeleteReason`，`types.ts:576`）。Store
/// 可据此决定保留策略：比如终态归档 vs 中途废弃走不同的清理路径。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeleteReason {
    Finished,
    Errored,
    Aborted,
}

impl DeleteReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            DeleteReason::Finished => "finished",
            DeleteReason::Errored => "errored",
            DeleteReason::Aborted => "aborted",
        }
    }
}

/// The store contract, modelled after TanStack Workflow's two-surface design:
///
/// 1. **Metadata surface** — `get/set/delete` a small [`RunState`] envelope.
/// 2. **Append-only event-log surface** — `append_event` with a CAS
///    `expected_next_index`, `get_events` for resume, and `subscribe` for live
///    fan-out.
///
/// The engine never mutates events; state is always derived by replaying the
/// log (see [`crate::event::fold_step_states`]).
///
/// ## 与 TanStack 的对应
///
/// 上游有**三个**相关名字，其中两个只差一个 `Store` 后缀，容易看错：
///
/// | 上游 | 位置 | 是什么 |
/// | ---- | ---- | ------ |
/// | `RunStore` | `workflow-core/src/types.ts:599` | core 的接口，引擎用；**就是本 trait** |
/// | `WorkflowRunStoreAdapter = RunStore` | 同上 `:305` 在 runtime 侧 | 一行 `export type` 真别名 |
/// | `WorkflowRunStoreAdapterStore` | `workflow-runtime/src/types.ts:290` | **独立 interface，不是别名** |
/// | `WorkflowExecutionStore extends …AdapterStore` | `:307` | runtime 的扩展契约 |
///
/// 后两个本 crate 与 `workflow-runtime` 都**不设对应物**：
///
/// - `WorkflowRunStoreAdapter` 是 TS 的 `type` 别名，零成本。Rust 的 trait
///   别名要 `#![feature(trait_alias)]`（仍 unstable），`trait A: B {}` 则是新
///   trait——每个实现者都得多写空 `impl`。它的唯一用途是标注「适配器的输出
///   类型」，而适配器本身我们也不需要（见下条）。
/// - `WorkflowRunStoreAdapterStore` **不是 `RunStore` 的重复**：它在事件读写上
///   有真实差别（批量 `appendEvents`、返回带 `eventIndex` / `createdAt` 的
///   `StoredWorkflowEvent`）。它存在是因为**上游正在迁移中**——core 还在用旧的
///   `RunStore`，runtime 已用新的，`createRunStoreAdapter` 负责把新的降格成
///   旧的。
///
/// 于是 runtime 的扩展 trait 继承的是 `WorkflowRunStoreAdapterStore`，
/// **不经过本 trait**（`WorkflowExecutionStore: WorkflowRunStoreAdapterStore`）。
/// 两者是**平行的两套方法**：`load_run_state` vs `get_run_state`、
/// `append_events` vs `append_event`，没有自动转换关系。
///
/// ## 本 trait 里哪些方法是**本地扩展**（上游没有）
///
/// 对照上游时按这张表看，**不必**为下列方法去找对端——找不到是正常的：
///
/// | 方法 | 上游有吗 | 说明 |
/// | ---- | -------- | ---- |
/// | `get_run_state` / `set_run_state` / `delete_run` | ✅ | 一一对应 |
/// | `append_event` / `get_events` / `subscribe` | ✅ | 一一对应 |
/// | [`truncate_log_at_step`](Self::truncate_log_at_step) | ❌ **本地扩展** | 支撑 `continue_from`；上游连 `continueFrom` 都没有 |
///
/// 详见 `docs/tanstack-alignment.md` 的「保留了分歧（本地扩展 / 一等公民）」。
/// 这也是为什么 runtime 的新契约 `WorkflowExecutionStore` 没有截断能力——它对齐的
/// 是上游，而截断是本仓独有的。
///
/// ⚠️ **要喂给 core 的 `run_workflow`，实现者必须额外 `impl RunStore`**：
/// 该函数的入参是 `Arc<dyn RunStore>`（具体 trait 对象），而 supertrait 只保证
/// `WorkflowRunStoreAdapterStore` 的父关系，跟本 trait 无关。
/// 完整论证见 `docs/runtime-design.md` 的 D1。
pub trait RunStore: Send + Sync {
    fn get_run_state(&self, run_id: &str) -> Result<Option<RunState>, StoreError>;
    fn set_run_state(&self, run_id: &str, state: &RunState) -> Result<(), StoreError>;
    fn delete_run(&self, run_id: &str, reason: DeleteReason) -> Result<(), StoreError>;

    /// Appends an event at `expected_next_index`. Implementations MUST reject
    /// the write with [`StoreError::Conflict`] if the current log length
    /// differs — this is the optimistic-concurrency guard that lets a
    /// coordinator serialize concurrent writers.
    fn append_event(
        &self,
        run_id: &str,
        expected_next_index: usize,
        event: &WorkflowEvent,
    ) -> Result<(), StoreError>;
    fn get_events(&self, run_id: &str) -> Result<Vec<WorkflowEvent>, StoreError>;

    /// 在 `step_id` 的**最新终态 checkpoint** 处截断事件日志（**含**该 checkpoint）：
    /// 它及其之后的全部事件被丢弃，前缀保留。供 `continue_from` 使用——重放 handler
    /// 时前缀短路、后缀从零重跑。
    ///
    /// # ⚠️ 这是**本地扩展**，上游 TanStack 没有
    ///
    /// 上游的 `RunStore`（`types.ts:600-627`）只有 6 个方法
    /// （`getRunState` / `setRunState` / `deleteRun` / `appendEvent` / `getEvents`
    /// / `subscribe?`），**既没有本方法，也没有 `continueFrom`**。两者都是本仓的
    /// 一等公民扩展，动机与论证见 `docs/tanstack-alignment.md` 的
    /// 「保留了分歧（本地扩展 / 一等公民）」。
    ///
    /// 所以对照上游时**不必**去找它的对端——找不到是正常的。这也解释了为什么新契约
    /// `WorkflowExecutionStore` 没有对等能力：它对齐的是上游，而截断是本仓独有的。
    ///
    /// # no-op 语义（有意为之，不是实现偷懒）
    ///
    /// **目标 step 没有终态 checkpoint 时，保持日志原样、不报错**。因为没 checkpoint
    /// 的 step 重放时本来就会真跑（日志里没它的终态 → 短路不命中），无须截断。
    ///
    /// 与之相对，**"做不到截断"必须报错**：不支持该能力的 store 要返回
    /// [`StoreError::Io`]，**不许静默 no-op** —— 否则 `continue_from` 会看起来成功、
    /// 实际重放时全部短路，用户以为重跑了却没有。
    fn truncate_log_at_step(&self, run_id: &str, step_id: &str) -> Result<(), StoreError>;

    /// Live subscription: a receiver that sees every future event appended to
    /// this run's log. `None` if the store does not support subscriptions.
    fn subscribe(&self, run_id: &str) -> Option<Receiver<WorkflowEvent>> {
        let _ = run_id;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq)]
    #[serde(rename_all = "camelCase")]
    struct OrderIn {
        order_id: String,
    }

    #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq)]
    struct OrderOut {
        ok: bool,
    }

    fn erased(output: Option<serde_json::Value>) -> RunState {
        RunState {
            run_id: "r1".into(),
            workflow_id: "order".into(),
            workflow_version: Some("v2".into()),
            status: RunStatus::Finished,
            input: serde_json::json!({ "orderId": "A-1" }),
            output,
            error: None,
            waiting_for: None,
            pending_approval: None,
            created_at: 1,
            updated_at: 2,
        }
    }

    #[test]
    fn into_typed_narrows_input_and_output() {
        let typed: RunState<OrderIn, OrderOut> = erased(Some(serde_json::json!({ "ok": true })))
            .into_typed()
            .unwrap();
        assert_eq!(
            typed.input,
            OrderIn {
                order_id: "A-1".into()
            }
        );
        assert_eq!(typed.output, Some(OrderOut { ok: true }));
        // 非 input/output 字段原样搬运。
        assert_eq!(typed.run_id, "r1");
        assert_eq!(typed.workflow_version.as_deref(), Some("v2"));
        assert_eq!(typed.status, RunStatus::Finished);
        assert_eq!(typed.created_at, 1);
    }

    #[test]
    fn into_typed_keeps_absent_output_absent() {
        let typed: RunState<OrderIn, OrderOut> = erased(None).into_typed().unwrap();
        assert_eq!(typed.output, None, "output 为 None 时不该去解析出默认值");
    }

    #[test]
    fn into_typed_reports_shape_mismatch() {
        let err = erased(Some(serde_json::json!({ "ok": "yes" })))
            .into_typed::<OrderIn, OrderOut>()
            .unwrap_err();
        assert!(
            err.to_string().contains("invalid type"),
            "形状不匹配应报 serde 类型错误，实际为 {err}"
        );
    }

    /// 擦除形态仍能被 store 序列化/反序列化（RunState 的持久化兼容性）。
    #[test]
    fn erased_run_state_roundtrips_through_json() {
        let st = erased(Some(serde_json::json!({ "ok": true })));
        let json = serde_json::to_string(&st).unwrap();
        let back: RunState = serde_json::from_str(&json).unwrap();
        assert_eq!(back.input, st.input);
        assert_eq!(back.output, st.output);
    }

    #[test]
    fn run_state_error_serializes_as_object() {
        let mut st = erased(None);
        st.error = Some(RunError::cancelled());
        let v = serde_json::to_value(&st).unwrap();
        assert_eq!(v["error"]["name"], "Aborted");
        assert_eq!(v["error"]["message"], "workflow aborted");
        // 没有 stack 字段（见 RunError 文档）。
        assert!(v["error"].get("stack").is_none());
    }

    #[test]
    fn delete_reason_serializes_lowercase() {
        for (reason, s) in [
            (DeleteReason::Finished, "finished"),
            (DeleteReason::Errored, "errored"),
            (DeleteReason::Aborted, "aborted"),
        ] {
            assert_eq!(reason.as_str(), s);
            assert_eq!(serde_json::to_value(reason).unwrap(), serde_json::json!(s));
        }
    }

    /// `step_id` 可选：`None` 时不该出现在序列化的 RunState 里（对齐 TS 的可选字段）。
    #[test]
    fn wait_for_state_omits_absent_step_id() {
        let w = WaitForState {
            step_id: None,
            signal_name: "payment".into(),
            deadline: None,
        };
        let v = serde_json::to_value(&w).unwrap();
        assert!(v.get("step_id").is_none());
        assert_eq!(v["signal_name"], "payment");
        // 反序列化回来仍是 None。
        let back: WaitForState = serde_json::from_value(v).unwrap();
        assert_eq!(back.step_id, None);
    }

    #[test]
    fn delete_run_accepts_reason() {
        let store = InMemoryStore::new();
        store
            .set_run_state("r", &erased(None))
            .expect("写入 RunState");
        store.delete_run("r", DeleteReason::Finished).unwrap();
        assert!(store.get_run_state("r").unwrap().is_none());
    }
}
