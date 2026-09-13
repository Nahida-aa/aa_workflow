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

    /// Cuts the log at `step_id`'s **latest terminal checkpoint** (inclusive):
    /// that checkpoint and every later event are dropped, the prefix is kept.
    /// Used by `continue_from` so a replayed handler re-executes the step's
    /// suffix from scratch. A step with no terminal checkpoint leaves the log
    /// untouched (there is nothing to cut — resume would re-run it anyway).
    /// Stores that cannot truncate must reject this with
    /// [`StoreError::Io`] rather than silently no-oping.
    fn truncate_runs(&self, run_id: &str, step_id: &str) -> Result<(), StoreError>;

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
