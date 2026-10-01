//! Host 无关的薄壳辅助：把 start / resume 收成一个驱动入口
//! （对齐 TanStack `runtime.ts` 的位置）。signals / pause 由引擎的
//! [`ApprovalCtx`](aa_workflow_core::define::WorkflowCtx::approve) +
//! [`signal_run`](aa_workflow_core::signal_run) 承担；示例 workflow 见
//! [`approval_review`](crate::workflows::approval_review)。

use std::sync::Arc;

use aa_workflow_core::{RunId, RunOutcome, RunStore, RunWorkflowOptions, WorkflowDefinition, WorkflowEvent, run_workflow};

/// 可选的事件订阅者（每个 `ctx.step` 落盘事件都会回调）。首参 `run_id` 由
/// core 直接给出（同 [`aa_workflow_core::RunWorkflowOptions::publish`]）。
pub type EventSubscriberFn = Arc<dyn Fn(&RunId, WorkflowEvent) + Send + Sync>;

/// 把事件打到 tracing 的默认订阅者（`examples.runtime` target）。
pub fn tracing_publish() -> EventSubscriberFn {
    Arc::new(|run_id, ev| tracing::info!(target: "examples.runtime", "[{run_id}] {ev:?}"))
}

/// 一次驱动调用（跑新 run 或续跑某个 run_id 都走这里）。
#[derive(Default)]
pub struct DriveOpts<'a> {
    /// 复用该 run_id ⇒ resume（成功 step 短路、失败 rethrow）。
    pub run_id: Option<&'a str>,
    /// 命中即停（对齐 run_pipeline 的 targetStep）。
    pub target_step: Option<&'a str>,
    /// 事件订阅者；`None` 则不订阅。
    pub publish: Option<EventSubscriberFn>,
}

impl DriveOpts<'_> {
    /// 组装引擎入参。`workflow` / `store` 由调用方传入——它们是
    /// [`RunWorkflowOptions`] 的**必填项**（对齐上游 `RunWorkflowOptions`）。
    fn into_run_options<TInput, TOutput, TState, TCtxExt>(
        &self,
        workflow: Arc<WorkflowDefinition<TInput, TOutput, TState, TCtxExt>>,
        store: Arc<dyn RunStore>,
        input: serde_json::Value,
    ) -> RunWorkflowOptions {
        let mut ro = RunWorkflowOptions::new(workflow, store).input(input);
        if let Some(id) = self.run_id {
            ro = ro.run_id(id);
        }
        if let Some(ts) = self.target_step {
            ro = ro.target_step(ts);
        }
        ro.publish(self.publish.clone())
    }
}

/// 跑/续跑一条 workflow，返回引擎的 [`RunOutcome`]。
pub async fn drive<TInput, TOutput, TState, TCtxExt>(
    workflow: &WorkflowDefinition<TInput, TOutput, TState, TCtxExt>,
    store: Arc<dyn RunStore>,
    input: serde_json::Value,
    opts: DriveOpts<'_>,
) -> anyhow::Result<RunOutcome> {
    let options = opts.into_run_options(Arc::new(workflow.clone()), store, input);
    run_workflow(options)
        .outcome().await
        .map_err(|e| anyhow::anyhow!("{e}"))
}
