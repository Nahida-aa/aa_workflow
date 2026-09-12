//! Host 无关的薄壳辅助：把 start / resume / `continue_from` 收成一个驱动入口
//! （对齐 TanStack `runtime.ts` 的位置，但只做现有引擎的能力）。
//!
//! signals / `__timer` 唤醒等要等引擎补了 pause 语义再进来。

use std::sync::Arc;

use workflow_core::{run_workflow, RunEvent, RunOptions, RunOutcome, RunStore, Workflow};

/// 可选的事件订阅者（每个 `ctx.step` 落盘事件都会回调）。
pub type EventSubscriber = Arc<dyn Fn(&RunEvent) + Send + Sync>;

/// 把事件打到 tracing 的默认订阅者（`examples.runtime` target）。
pub fn tracing_publisher() -> EventSubscriber {
    Arc::new(|ev| tracing::info!(target: "examples.runtime", "{ev:?}"))
}

/// 一次驱动调用（跑新 run 或续跑某个 run_id 都走这里）。
#[derive(Default)]
pub struct DriveOpts<'a> {
    /// 复用该 run_id ⇒ resume（成功 step 短路、失败 rethrow）。
    pub run_id: Option<&'a str>,
    /// 从该 step 最新终态 checkpoint 截断后重跑后缀（失败重试/继续）。
    pub continue_from: Option<&'a str>,
    /// 命中即停（对齐 run_pipeline 的 targetStep）。
    pub target_step: Option<&'a str>,
    /// 事件订阅者；`None` 则不订阅。
    pub publisher: Option<EventSubscriber>,
}

impl DriveOpts<'_> {
    fn into_run_options(&self, input: serde_json::Value) -> RunOptions {
        let mut ro = RunOptions::new(input);
        if let Some(id) = self.run_id {
            ro = ro.run_id(id);
        }
        if let Some(cf) = self.continue_from {
            ro = ro.continue_from(cf);
        }
        if let Some(ts) = self.target_step {
            ro = ro.target_step(ts);
        }
        ro
    }
}

/// 跑/续跑一条 workflow，返回引擎的 [`RunOutcome`]。
pub async fn drive(
    workflow: &Workflow,
    store: Arc<dyn RunStore>,
    input: serde_json::Value,
    opts: DriveOpts<'_>,
) -> anyhow::Result<RunOutcome> {
    run_workflow(workflow, store, &opts.into_run_options(input), opts.publisher)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_run_store::FileRunStore;
    use crate::workflows::email_digest;
    use workflow_core::RunStatus;

    #[tokio::test]
    async fn file_store_survives_restart_and_resume() {
        let base = std::env::temp_dir().join(format!(
            "wf_examples_runtime_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        let wf = email_digest();
        let input = serde_json::json!({ "days": 7 });

        // 第一顿（= 进程 A）：落盘 FileRunStore
        let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(&base));
        let out1 = drive(
            &wf,
            store.clone(),
            input.clone(),
            DriveOpts {
                run_id: Some("digest:r"),
                publisher: Some(tracing_publisher()),
                ..DriveOpts::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(out1.status, RunStatus::Finished);
        let events_1 = store.get_events("digest:r").unwrap();
        let scan_ts_1 = events_1
            .iter()
            .find_map(|e| match e {
                RunEvent::StepFinished { step_id, ts, .. } if step_id == "scan-events" => {
                    Some(*ts)
                }
                _ => None,
            })
            .unwrap();

        // "进程重启"：全新 store 实例打开同一 base 再续跑 → 全部短路
        let store2: Arc<dyn RunStore> = Arc::new(FileRunStore::new(&base));
        let out2 = drive(
            &wf,
            store2.clone(),
            input,
            DriveOpts {
                run_id: Some("digest:r"),
                ..DriveOpts::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(out2.status, RunStatus::Finished);
        let events_2 = store2.get_events("digest:r").unwrap();
        assert_eq!(
            events_2
                .iter()
                .find_map(|e| match e {
                    RunEvent::StepFinished { step_id, ts, .. }
                        if step_id == "scan-events" =>
                    {
                        Some(*ts)
                    }
                    _ => None,
                })
                .unwrap(),
            scan_ts_1,
            "resume 后 scan-events 的 checkpoint 未被触碰"
        );

        let _ = std::fs::remove_dir_all(&base);
    }
}