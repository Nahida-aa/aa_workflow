//! Host 无关的薄壳辅助：把 start / resume / `continue_from` 收成一个驱动入口
//! （对齐 TanStack `runtime.ts` 的位置）。signals / pause 由引擎的
//! [`ApprovalCtx`](workflow_core::define::WorkflowCtx::approve) +
//! [`signal_run`](workflow_core::signal_run) 承担；示例 workflow 见
//! [`approval_review`](crate::workflows::approval_review)。

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
    use std::time::Duration;
    use workflow_core::RunStatus;

    /// 轮询直到 run 在 `step_id` 挂起。
    async fn wait_paused<S: RunStore + ?Sized>(store: &Arc<S>, run_id: &str, step_id: &str) {
        for _ in 0..2000 {
            let evs = store.get_events(run_id).unwrap();
            if evs.iter().any(|e| {
                matches!(e, RunEvent::StepPaused { step_id: id, .. } if id == step_id)
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("run {run_id} never paused at {step_id}");
    }

    fn finished_count(events: &[RunEvent], step: &str) -> usize {
        events
            .iter()
            .filter(|e| matches!(e, RunEvent::StepFinished { step_id, .. } if step_id == step))
            .count()
    }

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

    #[tokio::test]
    async fn file_store_pause_survives_restart() {
        // 落盘目录放仓库内 tmp/（约定：测试不用系统 /tmp）
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tmp")
            .join(format!("approve_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(&base));
        let wf = crate::workflows::approval_review();

        // 进程 A：跑到 approve 等待点后"崩溃"（abort，不写终态）
        let store2 = store.clone();
        let wf2 = wf.clone();
        let t1 = tokio::spawn(async move {
            drive(
                &wf2,
                store2,
                serde_json::json!({}),
                DriveOpts {
                    run_id: Some("approve:r"),
                    ..DriveOpts::default()
                },
            )
            .await
        });
        for _ in 0..2000 {
            let evs = store.get_events("approve:r").unwrap();
            if evs
                .iter()
                .any(|e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "review"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        t1.abort();
        let _ = t1.await;

        // 进程 B：全新 FileRunStore 打开同一 base 续跑 → 不重复挂起等待点
        let store2: Arc<dyn RunStore> = Arc::new(FileRunStore::new(&base));
        let store2_spawn = store2.clone();
        let wf2 = wf.clone();
        let t2 = tokio::spawn(async move {
            drive(
                &wf2,
                store2_spawn.clone(),
                serde_json::json!({}),
                DriveOpts {
                    run_id: Some("approve:r"),
                    ..DriveOpts::default()
                },
            )
            .await
            .map(|out| (out, store2_spawn))
        });
        for _ in 0..2000 {
            let evs = store2.get_events("approve:r").unwrap();
            if evs
                .iter()
                .any(|e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "review"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            store2
                .get_events("approve:r")
                .unwrap()
                .iter()
                .filter(|e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "review"))
                .count(),
            1,
            "进程 B 不能重复 append StepPaused"
        );

        workflow_core::signal_run(store2.as_ref(), "approve:r", "review", serde_json::json!({ "ok": true }))
            .unwrap();
        let (out, store2b) = t2.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "ok": true })));
        let events = store2b.get_events("approve:r").unwrap();
        let pauses = events
            .iter()
            .filter(|e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "review"))
            .count();
        let resumes = events
            .iter()
            .filter(|e| matches!(e, RunEvent::StepResume { step_id, .. } if step_id == "review"))
            .count();
        assert_eq!(pauses, 1, "整个生命周期只应有一个 StepPaused");
        assert_eq!(resumes, 1);

        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn invoice_double_sleep_survives_restart() {
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tmp")
            .join(format!("invoice_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);

        // 进程 A：跑到第一个 sleep 挂起后"崩溃"（abort，不写终态）。
        let store: Arc<dyn RunStore> = Arc::new(FileRunStore::new(&base));
        let wf = crate::workflows::invoice();
        let store2 = store.clone();
        let wf2 = wf.clone();
        let t1 = tokio::spawn(async move {
            drive(
                &wf2,
                store2,
                serde_json::json!({ "orderId": "i:r", "t1": 400, "t2": 700 }),
                DriveOpts {
                    run_id: Some("invoice:r"),
                    ..DriveOpts::default()
                },
            )
            .await
        });
        wait_paused(&store, "invoice:r", "settle-due-1").await;
        t1.abort();
        let _ = t1.await;

        // 进程 B：全新 FileRunStore 打开同一 base 续跑。sleep 的 deadline 从日志
        // 恢复——不再重复 append `StepPaused`，两次定时器各自到期后收尾。
        let store2: Arc<dyn RunStore> = Arc::new(FileRunStore::new(&base));
        let out = drive(
            &wf,
            store2.clone(),
            serde_json::json!({ "orderId": "i:r", "t1": 400, "t2": 700 }),
            DriveOpts {
                run_id: Some("invoice:r"),
                ..DriveOpts::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output.unwrap()["settled"], true);

        let events = store2.get_events("invoice:r").unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| {
                    matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "settle-due-1")
                })
                .count(),
            1,
            "进程 B 不重复 append 第一个 sleep 的 StepPaused"
        );
        assert_eq!(finished_count(&events, "settle-invoice"), 1);

        let _ = std::fs::remove_dir_all(&base);
    }
}