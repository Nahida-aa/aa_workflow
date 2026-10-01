//! 重启存活：落盘 store 换实例后 resume / 挂起等待点 / 定时器推进。
//!
//! 这三条原先住在 `examples/shared/src/runtime.rs`，用的是 `FileRunStore`——
//! 它实现的是 core 的**旧契约** `RunStore`。既然新 store 一律实现
//! `WorkflowExecutionStore`（上游 `docs/api/store-adapters.md` 的规定），这里
//! 改用 `FileExecutionStore` + `create_run_store_adapter`，与 `reopen_and_e2e.rs`
//! 走同一条路。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use aa_workflow_core::{RunStore, RunStatus, WorkflowEvent, signal_run};
use aa_workflow_runtime::run_store_adapter::{
    WorkflowExecutionStore, create_run_store_adapter,
};
use example_shared::runtime::{DriveOpts, drive, tracing_publish};
use example_shared::workflows;
use example_store_file::file_store::FileExecutionStore;

/// 落盘 store → core 引擎能吃的 `Arc<dyn RunStore>`（经降格适配器）。
fn core_store(base: &Path) -> Arc<dyn RunStore> {
    create_run_store_adapter(Arc::new(FileExecutionStore::new(base)) as Arc<dyn WorkflowExecutionStore>)
}

/// 测试落盘目录写仓库内 `tmp/`，不用系统 `/tmp`。
fn base_dir(tag: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join(format!("{tag}_{}", std::process::id()))
}

    fn finished_count(events: &[WorkflowEvent], step: &str) -> usize {
        events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::StepFinished { step_id, .. } if step_id == step))
            .count()
    }

    #[tokio::test]
    async fn file_store_survives_restart_and_resume() {
        let base = base_dir("restart");
        let _ = std::fs::remove_dir_all(&base);
        let wf = workflows::email_digest();
        let input = serde_json::json!({ "days": 7 });

        // 第一顿（= 进程 A）：落盘 FileExecutionStore
        let store = core_store(&base);
        let out1 = drive(
            &wf,
            store.clone(),
            input.clone(),
            DriveOpts {
                run_id: Some("digest:r"),
                publish: Some(tracing_publish()),
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
                WorkflowEvent::StepFinished { step_id, ts, .. } if step_id == "scan-events" => {
                    Some(*ts)
                }
                _ => None,
            })
            .unwrap();

        // "进程重启"：全新 store 实例打开同一 base 再续跑 → 全部短路
        let store2 = core_store(&base);
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
                    WorkflowEvent::StepFinished { step_id, ts, .. } if step_id == "scan-events" => {
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
        let base = base_dir("approve");
        let _ = std::fs::remove_dir_all(&base);
        let store = core_store(&base);
        let wf = workflows::approval_review();

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
            if evs.iter().any(
                |e| matches!(e, WorkflowEvent::StepPaused { step_id, .. } if step_id == "review"),
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        t1.abort();
        let _ = t1.await;

        // 进程 B：全新 FileExecutionStore 打开同一 base 续跑 → 不重复挂起等待点
        let store2 = core_store(&base);
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
            if evs.iter().any(
                |e| matches!(e, WorkflowEvent::StepPaused { step_id, .. } if step_id == "review"),
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            store2
                .get_events("approve:r")
                .unwrap()
                .iter()
                .filter(
                    |e| matches!(e, WorkflowEvent::StepPaused { step_id, .. } if step_id == "review")
                )
                .count(),
            1,
            "进程 B 不能重复 append StepPaused"
        );

        aa_workflow_core::signal_run(
            store2.as_ref(),
            "approve:r",
            "review",
            serde_json::json!({ "ok": true }),
        )
        .unwrap();
        let (out, store2b) = t2.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "ok": true })));
        let events = store2b.get_events("approve:r").unwrap();
        let pauses = events
            .iter()
            .filter(
                |e| matches!(e, WorkflowEvent::StepPaused { step_id, .. } if step_id == "review"),
            )
            .count();
        let resumes = events
            .iter()
            .filter(
                |e| matches!(e, WorkflowEvent::StepResume { step_id, .. } if step_id == "review"),
            )
            .count();
        assert_eq!(pauses, 1, "整个生命周期只应有一个 StepPaused");
        assert_eq!(resumes, 1);

        let _ = std::fs::remove_dir_all(&base);
    }

    /// 进程 A 挂起在 sleep → 进程退出（挂起时本来就没人在跑）→ 进程 B 用全新
    /// `FileExecutionStore` 打开同一 base，投递 timer 后跑完。
    ///
    /// D3 之后这就是**常态**：挂起即返回，本来就没有「同一个进程等着」这回事。
    #[tokio::test]
    async fn invoice_double_sleep_survives_restart() {
        let base = base_dir("invoice");
        let _ = std::fs::remove_dir_all(&base);

        let input = serde_json::json!({ "orderId": "i:r", "t1": 10, "t2": 20 });

        // 进程 A：跑到第一个 sleep 挂起（挂起 = 结束，进程可以退）。
        let store = core_store(&base);
        let wf = workflows::invoice();
        let first = drive(
            &wf,
            store.clone(),
            input.clone(),
            DriveOpts {
                run_id: Some("invoice:r"),
                ..DriveOpts::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(first.status, RunStatus::Paused);
        assert_eq!(
            store
                .get_run_state("invoice:r")
                .unwrap()
                .unwrap()
                .waiting_for
                .unwrap()
                .step_id
                .as_deref(),
            Some("settle-due-1"),
            "应停在第一个 sleep"
        );
        drop(store); // 进程 A 退出

        // 进程 B：全新 FileExecutionStore 打开同一 base 续跑。逐个投递 timer 推进。
        let store2 = core_store(&base);
        let mut out = None;
        for _ in 0..4 {
            let st = store2.get_run_state("invoice:r").unwrap().unwrap();
            if st.status != RunStatus::Paused {
                break;
            }
            let w = st.waiting_for.expect("sleep 挂起应投影 waiting_for");
            assert_eq!(w.signal_name, "__timer");
            let step_id = w.step_id.expect("timer 挂起应有 step_id");
            signal_run(
                store2.as_ref(),
                "invoice:r",
                &step_id,
                serde_json::Value::Null,
            )
            .unwrap();
            out = Some(
                drive(
                    &wf,
                    store2.clone(),
                    input.clone(),
                    DriveOpts {
                        run_id: Some("invoice:r"),
                        ..DriveOpts::default()
                    },
                )
                .await
                .unwrap(),
            );
        }
        let out = out.expect("应至少驱动一次");
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output.unwrap()["settled"], true);

        let events = store2.get_events("invoice:r").unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| {
                    matches!(e, WorkflowEvent::StepPaused { step_id, .. } if step_id == "settle-due-1")
                })
                .count(),
            1,
            "进程 B 不重复 append 第一个 sleep 的 StepPaused"
        );
        assert_eq!(finished_count(&events, "settle-invoice"), 1);

        let _ = std::fs::remove_dir_all(&base);
    }
