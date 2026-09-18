//! 契约套件之外的补充测试（契约本身在 `tests/contract.rs`）。
//!
//! 两条契约套件覆盖不到的：
//!
//! 1. **跨实例重开**——进程 A 写、进程 B（新实例）打开同一 base 能读到并续跑。
//!    这是「落盘」相对内存实现的**唯一实质差别**，必须自己验。
//! 2. **接 runtime 端到端**——`define_workflow_runtime` 驱动一个含**并行 step**
//!    的 workflow，验证 store 真能当引擎后端用（并行 step 会并发 append，走 CAS）。

use std::sync::Arc;

use workflow_core::{
    BaseCtx, CreateWorkflowConfig, RunStatus, RunWorkflowOptions, StepCtx, create_workflow, run_workflow,
};
use workflow_runtime::run_store_adapter::{
    WorkflowExecutionStore, WorkflowRunStoreAdapterStore, create_run_store_adapter,
};
use workflow_runtime::types::*;
use workflow_store_file::FileExecutionStore;

/// 造一个带 `step_id` 的最小事件（契约套件里也有同名的本地 helper）。
fn custom(step: &str, ts: i64) -> workflow_core::WorkflowEvent {
    workflow_core::WorkflowEvent::Custom {
        ts,
        run_id: "run-1".into(),
        name: step.into(),
        value: serde_json::json!({ "step": step }),
    }
}

fn temp_base(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "wf_store_file_{tag}_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("建临时目录");
    dir
}

/// 跨实例：A 写 → B（全新实例，同一 base）能读到同样的状态与事件。
#[test]
fn reopens_and_reads_across_instances() {
    let base = temp_base("reopen");

    let a = FileExecutionStore::new(&base);
    a.create_run(CreateRunArgs {
        run_id: "run-1".into(),
        workflow_id: "w".into(),
        workflow_version: None,
        input: serde_json::json!({ "n": 1 }),
        now: 10,
    })
    .unwrap();
    a.append_events(AppendEventsArgs {
        run_id: "run-1".into(),
        expected_next_index: 0,
        events: vec![custom("a", 1), custom("b", 2)],
    })
    .unwrap();
    a.claim_run(ClaimRunArgs {
        run_id: "run-1".into(),
        lease_owner: "worker-a".into(),
        lease_ms: 100,
        now: 10,
    })
    .unwrap();

    // 进程 B：全新实例打开同一 base。
    let b = FileExecutionStore::new(&base);
    let run = b.load_run("run-1").unwrap().expect("B 应读到 A 写的 run");
    assert_eq!(run.input, serde_json::json!({ "n": 1 }));
    assert_eq!(run.status, WorkflowExecutionStatus::Running);
    assert_eq!(
        run.lease.as_ref().map(|l| l.owner.as_str()),
        Some("worker-a"),
        "lease 也应跨实例可见（这是落盘相对内存的实质差别）"
    );

    let events = b
        .read_events(ReadEventsArgs {
            run_id: "run-1".into(),
            from_index: None,
        })
        .unwrap();
    assert_eq!(events.len(), 2, "事件日志应跨实例可见且保序");
    assert_eq!(events[0].event_index, 0);
    assert_eq!(events[1].event_index, 1);

    // B 能续写（CAS 从 2 开始）。
    b.append_events(AppendEventsArgs {
        run_id: "run-1".into(),
        expected_next_index: 2,
        events: vec![custom("c", 3)],
    })
    .unwrap();
    let events = b
        .read_events(ReadEventsArgs {
            run_id: "run-1".into(),
            from_index: None,
        })
        .unwrap();
    assert_eq!(events.len(), 3);

    let _ = std::fs::remove_dir_all(&base);
}

/// 接 core 的 `run_workflow` 端到端：并行 step（`try_join!`）会并发 append，
/// 走 CAS。这条验证 store 真能当引擎后端，而不只是过契约。
#[tokio::test]
async fn drives_a_parallel_workflow() {
    let base = temp_base("e2e");

    let wf = create_workflow(
        CreateWorkflowConfig::new("parallel-e2e").input::<serde_json::Value>(),
    )
    .handler(|ctx: BaseCtx<serde_json::Value>| async move {
        let a = ctx.clone();
        let b = ctx.clone();
        let (ra, rb) = tokio::try_join!(
            async move {
                a.step("left", |_sc: StepCtx| async move {
                    Ok(serde_json::json!({ "side": "left" }))
                })
                .await
            },
            async move {
                b.step("right", |_sc: StepCtx| async move {
                    Ok(serde_json::json!({ "side": "right" }))
                })
                .await
            },
        )?;
        Ok(serde_json::json!({ "left": ra, "right": rb }))
    });

    let store: Arc<dyn WorkflowExecutionStore> = Arc::new(FileExecutionStore::new(&base));
    let core_store = create_run_store_adapter(store.clone());

    let out = run_workflow(
        &RunWorkflowOptions::new(Arc::new(wf.clone().into_workflow()), core_store)
            .input(serde_json::json!({ "n": 1 })),
    )
    .await
    .expect("run_workflow");

    assert_eq!(out.status, RunStatus::Finished, "workflow 应跑完");

    // 两个并行 step 的终态都落了盘。
    let events = store
        .read_events(ReadEventsArgs {
            run_id: out.run_id.clone(),
            from_index: None,
        })
        .unwrap();
    for step in ["left", "right"] {
        let n = events
            .iter()
            .filter(|e| {
                e.event_type == "STEP_FINISHED" && e.step_id.as_deref() == Some(step)
            })
            .count();
        assert_eq!(n, 1, "{step} 应有且仅有 1 条 STEP_FINISHED");
    }

    // 重开一个实例：run 应处于终态（落盘可信）。
    let reopened = FileExecutionStore::new(&base);
    let run = reopened.load_run(&out.run_id).unwrap().expect("重开应读到");
    assert_eq!(run.status, WorkflowExecutionStatus::Finished);

    let _ = std::fs::remove_dir_all(&base);
}
