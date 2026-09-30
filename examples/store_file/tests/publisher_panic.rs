//! publisher panic 隔离（对齐上游 `run-workflow.ts:128-134`）。
//!
//! 上游原文注释：
//!
//! > Best-effort fan-out. A misbehaving publisher must not break the run —
//! > swallow and continue.
//!
//! 我们原先**没有**这层保护，实测宿主 publisher 里的 panic 会把整个 durable run
//! 掀掉——已经 append 的日志停在半截、workflow 不跑完。宿主代码不该有能力
//! 损毁引擎的持久状态，所以补上。

use std::sync::Arc;

use aa_workflow_core::{BaseCtx, CreateWorkflowConfig, RunStatus, RunWorkflowOptions, StepCtx, WorkflowEvent, create_workflow, run_workflow};
use aa_workflow_runtime::run_store_adapter::{WorkflowExecutionStore, create_run_store_adapter};
use aa_workflow_runtime::types::ReadEventsArgs;
use example_store_file::FileExecutionStore;

fn store_of(tag: &str) -> Arc<dyn WorkflowExecutionStore> {
    let dir = std::env::temp_dir().join(format!(
        "wf_pub_panic_{tag}_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("建临时目录");
    Arc::new(FileExecutionStore::new(&dir))
}

/// publisher 每个事件都 panic，run 仍应跑完。
#[tokio::test]
async fn panicking_publisher_does_not_break_the_run() {
    let store = store_of("all");
    let wf = create_workflow(CreateWorkflowConfig::new("probe").input::<serde_json::Value>())
        .handler(|ctx: BaseCtx<serde_json::Value>| async move {
            ctx.step("a", move |_sc: StepCtx| async move { Ok(serde_json::Value::Null) })
                .await
        });

    let out = run_workflow(
        &RunWorkflowOptions::new(wf, create_run_store_adapter(store.clone()))
            .input(serde_json::json!({}))
            .run_id("r")
            .publisher(Some(Arc::new(|_e: WorkflowEvent| {
                panic!("publisher 炸了");
            }))),
    )
    .await
    .expect("publisher panic 不得逃逸出 run_workflow");

    assert_eq!(out.status, RunStatus::Finished, "run 应照常跑完");
    assert_eq!(out.output, Some(serde_json::Value::Null));

    // 关键：日志必须**完整**——隔离 panic 不能反过来吞掉已 append 的 checkpoint。
    let logged = store
        .read_events(ReadEventsArgs { run_id: "r".into(), from_index: None })
        .expect("读事件");
    let types: Vec<&str> = logged.iter().map(|e| e.event_type.as_str()).collect();
    assert_eq!(
        types,
        vec!["STEP_FINISHED", "RUN_FINISHED"],
        "持久状态应完好，publisher 炸了也不影响"
    );
}

/// 只在某一个事件上 panic：之后的 publish 仍要继续工作。
///
/// 防的是「一次 panic 之后 publisher 被毒化」——`catch_unwind` 之后还得能继续调。
#[tokio::test]
async fn publisher_keeps_working_after_one_panic() {
    let store = store_of("one");
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let sink = seen.clone();

    let wf = create_workflow(CreateWorkflowConfig::new("probe").input::<serde_json::Value>())
        .handler(|ctx: BaseCtx<serde_json::Value>| async move {
            ctx.step("a", move |_sc: StepCtx| async move { Ok(serde_json::Value::Null) })
                .await
        });

    run_workflow(
        &RunWorkflowOptions::new(wf, create_run_store_adapter(store))
            .input(serde_json::json!({}))
            .run_id("r")
            .publisher(Some(Arc::new(move |e: WorkflowEvent| {
                let t = e.type_name();
                // 只炸 RUN_STARTED（第一个事件），后面的必须照常送达。
                assert_ne!(t, "RUN_STARTED", "这一步就是要 panic");
                sink.lock().unwrap().push(t.to_string());
            }))),
    )
    .await
    .expect("单次 panic 不得逃逸");

    let got = seen.lock().unwrap().clone();
    assert!(
        got.contains(&"STEP_FINISHED".to_string()),
        "panic 之后 publisher 仍应继续收到事件，实际：{got:?}"
    );
}
