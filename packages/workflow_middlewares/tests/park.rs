//! 验证中间件在 Rust 侧的真实行为。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use aa_workflow_core::define::StepCtx;
use aa_workflow_core::event::WorkflowEvent;
use aa_workflow_core::{
    create_workflow, run_workflow, signal_event, CreateWorkflowConfig, InMemoryStore,
    RunStore, RunWorkflowOptions, WorkflowCtx,
};

use aa_workflow_middlewares::{interim_goals, park_after_handler, INTERIM_GOAL_SIGNAL};

/// handler 跑完后停：第一次 paused；投信号后续驱到 finished，且 step 不重跑。
#[tokio::test]
async fn park_after_handler_parks_once_then_resumes() {
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();

    let wf = create_workflow(CreateWorkflowConfig::new("park-test").input::<serde_json::Value>())
        .middleware::<()>(park_after_handler("review"))
        .handler(move |ctx: WorkflowCtx| {
            let c = c.clone();
            async move {
                ctx.step("work", move |_sc: StepCtx| {
                    let c = c.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        Ok(serde_json::json!({ "done": true }))
                    }
                })
                .await?;
                Ok(serde_json::json!({ "ok": true }))
            }
        });

    let store = Arc::new(InMemoryStore::new());

    // 第一次 drive：handler 跑完 → 泊车
    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
            .input(serde_json::json!({}))
            .run_id("p:1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1, "第一次：step 执行 1 次");
    assert_ne!(
        format!("{:?}", out.status),
        "Finished",
        "handler 已跑完但应停在泊车点，实际 {:?}",
        out.status
    );

    // 日志里应有一条 __interim-goal 的 SignalAwaited
    let has_await = store.get_events("p:1").unwrap().iter().any(|e| {
        matches!(
            e,
            WorkflowEvent::SignalAwaited { name, .. } if name == INTERIM_GOAL_SIGNAL
        )
    });
    assert!(has_await, "应有一条 __interim-goal 的 SignalAwaited");

    // 投信号续驱
    signal_event(
        store.as_ref(),
        "p:1",
        INTERIM_GOAL_SIGNAL,
        None,
        serde_json::Value::Null,
        None,
    )
    .expect("投递应成功");

    let out2 = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone())
            .input(serde_json::json!({}))
            .run_id("p:1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(
        format!("{:?}", out2.status),
        "Finished",
        "续驱后应 finished，实际 {:?}",
        out2.status
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "续驱重放时 step 必须短路，不重跑"
    );
}

/// `interim_goals()` 是别名，行为一致（同样在 handler 之后泊）。
#[tokio::test]
async fn interim_goals_alias_parks() {
    let wf = create_workflow(CreateWorkflowConfig::new("alias").input::<serde_json::Value>())
        .middleware::<()>(interim_goals())
        .handler(|ctx: WorkflowCtx| async move {
            ctx.step("a", |_sc: StepCtx| async move { Ok(serde_json::json!(1)) })
                .await?;
            Ok(serde_json::json!({ "ok": true }))
        });
    let store = Arc::new(InMemoryStore::new());
    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone())
            .input(serde_json::json!({}))
            .run_id("a:1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_ne!(format!("{:?}", out.status), "Finished");
}
