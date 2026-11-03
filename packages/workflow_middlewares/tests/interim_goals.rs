//! 验证「目标达成即停」——与 TS 版 `interimGoals` 语义对齐。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aa_workflow_core::define::StepCtx;
use aa_workflow_core::event::WorkflowEvent;
use aa_workflow_core::{
    create_workflow, run_workflow, signal_event, CreateWorkflowConfig, InMemoryStore,
    RunStore, RunWorkflowOptions, WorkflowCtx,
};

use aa_workflow_middlewares::{interim_goals, INTERIM_GOAL_SIGNAL};

/// 记执行顺序，用来断言「目标之后的 step 没跑」。
fn recorder() -> (Arc<Mutex<Vec<String>>>, Arc<Mutex<Vec<String>>>) {
    let a = Arc::new(Mutex::new(Vec::new()));
    let b = a.clone();
    (a, b)
}

#[tokio::test]
async fn parks_at_the_goal_step_and_does_not_run_later_steps() {
    let (log, log_in) = recorder();
    let log2 = log_in.clone();

    let store = Arc::new(InMemoryStore::new());
    let store_for_hook = store.clone();

    let wf = create_workflow(CreateWorkflowConfig::new("goal").input::<serde_json::Value>())
        .middleware([interim_goals(vec!["b".into()], move |run_id| {
            Ok(store_for_hook.get_events(run_id)?)
        })])
        .handler(move |ctx: WorkflowCtx| {
            let log = log2.clone();
            async move {
                for id in ["a", "b", "c"] {
                    let log = log.clone();
                    let id2 = id.to_string();
                    let sid = id.to_string();
                    ctx.step(id, move |_sc: StepCtx| {
                        let log = log.clone();
                        let id2 = id2.clone();
                        async move {
                            log.lock().unwrap().push(id2);
                            Ok(serde_json::json!({ "id": sid }))
                        }
                    })
                    .await?;
                }
                Ok(serde_json::json!({ "ok": true }))
            }
        });

    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
            .input(serde_json::json!({}))
            .run_id("g:1"),
    )
    .outcome()
    .await
    .unwrap();

    // 关键断言：停在 b 之后，c 没跑
    assert_eq!(
        log.lock().unwrap().clone(),
        vec!["a".to_string(), "b".to_string()],
        "目标 b 达成后应立刻停，c 不该执行"
    );
    assert_ne!(format!("{:?}", out.status), "Finished");

    // 日志里有一条 __interim-goal 的 SignalAwaited，meta 带目标
    let has_await = store.get_events("g:1").unwrap().iter().any(|e| {
        matches!(e, WorkflowEvent::SignalAwaited { name, .. } if name == INTERIM_GOAL_SIGNAL)
    });
    assert!(has_await, "应有 __interim-goal 的 SignalAwaited");

    // 续驱 → 跑完 c，且 a/b 不重跑
    signal_event(
        store.as_ref(),
        "g:1",
        INTERIM_GOAL_SIGNAL,
        None,
        serde_json::Value::Null,
        None,
    )
    .unwrap();
    let out2 = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone())
            .input(serde_json::json!({}))
            .run_id("g:1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(format!("{:?}", out2.status), "Finished");
    assert_eq!(
        log.lock().unwrap().clone(),
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
        "续驱只应补跑 c"
    );
}

#[tokio::test]
async fn no_goals_means_no_parking() {
    let store = Arc::new(InMemoryStore::new());
    let store_for_hook = store.clone();
    let ran = Arc::new(AtomicUsize::new(0));
    let ran_in = ran.clone();

    let wf = create_workflow(CreateWorkflowConfig::new("noop").input::<serde_json::Value>())
        .middleware([interim_goals(vec![], move |run_id| {
            let _ = store_for_hook.get_events(run_id)?;
            Ok(vec![])
        })])
        .handler(move |ctx: WorkflowCtx| {
            let ran = ran_in.clone();
            async move {
                ctx.step("a", move |_sc: StepCtx| {
                    let ran = ran.clone();
                    async move {
                        ran.fetch_add(1, Ordering::SeqCst);
                        Ok(serde_json::json!(1))
                    }
                })
                .await?;
                Ok(serde_json::json!({ "ok": true }))
            }
        });

    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone())
            .input(serde_json::json!({}))
            .run_id("n:1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(format!("{:?}", out.status), "Finished", "空目标 = 不泊车");
    assert_eq!(ran.load(Ordering::SeqCst), 1);
}
