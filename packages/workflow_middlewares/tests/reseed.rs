//! 验证 redo：不在 redoSet 里的 step 不执行闭包、复用源 run 的结果。

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aa_workflow_core::define::StepCtx;
use aa_workflow_core::event::WorkflowEvent;
use aa_workflow_core::{
    create_workflow, run_workflow, CreateWorkflowConfig, InMemoryStore, RunStore,
    RunWorkflowOptions, WorkflowCtx,
};

use aa_workflow_middlewares::{reseed, ReseedPlan};

/// 源 run 的结果提取器：把日志里的 STEP_FINISHED 收成 map。
fn loader(store: Arc<InMemoryStore>) -> impl Fn(&str) -> anyhow::Result<HashMap<String, serde_json::Value>>
+ Send
+ Sync
+ 'static {
    move |run_id: &str| {
        let mut m = HashMap::new();
        for e in store.get_events(run_id)? {
            if let WorkflowEvent::StepFinished {
                step_id,
                result: Some(r),
                ..
            } = e
            {
                m.insert(step_id, r);
            }
        }
        Ok(m)
    }
}

fn make_wf(
    exec: Arc<Mutex<Vec<String>>>,
    mw: Option<aa_workflow_core::middleware::Middleware>,
) -> impl Fn() -> aa_workflow_core::define::WorkflowDefinition<serde_json::Value, serde_json::Value, serde_json::Value>
+ Clone {
    move || {
        let exec = exec.clone();
        let mut builder =
            create_workflow(CreateWorkflowConfig::new("redo").input::<serde_json::Value>());
        if let Some(m) = mw.clone() {
            builder = builder.middleware([m]);
        }
        builder.handler(
            move |ctx: WorkflowCtx| {
                let exec = exec.clone();
                async move {
                    for id in ["a", "b", "c"] {
                        let exec = exec.clone();
                        let sid = id.to_string();
                        ctx.step(id, move |_sc: StepCtx| {
                            let exec = exec.clone();
                            let sid = sid.clone();
                            async move {
                                exec.lock().unwrap().push(sid.clone());
                                Ok(serde_json::json!({ "id": sid }))
                            }
                        })
                        .await?;
                    }
                    Ok(serde_json::json!({ "ok": true }))
                }
            },
        )
    }
}

#[tokio::test]
async fn redo_reuses_steps_outside_redo_set() {
    let store = Arc::new(InMemoryStore::new());
    let exec = Arc::new(Mutex::new(Vec::new()));

    // rev1：全跑
    let wf1 = make_wf(exec.clone(), None)();
    run_workflow(
        RunWorkflowOptions::new(Arc::new(wf1), store.clone())
            .input(serde_json::json!({}))
            .run_id("rev1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(
        exec.lock().unwrap().clone(),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );

    // rev2：只重跑 c，a/b 复用
    exec.lock().unwrap().clear();
    let wf2 = make_wf(
        exec.clone(),
        Some(reseed(
            ReseedPlan {
                source_run_id: "rev1".into(),
                redo_set: vec!["c".into()],
            },
            loader(store.clone()),
        )),
    )();
    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf2), store.clone())
            .input(serde_json::json!({}))
            .run_id("rev2"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(format!("{:?}", out.status), "Finished");
    // 关键：只有 c 真正执行过
    assert_eq!(
        exec.lock().unwrap().clone(),
        vec!["c".to_string()],
        "a/b 应被复用、闭包不执行"
    );

    // 但 rev2 的日志里 a/b/c 都有 checkpoint（重放可短路）
    let finished: Vec<String> = store
        .get_events("rev2")
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::StepFinished { step_id, .. } => Some(step_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(finished, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn redo_with_empty_redo_set_reuses_everything() {
    let store = Arc::new(InMemoryStore::new());
    let exec = Arc::new(Mutex::new(Vec::new()));

    let wf1 = make_wf(exec.clone(), None)();
    run_workflow(
        RunWorkflowOptions::new(Arc::new(wf1), store.clone())
            .input(serde_json::json!({}))
            .run_id("s1"),
    )
    .outcome()
    .await
    .unwrap();

    exec.lock().unwrap().clear();
    let wf2 = make_wf(
        exec.clone(),
        Some(reseed(
            ReseedPlan {
                source_run_id: "s1".into(),
                redo_set: vec![],
            },
            loader(store.clone()),
        )),
    )();
    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf2), store.clone())
            .input(serde_json::json!({}))
            .run_id("s2"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(format!("{:?}", out.status), "Finished");
    assert!(
        exec.lock().unwrap().is_empty(),
        "空 redoSet = 全部复用，零业务执行"
    );
}

/// 源 run 不存在 → 查不到任何结果 → 全部真跑（与引擎参数行为一致）。
#[tokio::test]
async fn redo_with_missing_source_runs_everything() {
    let store = Arc::new(InMemoryStore::new());
    let exec: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let ran = Arc::new(AtomicUsize::new(0));
    let ran_in = ran.clone();

    let wf = create_workflow(CreateWorkflowConfig::new("redo2").input::<serde_json::Value>())
        .middleware([reseed(
            ReseedPlan {
                source_run_id: "ghost".into(),
                redo_set: vec![],
            },
            loader(store.clone()),
        )])
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
            .run_id("m1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(format!("{:?}", out.status), "Finished");
    assert_eq!(ran.load(Ordering::SeqCst), 1, "源不存在 → 业务函数真跑");
    void(exec);
}

fn void<T>(_: T) {}
