//! 验证 DAG 策略中间件：redo 复用 + 临时目标泊车，读 `input.__dagPlan`。

use std::sync::Arc;
use std::sync::Mutex;

use aa_workflow_core::define::StepCtx;
use aa_workflow_core::event::WorkflowEvent;
use aa_workflow_dag::DagShape;
use aa_workflow_core::{
    create_workflow, run_workflow, CreateWorkflowConfig, InMemoryStore, RunStore,
    RunWorkflowOptions, WorkflowCtx,
};

use aa_workflow_middlewares::{dag_policies, INTERIM_GOAL_SIGNAL};

fn shape() -> Arc<DagShape> {
    // 菱形：load → (enrich, translate) → merge → publish
    let load: &[&str] = &[];
    let with_load: &[&str] = &["load"];
    let with_pair: &[&str] = &["enrich", "translate"];
    let with_merge: &[&str] = &["merge"];
    Arc::new(
        DagShape::build(&[
            ("load", load),
            ("enrich", with_load),
            ("translate", with_load),
            ("merge", with_pair),
            ("publish", with_merge),
        ])
        .unwrap(),
    )
}

fn loader(store: Arc<InMemoryStore>) -> Arc<aa_workflow_middlewares::GetEvents> {
    Arc::new(move |run_id: &str| {
        use aa_workflow_core::RunStore;
        Ok(store.get_events(run_id)?)
    })
}

#[tokio::test]
async fn redo_reuse_and_goal_park_via_dag_plan() {
    let store = Arc::new(InMemoryStore::new());
    let exec: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let mk = {
        let store = store.clone();
        let exec = exec.clone();
        move || {
            let exec = exec.clone();
            let store = store.clone();
            create_workflow(CreateWorkflowConfig::new("dag-pol").input::<serde_json::Value>())
                .middleware::<()>(dag_policies(shape(), loader(store)))
                .handler(move |ctx: WorkflowCtx| {
                    let exec = exec.clone();
                    async move {
                        for id in ["load", "enrich", "translate", "merge", "publish"] {
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
                })
        }
    };

    // rev1：无 plan，全跑
    run_workflow(
        RunWorkflowOptions::new(Arc::new(mk()), store.clone())
            .input(serde_json::json!({}))
            .run_id("r1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(
        exec.lock().unwrap().clone(),
        vec![
            "load".to_string(),
            "enrich".to_string(),
            "translate".to_string(),
            "merge".to_string(),
            "publish".to_string(),
        ]
    );

    // rev2：from=translate（重跑 translate/merge/publish），goals=[merge]
    // 期望：load/enrich 复用（不执行），translate/merge 真跑，merge 达成后泊车
    //（publish 不执行）。
    exec.lock().unwrap().clear();
    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(mk()), store.clone())
            .input(serde_json::json!({
                "__dagPlan": {
                    "source_run_id": "r1",
                    "from": ["translate"],
                    "goals": ["merge"],
                }
            }))
            .run_id("r2"),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(
        exec.lock().unwrap().clone(),
        vec!["translate".to_string(), "merge".to_string()],
        "load/enrich 复用、merge 达成即停、publish 不跑"
    );
    assert_ne!(format!("{:?}", out.status), "Finished", "应泊车");
    assert!(
        store
            .get_events("r2")
            .unwrap()
            .iter()
            .any(|e| matches!(
                e,
                WorkflowEvent::SignalAwaited { name, .. } if name == INTERIM_GOAL_SIGNAL
            )),
        "应有 __interim-goal 的 SignalAwaited"
    );

    // rev2 日志里 load/enrich 也有 checkpoint（复用提交的）→ 重放可短路
    let finished: Vec<String> = store
        .get_events("r2")
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::StepFinished { step_id, .. } => Some(step_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        finished,
        vec![
            "load".to_string(),
            "enrich".to_string(),
            "translate".to_string(),
            "merge".to_string(),
        ],
        "load/enrich/translate/merge 都有 checkpoint"
    );

    // 续驱：publish 补跑（load/enrich/translate/merge 短路）
    exec.lock().unwrap().clear();
    let out3 = run_workflow(
        RunWorkflowOptions::new(Arc::new(mk()), store.clone())
            .input(serde_json::json!({}))
            .run_id("r2"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(format!("{:?}", out3.status), "Finished");
    assert_eq!(
        exec.lock().unwrap().clone(),
        vec!["publish".to_string()],
        "续驱只补跑 publish"
    );
}

#[tokio::test]
async fn no_plan_means_no_behavior() {
    let store = Arc::new(InMemoryStore::new());
    let exec: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let wf = create_workflow(CreateWorkflowConfig::new("no-plan").input::<serde_json::Value>())
        .middleware::<()>(dag_policies(shape(), loader(store.clone())))
        .handler({
            let exec = exec.clone();
            move |ctx: WorkflowCtx| {
                let exec = exec.clone();
                async move {
                    ctx.step("a", move |_sc: StepCtx| {
                        let exec = exec.clone();
                        async move {
                            exec.lock().unwrap().push("a".to_string());
                            Ok(serde_json::json!(1))
                        }
                    })
                    .await?;
                    Ok(serde_json::json!({ "ok": true }))
                }
            }
        });

    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone())
            .input(serde_json::json!({}))
            .run_id("np:1"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(format!("{:?}", out.status), "Finished", "无 plan = 无行为");
    assert_eq!(exec.lock().unwrap().len(), 1);
}
