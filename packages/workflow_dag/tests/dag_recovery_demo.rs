//! DAG 恢复 demo（五个场景）——全部走 **runtime 正门**（`define_workflow_runtime`
//! + `start_run` / `deliver_signal` / `sweep`），与正主集成测试
//! examples/store_file/tests/dub_sf_ocr.rs 同构。core 不知道任何 DAG；
//! ⑤ 的 `DagShape` 是「DAG 术语」唯一合法驻地（aa_graph 闭包展开 → 名单透传）。
//!
//! ① interimGoals 泊车 + deliver_signal 续驱  ② compensate 逆序撤销
//! ③ durable retry 泊车 + sweep 续跑  ④ reexecuteFrom redo（名单透传）
//! ⑤ 形状图合龙（dub_sf_ocr 同款 10 步）

use aa_workflow_core::{
    CreateWorkflowConfig, ReexecuteFrom, StepCtx, WorkflowCtx, WorkflowDefinition, WorkflowEvent,
    create_workflow,
};
use aa_workflow_dag::DagShape;
use aa_workflow_runtime::{
    InMemoryExecutionStore, WorkflowRegistration, WorkflowRuntimeConfig, WorkflowRuntimeDefinition,
    WorkflowRuntimeDeliverSignalArgs, WorkflowRuntimeRunResultKind, WorkflowRuntimeStartRunArgs,
    WorkflowRuntimeSweepArgs, create_run_store_adapter, define_workflow_runtime,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type UndoLog = Arc<Mutex<Vec<&'static str>>>;

type UndoHandler = Arc<
    dyn Fn(StepCtx) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>,
        > + Send
        + Sync,
>;

fn make_undo(log: &UndoLog, name: &'static str) -> UndoHandler {
    let calls = Arc::clone(log);
    Arc::new(move |_sc: StepCtx| {
        let calls = Arc::clone(&calls);
        Box::pin(async move {
            calls.lock().unwrap().push(name);
            Ok(())
        })
    })
}

#[derive(Default)]
struct TestLog {
    runs: std::collections::HashMap<String, usize>,
    finished: Vec<String>,
}

impl TestLog {
    fn note_start(&mut self, id: &str) {
        *self.runs.entry(id.to_string()).or_insert(0) += 1;
    }
    fn note_finish(&mut self, id: &str) {
        self.finished.push(id.to_string());
    }
}

/// 每个测试自己的 runtime：注册表 + 真实 `InMemoryExecutionStore`。返回双柄
/// （rt 走四方法面；mem 直读——⑤ 的源日志从 store 面 `get_events` 取，保证
/// imported_from 的 index 以**持久化日志**为基准）。
fn demo_runtime(
    workflows: Vec<(&'static str, WorkflowDefinition)>,
) -> (Arc<WorkflowRuntimeDefinition>, Arc<InMemoryExecutionStore>) {
    let mem: Arc<InMemoryExecutionStore> = Arc::new(InMemoryExecutionStore::default());
    let mut reg: HashMap<String, WorkflowRegistration> = HashMap::new();
    for (id, wf) in workflows {
        reg.insert(
            id.to_string(),
            WorkflowRegistration {
                load: Arc::new(move || wf.clone().into()),
                previous_versions: HashMap::new(),
                version: None,
                schedules: vec![],
            },
        );
    }
    (
        Arc::new(define_workflow_runtime(WorkflowRuntimeConfig::new(
            Arc::clone(&mem) as _,
            reg,
        ))),
        mem,
    )
}

fn three_step_wf(log: &Arc<Mutex<TestLog>>) -> WorkflowDefinition {
    create_workflow(CreateWorkflowConfig::new("w").input::<serde_json::Value>()).handler({
        let log = log.clone();
        move |ctx: WorkflowCtx| {
            let log = log.clone();
            async move {
                for id in ["a", "b", "c"] {
                    let log = log.clone();
                    let id_s = id.to_string();
                    let id_c = id_s.clone();
                    ctx.step(&id_s, move |_sc: StepCtx| {
                        let (log, id) = (log.clone(), id_c.clone());
                        async move {
                            log.lock().unwrap().note_start(&id);
                            log.lock().unwrap().note_finish(&id);
                            Ok(serde_json::Value::Null)
                        }
                    })
                    .await?;
                }
                Ok(serde_json::Value::Null)
            }
        }
    })
}

/// ① 临时目标：interimGoals = ["b"]——b checkpoint 后经保留信号
///    `__interim-goal` 泊车（`kind: Paused`，c 缺席即待续）；宿主续驱 =
///    `deliver_signal("__interim-goal")`（runtime 原生 recover 路径；
///    重复 start_run 对非 Queued 的 run 只会 NotClaimable）→ c 补齐。
#[tokio::test]
async fn demo_interim_goals_park_and_continue() {
    let log = Arc::new(Mutex::new(TestLog::default()));
    let (rt, _mem) = demo_runtime(vec![("w", three_step_wf(&log))]);

    let first = rt
        .start_run(WorkflowRuntimeStartRunArgs {
            workflow_id: "w".into(),
            run_id: "demo:goal".into(),
            input: serde_json::json!({}),
            interim_goals: Some(vec!["b".into()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        matches!(first.kind, WorkflowRuntimeRunResultKind::Paused),
        "goal 后泊车"
    );
    {
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 1);
        assert_eq!(l.runs.get("c").copied().unwrap_or(0), 0, "c 未执行");
    }

    let second = rt
        .deliver_signal(WorkflowRuntimeDeliverSignalArgs {
            run_id: "demo:goal".into(),
            signal_id: "continue-goal".into(),
            name: "__interim-goal".into(),
            payload: serde_json::json!({}),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        matches!(second.kind, WorkflowRuntimeRunResultKind::Completed),
        "续驱补齐缺席的 c"
    );
    {
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1, "上游短路：已完成的不重跑");
        assert_eq!(l.runs["b"], 1);
        assert_eq!(l.runs["c"], 1, "续驱只补缺席的 c");
    }
}

/// ② 补偿（undo 轴）：三步各登记撤销，c 抛错 → 逆序撤销 [c, b, a]；
///    全部撤销成功 run 仍 errored（携带原始错误）。
#[tokio::test]
async fn demo_compensate_reverse_undo() {
    let undo_log: UndoLog = Arc::new(Mutex::new(Vec::new()));
    let undo_a = make_undo(&undo_log, "undo-a");
    let undo_b = make_undo(&undo_log, "undo-b");
    let undo_c = make_undo(&undo_log, "undo-c");

    let wf = create_workflow(CreateWorkflowConfig::new("w").input::<serde_json::Value>()).handler(
        move |ctx: WorkflowCtx| {
            let undo_a = undo_a.clone();
            let undo_b = undo_b.clone();
            let undo_c = undo_c.clone();
            async move {
                // 登记先于效果：撤销幂等（容忍"尚无效果"），逆序触发
                ctx.compensate(
                    "undo-a",
                    move |sc: StepCtx| undo_a(sc),
                    aa_workflow_core::RetryPolicy::new(1, aa_workflow_core::Backoff::Fixed { base_ms: 1 }),
                )?;
                ctx.compensate(
                    "undo-b",
                    move |sc: StepCtx| undo_b(sc),
                    aa_workflow_core::RetryPolicy::new(1, aa_workflow_core::Backoff::Fixed { base_ms: 1 }),
                )?;
                ctx.compensate(
                    "undo-c",
                    move |sc: StepCtx| undo_c(sc),
                    aa_workflow_core::RetryPolicy::new(1, aa_workflow_core::Backoff::Fixed { base_ms: 1 }),
                )?;
                let _ = ctx
                    .step("a", move |_sc: StepCtx| async move { Ok(serde_json::Value::Null) })
                    .await?;
                let _ = ctx
                    .step("b", move |_sc: StepCtx| async move { Ok(serde_json::Value::Null) })
                    .await?;
                let _ = ctx
                    .step("c", move |_sc: StepCtx| async move {
                        Err::<serde_json::Value, _>(anyhow::anyhow!("c 失败"))
                    })
                    .await?;
                Ok(serde_json::Value::Null)
            }
        },
    );
    let (rt, _mem) = demo_runtime(vec![("w", wf)]);

    let out = rt
        .start_run(WorkflowRuntimeStartRunArgs {
            workflow_id: "w".into(),
            run_id: "demo:undo".into(),
            input: serde_json::json!({}),
            ..Default::default()
        })
        .await
        .unwrap();

    assert!(
        matches!(out.kind, WorkflowRuntimeRunResultKind::Errored),
        "全部撤销成功仍 errored"
    );
    assert_eq!(
        *undo_log.lock().unwrap(),
        vec!["undo-c", "undo-b", "undo-a"],
        "逆登记序撤销"
    );
}

/// ③ 耐久重试：flaky 步骤 attempt 1 失败 → STEP_RETRY_SCHEDULED 泊车；
///    到期 timer 由 **sweep** 捞起续跑（runtime 的 recover 之家）→ attempt 2
///    成功；attempts [1, 2] 跨驱动携带。
#[tokio::test]
async fn demo_durable_retry_park_and_resume() {
    let attempts_seen: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let handler_seen = Arc::clone(&attempts_seen);

    let wf = create_workflow(CreateWorkflowConfig::new("w").input::<serde_json::Value>()).handler(
        move |ctx: WorkflowCtx| {
            let seen = Arc::clone(&handler_seen);
            async move {
                let opts = aa_workflow_core::StepOptions {
                    retry: Some(aa_workflow_core::RetryPolicy {
                        max_attempts: 2,
                        backoff: aa_workflow_core::Backoff::Fixed { base_ms: 30 },
                        durable: true,
                    }),
                    ..Default::default()
                };
                let _ = ctx
                    .step_with(
                        "flaky",
                        opts,
                        move |sc: StepCtx| {
                            let attempt = sc.attempt;
                            seen.lock().unwrap().push(attempt);
                            async move {
                                if attempt == 1 {
                                    Err::<serde_json::Value, _>(anyhow::anyhow!("flaky"))
                                } else {
                                    Ok(serde_json::json!("ok"))
                                }
                            }
                        },
                    )
                    .await?;
                Ok(serde_json::Value::Null)
            }
        },
    );
    let (rt, _mem) = demo_runtime(vec![("w", wf)]);

    let first = rt
        .start_run(WorkflowRuntimeStartRunArgs {
            workflow_id: "w".into(),
            run_id: "demo:retry".into(),
            input: serde_json::json!({}),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        matches!(first.kind, WorkflowRuntimeRunResultKind::Paused),
        "attempt 1 失败 → 泊车"
    );
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;

    let swept = rt
        .sweep(WorkflowRuntimeSweepArgs::default())
        .await
        .unwrap();
    assert!(!swept.timers.is_empty(), "到期 retry timer 被 sweep 捞起");
    assert!(
        swept
            .timers
            .iter()
            .all(|r| matches!(r.kind, WorkflowRuntimeRunResultKind::Completed)),
        "重驱续跑至成功"
    );
    assert_eq!(
        *attempts_seen.lock().unwrap(),
        vec![1, 2],
        "attempt 序号跨驱动携带"
    );
}

/// ④ redo（INVALIDATE 侧）：`reexecuteFrom` 经 start_run 透传——源 run 幸存
///    checkpoint 导入（带 imported_from 溯源），失效名单里的 b 重跑，a/c 短路。
///    名单封闭性是调用者的不变量（`unsafe` 之名由此而来）。
#[tokio::test]
async fn demo_reexecute_from_invalidate_rerun() {
    let log = Arc::new(Mutex::new(TestLog::default()));
    let (rt, _mem) = demo_runtime(vec![("w", three_step_wf(&log))]);

    let first = rt
        .start_run(WorkflowRuntimeStartRunArgs {
            workflow_id: "w".into(),
            run_id: "demo:src".into(),
            input: serde_json::json!({}),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(matches!(first.kind, WorkflowRuntimeRunResultKind::Completed));

    let redo = rt
        .start_run(WorkflowRuntimeStartRunArgs {
            workflow_id: "w".into(),
            run_id: "demo:redo".into(),
            input: serde_json::json!({}),
            reexecute_from: Some(ReexecuteFrom::new("demo:src").with_invalidate(vec!["b".into()])),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(matches!(redo.kind, WorkflowRuntimeRunResultKind::Completed));
    {
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1, "a 导入短路");
        assert_eq!(l.runs["b"], 2, "b 被失效重跑（source 1 次 + redo 1 次）");
        assert_eq!(l.runs["c"], 1, "c 导入短路");
    }

    // 溯源：导入的 checkpoint 带 imported_from（index 以源 run 持久化日志为基准）
    let imported_a = redo.events.iter().find_map(|e| match e {
        WorkflowEvent::StepFinished { step_id, meta, .. } if step_id == "a" => meta.clone(),
        _ => None,
    });
    assert!(
        imported_a.unwrap().imported_from.is_some(),
        "a 的 checkpoint 来自导入"
    );
}

/// ⑤ 形状图合龙（dub_sf_ocr 同款真实形状，examples/store_file 10 步）：
///    两条并行分支（人声轴 separate→separate_after；字幕轴
///    sf_ocr_pre→sf_ocr→sf_ocr_fix→translate）+ 汇聚链
///    （split_audio 吃两支产物 → tts → mix_audio → mix_video）。
///    场景：翻译服务故障——translate 失败，其余 5 步成功在案。
///    `DagShape::reexecute_plan` 缺省 from = 未成功节点（translate 及其下游
///    4 步），闭包展开后交给 start_run 的 reexecute_from；修复后重跑恰好
///    那 5 步，OCR 链与人声轴全部导入短路。形状只是名单计算依据——真实
///    handler 的 try_join! 并行在 demo 里串行化（执行序与形状解耦）。
#[tokio::test]
async fn demo_dub_sf_ocr_shape_closure_redo() {
    let log = Arc::new(Mutex::new(TestLog::default()));
    // 依赖方向与 examples/store_file/src/dub_sf_ocr.rs 的 handler 逐步对应：
    // split_audio 同时吃两支的产物（separated + translated）——两条分支在此汇合。
    let shape = DagShape::build(&[
        ("separate", &[]),
        ("separate_after", &["separate"]),
        ("sf_ocr_pre", &[]),
        ("sf_ocr", &["sf_ocr_pre"]),
        ("sf_ocr_fix", &["sf_ocr"]),
        ("translate", &["sf_ocr_fix"]),
        ("split_audio", &["separate_after", "translate"]),
        ("tts", &["split_audio"]),
        ("mix_audio", &["separate_after", "tts"]),
        ("mix_video", &["separate_after", "mix_audio"]),
    ])
    .expect("dub 形状无环");
    let (rt, mem) = demo_runtime(vec![("dub_sf_ocr", dub_wf(&log))]);

    // 源 run：翻译服务故障（执行序串行化：分支 A 全部 checkpoint 后分支 B
    // 才走到 translate，保证确定性）
    let first = rt
        .start_run(WorkflowRuntimeStartRunArgs {
            workflow_id: "dub_sf_ocr".into(),
            run_id: "dub:src".into(),
            input: serde_json::json!({ "translate_down": true }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        matches!(first.kind, WorkflowRuntimeRunResultKind::Errored),
        "translate 失败 → errored"
    );

    // 缺省 from = 源日志未成功节点；日志从 store 面取（持久化序，溯源
    // index 的基准与 core 测试同款）
    let core_store = create_run_store_adapter(Arc::clone(&mem) as _);
    let source_events = core_store.get_events("dub:src").unwrap();
    let plan = shape
        .reexecute_plan("dub:src", &source_events, None)
        .expect("from 缺省不引入未知 step");
    assert_eq!(
        plan.unsafe_invalidate,
        vec!["mix_audio", "mix_video", "split_audio", "translate", "tts"],
        "闭包展开 = 种子∪后继；无关分支不在名单"
    );

    // redo：翻译服务修好了（世界变了）——名单里 5 步真实重跑，其余 5 步
    // 导入短路
    let redo = rt
        .start_run(WorkflowRuntimeStartRunArgs {
            workflow_id: "dub_sf_ocr".into(),
            run_id: "dub:redo".into(),
            input: serde_json::json!({}),
            reexecute_from: Some(plan),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(matches!(redo.kind, WorkflowRuntimeRunResultKind::Completed));
    {
        let l = log.lock().unwrap();
        for id in ["separate", "separate_after", "sf_ocr_pre", "sf_ocr", "sf_ocr_fix"] {
            assert_eq!(l.runs[id], 1, "{id} 导入短路（source 1 次，redo 0 次）");
        }
        assert_eq!(l.runs["translate"], 2, "translate 重跑（source 失败 1 次 + redo 1 次）");
        for id in ["split_audio", "tts", "mix_audio", "mix_video"] {
            assert_eq!(l.runs[id], 1, "{id} 重跑恰好一次（redo）");
        }
    }
}

/// dub_sf_ocr 同款 10 步（执行序串行化）。翻译服务是否故障由 **input** 决定
/// （`translate_down: true` = 源 run 的故障世界；缺省 = 服务已恢复）——redo 的
/// 本义是「世界变了再执行」，这里世界的变化就是服务恢复。真实 handler
/// （examples/store_file）两条分支 try_join! 并行、产物是强类型结构体——
/// demo 只保形状与语义，产物压成 step id。
fn dub_wf(log: &Arc<Mutex<TestLog>>) -> WorkflowDefinition {
    create_workflow(CreateWorkflowConfig::new("dub_sf_ocr").input::<serde_json::Value>()).handler({
        let log = log.clone();
        move |ctx: WorkflowCtx| {
            let log = log.clone();
            let translate_down = ctx.input["translate_down"].as_bool().unwrap_or(false);
            async move {
                for id in [
                    "separate",
                    "separate_after",
                    "sf_ocr_pre",
                    "sf_ocr",
                    "sf_ocr_fix",
                    "translate",
                    "split_audio",
                    "tts",
                    "mix_audio",
                    "mix_video",
                ] {
                    let log = log.clone();
                    let id_s = id.to_string();
                    let id_c = id_s.clone();
                    let fail = translate_down && id == "translate";
                    ctx.step(&id_s, move |_sc: StepCtx| {
                        let (log, id) = (log.clone(), id_c.clone());
                        async move {
                            log.lock().unwrap().note_start(&id);
                            if fail {
                                return Err::<serde_json::Value, _>(anyhow::anyhow!(
                                    "翻译服务不可用"
                                ));
                            }
                            Ok(serde_json::json!(id))
                        }
                    })
                    .await?;
                }
                Ok(serde_json::Value::Null)
            }
        }
    })
}
