//! DAG 恢复 demo（三个已实现原语各一个可读场景）——engine 内 demo 测试的
//! 集成测试镜像：只组合 runtime 正门（`run_workflow`）+ 引擎原语，无 DAG
//! 算法依赖。上层（aa-dag / workflow-dag）的重跑集、分段、续驱都落在
//! 这三个原语的组合上。
//!
//! ① interimGoals 泊车 + 续驱  ② compensate 逆序撤销  ③ durable retry 泊车/续跑
//! ④ reexecuteFrom redo（失效名单导入重跑）  ⑤ 形状图合龙：aa_graph 闭包展开
//!    → 名单透传（本 crate 的 DagShape 是「DAG 术语」唯一合法驻地，core 不知道图）

use aa_workflow_core::{
    Backoff, ImportedFrom, InMemoryStore, ReexecuteFrom, RetryPolicy, RunStatus, RunStore,
    RunWorkflowOptions, StepCtx, StepOptions, WorkflowCtx, WorkflowDefinition, WorkflowEvent,
    run_workflow,
};
use aa_workflow_dag::DagShape;
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

fn three_step_wf(log: &Arc<Mutex<TestLog>>) -> WorkflowDefinition {
    WorkflowDefinition::new("w").handler({
        let log = log.clone();
        move |ctx: WorkflowCtx| {
            let log = log.clone();
            async move {
                for id in ["a", "b", "c"] {
                    let log = log.clone();
                    let id = id.to_string();
                    let id_c = id.clone();
                    ctx.step(&id, move |_sc: StepCtx| {
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

/// ① 临时目标：a → b → c，interimGoals = ["b"]——b checkpoint 后泊车
///    （run → paused，c 缺席即待续）；续驱（不带 interimGoals）→ b 短路、
///    c 补齐 → Finished。宿主扩展边界的语义同此（重驱时带新名单）。
#[tokio::test]
async fn demo_interim_goals_park_and_continue() {
    let store = Arc::new(InMemoryStore::new());
    let log = Arc::new(Mutex::new(TestLog::default()));
    let wf = WorkflowDefinition::new("w").handler({
        let log = log.clone();
        move |ctx: WorkflowCtx| {
            let log = log.clone();
            async move {
                for id in ["a", "b", "c"] {
                    let log = log.clone();
                    let id = id.to_string();
                    let id_c = id.clone();
                    ctx.step(&id, move |_sc: StepCtx| {
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
    });

    let first = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
            .input(serde_json::json!({}))
            .run_id("demo:goal")
            .interim_goals(vec!["b".into()]),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(first.status, RunStatus::Paused, "goal 后泊车（c 缺席）");
    {
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 1);
        assert_eq!(l.runs.get("c").copied().unwrap_or(0), 0, "c 未执行");
    }

    // 续驱：不带 interimGoals → 泊车边界解除，缺席的 c 补齐 → Finished
    let second = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
            .input(serde_json::json!({}))
            .run_id("demo:goal"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(second.status, RunStatus::Finished);
    {
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1, "上游短路：已完成的不重跑");
        assert_eq!(l.runs["b"], 1);
        assert_eq!(l.runs["c"], 1, "续驱只补缺席的 c");
    }
}

/// ② 补偿（undo 轴）：三步各登记撤销，c 抛错 → 逆序撤销 [c, b, a]；
///    全部撤销成功 run 仍 errored（携带原始错误）——补偿修复外部世界，
///    不修改 run 的失败定性。
#[tokio::test]
async fn demo_compensate_reverse_undo() {
    let store = Arc::new(InMemoryStore::new());
    let undo_log: UndoLog = Arc::new(Mutex::new(Vec::new()));
    let undo_a = make_undo(&undo_log, "undo-a");
    let undo_b = make_undo(&undo_log, "undo-b");
    let undo_c = make_undo(&undo_log, "undo-c");

    let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| {
        let undo_a = undo_a.clone();
        let undo_b = undo_b.clone();
        let undo_c = undo_c.clone();
        async move {
            // 登记先于效果：撤销幂等（容忍"尚无效果"），逆序触发
            ctx.compensate(
                "undo-a",
                move |sc: StepCtx| undo_a(sc),
                RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
            )?;
            ctx.compensate(
                "undo-b",
                move |sc: StepCtx| undo_b(sc),
                RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
            )?;
            ctx.compensate(
                "undo-c",
                move |sc: StepCtx| undo_c(sc),
                RetryPolicy::new(1, Backoff::Fixed { base_ms: 1 }),
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
    });

    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), store.clone()).input(serde_json::json!({})),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(out.status, RunStatus::Errored, "全部撤销成功仍 errored");
    assert_eq!(
        *undo_log.lock().unwrap(),
        vec!["undo-c", "undo-b", "undo-a"],
        "逆登记序撤销"
    );
}

/// ③ 耐久重试：flaky 步骤 attempt 1 失败 → STEP_RETRY_SCHEDULED 泊车；
///    重驱 attempt 2 成功 → Finished；attempts [1, 2] 跨驱动携带。
#[tokio::test]
async fn demo_durable_retry_park_and_resume() {
    let store = Arc::new(InMemoryStore::new());
    let attempts_seen: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let handler_seen = Arc::clone(&attempts_seen);
    let wf = WorkflowDefinition::new("w").handler(move |ctx: WorkflowCtx| {
        let seen = Arc::clone(&handler_seen);
        async move {
            let opts = StepOptions {
                retry: Some(RetryPolicy {
                    max_attempts: 2,
                    backoff: Backoff::Fixed { base_ms: 30 },
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
    });

    let first = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
            .input(serde_json::json!({}))
            .run_id("demo:retry"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(first.status, RunStatus::Paused, "attempt 1 失败 → 泊车");
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;

    let second = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
            .input(serde_json::json!({}))
            .run_id("demo:retry"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(second.status, RunStatus::Finished, "重驱续跑至成功");
    assert_eq!(
        *attempts_seen.lock().unwrap(),
        vec![1, 2],
        "attempt 序号跨驱动携带"
    );
}

/// ④ redo（INVALIDATE 侧）：`reexecuteFrom` 以新 run 起步，导入源 run 幸存
///    的 checkpoint（带 `imported_from` 溯源），失效名单里的 b 重跑——a/c
///    短路于导入的 checkpoint。c 拿着 b 的旧结果：名单封闭性是调用者的
///    不变量，引擎没有图、只透传名单（`unsafe` 之名由此而来）。
#[tokio::test]
async fn demo_reexecute_from_invalidate_rerun() {
    let store = Arc::new(InMemoryStore::new());
    let src_log = Arc::new(Mutex::new(TestLog::default()));
    let first = run_workflow(
        RunWorkflowOptions::new(three_step_wf(&src_log), store.clone())
            .input(serde_json::json!({})),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(first.status, RunStatus::Finished);
    let source_run_id = first.run_id.clone();

    // redo：失效名单 = ["b"]（使用者自己提供）
    let redo_log = Arc::new(Mutex::new(TestLog::default()));
    let out = run_workflow(
        RunWorkflowOptions::new(three_step_wf(&redo_log), store.clone())
            .input(serde_json::json!({}))
            .run_id("demo:redo")
            .reexecute_from(
                ReexecuteFrom::new(source_run_id.clone()).with_invalidate(vec!["b".into()]),
            ),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);
    {
        let l = redo_log.lock().unwrap();
        assert_eq!(l.runs.get("a").copied().unwrap_or(0), 0, "a 导入短路");
        assert_eq!(l.runs["b"], 1, "b 被失效重跑");
        assert_eq!(l.runs.get("c").copied().unwrap_or(0), 0, "c 导入短路");
    }

    // 溯源：导入的 checkpoint 带 imported_from；index 以源 run 持久化日志
    // 为基准（RUN_STARTED 不落盘、不占下标）
    let src_events = store.get_events(&source_run_id).unwrap();
    let src_idx_of_a = src_events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::StepFinished { step_id, .. } if step_id == "a"))
        .unwrap();
    let redo_events = store.get_events("demo:redo").unwrap();
    let imported_a = redo_events.iter().find_map(|e| match e {
        WorkflowEvent::StepFinished { step_id, meta, .. } if step_id == "a" => meta.clone(),
        _ => None,
    });
    assert_eq!(
        imported_a.unwrap().imported_from,
        Some(ImportedFrom {
            run_id: source_run_id,
            index: src_idx_of_a,
        }),
        "a 的 checkpoint 来自导入"
    );
}

/// ⑤ 形状图合龙（dub_sf_ocr 同款真实形状，examples/store_file 10 步）：
///    两条并行分支（人声轴 separate→separate_after；字幕轴
///    sf_ocr_pre→sf_ocr→sf_ocr_fix→translate）+ 汇聚链
///    （split_audio 吃两支产物 → tts → mix_audio → mix_video）。
///    场景：翻译服务故障——translate 失败，其余 5 步成功在案。
///    `DagShape::reexecute_plan` 缺省 from = 未成功节点 {translate 及其下游
///    4 步}，闭包展开后交给 reexecute_from；修复后重跑恰好那 5 步，
///    OCR 链与人声轴全部导入短路。形状只是名单计算依据——真实 handler
///    的 try_join! 并行在 demo 里串行化（执行序与形状解耦，见 ④ 的教训）。
#[tokio::test]
async fn demo_dub_sf_ocr_shape_closure_redo() {
    let store = Arc::new(InMemoryStore::new());
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

    // 源 run：翻译服务故障，translate 失败（执行序串行化：分支 A 全部
    // checkpoint 后分支 B 才走到 translate，保证确定性）
    let src_log = Arc::new(Mutex::new(TestLog::default()));
    let first = run_workflow(
        RunWorkflowOptions::new(dub_wf(&src_log, Some("translate")), store.clone())
            .input(serde_json::json!({}))
            .run_id("dub:src"),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(first.status, RunStatus::Errored, "translate 失败 → errored");
    let source_run_id = first.run_id.clone();

    // 缺省 from = 源日志未成功节点：translate + 它下游的 4 步（汇聚链没跑到）
    let source_events = store.get_events(&source_run_id).unwrap();
    let plan = shape
        .reexecute_plan(&source_run_id, &source_events, None)
        .expect("from 缺省不引入未知 step");
    assert_eq!(
        plan.unsafe_invalidate,
        vec!["mix_audio", "mix_video", "split_audio", "translate", "tts"],
        "闭包展开：翻译轴 + 汇聚链；人声轴与 OCR 链不在名单"
    );

    // redo：翻译服务修好了——名单里 5 步真实重跑，其余 5 步导入短路
    let redo_log = Arc::new(Mutex::new(TestLog::default()));
    let out = run_workflow(
        RunWorkflowOptions::new(dub_wf(&redo_log, None), store.clone())
            .input(serde_json::json!({}))
            .run_id("dub:redo")
            .reexecute_from(plan),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);
    {
        let l = redo_log.lock().unwrap();
        for id in ["separate", "separate_after", "sf_ocr_pre", "sf_ocr", "sf_ocr_fix"] {
            assert_eq!(l.runs.get(id).copied().unwrap_or(0), 0, "{id} 导入短路");
        }
        for id in ["translate", "split_audio", "tts", "mix_audio", "mix_video"] {
            assert_eq!(l.runs[id], 1, "{id} 重跑恰好一次");
        }
    }
}

/// dub_sf_ocr 同款 10 步（执行序串行化；`fail_at` 指名的 step 失败）。
/// 真实 handler（examples/store_file）两条分支 try_join! 并行、产物是
/// 强类型结构体——demo 只保形状与语义，产物压成 step id。
fn dub_wf(log: &Arc<Mutex<TestLog>>, fail_at: Option<&'static str>) -> WorkflowDefinition {
    WorkflowDefinition::new("dub_sf_ocr").handler({
        let log = log.clone();
        move |ctx: WorkflowCtx| {
            let log = log.clone();
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
                    let fail = fail_at == Some(id);
                    ctx.step(&id_s, move |_sc: StepCtx| {
                        let (log, id) = (log.clone(), id.to_string());
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
