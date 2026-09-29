//! `dub_sf_ocr` 在 `FileExecutionStore`（新契约）上的测试。
//!
//! workflow 定义在 `src/dub_sf_ocr.rs`——本文件只放测试。
//!
//! 这批测试从 `examples/shared` 搬来时做了**一处实质改动**：驱动 store 从
//! `InMemoryStore`（core 的旧契约 `RunStore`）换成 [`FileExecutionStore`]
//! （新契约 `WorkflowExecutionStore`，经 `create_run_store_adapter` 降格给
//! `run_workflow`）。这让它们同时验两件事：
//!
//! 1. **两分支并行形状**能在引擎上跑通，且并发真的发生；
//! 2. 这些行为在**落盘** store 上也成立（不只是内存）。

use std::sync::Arc;

use aa_workflow_core::{RunWorkflowOptions, RunStatus, WorkflowEvent, run_workflow};
use aa_workflow_runtime::run_store_adapter::{
    WorkflowExecutionStore, WorkflowRunStoreAdapterStore, create_run_store_adapter,
};
use aa_workflow_runtime::types::{ReadEventsArgs, StoredWorkflowEvent};
use example_store_file::dub_sf_ocr::{dub_probe, dub_sf_ocr};
use example_store_file::FileExecutionStore;

/// 序列化共享并发探针的测试（tokio 各 test 默认并行跑，探针是全局的）。
static DUB_PROBE_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

fn dub_input() -> serde_json::Value {
    serde_json::json!({ "videoDir": "/w/1" })
}

/// `dub_sf_ocr` 的 10 个 step。
const DUB_ALL_STEPS: [&str; 10] = [
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
];

/// 一个独立临时目录 + 指向它的落盘 store（**新契约**）。
///
/// 返回 `Arc<FileExecutionStore>` 而非裸值：store 刻意**不实现 `Clone`**
/// （克隆会得到一把**独立**的锁，破坏「单进程串行化」的语义）。要共享就 `Arc`。
fn temp_store(tag: &str) -> (Arc<FileExecutionStore>, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "wf_store_file_dub_{tag}_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("建临时目录");
    (Arc::new(FileExecutionStore::new(&dir)), dir)
}

/// 某 step 的 `StepFinished` 条数。
fn finished_count(events: &[StoredWorkflowEvent], step: &str) -> usize {
    events
        .iter()
        .filter(|e| e.event_type == "STEP_FINISHED" && e.step_id.as_deref() == Some(step))
        .count()
}

/// 某 step 的 `StepFinished` 时间戳（用于断言「没被重跑」）。
fn sf_ts(events: &[StoredWorkflowEvent], step: &str) -> i64 {
    events
        .iter()
        .find(|e| e.event_type == "STEP_FINISHED" && e.step_id.as_deref() == Some(step))
        .map(|e| e.created_at)
        .expect("应有 StepFinished")
}

/// 某 step 的 durable 结果（`STEP_FINISHED` 的 result）。
fn step_output(events: &[StoredWorkflowEvent], step: &str) -> serde_json::Value {
    events
        .iter()
        .find(|e| e.event_type == "STEP_FINISHED" && e.step_id.as_deref() == Some(step))
        .and_then(|e| match &e.event {
            WorkflowEvent::StepFinished { result, .. } => result.clone(),
            _ => None,
        })
        .unwrap_or_default()
}

/// 读回事件日志（走 store 的 `read_events`，证明确实落了盘）。
fn events_of(store: &Arc<FileExecutionStore>, run_id: &str) -> Vec<StoredWorkflowEvent> {
    store
        .read_events(ReadEventsArgs {
            run_id: run_id.to_string(),
            from_index: None,
        })
        .expect("读事件")
}

#[tokio::test]
async fn dub_sf_ocr_runs_all_ten_steps_once() {
    let _guard = DUB_PROBE_LOCK.lock().await;
    dub_probe::reset();

    let (store, dir) = temp_store("all_steps");
    let core_store = create_run_store_adapter(store.clone() as Arc<dyn WorkflowExecutionStore>);
    let out = run_workflow(&RunWorkflowOptions::new(Arc::new(dub_sf_ocr().into_workflow()), core_store).input(dub_input()))
    .await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);

    let events = events_of(&store, &out.run_id);
    for step in DUB_ALL_STEPS {
        assert_eq!(
            finished_count(&events, step),
            1,
            "{step} 应恰好 StepFinished 一次"
        );
    }

    let _ = std::fs::remove_dir_all(dir);
}

/// 两分支真的同时在飞：并发峰值 ≥ 2。
///
/// 这条同时把引擎的**并发路径**逼出来——两个 durable step 并发
/// `append_events` 必然撞 CAS 冲突，靠引擎 rebase+retry 吸收。峰值到 2
/// 说明冲突被吸收；若卡在 1，说明 step 执行被串行化了。
///
/// 在**落盘** store 上跑，这层验证更有意义：CAS 走的是文件（进程内 Mutex 串行化
/// + `expected_next_index` 校验），不是内存 map。
#[tokio::test]
async fn dub_sf_ocr_branches_run_concurrently() {
    let _guard = DUB_PROBE_LOCK.lock().await;
    dub_probe::reset();

    let (store, dir) = temp_store("concurrent");
    let core_store = create_run_store_adapter(store.clone() as Arc<dyn WorkflowExecutionStore>);
    let out = run_workflow(&RunWorkflowOptions::new(Arc::new(dub_sf_ocr().into_workflow()), core_store).input(dub_input()))
    .await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);

    let peak = dub_probe::peak();
    assert!(
        peak >= 2,
        "两分支应并行（并发峰值 {peak} < 2）——若失败，说明并发 append 未被引擎吸收 \
         或 step 执行被串行化"
    );

    let _ = std::fs::remove_dir_all(dir);
}

/// 同 `run_id` 重跑：全部短路，不产生新的 `StepFinished`。
#[tokio::test]
async fn dub_sf_ocr_replay_short_circuits() {
    let _guard = DUB_PROBE_LOCK.lock().await;
    dub_probe::reset();

    let (store, dir) = temp_store("replay");
    let wf = dub_sf_ocr().into_workflow();
    let core_store =
        create_run_store_adapter(store.clone() as Arc<dyn WorkflowExecutionStore>);
    let out = run_workflow(
        &RunWorkflowOptions::new(Arc::new(wf.clone()), core_store.clone()).input(dub_input()),
    )
    .await
    .unwrap();
    let run_id = out.run_id.clone();
    let ts_1 = sf_ts(&events_of(&store, &run_id), "separate");

    let again = run_workflow(
        &RunWorkflowOptions::new(Arc::new(wf.clone()), core_store)
            .input(dub_input())
            .run_id(run_id.clone()),
    )
    .await
    .unwrap();
    assert_eq!(again.status, RunStatus::Finished);

    let events = events_of(&store, &run_id);
    assert_eq!(sf_ts(&events, "separate"), ts_1, "checkpoint 未被触碰");
    for step in DUB_ALL_STEPS {
        assert_eq!(finished_count(&events, step), 1, "{step} 重跑不应重记");
    }

    let _ = std::fs::remove_dir_all(dir);
}

/// 每个 step 都**读 typed state**：产物路径由 `state.video_dir` 派生，译文文件名
/// 带 `state.target_lang`。换个输入，10 个 step 的产物路径应全变——若哪天有人
/// 把路径写死成字面量，这条会红。
#[tokio::test]
async fn dub_sf_ocr_steps_read_typed_state() {
    let _guard = DUB_PROBE_LOCK.lock().await;
    dub_probe::reset();

    let (store, dir) = temp_store("typed_state");
    let core_store =
        create_run_store_adapter(store.clone() as Arc<dyn WorkflowExecutionStore>);
    let out = run_workflow(&RunWorkflowOptions::new(Arc::new(dub_sf_ocr().into_workflow()), core_store).input(serde_json::json!({ "videoDir": "/w/1", "targetLang": "vi" })))
    .await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);

    let events = events_of(&store, &out.run_id);
    for step in DUB_ALL_STEPS {
        let artifact = step_output(&events, step);
        let path = artifact["artifact"].as_str().unwrap_or_default();
        assert!(
            path.starts_with("/w/1/"),
            "{step} 的产物路径应由 state.video_dir 派生，实际 {path}"
        );
        assert_eq!(
            artifact["targetLang"], "vi",
            "{step} 应读到 typed state 的 target_lang"
        );
    }

    // translate 的译文文件名带目标语言。
    assert_eq!(
        step_output(&events, "translate")["artifact"],
        "/w/1/translate/vi.json"
    );

    let _ = std::fs::remove_dir_all(dir);
}

/// `targetLang` 缺省时由 `initialize` 兜成 `"zh"`——state 是**重建**的，
/// 不是从上次落盘读的。
#[tokio::test]
async fn dub_sf_ocr_state_defaults_target_lang() {
    let _guard = DUB_PROBE_LOCK.lock().await;
    dub_probe::reset();

    let (store, dir) = temp_store("default_lang");
    let core_store =
        create_run_store_adapter(store.clone() as Arc<dyn WorkflowExecutionStore>);
    let out = run_workflow(&RunWorkflowOptions::new(Arc::new(dub_sf_ocr().into_workflow()), core_store).input(serde_json::json!({ "videoDir": "/w/2" })))
    .await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);

    let events = events_of(&store, &out.run_id);
    assert_eq!(
        step_output(&events, "translate")["artifact"],
        "/w/2/translate/zh.json"
    );

    let _ = std::fs::remove_dir_all(dir);
}

/// `continue_from` 在**新契约**下**走不通**——这条把边界固化下来。
///
/// 搬迁时从 `InMemoryStore`（core 的旧契约 `RunStore`）换到
/// `FileExecutionStore`（`WorkflowExecutionStore`）后，这条测试**第一次红了**，
/// 暴露出一个既成事实：
///
/// | 层 | 有 `truncate_log_at_step` 吗 |
/// | --- | --- |
/// | core 的 `RunStore`（旧） | ✅ 有 —— `continue_from` 靠它 |
/// | `WorkflowExecutionStore`（新） | ❌ **没有** |
/// | 上游 TS 的 `createRunStoreAdapter` | ❌ 也没有（已核实：上游 runtime 无 `truncateRuns`） |
///
/// 适配器如实报错而不静默 no-op（`run_store_adapter.rs:451`），所以走新契约的
/// store 用不了 `continue_from`。**这是上游的既定边界，不是本 crate 的缺陷**。
///
/// 保留这条断言而不是删掉测试：哪天新契约补上截断能力，它会红，提醒我们改回来。
#[tokio::test]
async fn dub_sf_ocr_continue_from_is_unsupported_on_new_contract() {
    let _guard = DUB_PROBE_LOCK.lock().await;
    dub_probe::reset();

    let (store, dir) = temp_store("continue_from");
    let wf = dub_sf_ocr().into_workflow();
    let core_store =
        create_run_store_adapter(store.clone() as Arc<dyn WorkflowExecutionStore>);
    let out = run_workflow(
        &RunWorkflowOptions::new(Arc::new(wf.clone()), core_store.clone()).input(dub_input()),
    )
    .await
    .unwrap();
    let run_id = out.run_id.clone();

    let resumed = run_workflow(
        &RunWorkflowOptions::new(Arc::new(wf.clone()), core_store)
            .input(dub_input())
            .run_id(run_id.clone())
            .continue_from("tts"),
    )
    .await;

    let err = resumed.expect_err("新契约无 truncate，continue_from 应报错");
    let msg = err.to_string();
    assert!(
        msg.contains("truncate_log_at_step"),
        "应明确指向缺失的 truncate_log_at_step，实际：{msg}"
    );

    // 报错之后，原有的事件日志应**完好无损**（适配器报错而非静默改坏日志）。
    let events = events_of(&store, &run_id);
    for step in DUB_ALL_STEPS {
        assert_eq!(
            finished_count(&events, step),
            1,
            "{step} 的终态不该被失败的 continue_from 破坏"
        );
    }

    let _ = std::fs::remove_dir_all(dir);
}
