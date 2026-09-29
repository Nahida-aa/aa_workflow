//! `dub_sf_ocr` 在 `FileExecutionStore`（新契约）上的测试。
//!
//! workflow 定义在 `src/dub_sf_ocr.rs`——本文件只放测试。
//!
//! 驱动走 **runtime 层**（`define_workflow_runtime` + `start_run`），并通过
//! 使用者侧自定义的 [`get_workflow_runtime`] 访问器组装。与早期版本（直驱
//! core `run_workflow`）的差别：
//!
//! 1. **示范用户侧模式**：crate 不提供全局单例——store 从哪来、注册哪些
//!    workflow 是使用者的事，示例里自定义一个 `get_workflow_runtime`；
//! 2. **接 runtime 语义**：认领（lease）→ 驱动 → 收尾，含「已有非 Queued
//!    run 拒绝再驱动」的幂等边界（见 replay 测试）；
//! 3. 两分支并行形状在引擎上跑通，且并发真的发生。
//!
//! 例外：`continue_from` 的边界测试留在 adapter 直驱形态（`dub_sf_ocr_
//! continue_from_is_unsupported_on_new_contract`）——runtime 的 `start_run`
//! 没有 continue_from 入口（对齐上游），它测的是 core 层边界。

use std::collections::HashMap;
use std::sync::Arc;

use aa_workflow_core::{WorkflowEvent, RunWorkflowOptions, run_workflow};
use aa_workflow_runtime::run_store_adapter::{
    WorkflowExecutionStore, WorkflowRunStoreAdapterStore, create_run_store_adapter,
};
use aa_workflow_runtime::types::{ReadEventsArgs, StoredWorkflowEvent, WorkflowExecutionStatus};
use aa_workflow_runtime::{
    RunResult, RunResultKind, WorkflowRegistration, WorkflowRuntimeConfig,
    WorkflowRuntimeDefinition, WorkflowRuntimeStartRunArgs, define_workflow_runtime,
};
use example_store_file::dub_sf_ocr::{dub_probe, dub_sf_ocr};
use example_store_file::FileExecutionStore;

/// 序列化共享并发探针的测试（tokio 各 test 默认并行跑，探针是全局的）。
static DUB_PROBE_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

/// **使用者侧自定义**的 runtime 访问器（本示例的约定形态）：按任务目录组装
/// store + workflow 注册表并定义 runtime。
///
/// 「读或定义」的语义：`WorkflowRuntimeDefinition` 不持有执行进度状态——
/// 真源（事件日志 / lease / run 状态）在 store 里；同一 base 跨进程「读」
/// 就是重建一个 definition 指向它（跨实例重开见 `reopen_and_e2e.rs`）。
/// 简单例子不做配置源解释，每个测试用自己的临时 base 调一次。
fn get_workflow_runtime(base: &std::path::Path) -> WorkflowRuntimeDefinition {
    let store: Arc<dyn WorkflowExecutionStore> = Arc::new(FileExecutionStore::new(base));
    let mut workflows = HashMap::new();
    workflows.insert(
        "dub_sf_ocr".to_string(),
        WorkflowRegistration {
            load: Arc::new(|| dub_sf_ocr().into_workflow()),
            previous_versions: HashMap::new(),
            version_override: None,
            schedules: vec![],
        },
    );
    define_workflow_runtime(WorkflowRuntimeConfig::new(store, workflows))
}

fn temp_base(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "wf_store_file_dub_{tag}_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("建临时目录");
    dir
}

/// 落盘 store 直驱（不经 runtime）——仅 continue_from 边界测试用。
fn temp_store(tag: &str) -> (Arc<FileExecutionStore>, std::path::PathBuf) {
    let dir = temp_base(tag);
    (Arc::new(FileExecutionStore::new(&dir)), dir)
}

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

/// 用示例的 runtime 驱动一次 dub_sf_ocr（run_id 调用方定——确定性、可重放）。
async fn drive(rt: &WorkflowRuntimeDefinition, run_id: &str, input: serde_json::Value) -> RunResult {
    rt.start_run(WorkflowRuntimeStartRunArgs {
        workflow_id: "dub_sf_ocr".to_string(),
        run_id: run_id.to_string(),
        input,
        ..Default::default()
    })
    .await
    .expect("start_run")
}

/// 读回事件日志（走 store 的 `read_events`，证明确实落了盘）。
fn events_of(rt: &WorkflowRuntimeDefinition, run_id: &str) -> Vec<StoredWorkflowEvent> {
    rt.config
        .store
        .read_events(ReadEventsArgs {
            run_id: run_id.to_string(),
            from_index: None,
        })
        .expect("读事件")
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

#[tokio::test]
async fn dub_sf_ocr_runs_all_ten_steps_once() {
    let _guard = DUB_PROBE_LOCK.lock().await;
    dub_probe::reset();

    let base = temp_base("all_steps");
    let rt = get_workflow_runtime(&base);
    let out = drive(&rt, "dub-1", dub_input()).await;
    assert_eq!(out.kind, RunResultKind::Completed);

    let events = events_of(&rt, "dub-1");
    for step in DUB_ALL_STEPS {
        assert_eq!(
            finished_count(&events, step),
            1,
            "{step} 应恰好 StepFinished 一次"
        );
    }

    let _ = std::fs::remove_dir_all(base);
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

    let base = temp_base("concurrent");
    let rt = get_workflow_runtime(&base);
    let out = drive(&rt, "dub-1", dub_input()).await;
    assert_eq!(out.kind, RunResultKind::Completed);

    let peak = dub_probe::peak();
    assert!(
        peak >= 2,
        "两分支应并行（并发峰值 {peak} < 2）——若失败，说明并发 append 未被引擎吸收 \
         或 step 执行被串行化"
    );

    let _ = std::fs::remove_dir_all(base);
}

/// 同 run_id 重复 `start_run`：runtime 层的幂等边界——已存在的 run 不在
/// Queued 状态时**拒绝再驱动**（`RunResultKind::NotClaimable`），事件日志
/// 一条不增。
///
/// 注意与 core 层的重放短路的分工：`run_workflow` 同 run_id 重跑是
/// 「重放 + 逐步短路」；runtime 的 `start_run` 在其上多了一道**认领闸**，
/// 非法重入直接不进引擎。两者都不产生新的 `StepFinished`。
#[tokio::test]
async fn dub_sf_ocr_replay_short_circuits() {
    let _guard = DUB_PROBE_LOCK.lock().await;
    dub_probe::reset();

    let base = temp_base("replay");
    let rt = get_workflow_runtime(&base);
    drive(&rt, "dub-1", dub_input()).await;
    let ts_1 = sf_ts(&events_of(&rt, "dub-1"), "separate");

    let again = drive(&rt, "dub-1", dub_input()).await;
    assert_eq!(again.kind, RunResultKind::NotClaimable, "终态 run 拒绝再驱动");
    assert_eq!(
        again.run.expect("NotClaimable 应带 run").status,
        WorkflowExecutionStatus::Finished
    );

    let events = events_of(&rt, "dub-1");
    assert_eq!(sf_ts(&events, "separate"), ts_1, "checkpoint 未被触碰");
    for step in DUB_ALL_STEPS {
        assert_eq!(finished_count(&events, step), 1, "{step} 重跑不应重记");
    }

    let _ = std::fs::remove_dir_all(base);
}

/// 每个 step 都**读 typed state**：产物路径由 `state.video_dir` 派生，译文文件名
/// 带 `state.target_lang`。换个输入，10 个 step 的产物路径应全变——若哪天有人
/// 把路径写死成字面量，这条会红。
#[tokio::test]
async fn dub_sf_ocr_steps_read_typed_state() {
    let _guard = DUB_PROBE_LOCK.lock().await;
    dub_probe::reset();

    let base = temp_base("typed_state");
    let rt = get_workflow_runtime(&base);
    let out = rt
        .start_run(WorkflowRuntimeStartRunArgs {
            workflow_id: "dub_sf_ocr".to_string(),
            run_id: "dub-1".to_string(),
            input: serde_json::json!({ "videoDir": "/w/1", "targetLang": "vi" }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(out.kind, RunResultKind::Completed);

    let events = events_of(&rt, "dub-1");
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

    let _ = std::fs::remove_dir_all(base);
}

/// `targetLang` 缺省时由 `initialize` 兜成 `"zh"`——state 是**重建**的，
/// 不是从上次落盘读的。
#[tokio::test]
async fn dub_sf_ocr_state_defaults_target_lang() {
    let _guard = DUB_PROBE_LOCK.lock().await;
    dub_probe::reset();

    let base = temp_base("default_lang");
    let rt = get_workflow_runtime(&base);
    let out = drive(&rt, "dub-1", serde_json::json!({ "videoDir": "/w/2" })).await;
    assert_eq!(out.kind, RunResultKind::Completed);

    let events = events_of(&rt, "dub-1");
    assert_eq!(
        step_output(&events, "translate")["artifact"],
        "/w/2/translate/zh.json"
    );

    let _ = std::fs::remove_dir_all(base);
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
/// 这条特意留在 **adapter 直驱形态**（不经 runtime）：runtime 的 `start_run`
/// 没有 continue_from 入口（对齐上游），该边界属于 core 层。保留断言而不是删掉
/// 测试：哪天新契约补上截断能力，它会红，提醒我们改回来。
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
    let events = store
        .read_events(ReadEventsArgs {
            run_id: run_id.clone(),
            from_index: None,
        })
        .unwrap();
    for step in DUB_ALL_STEPS {
        assert_eq!(
            finished_count(&events, step),
            1,
            "{step} 的终态不该被失败的 continue_from 破坏"
        );
    }

    let _ = std::fs::remove_dir_all(dir);
}
