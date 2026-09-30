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

use aa_workflow_core::{RunStatus, RunWorkflowOptions, WorkflowEvent, run_workflow};
use aa_workflow_runtime::run_store_adapter::{WorkflowExecutionStore, WorkflowRunStoreAdapterStore, create_run_store_adapter};
use aa_workflow_runtime::types::{ReadEventsArgs, StoredWorkflowEvent, WorkflowExecutionStatus};
use aa_workflow_runtime::{RunResult, RunResultKind, WorkflowRegistration, WorkflowRuntimeConfig, WorkflowRuntimeDefinition, WorkflowRuntimeStartRunArgs, define_workflow_runtime};
use example_store_file::dub_sf_ocr::{dub_probe, dub_sf_ocr};
use example_store_file::FileExecutionStore;

/// 序列化共享并发探针的测试（tokio 各 test 默认并行跑，探针是全局的）。
static DUB_PROBE_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

/// **使用者侧自定义**的 runtime 访问器：get-or-define。
///
/// 「读」：同 base 已定义过（同进程）→ 直接复用**同一个实例**——共享实例
/// 就是共享同一把 store 锁（`FileExecutionStore` 刻意不 Clone，克隆会破坏
/// 「单进程串行化」语义），这不是便利是要求；
/// 「定义」：首次遇到该 base → 组装 store + workflow 注册表 →
/// `define_workflow_runtime`。
///
/// 跨进程的「读」不在此函数内：definition 不持有执行进度状态，真源（事件
/// 日志 / lease / run 状态）在 store 里——另一进程对同一 base 重建一个
/// definition 指向它即可（跨实例重开见 `reopen_and_e2e.rs`）。
fn get_workflow_runtime(base: &std::path::Path) -> Arc<WorkflowRuntimeDefinition> {
    static RUNTIMES: std::sync::OnceLock<
        std::sync::Mutex<HashMap<std::path::PathBuf, Arc<WorkflowRuntimeDefinition>>>,
    > = std::sync::OnceLock::new();
    let cache = RUNTIMES.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut cache = cache.lock().unwrap();
    cache
        .entry(base.to_path_buf())
        .or_insert_with(|| {
            let store: Arc<dyn WorkflowExecutionStore> = Arc::new(FileExecutionStore::new(base));
            let mut workflows = HashMap::new();
            workflows.insert(
                "dub_sf_ocr".to_string(),
                WorkflowRegistration {
                    load: Arc::new(|| dub_sf_ocr().into()),
                    previous_versions: HashMap::new(),
                    version: None,
                    schedules: vec![],
                },
            );
            Arc::new(define_workflow_runtime(WorkflowRuntimeConfig::new(store, workflows)))
        })
        .clone()
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

    // get 的「读」半边：同 base 再取 → 同一实例（复用 store 锁，不重建）。
    let rt_again = get_workflow_runtime(&base);
    assert!(Arc::ptr_eq(&rt, &rt_again), "同 base 再 get 应复用同一实例");

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

/// 每个 step 有**自己的**返回形状，且路径由 typed state 派生。
///
/// 这条以前是个循环，断言 10 个 step 的结果都有 `["artifact"]` 和
/// `["targetLang"]`——那是在强制一个假形状：`target_lang` 出现在每个 step 的
/// 返回里，只是把 state 回显伪装成产物，而真正的产物差异全压在一个字符串
/// 字面量里。现在逐步骤钉死完整 JSON。
///
/// 换句话说：这张表是「step 返回值形状」的唯一真源，谁改了某个 step 产出什么，
/// 这里立刻红。
#[tokio::test]
async fn dub_sf_ocr_each_step_has_its_own_shape() {
    let _guard = DUB_PROBE_LOCK.lock().await;
    dub_probe::reset();

    let base = temp_base("shapes");
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

    // 10 个 step → 10 份互不相同的产物。`sf_ocr_pre` 的 frames 往下传到
    // `sf_ocr`，`tts` 的 wavs 决定 `mix_audio` 的时长——所以这张表同时也是
    // 「数据真的在 step 之间流动」的证据，而不只是形状快照。
    let expected: [(&str, serde_json::Value); 10] = [
        (
            "separate",
            serde_json::json!({
                "vocals": "/w/1/separate/target_3_vocals.wav",
                "bgm": "/w/1/separate/target_bgm.wav",
            }),
        ),
        (
            "separate_after",
            serde_json::json!({
                // 人声轨原样来自 separate 的产物
                "vocals": "/w/1/separate/target_3_vocals.wav",
                "bgm": "/w/1/separate_after/target_bgm.wav",
            }),
        ),
        (
            "sf_ocr_pre",
            serde_json::json!({ "dir": "/w/1/sf_ocr_pre/frames", "frames": 12 }),
        ),
        (
            "sf_ocr",
            // segments 来自 sf_ocr_pre 的 frames
            serde_json::json!({ "srt": "/w/1/sf_ocr/raw.srt", "segments": 12 }),
        ),
        (
            "sf_ocr_fix",
            serde_json::json!({ "srt": "/w/1/sf_ocr_fix/fixed.srt", "repaired": 2 }),
        ),
        (
            "translate",
            // path 带 state 的目标语言；segments 来自 sf_ocr_fix 的 repaired
            serde_json::json!({
                "path": "/w/1/translate/vi.json",
                "targetLang": "vi",
                "segments": 3,
            }),
        ),
        (
            "split_audio",
            serde_json::json!({ "path": "/w/1/split_audio/timings.json", "segments": 3 }),
        ),
        (
            "tts",
            // wavs 来自 split_audio 的 segments
            serde_json::json!({ "dir": "/w/1/tts/wavs", "wavs": 3 }),
        ),
        (
            "mix_audio",
            // ms = 1000 + wavs * 120
            serde_json::json!({ "path": "/w/1/mix_audio/audio_dubbing.wav", "ms": 1360 }),
        ),
        (
            "mix_video",
            // ms 原样来自 mix_audio
            serde_json::json!({
                "path": "/w/1/mix_video/dub_sf_ocr.mp4",
                "ms": 1360,
                // 记录 mux 进去的是哪条音轨
                "audio": "/w/1/mix_audio/audio_dubbing.wav",
            }),
        ),
    ];

    let events = events_of(&rt, "dub-1");
    for (step, want) in expected {
        assert_eq!(
            step_output(&events, step),
            want,
            "{step} 的产物形状/内容不符"
        );
    }

    let _ = std::fs::remove_dir_all(base);
}

/// 10 个 step 的产物形状**互不相同**——不是同一个类型换了个字符串。
///
/// 唯一的例外是 `separate` / `separate_after`：两轮都产出一对音轨，同形状是
/// 如实如此，不该硬造第二个形状相同的类型。这条例外显式写在这里，而不是靠
/// 「碰巧不撞」蒙混。
#[tokio::test]
async fn dub_sf_ocr_step_shapes_are_distinct() {
    let _guard = DUB_PROBE_LOCK.lock().await;
    dub_probe::reset();

    let base = temp_base("distinct");
    let rt = get_workflow_runtime(&base);
    drive(&rt, "dub-1", dub_input()).await;

    let events = events_of(&rt, "dub-1");
    // 只看 key 集合（值不同是正常的），按 step 名索引。
    let fields: HashMap<&str, Vec<String>> = DUB_ALL_STEPS
        .iter()
        .map(|s| {
            let v = step_output(&events, s);
            let obj = v.as_object().unwrap_or_else(|| panic!("{s} 的产物应是对象，实际 {v}"));
            let mut keys: Vec<String> = obj.keys().cloned().collect();
            keys.sort();
            (*s, keys)
        })
        .collect();

    for (i, a) in DUB_ALL_STEPS.iter().enumerate() {
        for b in &DUB_ALL_STEPS[i + 1..] {
            let both_separate = matches!(*a, "separate" | "separate_after")
                && matches!(*b, "separate" | "separate_after");
            assert!(
                both_separate || fields[a] != fields[b],
                "{a} 与 {b} 的产物形状不该相同：{:?} vs {:?}",
                fields[a],
                fields[b]
            );
        }
    }

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
    let tr = step_output(&events, "translate");
    assert_eq!(tr["path"], "/w/2/translate/zh.json");
    assert_eq!(tr["targetLang"], "zh");

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
    let wf = dub_sf_ocr();
    let core_store =
        create_run_store_adapter(store.clone() as Arc<dyn WorkflowExecutionStore>);
    let out = run_workflow(
        &RunWorkflowOptions::new(Arc::new(wf.clone()), core_store.clone()).input(dub_input()),
    )
    .outcome().await
    .unwrap();
    let run_id = out.run_id.clone();

    let resumed = run_workflow(
        &RunWorkflowOptions::new(Arc::new(wf.clone()), core_store)
            .input(dub_input())
            .run_id(run_id.clone())
            .continue_from("tts"),
    )
    .outcome().await
    .unwrap();

    // 失败是**事件**（`RUN_ERRORED`）而不是 `Err`——run_workflow 的流没有 Err
    // 变体，对齐上游 `drive().catch()` 统一 emit `RUN_ERRORED`。
    // 状态也从 store 读，所以这条 drive 的落盘状态必须是 `Errored`，不能还留着
    // 上一次的 `Finished`。
    assert_eq!(
        resumed.status,
        RunStatus::Errored,
        "截断失败应把 run 标成 Errored，而不是留下上次的 Finished"
    );
    let msg = resumed.error.as_ref().expect("Errored 必须带 error").to_string();
    assert!(
        msg.contains("truncate_log_at_step"),
        "应明确指向缺失的 truncate_log_at_step，实际：{msg}"
    );
    let run_id = resumed.run_id.clone();

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
