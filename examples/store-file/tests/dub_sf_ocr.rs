//! LocalDub `dub_sf_ocr` pipeline 的形状模型，跑在 **`FileExecutionStore`** 上。
//!
//! 从 `examples/shared` 搬过来的（那边已删除）。搬过来时做了**一处实质改动**：
//! 驱动 store 从 `InMemoryStore`（core 的旧契约 `RunStore`）换成
//! [`FileExecutionStore`]（新契约 `WorkflowExecutionStore`，经
//! `create_run_store_adapter` 降格给 `run_workflow`）。
//!
//! 这让这批测试同时验两件事：
//!
//! 1. **dub_sf_ocr 的两分支并行形状**能在引擎上跑通，且并发真的发生；
//! 2. 这些行为在**落盘** store 上也成立（不只是内存）。
//!
//! # 形状
//!
//! ```text
//!              ┌─ separate ──► separate_after ─────────────────► (bgm)
//! import ──────┤                                                   │
//!              └─ sf_ocr_pre ► sf_ocr ► sf_ocr_fix ► translate ─┐  │
//!                                                               ▼  ▼
//!                                split_audio ◄──────────────────┘  │
//!                                    │                             │
//!                                tts ► mix_audio ► mix_video ◄─────┘
//! ```
//!
//! 两分支**零跨分支读**（A 只吃 `audio_source.wav`，B 只吃 `video_source.mp4`），
//! 所以 `try_join!` 是安全的；分支内部仍是词法 `.await` 串行（`separate_after`
//! 要 `separate` 的 stems）。本示例的 step **只读** `ctx.state`——写入必须发生在
//! join 之后（见 `aa-workflow/AGENTS.md` 的「`ctx.state` 的写入位置准则」）。
//!
//! 不是 LocalDub 的接线，只是把它的依赖图形状搬过来。

use std::sync::Arc;

use workflow_core::{
    BaseCtx, CreateWorkflowConfig, RunOptions, RunStatus, StepCtx, WorkflowDefinition,
    WorkflowEvent, create_workflow, run_workflow,
};
use workflow_runtime::run_store_adapter::{
    WorkflowExecutionStore, WorkflowRunStoreAdapterStore, create_run_store_adapter,
};
use workflow_runtime::types::{ReadEventsArgs, StoredWorkflowEvent};
use workflow_store_file::FileExecutionStore;

// ========================================================================
// dub_sf_ocr 的定义
// ========================================================================

/// dub-sf-ocr 输入：示例层不落盘，`videoDir` 只用于拼产物路径。
///
/// 派生 `Clone` 是**并行分支所需**：`BaseCtx<TInput>: Clone` 要求 `TInput: Clone`，
/// 两条 `try_join!` 分支要各持一份 `ctx.clone()`。
#[derive(serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DubSfOcrInput {
    pub video_dir: String,
    /// 目标语言；缺省由 `initialize` 兜成 `"zh"`。
    #[serde(default)]
    pub target_lang: Option<String>,
}

/// dub-sf-ocr 的 **typed state**（Rust 版 zod `stateSchema`）。
///
/// 每个 step 都**读带类型的 state**，而不是从 `serde_json::Value` 里 `.get()` 链取值。
/// `.state::<T>()` 声明契约后，handler 的 `ctx.state` 就是 `DubSfOcrState`，
/// 字段直接访问、编译期校验。
///
/// state **不落盘**：每次 start / resume 都由 `initialize(input)` 重建，handler
/// 从头重跑（与 TanStack 一致）。
///
/// ## 与并行分支的交互（重要）
///
/// `ctx.clone()` 是**快照分裂**——两条 `try_join!` 分支各持一份 state 副本，
/// 互不共享。本示例的 step 只**读** state，所以分裂无害；若要在 step 闭包里**写**
/// state，那条修改既对另一分支不可见，也会在 replay 时随闭包短路而丢失。
/// 需要跨分支可见的可变状态，只能走 step 的**返回值**（durable 结果）。
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DubSfOcrState {
    pub video_dir: String,
    pub target_lang: String,
    pub output_mode: String,
    pub subtitle_source: String,
}

/// 并发探针：记录同时在飞的 step 数峰值。
///
/// 放在模块级是因为 step 闭包要 `Clone + Send + 'static`，没法把可变引用借进去。
pub mod dub_probe {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);

    /// 进入一个 step。
    pub fn enter() {
        let n = IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
        PEAK.fetch_max(n, Ordering::SeqCst);
    }

    /// 离开一个 step。
    pub fn leave() {
        IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    }

    /// 峰值并发数（两分支并行时应 ≥ 2）。
    pub fn peak() -> usize {
        PEAK.load(Ordering::SeqCst)
    }

    /// 复位（测试用）。
    pub fn reset() {
        IN_FLIGHT.store(0, Ordering::SeqCst);
        PEAK.store(0, Ordering::SeqCst);
    }
}

/// 桩 step：**从 typed state 读**字段，拼出它「产出」的产物路径 →
/// 记录 → 计并发 → 模拟耗时。
///
/// 产物路径**由 state 派生**（而不是硬编码字面量），这正是「每个 step 都读带类型
/// 的 state」的落点：换个 `videoDir` / `targetLang`，10 个 step 的产物路径全变。
///
/// 不落盘、不读 LocalDub（示例层强约束）。
async fn dub_step(
    st: DubSfOcrState,
    artifact: &str,
    millis: u64,
) -> anyhow::Result<serde_json::Value> {
    dub_probe::enter();
    let path = format!("{}/{}", st.video_dir, artifact);
    tracing::info!(
        target: "examples",
        "dub-sf-ocr: {path} (mode={}, source={}, lang={})",
        st.output_mode,
        st.subtitle_source,
        st.target_lang
    );
    tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
    dub_probe::leave();
    Ok(serde_json::json!({
        "artifact": path,
        "targetLang": st.target_lang,
        "outputMode": st.output_mode,
    }))
}

/// 每个桩 step 的模拟耗时。两分支并行跑完的总时长 ≈ `max(A, B) + 尾部`，
/// 明显短于 10 步串行。
const DUB_STEP_MS: u64 = 50;

/// LocalDub `dub_sf_ocr` pipeline 的形状：**两分支并行 → split_audio 汇聚**。
///
/// 分支各持 `ctx.clone()`——`step(&self)` 借用 ctx，两个 async 块不能同时 move
/// 同一个 ctx。注意 clone 会**连带分裂 state 快照**（见 [`DubSfOcrState`]），
/// 本示例 step 只读 state，故无影响。
pub fn dub_sf_ocr() -> WorkflowDefinition<DubSfOcrInput, serde_json::Value, DubSfOcrState> {
    create_workflow(
        CreateWorkflowConfig::new("dub-sf-ocr")
            .input::<DubSfOcrInput>()
            .state::<DubSfOcrState>()
            // state 不落盘：每次 start / resume 都从这里重建。
            .initialize(|input| {
                let video_dir = input["videoDir"].as_str().unwrap_or_default().to_string();
                let target_lang = input["targetLang"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("zh")
                    .to_string();
                Ok(serde_json::json!({
                    "videoDir": video_dir,
                    "targetLang": target_lang,
                    "outputMode": "dub",
                    "subtitleSource": "sf_ocr",
                }))
            }),
    )
    .handler(|ctx: BaseCtx<DubSfOcrInput, DubSfOcrState>| async move {
        let video_dir = ctx.input.video_dir.clone();

        let ctx_a = ctx.clone(); // 分支 A：音源分离
        let ctx_b = ctx.clone(); // 分支 B：OCR 提字 + 翻译

        let (branch_a, branch_b) = tokio::try_join!(
            async move {
                let st: DubSfOcrState = (*ctx_a.state).clone();
                let vocals = ctx_a
                    .step("separate", move |_sc: StepCtx| {
                        let st = st.clone();
                        async move { dub_step(st, "separate/target_3_vocals.wav", DUB_STEP_MS).await }
                    })
                    .await?;
                let st: DubSfOcrState = (*ctx_a.state).clone();
                let bgm = ctx_a
                    .step("separate_after", move |_sc: StepCtx| {
                        let st = st.clone();
                        async move {
                            dub_step(st, "separate_after/target_bgm.wav", DUB_STEP_MS).await
                        }
                    })
                    .await?;
                Ok::<_, anyhow::Error>(serde_json::json!({ "vocals": vocals, "bgm": bgm }))
            },
            async move {
                let st: DubSfOcrState = (*ctx_b.state).clone();
                ctx_b
                    .step("sf_ocr_pre", move |_sc: StepCtx| {
                        let st = st.clone();
                        async move { dub_step(st, "sf_ocr_pre/frames", DUB_STEP_MS).await }
                    })
                    .await?;
                let st: DubSfOcrState = (*ctx_b.state).clone();
                ctx_b
                    .step("sf_ocr", move |_sc: StepCtx| {
                        let st = st.clone();
                        async move { dub_step(st, "sf_ocr/frames.json", DUB_STEP_MS).await }
                    })
                    .await?;
                let st: DubSfOcrState = (*ctx_b.state).clone();
                ctx_b
                    .step("sf_ocr_fix", move |_sc: StepCtx| {
                        let st = st.clone();
                        async move { dub_step(st, "sf_ocr_fix/srt.json", DUB_STEP_MS).await }
                    })
                    .await?;
                let st: DubSfOcrState = (*ctx_b.state).clone();
                let translated = ctx_b
                    .step("translate", move |_sc: StepCtx| {
                        let st = st.clone();
                        // 译文文件名带 state 里的目标语言——typed state 真的被用到。
                        async move {
                            let name = format!("translate/{}.json", st.target_lang);
                            dub_step(st, &name, DUB_STEP_MS).await
                        }
                    })
                    .await?;
                Ok::<_, anyhow::Error>(translated)
            },
        )?;

        // 汇聚：split_audio 吃 A 的人声 + B 的译文。
        let st: DubSfOcrState = (*ctx.state).clone();
        let split = ctx
            .step("split_audio", move |_sc: StepCtx| {
                let st = st.clone();
                async move { dub_step(st, "split_audio/timings.json", DUB_STEP_MS).await }
            })
            .await?;
        let st: DubSfOcrState = (*ctx.state).clone();
        let tts = ctx
            .step("tts", move |_sc: StepCtx| {
                let st = st.clone();
                async move { dub_step(st, "tts/wavs", DUB_STEP_MS).await }
            })
            .await?;
        let st: DubSfOcrState = (*ctx.state).clone();
        let mix_audio = ctx
            .step("mix_audio", move |_sc: StepCtx| {
                let st = st.clone();
                async move { dub_step(st, "mix_audio/audio_dubbing.wav", DUB_STEP_MS).await }
            })
            .await?;
        let st: DubSfOcrState = (*ctx.state).clone();
        let mix_video = ctx
            .step("mix_video", move |_sc: StepCtx| {
                // 这里才用上分支 A 的 bgm。
                let st = st.clone();
                async move { dub_step(st, "mix_video/dub_sf_ocr/{id}.mp4", DUB_STEP_MS).await }
            })
            .await?;

        Ok(serde_json::json!({
            "videoDir": video_dir,
            "pipeline": "dub_sf_ocr",
            "vocals": branch_a["vocals"],
            "bgm": branch_a["bgm"],
            "translated": branch_b,
            "splitAudio": split,
            "tts": tts,
            "mixAudio": mix_audio,
            "mixVideo": mix_video,
        }))
    })
}

// ========================================================================
// 测试
// ========================================================================

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
    let out = run_workflow(
        &dub_sf_ocr(),
        core_store,
        &RunOptions::new(dub_input()),
        None,
    )
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
    let out = run_workflow(
        &dub_sf_ocr(),
        core_store,
        &RunOptions::new(dub_input()),
        None,
    )
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
    let wf = dub_sf_ocr();
    let core_store =
        create_run_store_adapter(store.clone() as Arc<dyn WorkflowExecutionStore>);
    let out = run_workflow(&wf, core_store.clone(), &RunOptions::new(dub_input()), None)
        .await
        .unwrap();
    let run_id = out.run_id.clone();
    let ts_1 = sf_ts(&events_of(&store, &run_id), "separate");

    let again = run_workflow(
        &wf,
        core_store,
        &RunOptions::new(dub_input()).run_id(run_id.clone()),
        None,
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
    let out = run_workflow(
        &dub_sf_ocr(),
        core_store,
        &RunOptions::new(serde_json::json!({ "videoDir": "/w/1", "targetLang": "vi" })),
        None,
    )
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
    let out = run_workflow(
        &dub_sf_ocr(),
        core_store,
        &RunOptions::new(serde_json::json!({ "videoDir": "/w/2" })),
        None,
    )
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
/// | 层 | 有 `truncate_runs` 吗 |
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
    let wf = dub_sf_ocr();
    let core_store =
        create_run_store_adapter(store.clone() as Arc<dyn WorkflowExecutionStore>);
    let out = run_workflow(&wf, core_store.clone(), &RunOptions::new(dub_input()), None)
        .await
        .unwrap();
    let run_id = out.run_id.clone();

    let resumed = run_workflow(
        &wf,
        core_store,
        &RunOptions::new(dub_input())
            .run_id(run_id.clone())
            .continue_from("tts"),
        None,
    )
    .await;

    let err = resumed.expect_err("新契约无 truncate，continue_from 应报错");
    let msg = err.to_string();
    assert!(
        msg.contains("truncate_runs"),
        "应明确指向缺失的 truncate_runs，实际：{msg}"
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
