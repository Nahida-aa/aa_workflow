//! LocalDub `dub_sf_ocr` pipeline 的形状模型（可复用的 workflow 定义）。
//!
//! 放在 `src/` 而不是测试里，是因为它是一个**完整的示例 workflow**——别人可以
//! `use example_store_file::dub_sf_ocr` 拿去驱动自己的 store。测试在
//! `tests/dub_sf_ocr.rs`。
//!
//! 从 `examples/shared` 搬过来的（那边已删除）。
//!
//! # 它示范两件事
//!
//! 1. **两分支并行的编排**（`tokio::try_join!`）——这是 LocalDub 现阶段的串行
//!    for 循环做不到的；
//! 2. **typed state 的只读用法**（`.state::<T>()` + `initialize`），以及
//!    「写 state 必须发生在 join 之后」这条准则（见 aa-workflow 的 AGENTS.md）。
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

use aa_workflow_core::{
    BaseCtx, CreateWorkflowConfig, StepCtx, WorkflowDefinition, create_workflow,
};

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
