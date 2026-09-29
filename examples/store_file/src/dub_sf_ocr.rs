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
//! 要 `separate` 的 stems）。
//!
//! 本示例的 step **只读** `ctx.state`，而且这条约束是**类型层面**保证的：每个 step
//! 函数的 state 参数都取 `&DubSfOcrState`，签名上就没有写 state 的路径
//! （见 [`stub`] 的文档）。写入必须发生在 join 之后，且跨步骤传数据走 step 的**返回值**
//! —— 见 `aa-workflow/docs/concepts/ctx-state.md` 与 AGENTS.md 的「`ctx.state` 的
//! 写入位置准则」。
//!
//! 不是 LocalDub 的接线，只是把它的依赖图形状搬过来。

use std::sync::Arc;

use aa_workflow_core::{
    BaseCtx, CreateWorkflowConfig, StepCtx, WorkflowDefinition, create_workflow,
};

// ========================================================================
// dub_sf_ocr 的定义
// ========================================================================

/// dub-sf-ocr 输入：示例层不落盘，`videoDir` 只用于拼产物路径。
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
/// 本示例两条分支**共享**同一个 `ctx`（全程只读，见 [`dub_sf_ocr`]），所以
/// 这里描述的分裂**没有发生**。但你若让某一分支**写** state，就必须
/// `ctx.clone()` 分出去——`StateHandle::clone` 是**快照分裂**，两条分支各持
/// 一份 state 副本，互不共享：那份修改既对另一分支不可见，join 之后从原始
/// `ctx.state` 也读不回来（`Deref` 走 `&self.st`，不回查共享的 mirror）。
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

// ─── 每个 step 一个返回类型 ────────────────────────────────────────────────
//
// 之前 10 个 step 共用一个 `DubArtifact { artifact, target_lang, output_mode }`，
// 形状对每个 step 都是假的：`target_lang` / `output_mode` 只是把 state 回显一遍，
// 真正的产物差异全压在一个字符串字面量里。现在每个 step 返回**它实际产出的东西**，
// 于是 `step<T>` 的泛型才真的在做事——10 个 step 推导出 9 个不同的 `T`。
//
// `separate` 和 `separate_after` 共用 `Separated` 是如实反映：两轮都产出
// 同样的一对音轨，没有必要硬造第二个形状相同的类型。

/// `separate` / `separate_after`：分离出的人声 + 伴奏两条轨。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Separated {
    vocals: String,
    bgm: String,
}

/// `sf_ocr_pre`：subtitle-finder 抽出的关键帧。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Keyframes {
    dir: String,
    frames: u32,
}

/// `sf_ocr`：关键帧识别出的原始字幕（未校正）。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawSubtitle {
    srt: String,
    segments: u32,
}

/// `sf_ocr_fix`：校正后的字幕 + 实际改动数。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixedSubtitle {
    srt: String,
    repaired: u32,
}

/// `translate`：译文。`target_lang` 属于**这个** step 的结果（之前它出现在
/// 每个 step 的返回里，那是把 state 回显伪装成产物）。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Translated {
    path: String,
    target_lang: String,
    segments: u32,
}

/// `split_audio`：按字幕时间轴切分出的分段表。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Timings {
    path: String,
    segments: u32,
}

/// `tts`：逐段合成出的一批 wav。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TtsBatch {
    dir: String,
    wavs: u32,
}

/// `mix_audio`：人声轨被 TTS 替换后的成品音轨。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MixedAudio {
    path: String,
    ms: u64,
}

/// `mix_video`：成品视频。多带一条 `audio`——记录 mux 进去的是哪条音轨，
/// 这不是凑形状：混音出问题时这是第一个要看的字段。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MixedVideo {
    path: String,
    ms: u64,
    audio: String,
}

/// 桩 step 的公共**行为**：计并发 → 记一行 → 模拟耗时。
///
/// 只抽行为，不抽签名。每个 step 的入参（消费上游产物）和返回类型各不相同，
/// 那正是这个示例要展示的东西——把它们压成一个 `dub_step(&st, "字面量", ms)`
/// 就等于把 `step<T>` 退化回 `Value`。
///
/// 所有入参都以 `&DubSfOcrState` 打头且只取共享引用，于是「step 只读 state」
/// 这条规则由类型系统保证：`&DubSfOcrState` 没有 `DerefMut` 路径，写不进去。
/// 这与 `docs/concepts/ctx-state.md` 的结论一致——step 闭包唯一耐久的输出
/// 通道是返回值，state 不是那条通道，所以这里连类型都不给它留。
async fn stub(st: &DubSfOcrState, name: &str, millis: u64) {
    dub_probe::enter();
    tracing::info!(
        target: "examples",
        "dub-sf-ocr: {name} (mode={}, source={}, lang={})",
        st.output_mode,
        st.subtitle_source,
        st.target_lang
    );
    tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
    dub_probe::leave();
}

/// 分支 A 第 1 步：无入参产物，产出两条轨。
async fn separate(st: &DubSfOcrState) -> anyhow::Result<Separated> {
    stub(st, "separate", 50).await;
    Ok(Separated {
        vocals: format!("{}/separate/target_3_vocals.wav", st.video_dir),
        bgm: format!("{}/separate/target_bgm.wav", st.video_dir),
    })
}

/// 分支 A 第 2 步：**消费**上一轮的分轨。
async fn separate_after(st: &DubSfOcrState, prev: &Separated) -> anyhow::Result<Separated> {
    stub(st, "separate_after", 50).await;
    Ok(Separated {
        // 人声轨是上一轮的输入，所以这里能看到数据真的流过来了。
        vocals: prev.vocals.clone(),
        bgm: format!("{}/separate_after/target_bgm.wav", st.video_dir),
    })
}

/// 分支 B 第 1 步：抽关键帧。
async fn sf_ocr_pre(st: &DubSfOcrState) -> anyhow::Result<Keyframes> {
    stub(st, "sf_ocr_pre", 50).await;
    Ok(Keyframes {
        dir: format!("{}/sf_ocr_pre/frames", st.video_dir),
        frames: 12,
    })
}

/// 分支 B 第 2 步：**消费**关键帧目录，产出字幕。
async fn sf_ocr(st: &DubSfOcrState, frames: &Keyframes) -> anyhow::Result<RawSubtitle> {
    stub(st, "sf_ocr", 50).await;
    Ok(RawSubtitle {
        srt: format!("{}/sf_ocr/raw.srt", st.video_dir),
        segments: frames.frames,
    })
}

/// 分支 B 第 3 步：**消费**原始字幕，产出校正后的。
async fn sf_ocr_fix(st: &DubSfOcrState, raw: &RawSubtitle) -> anyhow::Result<FixedSubtitle> {
    stub(st, "sf_ocr_fix", 50).await;
    Ok(FixedSubtitle {
        srt: format!("{}/sf_ocr_fix/fixed.srt", st.video_dir),
        // 修复数不可能超过原始段数——`raw` 真的参与了计算。
        repaired: 2.min(raw.segments),
    })
}

/// 分支 B 第 4 步：**消费**校正后的字幕 + 读 state 的目标语言。
///
/// 这是全流程唯一正当使用 `target_lang` 的地方——译文的目标语言本来就是
/// translate 这一步的产物。之前它出现在 10 个 step 的返回里，等于把 state
/// 回显伪装成产物。
async fn translate(st: &DubSfOcrState, fixed: &FixedSubtitle) -> anyhow::Result<Translated> {
    stub(st, "translate", 50).await;
    Ok(Translated {
        path: format!("{}/translate/{}.json", st.video_dir, st.target_lang),
        target_lang: st.target_lang.clone(),
        segments: fixed.repaired + 1,
    })
}

/// 汇聚第 1 步：**同时消费两支**——分支 A 的人声轨 + 分支 B 的译文。
///
/// 之前这条注释写着「split_audio 吃 A 的人声 + B 的译文」，但签名里两者都没接。
async fn split_audio(
    st: &DubSfOcrState,
    vocals: &Separated,
    translated: &Translated,
) -> anyhow::Result<Timings> {
    stub(st, "split_audio", 50).await;
    let path = format!("{}/split_audio/timings.json", st.video_dir);
    tracing::debug!(
        target: "examples",
        "按 {} 段译文切分 {}", translated.segments, vocals.vocals
    );
    Ok(Timings {
        path,
        segments: translated.segments,
    })
}

/// 汇聚第 2 步：**消费**分段表，产出逐段 wav。
async fn tts(st: &DubSfOcrState, timings: &Timings) -> anyhow::Result<TtsBatch> {
    stub(st, "tts", 50).await;
    Ok(TtsBatch {
        dir: format!("{}/tts/wavs", st.video_dir),
        wavs: timings.segments,
    })
}

/// 汇聚第 3 步：**消费**人声轨 + TTS 批次。
async fn mix_audio(
    st: &DubSfOcrState,
    vocals: &Separated,
    tts: &TtsBatch,
) -> anyhow::Result<MixedAudio> {
    stub(st, "mix_audio", 50).await;
    let path = format!("{}/mix_audio/audio_dubbing.wav", st.video_dir);
    tracing::debug!(target: "examples", "用 {} 覆盖 {} 段 TTS", vocals.vocals, tts.wavs);
    Ok(MixedAudio {
        path,
        ms: 1_000 + tts.wavs as u64 * 120,
    })
}

/// 汇聚第 4 步：**消费**伴奏轨 + 成品音轨，产出成品视频。
///
/// 之前这里写的是字面量 `"mix_video/dub_sf_ocr/{id}.mp4"`——那既不是
/// format string，也没有 `id` 这个变量，是个从 LocalDub 抄过来忘了改的壳。
async fn mix_video(
    st: &DubSfOcrState,
    separated: &Separated,
    audio: &MixedAudio,
) -> anyhow::Result<MixedVideo> {
    stub(st, "mix_video", 50).await;
    let path = format!("{}/mix_video/dub_sf_ocr.mp4", st.video_dir);
    tracing::debug!(target: "examples", "{} 叠 {} -> {}", separated.bgm, audio.path, path);
    Ok(MixedVideo {
        path,
        ms: audio.ms,
        audio: audio.path.clone(),
    })
}

/// LocalDub `dub_sf_ocr` pipeline 的形状：**两分支并行 → split_audio 汇聚**。
///
/// 两条分支**共享同一个 `&ctx`**，不 `clone()`：`step(&self)` 只拿共享借用，
/// 并发的 `&ctx` 是合法的（只有某一分支要**写** state 时，`DerefMut` 才会
/// 需要 `&mut ctx` 而与另一分支的 `&ctx` 冲突——那时才必须 `ctx.clone()`）。
/// 代价是分支块要用 `async` 而非 `async move`：后者会按值捕获 `ctx`。
///
/// 只读，所以不存在 [`DubSfOcrState`] 说的快照分裂问题。
pub fn dub_sf_ocr() -> WorkflowDefinition<DubSfOcrInput, serde_json::Value, DubSfOcrState> {
    create_workflow(
        CreateWorkflowConfig::new("dub-sf-ocr")
            .input::<DubSfOcrInput>()
            .state::<DubSfOcrState>()
            // state 不落盘：每次 start / resume 都从这里重建。
            .initialize(|input: &DubSfOcrInput| {
                Ok(DubSfOcrState {
                    video_dir: input.video_dir.clone(),
                    target_lang: input
                        .target_lang
                        .clone()
                        .unwrap_or_else(|| "zh".to_string()),
                    output_mode: "dub".to_string(),
                    subtitle_source: "sf_ocr".to_string(),
                })
            }),
    )
    .handler(|ctx: BaseCtx<DubSfOcrInput, DubSfOcrState>| async move {
        let video_dir = ctx.input.video_dir.clone();

        // 只读快照，clone 一次即可。
        //
        // step 闭包的 bound 是 `FnOnce(StepCtx) -> Fut + Clone + Send + 'static`，
        // `'static` 只禁止**借用** `ctx`，不禁止共享所有权。所以每个 step 各写
        // 一遍 `(*ctx.state).clone()` 是 10 次无谓的深拷贝（每次 4 个 String）。
        //
        // ⚠️ 成立前提：本 workflow 在 `initialize` 之后不再写 state。若中途有写，
        // 早先取的那份快照会变陈旧，必须在写之后重新取。
        let state = Arc::new((*ctx.state).clone());

        // 两条分支共享同一个 `&ctx`——`step(&self)` 只拿共享借用，并发的 `&ctx`
        // 合法，不需要 `ctx.clone()`。分支块用 `async`（非 `async move`）：
        // `move` 会试图按值捕获 `ctx`，两个块各抢一份就冲突了。
        //
        // `try_join!` 返回 `Result<(A, B), E>`——A / B **不需要**同类型（只有
        // error 类型要统一），所以这里 A 是 `Separated`、B 是 `Translated`。
        let (separated, translated) = tokio::try_join!(
            async {
                // 分支 A：分离人声 + 伴奏。
                let st = Arc::clone(&state);
                let sep = ctx
                    .step("separate", move |_sc: StepCtx| async move { separate(&st).await })
                    .await?;
                let st = Arc::clone(&state);
                ctx.step("separate_after", move |_sc: StepCtx| {
                    // 闭包是 `FnOnce + Clone`，重试时会被调用多次，所以上游产物在
                    // 闭包**体内**克隆（每次 attempt 一份），不能在外面 move 走。
                    let prev = sep.clone();
                    async move { separate_after(&st, &prev).await }
                })
                .await
            },
            async {
                // 分支 B：抽关键帧 → OCR → 校正 → 翻译，四步串成一条链。
                let st = Arc::clone(&state);
                let frames = ctx
                    .step("sf_ocr_pre", move |_sc: StepCtx| async move { sf_ocr_pre(&st).await })
                    .await?;
                let st = Arc::clone(&state);
                let raw = ctx
                    .step("sf_ocr", move |_sc: StepCtx| {
                        let prev = frames.clone();
                        async move { sf_ocr(&st, &prev).await }
                    })
                    .await?;
                let st = Arc::clone(&state);
                let fixed = ctx
                    .step("sf_ocr_fix", move |_sc: StepCtx| {
                        let prev = raw.clone();
                        async move { sf_ocr_fix(&st, &prev).await }
                    })
                    .await?;
                let st = Arc::clone(&state);
                ctx.step("translate", move |_sc: StepCtx| {
                    let prev = fixed.clone();
                    async move { translate(&st, &prev).await }
                })
                .await
            },
        )?;

        // workflow 出口要用的副本先留好（下面会把原件 move 进闭包）。克隆的都是
        // 几个 String，不是深拷贝 state。
        let out_separated = separated.clone();
        let out_translated = translated.clone();

        // 汇聚：split_audio 同时吃两支的产物。`separated` 后面还有两个 step 要用。
        let split_input = separated.clone();
        let st = Arc::clone(&state);
        let timings = ctx
            .step("split_audio", move |_sc: StepCtx| {
                let (sep, tr) = (split_input.clone(), translated.clone());
                async move { split_audio(&st, &sep, &tr).await }
            })
            .await?;
        let out_timings = timings.clone();

        let st = Arc::clone(&state);
        let tts = ctx
            .step("tts", move |_sc: StepCtx| {
                let prev = timings.clone();
                async move { tts(&st, &prev).await }
            })
            .await?;
        let out_tts = tts.clone();

        let split_audio_input = separated.clone();
        let st = Arc::clone(&state);
        let mixed_audio = ctx
            .step("mix_audio", move |_sc: StepCtx| {
                let (sep, wavs) = (split_audio_input.clone(), tts.clone());
                async move { mix_audio(&st, &sep, &wavs).await }
            })
            .await?;

        let out_mixed_audio = mixed_audio.clone();

        let st = Arc::clone(&state);
        let mix_video = ctx
            .step("mix_video", move |_sc: StepCtx| {
                let (sep, audio) = (separated.clone(), mixed_audio.clone());
                async move { mix_video(&st, &sep, &audio).await }
            })
            .await?;

        // workflow **出口**仍是 `serde_json::Value`（store 是 `dyn`，装不下泛型）
        // ——那是 `step<T>` 之外的另一回事，见 docs/quick-start.md 的差异速览。
        Ok(serde_json::json!({
            "videoDir": video_dir,
            "pipeline": "dub_sf_ocr",
            "separated": out_separated,
            "translated": out_translated,
            "timings": out_timings,
            "tts": out_tts,
            "mixAudio": out_mixed_audio,
            "mixVideo": mix_video,
        }))
    })
}
