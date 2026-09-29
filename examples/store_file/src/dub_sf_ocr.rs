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
//! 本示例的 step **只读** `ctx.state`，而且这条约束是**类型层面**保证的：step 桩函数
//! [`dub_step`] 的 state 参数取 `&DubSfOcrState`，签名上就没有写 state 的路径
//! （详见该函数的文档）。写入必须发生在 join 之后，且跨步骤传数据走 step 的**返回值**
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

/// 每个桩 step 的产物形状。**具名**是为了让「各 step 返回什么」在代码里可查，
/// 而不是散在一堆内联 `json!` 里。
///
/// ⚠️ 但要清楚：core 的 `step` 签名把返回类型固定成 `serde_json::Value`
/// （`define/mod.rs:182`），而上游 TS 是 `step<T>` 保留泛型参数。所以这个结构体在
/// `step()` 边界上仍会被 `to_value` 抹平，handler 侧拿到的还是 `Value`
/// （`branch_a["vocals"]` 那种无类型下标）。这是 core 相对上游的一处退化，
/// 示例层修不了 —— 要修得给 `step` 加泛型返回值。
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct DubArtifact {
    artifact: String,
    target_lang: String,
    output_mode: String,
}

/// 桩 step：**从 typed state 读**字段，拼出它「产出」的产物路径 →
/// 记录 → 计并发 → 模拟耗时。
///
/// `st` 取**共享引用**而非按值，这样「step 只读 state」这条规则由类型系统保证 ——
/// `dub_step` 在签名层面就**无法**写 state（`&DubSfOcrState` 没有 `DerefMut` 路径）。
/// 这与 `docs/concepts/ctx-state.md` 的结论一致：step 闭包的唯一耐久输出通道是
/// **返回值**，state 不是那条通道，所以这里连类型都不给它留。
///
/// 产物路径**由 state 派生**（而不是硬编码字面量），这正是「每个 step 都读带类型
/// 的 state」的落点：换个 `videoDir` / `targetLang`，10 个 step 的产物路径全变。
///
/// 不落盘、不读 LocalDub（示例层强约束）。
async fn dub_step(
    st: &DubSfOcrState,
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
    Ok(serde_json::to_value(DubArtifact {
        artifact: path,
        target_lang: st.target_lang.clone(),
        output_mode: st.output_mode.clone(),
    })?)
}

/// 每个桩 step 的模拟耗时。两分支并行跑完的总时长 ≈ `max(A, B) + 尾部`，
/// 明显短于 10 步串行。
const DUB_STEP_MS: u64 = 50;

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
        // step 闭包的 bound 是 `FnOnce(StepCtx) -> Fut + Clone + Send + 'static`
        // （`define/mod.rs:184`），`'static` 只禁止**借用** `ctx`，不禁止共享所有权。
        // 所以每个 step 各写一遍 `(*ctx.state).clone()` 是不必要的 —— 那是 10 次
        // 深拷贝（每次 4 个 String）。改成 `Arc` 后：1 次深拷贝 + 10 次引用计数 +1。
        //
        // ⚠️ 这个写法成立的前提是**本 workflow 在 `initialize` 之后不再写 state**。
        // 若中途有写，早先取的那份快照会变陈旧，必须改成在写之后重新取。
        let state = Arc::new((*ctx.state).clone());

        // 两条分支共享同一个 `&ctx`——`step(&self)` 只拿共享借用，并发的
        // `&ctx` 合法，不需要 `ctx.clone()`。分支块用 `async`（非 `async move`）：
        // `move` 会试图按值捕获 `ctx`，两个块各抢一份就冲突了。
        let (branch_a, branch_b) = tokio::try_join!(
            async {
                // 分支 A：音源分离
                let st = Arc::clone(&state);
                let vocals = ctx
                    .step("separate", move |_sc: StepCtx| async move {
                        dub_step(&st, "separate/target_3_vocals.wav", DUB_STEP_MS).await
                    })
                    .await?;
                let st = Arc::clone(&state);
                let bgm = ctx
                    .step("separate_after", move |_sc: StepCtx| async move {
                        dub_step(&st, "separate_after/target_bgm.wav", DUB_STEP_MS).await
                    })
                    .await?;
                Ok::<_, anyhow::Error>(serde_json::json!({ "vocals": vocals, "bgm": bgm }))
            },
            async {
                // 分支 B：OCR 提字 + 翻译
                let st = Arc::clone(&state);
                ctx.step("sf_ocr_pre", move |_sc: StepCtx| async move {
                    dub_step(&st, "sf_ocr_pre/frames", DUB_STEP_MS).await
                })
                .await?;
                let st = Arc::clone(&state);
                ctx.step("sf_ocr", move |_sc: StepCtx| async move {
                    dub_step(&st, "sf_ocr/frames.json", DUB_STEP_MS).await
                })
                .await?;
                let st = Arc::clone(&state);
                ctx.step("sf_ocr_fix", move |_sc: StepCtx| async move {
                    dub_step(&st, "sf_ocr_fix/srt.json", DUB_STEP_MS).await
                })
                .await?;
                let st = Arc::clone(&state);
                let translated = ctx
                    .step("translate", move |_sc: StepCtx| async move {
                        // 译文文件名带 state 里的目标语言——typed state 真的被用到。
                        let name = format!("translate/{}.json", st.target_lang);
                        dub_step(&st, &name, DUB_STEP_MS).await
                    })
                    .await?;
                Ok::<_, anyhow::Error>(translated)
            },
        )?;

        // 汇聚：split_audio 吃 A 的人声 + B 的译文。
        let st = Arc::clone(&state);
        let split = ctx
            .step("split_audio", move |_sc: StepCtx| async move {
                dub_step(&st, "split_audio/timings.json", DUB_STEP_MS).await
            })
            .await?;
        let st = Arc::clone(&state);
        let tts = ctx
            .step("tts", move |_sc: StepCtx| async move {
                dub_step(&st, "tts/wavs", DUB_STEP_MS).await
            })
            .await?;
        let st = Arc::clone(&state);
        let mix_audio = ctx
            .step("mix_audio", move |_sc: StepCtx| async move {
                dub_step(&st, "mix_audio/audio_dubbing.wav", DUB_STEP_MS).await
            })
            .await?;
        let st = Arc::clone(&state);
        let mix_video = ctx
            .step("mix_video", move |_sc: StepCtx| async move {
                // 这里才用上分支 A 的 bgm。
                dub_step(&st, "mix_video/dub_sf_ocr/{id}.mp4", DUB_STEP_MS).await
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
