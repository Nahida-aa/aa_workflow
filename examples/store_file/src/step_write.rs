//! 在 step 里读写 `ctx.state` 的三种机制对照（[`step_write`]）。
//!
//! **这个 workflow 只为一件事存在**：回答「在 step 闭包里改 state，下一个 step
//! 读不读得到」。三个 step 逐个摊开写（不循环——这是示例，重复三遍比 `for`
//! 更好读），每个 step 对**三种机制**各做一次读写，结果放在一起对照。
//!
//! # 结论：重放会跳过 step 闭包，所以 step 闭包应保持「纯」
//!
//! 核心机制只有一条（上游 `docs/concepts/replay-and-resume.md:31`）：
//!
//! > 2. Found → return the recorded result (or rethrow the recorded error).
//! >    **`fn` is NOT called.**
//!
//! 一条机制把代码分成两类，两类规则不同：
//!
//! | | step 闭包 `ctx.step(id, fn)` | handler 体内 |
//! | --- | --- | --- |
//! | replay 时 | **被跳过**，返回记录的结果 | **重跑** |
//! | 副作用 | 只能通过**返回值**输出 | 会被重新施加 |
//! | 要求 | 保持纯（无隐藏副作用） | 确定性（上游 `:42` determinism contract） |
//!
//! 所以 **step 闭包的唯一输出通道是它的返回值**。写 `ctx.state` 是「未被记录的
//! 副作用」——不在返回值里，日志里也没有，而 state 每次 drive 都由
//! `initialize` 重建（上游 `overview.md:33`「State is **derived**」、
//! `RunStore.md:16`「state the user mutates **inside the handler** is NOT
//! persisted」）。所以**不要在 step 闭包内写 `ctx.state`**。
//!
//! 精确化：step 闭包**不是**严格纯函数（上游例子 `ctx.step('flag', fetchFlag)`
//! 就有外部副作用）。区别在**副作用有没有被记录**——`fetchFlag()` 的效果进了
//! `STEP_FINISHED.result`，只真实执行一次，那是允许的。要求的是「无隐藏副作用」。
//!
//! 上游没把这条写进 Authoring rules（`primitives.md:6` 说每个原语有 one recipe
//! and one footgun，而 `step` 的 footgun 是 id 重复 `:50`，不是 state），
//! 所以它需要被显式说明。完整论证见 `docs/concepts/ctx-state.md`。
//!
//! ## 实测：TS 在这里会静默产生不一致
//!
//! 下面这三条是**在 TanStack 仓库实跑验证过的**（`workflow-core` vitest，
//! `inMemoryRunStore` + `simulateRestart`）。handler 在 s1 之后读一次
//! `ctx.state.writtenInsideStep`，s1 的闭包内做过 `ctx.state.writtenInsideStep += 1`：
//!
//! ```text
//! 首次 drive：  after-s1:1     ← 闭包内的写「生效」（闭包按引用捕获 ctx）
//! resume 后：   after-s1:0     ← 同一段代码、同一位置，读到 0
//! ```
//!
//! 首次 drive 的 `STATE_DELTA` 还会上报 `{"path":"/writtenInsideStep","value":1}`，
//! 而最终 `RUN_FINISHED.output` 里是 `after-s1:0`——**观测流和终态自相矛盾**。
//!
//! 这是正确性缺陷，不是风格问题：同一段 handler 在「首次」和「恢复后」走出的
//! 结果不同，取决于 run 是新的还是 resume 的。
//!
//! ## Rust 更早暴露这个错误（但理由不光彩）
//!
//! | | 闭包捕获语义 | 单次 drive 内 | resume 后 |
//! | --- | --- | --- | --- |
//! | TS | 按**引用**捕获 `ctx`（`types.ts:328`：`state` 是普通属性） | 写**看得见** → 误以为对 | 静默丢失 → 结果不一致 |
//! | Rust `StateHandle::clone` | **值拷贝** `st`（`state_handle.rs:66-73`） | 写**看不见** → 立刻发现 | 静默丢失 |
//!
//! TS 更危险：它让错误的写法**看起来是工作的**。Rust 至少让人在第一次运行时
//! 就发现——但同样是静默的：step 不失败，run 照样 `Finished`（见测试
//! `steps_still_succeed_despite_every_closure_write_being_dropped`）。
//!
//! `StateHandle::clone` 复制 `st`、只共享 mirror（`state_handle.rs:35-40`）。克隆体
//! 有自己的 `st` 字段（`:57`），`DerefMut` 改的是那一份（`:130`）；driver 原句柄的
//! `Deref` 读自己的 `st`（`:126`）——从 clone 那一刻起就分家了。
//!
//! 断链在**并行**下是刻意的（`state_handle.rs:39`：「并行 step 无法竞写 state」），
//! 但那不能作为串行下也写不了的正当理由——正确理由是上面那条纯性要求。
//!
//! ## 本示例的三个字段
//!
//! 同一个循环体里对三种机制各做一次读写，把差异摆在一起：
//!
//! | 字段 | 机制 | 链 | 结论 |
//! | --- | --- | --- | --- |
//! | `via_body` | handler **体外**写 `ctx.state` | **通** `[0,1,2]` | ✅ 会被重跑，**正解** |
//! | `via_arc` | 闭包内写 `Arc<Mutex<u32>>` | **通** `[0,1,2]` | ⚠️ 不是 `ctx.state`：无 `STATE_DELTA`、不过 schema、resume 不重演 |
//! | `via_closure` | 闭包内写 `StateHandle::clone()` | **断** `[0,0,0]` | ❌ 隐藏副作用，破坏纯性 |
//!
//! **正确写法**（`via_body`）：step 只负责算和返回，handler 用返回值写 state。
//!
//! ```ignore
//! let v = ctx.step("fetch", move |_| async move { compute().await });  // 闭包不碰 state
//! ctx.state.via_body = v;                                             // 体内写，依赖已重放的结果
//! ```
//!
//! 数据流走 **step 返回值 → 局部变量 → 下个 step 的入参**，不走 state。
//! `via_arc` 只适合步骤间的活体中间量，绝不能当持久状态。
//!
use std::sync::{Arc, Mutex};

use aa_workflow_core::{
    BaseCtx, CreateWorkflowConfig, StepCtx, WorkflowDefinition, create_workflow,
};

/// [`step_write`] 的输入：播种 `base`。
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct StepWriteInput {
    /// `initialize` 播种进 `base`。
    #[serde(default)]
    pub base: u32,
}

/// state 里两个字段，各对应一种写入位置（`via_arc` 不在 state 里，见下）。
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StepWriteState {
    /// 只由 `initialize` 写过——基准线，用来排除「被覆盖」的可能。
    pub base: u32,
    /// **对照组**：在 handler 体外（步骤之间）写。链是**通**的。
    pub via_body: u32,
    /// **反面**：在 step 闭包内经 `StateHandle::clone` 写。链是**断**的。
    pub via_closure: u32,
}

/// 三种写入机制的对照 workflow。
///
/// 三个 step，每个 step 的形状固定为五步：
///
/// 1. **arc 读** ——上一个 step 闭包内经 `Arc` 写的值（链通）；
/// 2. **体外读 + 写** `ctx.state.via_body` ——对照组（链通）；
/// 3. **抓句柄 clone** `ctx.state.clone()` ——反面教材；
/// 4. **耐久边界** `ctx.step(...)`，闭包内同时写 `via_closure`（断链）和 arc
///    （链通），并把三个读数**通过返回值**带出来；
/// 5. **driver 读** `ctx.state.via_closure` 与 arc ——对照闭包写了什么。
pub fn step_write() -> WorkflowDefinition<StepWriteInput, serde_json::Value, StepWriteState> {
    create_workflow(
        CreateWorkflowConfig::new("step-write")
            .input::<StepWriteInput>()
            .state::<StepWriteState>()
            .initialize(|input: &StepWriteInput| {
                Ok(StepWriteState {
                    base: input.base,
                    via_body: 0,
                    via_closure: 0,
                })
            }),
    )
    .handler(
        |mut ctx: BaseCtx<StepWriteInput, StepWriteState>| async move {
            // 「TS 式」的共享可变：Arc<Mutex<_>>。Arc 就是 Rust 的引用语义，闭包捕获
            // 它等于捕获同一个值——所以闭包内的写，下一个 step 读得到。
            // 代价：它**不是** ctx.state，没有 STATE_DELTA、不过 schema。
            let arc_counter = Arc::new(Mutex::new(0u32));

            // ================================================================
            // step s1
            // ================================================================

            // ① arc 读——initialize 播种的 0。
            let s1_arc_read = *arc_counter.lock().expect("arc lock");
            // ② 体外读 + 写 `ctx.state`（对照组，链通）
            let s1_body_read = ctx.state.via_body;
            ctx.state.via_body += 1;
            // ③ 两个捕获：句柄 clone（值拷贝，断链）+ Arc clone（共享，链通）
            let s1_inner = ctx.state.clone();
            let s1_arc = Arc::clone(&arc_counter);
            // ④ 闭包内：写 via_closure（断链）+ 写 arc（链通）
            let s1 = ctx
                .step("s1", move |_sc: StepCtx| {
                    let mut st = s1_inner; // DerefMut 需要 mut
                    let arc = s1_arc;
                    async move {
                        // 写前先读，才能和 driver 侧的读数对照。
                        let saw_closure = st.via_closure; // 恒为 0
                        st.via_closure += 1; // 写自己那份 st —— 断链
                        *arc.lock().expect("arc lock") += 1; // 写共享的那个 —— 链通
                        // ★ 正确通道：通过返回值交出去。durable，进日志、参与重放。
                        Ok(serde_json::json!({
                            "sawBody": st.via_body,          // 1（本步体外写产出的）
                            "sawClosure": saw_closure,      // 写前，恒为 0
                            "wrote": st.via_closure,         // 1（对它自己而言）
                            "arcWrote": *arc.lock().expect("arc lock"), // 1
                        }))
                    }
                })
                .await?;
            // ⑤ driver 读：体外写的看得见，闭包写的看不见；arc 看得见
            let s1_driver = ctx.state.via_closure; // → 0：副本的写回不到原件
            let s1_arc_after = *arc_counter.lock().expect("arc lock"); // → 1

            // ================================================================
            // step s2 —— 与 s1 形状完全相同，只有 step id 不同
            // ================================================================

            // ① arc 读——s1 闭包写的 1
            let s2_arc_read = *arc_counter.lock().expect("arc lock");
            // ② 体外读 + 写
            let s2_body_read = ctx.state.via_body;
            ctx.state.via_body += 1;
            // ③ 捕获
            let s2_inner = ctx.state.clone();
            let s2_arc = Arc::clone(&arc_counter);
            // ④ 闭包内读写
            let s2 = ctx
                .step("s2", move |_sc: StepCtx| {
                    let mut st = s2_inner;
                    let arc = s2_arc;
                    async move {
                        let saw_closure = st.via_closure; // 恒为 0
                        st.via_closure += 1;
                        *arc.lock().expect("arc lock") += 1;
                        Ok(serde_json::json!({
                            "sawBody": st.via_body,          // 2
                            "sawClosure": saw_closure,      // 0（s1 闭包写的没落地）
                            "wrote": st.via_closure,
                            "arcWrote": *arc.lock().expect("arc lock"), // 2
                        }))
                    }
                })
                .await?;
            // ⑤ driver 读
            let s2_driver = ctx.state.via_closure; // → 0
            let s2_arc_after = *arc_counter.lock().expect("arc lock"); // → 2

            // ================================================================
            // step s3
            // ================================================================

            // ① arc 读——s2 闭包写的 2
            let s3_arc_read = *arc_counter.lock().expect("arc lock");
            // ② 体外读 + 写
            let s3_body_read = ctx.state.via_body;
            ctx.state.via_body += 1;
            // ③ 捕获
            let s3_inner = ctx.state.clone();
            let s3_arc = Arc::clone(&arc_counter);
            // ④ 闭包内读写
            let s3 = ctx
                .step("s3", move |_sc: StepCtx| {
                    let mut st = s3_inner;
                    let arc = s3_arc;
                    async move {
                        let saw_closure = st.via_closure; // 恒为 0
                        st.via_closure += 1;
                        *arc.lock().expect("arc lock") += 1;
                        Ok(serde_json::json!({
                            "sawBody": st.via_body,          // 3
                            "sawClosure": saw_closure,      // 0
                            "wrote": st.via_closure,
                            "arcWrote": *arc.lock().expect("arc lock"), // 3
                        }))
                    }
                })
                .await?;
            // ⑤ driver 读
            let s3_driver = ctx.state.via_closure; // → 0
            let s3_arc_after = *arc_counter.lock().expect("arc lock"); // → 3

            Ok(serde_json::json!({
                "bodyReads": [s1_body_read, s2_body_read, s3_body_read],
                "arcReads": [s1_arc_read, s2_arc_read, s3_arc_read],
                "arcWrote": [s1["arcWrote"], s2["arcWrote"], s3["arcWrote"]],
                "arcAfter": [s1_arc_after, s2_arc_after, s3_arc_after],
                "closureSawBody": [s1["sawBody"], s2["sawBody"], s3["sawBody"]],
                "closureSawClosure": [s1["sawClosure"], s2["sawClosure"], s3["sawClosure"]],
                "closureWrote": [s1["wrote"], s2["wrote"], s3["wrote"]],
                "driverAfterClosure": [s1_driver, s2_driver, s3_driver],
                "finalBody": ctx.state.via_body,
                "finalClosure": ctx.state.via_closure,
                "base": ctx.state.base,
            }))
        },
    )
}
