//! # 长 step 的实时进度：`StepCtx::progress`
//!
//! 回答一个容易搞混的问题：**step 内部的进度能不能让外部看到？**
//!
//! 先说不能做什么：`ctx.state` 拿不到 step 内部进度,两个独立原因叠加——
//!
//! 1. **没有中途 flush 点。** `flush_state()` 只在耐久边界调用
//!    （`define/mod.rs` 的 5 处：`step_with` 前、wait/approve/sleep_until/yield_ 前），
//!    全在 step **之外**。step 跑 10 分钟，这 10 分钟里 driver 看不到任何 state。
//! 2. **就算有 flush 点也写不进去。** 闭包拿到的是 `ctx` 的引用（TS）或克隆的
//!    `StateHandle`（Rust），改不到 driver 的 mirror。详见
//!    `docs/concepts/ctx-state.md`。
//!
//! `StepCtx::progress(f64)` 是**专门为此存在的第三条通道**，性质和上面两个都不同：
//!
//! | | 即时 | 耐久 | 用途 |
//! | --- | --- | --- | --- |
//! | `StepCtx::progress` | ✅ | ❌ | 长 step 的进度条 / 心跳 |
//! | 写 `ctx.state` | ❌ | ❌ | —— 这个组合不存在 |
//! | 拆成多个 step | ✅ | ✅ | 需要重放正确的粒度 |
//!
//! **「即时 + 耐久」这个格子是空的,而且是故意空的。** 上游把它写得更死
//! （`primitives.md:113`，`ctx.emit` 的契约原文）："non-durable observability
//! event… **Don't use for** anything the engine should replay"。
//!
//! 注意 `progress` **不是上游原语** —— 上游 `rg "progress" workflow-core/src/`
//! 零命中，只有 `primitives.md:118` 里 `ctx.emit('progress', …)` 那个举例名字。
//! 我们把它做成了独立变体（带 `step_id`、值域 `0.0..=1.0`），消费方不必按
//! name 过滤；不需要 step 归因时上游对齐写法仍是 `ctx.emit`。见
//! `docs/tanstack-alignment.md` 分歧 6。
//!
//! **为什么不耐久,以及为什么这是对的**：事件定义上写死了
//! `/// Observability only (not persisted)`，`publish()` 只调 publisher 回调、
//! **从不 `append` 到日志**。假设某个 step 报了 50% 然后崩了——resume 时闭包
//! **整个被跳过**，那 50% 从没进过日志，恢复后是 0%。于是「step 内进度」和
//! 「往 state 里写进度」是**同一个错误**，只是写进了另一个载体。
//!
//! 所以本示例刻意把两件事并排放：
//!
//! - `slow` step 循环 5 次调 `sc.progress(..)` —— 观察者实时收到 5 条 `STEP_PROGRESS`
//! - `phase` step 不调 —— 它的状态变化只能等 `STEP_FINISHED`
//!
//! `tests/progress_report.rs` 断言的重点是那条**对比**：publisher 看到了 5 条
//! progress，但事件日志里一条都没有，而 `STEP_FINISHED` 在日志里。
//!
//! ## 顺带一提：`STEP_STARTED` 也不落盘
//!
//! 查日志持久化时容易误以为「Started/Finished 都记着」。实际只有 5 类事件
//! `append` 进日志（`engine/mod.rs` 的 5 处 `inner.append`）：
//!
//! | 落盘 | 仅 `publish` |
//! | --- | --- |
//! | `STEP_FINISHED` / `STEP_FAILED` / `STEP_PAUSED` | `RUN_STARTED` / `STEP_STARTED` |
//! | `NOW_RECORDED` / `UUID_RECORDED` | `STEP_PROGRESS` / `CUSTOM` / `STATE_DELTA` |
//!
//! 后果值得知道：**进程在 step 中途被杀，日志里查不到「当时在跑哪个 step」**
//! ——最后一条只是上一个 step 的 Finished。这不违反一致性（该 step 重来即可），
//! 但想诊断「上次崩在哪」只能靠别的手段。

use std::time::Duration;

use aa_workflow_core::{
    BaseCtx, CreateWorkflowConfig, StepCtx, WorkflowDefinition, create_workflow,
};

/// `progress_report` 的输入。
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProgressInput {
    /// `slow` step 循环几轮，每轮报一次进度。
    pub rounds: u32,
    /// 每轮睡多久（毫秒）——用来让观察者真的能看到「过程中」。
    pub tick_ms: u64,
}

/// state 全程只读，且 `initialize` 之后不写。
///
/// 刻意如此：这个示例要展示的是「进度走 progress 通道，不走 state」，
/// 所以 state 得保持干净，否则读者会以为进度被写进了这里。
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProgressState {
    pub base: u32,
}

/// `slow` 的产出：跑完了几轮。
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SlowOutcome {
    pub rounds: u32,
    /// 闭包在**整个 step 内**累计的进度上报次数。
    ///
    /// 注意这个数在 resume 后不会「接着上次的累计」——闭包被跳过，整个字段
    /// 由日志里记录的 `SlowOutcome` 原样恢复。
    pub reported: u32,
}

pub fn progress_report() -> WorkflowDefinition<ProgressInput, serde_json::Value, ProgressState> {
    create_workflow(
        CreateWorkflowConfig::new("progress-report")
            .input::<ProgressInput>()
            .state::<ProgressState>()
            .initialize(|input: &ProgressInput| Ok(ProgressState { base: input.rounds })),
    )
    .handler(|ctx: BaseCtx<ProgressInput, ProgressState>| async move {
        // ① 长 step：循环里逐次上报。这是 `progress` 唯一的正确用法——
        //    报「到哪了」，不是报「结果是什么」（结果该用返回值）。
        let slow = ctx
            .step("slow", move |sc: StepCtx| {
                let rounds = ctx.input.rounds;
                let tick = Duration::from_millis(ctx.input.tick_ms);
                async move {
                    let mut reported = 0u32;
                    for i in 1..=rounds {
                        tokio::time::sleep(tick).await;
                        // 0.0..=1.0。观察者立刻收到，引擎不落盘。
                        sc.progress(i as f64 / rounds as f64);
                        reported += 1;
                    }
                    Ok(SlowOutcome { rounds, reported })
                }
            })
            .await?;

        // ② 不调 progress 的 step：外部只能看到 Started → Finished 两态。
        //    这就是「拆 step」和「报 progress」的分工——前者的粒度是耐久的，
        //    后者的粒度是尽力而为的。
        let phase = ctx
            .step("phase", move |_sc: StepCtx| async move {
                Ok(slow.rounds)
            })
            .await?;

        Ok(serde_json::json!({
            "slow": slow,
            "phase": phase,
        }))
    })
}
