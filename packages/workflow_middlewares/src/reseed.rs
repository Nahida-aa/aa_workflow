//! redo（reseed run）的中间件版——基于 **before-step 钩子**。
//!
//! 语义与 TS 版（`@wf-demo/workflow-middlewares` 的 `reseed`）**等价**：
//! 不在 `redoSet` 里的 step **不执行闭包**，直接用源 run 的结果提交
//! `STEP_FINISHED`。
//!
//! # 与引擎参数 `reseed_run` 的差别
//!
//! | | 引擎 `reseed_run` | 本中间件 |
//! | --- | --- | --- |
//! | 结果复用 | ✅ | ✅ |
//! | 重放能短路 | ✅ | ✅（照常落 checkpoint） |
//! | `meta.importedFrom` 溯源 | ✅ | ❌（可从 plan + 源日志推算） |
//! | 读源时机 | 首次 drive（`startRun` 里） | 首次 drive（钩子首次被问到时） |
//! | 源在首驱前没了 | 导入 0 条 → 真跑 | 查不到 → 真跑（**相同**） |
//!
//! # 为什么需要 before-step 钩子
//!
//! `on_step_finished` 时 step 已经跑完了，救不回来。要「不执行闭包、直接给结果」
//! 只能在**执行之前**介入，这正是 [`Middleware::on_before_step`] 的 `Skip`。

use std::collections::HashMap;
use std::sync::Mutex;

use aa_workflow_core::middleware::{Middleware, StepHookOutcome};

/// 一次 redo 的计划：从哪里播种、哪些 step 重跑。
#[derive(Debug, Clone)]
pub struct ReseedPlan {
    /// 源 run id。
    pub source_run_id: String,
    /// 强制重跑的 stepId 名单（**不**复用它们的旧结果）。
    pub redo_set: Vec<String>,
}

/// 从源 run 读「step_id → 结果」。
///
/// 返回的 map 应只包含该 run 日志里 `STEP_FINISHED` 的结果；`redo_set` 过滤由本
/// 中间件负责（保持读函数简单）。
pub type LoadResults =
    dyn Fn(&str) -> anyhow::Result<HashMap<String, serde_json::Value>> + Send + Sync;

/// 按 plan 复用源 run 的 step 结果。
///
/// `plan` 在**构造时**给出（Rust 的中间件是 `Arc<dyn Fn>`，没有「运行时读 input」
/// 的默认行为；需要从 input 读，就自己在闭包里读 `ctx.input` 再决定是否挂载）。
///
/// # 例
///
/// ```ignore
/// let mw = reseed(
///     ReseedPlan { source_run_id: "rev1".into(), redo_set: vec!["translate".into()] },
///     |run_id| { /* 从 store 读 STEP_FINISHED → HashMap<step_id, result> */ Ok(Default::default()) },
/// );
/// ```
pub fn reseed<F>(plan: ReseedPlan, load_results: F) -> Middleware
where
    F: Fn(&str) -> anyhow::Result<HashMap<String, serde_json::Value>> + Send + Sync + 'static,
{
    let redo: std::collections::HashSet<String> = plan.redo_set.iter().cloned().collect();
    let plan = std::sync::Arc::new(plan);
    let load_results = std::sync::Arc::new(load_results);
    // 每个 run 只载入一次源结果（钩子会被每个 step 调用，不能每次都读日志）。
    let cache: std::sync::Arc<Mutex<Option<HashMap<String, serde_json::Value>>>> =
        std::sync::Arc::new(Mutex::new(None));

    Middleware::new().on_before_step(move |ev| {
        // 本 run 就是源 run？不做任何事（禁止自我播种）。
        if plan.source_run_id == ev.run_id {
            return Ok(StepHookOutcome::Continue);
        }
        // redoSet 里的 step 要真跑。
        if redo.contains(ev.step_id) {
            return Ok(StepHookOutcome::Continue);
        }

        let mut guard = cache.lock().expect("reseed cache lock");
        if guard.is_none() {
            *guard = Some(load_results(&plan.source_run_id)?);
        }
        match guard.as_ref().and_then(|m| m.get(ev.step_id)) {
            Some(result) => Ok(StepHookOutcome::Skip {
                result: result.clone(),
            }),
            // 源里没有这个 step（新加的 / 上次失败的）→ 真跑。
            None => Ok(StepHookOutcome::Continue),
        }
    })
}
