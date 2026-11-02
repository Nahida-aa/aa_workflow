//! 临时目标的中间件版——基于 **step 边界钩子**（`Middleware::on_step_finished`）。
//!
//! 语义与 TS 版（`@wf-demo/workflow-middlewares` 的 `interimGoals`）**等价**：
//! 目标全部 checkpoint 后，在**目标所在的那个 step 边界**停下，后面的 step 不跑。
//!
//! # 为什么需要钩子
//!
//! TS 能做到是因为 `ctx.step` 是可覆盖的函数属性；Rust 的 `ctx.step` 是方法，
//! 中间件原本只有 `wrap`（包裹整个 handler），只能「跑完再停」。step 边界钩子
//! 补上了这个能力，且它是**机制**（任何策略都能用），不绑定「临时目标」这一个用法。
//!
//! # 转移检查
//!
//! 与引擎内建版一致：若本 drive 启动时目标**已全部达成**（续驱、或 reseed 导入），
//! 则不泊车。这需要读日志——钩子只拿到 `run_id`，所以要**构造时注入
//! `get_events`**（与 TS 版 `reseed({ getEvents })` 的做法一致）。

use std::collections::HashSet;
use std::sync::Mutex;

use aa_workflow_core::middleware::{Middleware, StepFinishedEvent, StepHookOutcome};

/// 保留信号名（与 `__timer` 同族）。
pub const INTERIM_GOAL_SIGNAL: &str = "__interim-goal";

/// input 里的字段名（宿主约定，供调用方在自己的 input 里放目标）。
pub const INTERIM_GOALS_FIELD: &str = "__interimGoals";

/// 目标全部 checkpoint 后，在目标所在的 step 边界泊车。
///
/// # 行为
///
/// 1. 每个 step 的 `STEP_FINISHED` 落盘后，钩子被调用；
/// 2. 把该 step 记入「本 drive 已达成」；
/// 3. 若目标全部达成 → 返回 `Park`，引擎在该 step 边界停下，后续 step 不跑；
/// 4. 宿主投 `__interim-goal` 信号续驱；重放时已完成的 step 短路，
///    不会再触发钩子（它们不产生新的 checkpoint），也就不会重复泊车。
///
/// # 例
///
/// ```ignore
/// let mw = interim_goals(
///     vec!["merge".into()],
///     |run_id| {
///         // 从 store 读日志，收集 STEP_FINISHED 的 step_id
///         Ok(vec![])
///     },
/// );
/// ```
pub fn interim_goals<F>(goals: Vec<String>, get_events: F) -> Middleware
where
    F: Fn(&str) -> anyhow::Result<Vec<String>> + Send + Sync + 'static,
{
    let goals = std::sync::Arc::new(goals);
    let get_events = std::sync::Arc::new(get_events);
    let done: std::sync::Arc<Mutex<HashSet<String>>> =
        std::sync::Arc::new(Mutex::new(HashSet::new()));
    // 每个 run 只在第一次达成时泊一次；同 run 多次 drive 由转移检查兜住。
    let parked: std::sync::Arc<Mutex<HashSet<String>>> =
        std::sync::Arc::new(Mutex::new(HashSet::new()));

    Middleware::new().on_step_finished(move |ev: &StepFinishedEvent<'_>| {
        if goals.is_empty() {
            return Ok(StepHookOutcome::Continue);
        }

        // 转移检查：目标是否在本 step **之前**就已全部完成？
        // 是 → 续驱 / reseed 导入的旧 checkpoint，不该为它再泊（否则重复泊车）。
        // 本次刚完成的 step 也在日志里，所以判断时要把它本身排除。
        let finished_before: HashSet<String> = get_events(ev.run_id)?
            .into_iter()
            .filter(|id| id != ev.step_id)
            .collect();
        if goals.iter().all(|g| finished_before.contains(g)) {
            return Ok(StepHookOutcome::Continue);
        }

        // 本 drive 内累积
        let mut d = done.lock().expect("interim goals lock");
        d.insert(ev.step_id.to_string());
        if !goals.iter().all(|g| d.contains(g)) {
            return Ok(StepHookOutcome::Continue);
        }

        // 已经为这个 run 泊过就不再泊（防御：正常由转移检查兜住）
        let mut p = parked.lock().expect("interim park lock");
        if !p.insert(ev.run_id.to_string()) {
            return Ok(StepHookOutcome::Continue);
        }

        Ok(StepHookOutcome::Park {
            signal_name: INTERIM_GOAL_SIGNAL.to_string(),
            meta: Some(serde_json::json!({ INTERIM_GOALS_FIELD: *goals })),
        })
    })
}
