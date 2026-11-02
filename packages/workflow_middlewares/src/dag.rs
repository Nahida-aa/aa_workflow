//! DAG 策略中间件——**Rust 版**：redo（复用）+ 临时目标（泊车），三合一。
//!
//! # 形态与 TS 版的分工
//!
//! Rust 的 DAG 哲学：形状归 [`aa_workflow_dag::DagShape`]（声明期），执行归
//! handler（`try_join!` 是编译期代码）。所以本中间件**不接管调度**——它只承载
//! 策略，调度仍由用户 handler 负责。这与 TS 版 `dag(spec)`（接管 handler）是
//! 两种语言下的各自自然形态。
//!
//! # 行为（读 `ctx.input.__dagPlan`）
//!
//! ```json
//! { "__dagPlan": {
//!     "source_run_id": "rev1",   // redo：播种来源
//!     "from": ["translate"],     // redo：失效起点（闭包展开成重跑名单）
//!     "redo_set": ["x"],         // redo：裸名单（与 from 展开取并集）
//!     "goals": ["merge"]         // 临时目标：全 checkpoint 后泊车
//! } }
//! ```
//!
//! - **redo**：不在重跑名单里的 step，其源结果经 `on_before_step` 的 `Skip`
//!   直接提交（业务闭包不执行）；引擎照常落 `STEP_FINISHED`——重放可短路。
//!   闭包展开由 [`aa_workflow_dag::DagShape::reseed_plan`] 完成（库有图）。
//! - **临时目标**：goals 全部达成（真跑完成**或**被复用）后，在达成所在的
//!   step 边界停下，后续 step 不跑。
//! - 都未配置 → 中间件不产生任何行为。
//!
//! # 转移语义（无需显式转移检查）
//!
//! 钩子只在**真实执行的 step** 完成后触发——重放短路的 step 不触发。因此：
//! reseed 导入的目标在其复用点记入 `done`（复用算达成）；续驱 drive 里重放的
//! 历史 step 不触发钩子、不会被重复计入 → 不会重复泊车。`parked` 集合仅作防御。
//!
//! # 需要 store
//!
//! `reseed_plan` 要读源 run 日志——构造时注入 `get_events`。
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use aa_workflow_core::middleware::{Middleware, StepHookOutcome};
use aa_workflow_dag::DagShape;

/// 保留信号名（与 `__timer` 同族）。
pub const INTERIM_GOAL_SIGNAL: &str = "__interim-goal";

/// `ctx.input` 里的计划信封字段。
pub const DAG_PLAN_FIELD: &str = "__dagPlan";

/// 读某个 run 的日志（引擎事件数组）。
pub type GetEvents =
    dyn Fn(&str) -> anyhow::Result<Vec<aa_workflow_core::event::WorkflowEvent>> + Send + Sync;

/// drive 开始时从 input 解析出的策略状态（wrap 前置写入，两个钩子读取）。
#[derive(Default)]
struct PlanState {
    /// 临时目标（`goals`）。
    goals: Vec<String>,
    /// redo 复用表：step_id → 源结果。空 = 无 redo。
    lookup: HashMap<String, serde_json::Value>,
    /// 本 drive 已达成的目标（真跑完成 **或** 被复用）。
    done: HashSet<String>,
}

/// DAG 策略中间件：redo 复用 + 临时目标泊车，读 `input.__dagPlan`。
///
/// `shape` 提供图（`from` 闭包展开、缺省种子计算）；`get_events` 提供日志读取。
pub fn dag_policies(shape: Arc<DagShape>, get_events: Arc<GetEvents>) -> Middleware {
    let state: Arc<Mutex<Option<PlanState>>> = Arc::new(Mutex::new(None));
    // 防御：同一 run 只泊一次（正常由「重放不触发钩子」天然保证）。
    let parked: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    // wrap 的前置段负责解析 input；两个钩子读同一份 state。
    let state_for_wrap = state.clone();
    let get_events_for_wrap = get_events.clone();
    let shape_for_wrap = shape.clone();

    let state_for_before = state.clone();
    let state_for_finished = state.clone();
    let parked_for_finished = parked;

    let wrap: aa_workflow_core::middleware::CtxWrapper =
        Arc::new(move |ctx: aa_workflow_core::define::WorkflowCtx, next| {
            let get_events = get_events_for_wrap.clone();
            let shape = shape_for_wrap.clone();
            let state = state_for_wrap.clone();
            Box::pin(async move {
                // 前置：解析 input.__dagPlan → PlanState。
                let plan = ctx
                    .input
                    .get(DAG_PLAN_FIELD)
                    .and_then(|v| v.as_object())
                    .cloned();

                let mut st = PlanState::default();
                if let Some(obj) = plan {
                    if let Some(goals) = obj.get("goals").and_then(|v| v.as_array()) {
                        st.goals = goals
                            .iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .collect();
                    }

                    // redo：source_run_id 存在才启用；from 闭包展开交给
                    // DagShape::reseed_plan（库有图，含缺省种子计算）。
                    if let Some(source_run_id) =
                        obj.get("source_run_id").and_then(|v| v.as_str())
                    {
                        let source_log = get_events(source_run_id)?;
                        let from = obj
                            .get("from")
                            .and_then(|v| v.as_array())
                            .map(|a| {
                                a.iter()
                                    .filter_map(|v| v.as_str().map(str::to_string))
                                    .collect::<Vec<_>>()
                            });
                        let reseed_run =
                            shape.reseed_plan(source_run_id, &source_log, from)?;
                        let mut redo: HashSet<String> =
                            reseed_run.redo_set.iter().cloned().collect();
                        if let Some(extra) =
                            obj.get("redo_set").and_then(|v| v.as_array())
                        {
                            for v in extra {
                                if let Some(id) = v.as_str() {
                                    redo.insert(id.to_string());
                                }
                            }
                        }

                        // 源结果 → lookup（排除重跑名单）。
                        let source = get_events(source_run_id)?;
                        for e in &source {
                            if let aa_workflow_core::event::WorkflowEvent::StepFinished {
                                step_id,
                                result: Some(result),
                                ..
                            } = e
                            {
                                if !redo.contains(step_id) {
                                    st.lookup.insert(step_id.clone(), result.clone());
                                }
                            }
                        }
                    }
                }
                *state.lock().expect("dag policies state lock") = Some(st);

                let out = next.await?;

                // 收尾：清 state（本 drive 结束；下个 drive 重算）。
                *state.lock().expect("dag policies state lock") = None;
                Ok(out)
            })
        });

    let before: aa_workflow_core::middleware::BeforeStepHook = Arc::new(move |ev| {
        let state = state_for_before.clone();
        let guard = state.lock().expect("dag policies state lock");
        let Some(st) = guard.as_ref() else {
            return Ok(StepHookOutcome::Continue);
        };
        match st.lookup.get(ev.step_id) {
            Some(result) => Ok(StepHookOutcome::Skip {
                result: result.clone(),
            }),
            None => Ok(StepHookOutcome::Continue),
        }
    });

    let finished: aa_workflow_core::middleware::StepFinishedHook = Arc::new(move |ev| {
        let state = state_for_finished.clone();
        let parked = parked_for_finished.clone();
        let mut guard = state.lock().expect("dag policies state lock");
        let Some(st) = guard.as_mut() else {
            return Ok(StepHookOutcome::Continue);
        };
        if st.goals.is_empty() {
            return Ok(StepHookOutcome::Continue);
        }

        // 达成累积：真跑完成（本钩子触发）与被复用（before-step 的 Skip
        // 分支记入）都算达成。重放短路的 step 不触发钩子——历史目标天然
        // 不会重复计入。
        st.done.insert(ev.step_id.to_string());
        if !st.goals.iter().all(|g| st.done.contains(g)) {
            return Ok(StepHookOutcome::Continue);
        }

        // 防御：同一 run 只泊一次。
        if !parked.lock().expect("park lock").insert(ev.run_id.to_string()) {
            return Ok(StepHookOutcome::Continue);
        }

        // 本段已全部达成 → 清累积（泊车后续驱的新段重新累积）。
        st.done.clear();

        Ok(StepHookOutcome::Park {
            signal_name: INTERIM_GOAL_SIGNAL.to_string(),
            meta: Some(serde_json::json!({ "goals": st.goals })),
        })
    });

    Middleware {
        produce: None,
        wrap: Some(wrap),
        on_step_finished: Some(finished),
        on_before_step: Some(before),
    }
}
