//! 临时目标的中间件版——**Rust 形态**。
//!
//! # 与 TS 版的差别（先读这段）
//!
//! TS 版包装 `ctx.step`，能在**每个 step 完成后**检查「目标是否达成」，达成即泊车。
//! Rust 侧办不到，卡在两个硬约束上：
//!
//! 1. **`ctx.step` 是方法，不是可替换的字段** —— 中间件没有「包装单次 step 调用」
//!    的入口；[`Middleware::wrap`] 只能包裹整个 handler future。
//! 2. **`ctx` 上没有 store** —— 即便在 `wrap` 里想知道「跑过哪些 step」，也读不到
//!    日志（TS 侧中间件可以自己持有 store 引用，Rust 的 `wrap` 闭包可以在构造时
//!    捕获，但 `ctx` 拿不到 run 的日志）。
//!
//! 所以本实现**不做目标检查**，它只提供「handler 跑完后无条件泊一次」：
//!
//! | | TS 版 | 本实现 |
//! | --- | --- | --- |
//! | 检查时机 | 每个 step 完成后 | 无检查 |
//! | 「目标达成才停」 | ✅ | ❌ **无条件停** |
//! | 后面的 step 不跑 | ✅ | ❌ handler 已全跑完 |
//! | 泊车后可续驱 | ✅ | ✅ |
//!
//! # 什么时候它有用
//!
//! 语义是「**跑完这条 workflow，停一下等确认，再续驱**」——比如跑完一天的数据
//! 处理、等人工验收后再发布。这时「无条件停」正是想要的。
//!
//! # 想要 TS 那种「达成即停」
//!
//! 用引擎参数 [`RunWorkflowOptions::interim_goals`]：它在 `engineStep` 内部检查，
//! 天然是「每个 step 完成后」的时机，且能区分「导入的 checkpoint」等细节。
//! **这是推荐路径**，本中间件只在不便改引擎参数时作为退路。
//!
//! [`Middleware::wrap`]: aa_workflow_core::middleware::Middleware::wrap
//! [`RunWorkflowOptions::interim_goals`]: aa_workflow_core::engine::RunWorkflowOptions

use aa_workflow_core::define::WorkflowCtx;
use aa_workflow_core::middleware::Middleware;

/// 保留信号名（与 `__timer` 同族）。
pub const INTERIM_GOAL_SIGNAL: &str = "__interim-goal";

/// input 里的字段名（保留，供宿主约定使用）。
pub const INTERIM_GOALS_FIELD: &str = "__interimGoals";

/// **handler 跑完后泊车一次**；宿主投 `__interim-goal` 信号续驱。
///
/// `key` 是这次泊车的稳定 id——同一个 run 里多次泊车请给不同的 key，
/// 否则重放时 `findCheckpoint` 会匹配到同一个 `SignalResolved`。
///
/// # 行为
///
/// 1. 跑完整个 handler（拿它的返回值）；
/// 2. `wait_for_event(key, "__interim-goal")` 泊车；
/// 3. 宿主投信号后续驱：重放时该 `wait_for_event` 命中缓存直接返回，
///    **不会重复泊车**。
///
/// # 例
///
/// ```ignore
/// create_workflow(CreateWorkflowConfig::new("daily").input::<Value>())
///     .middleware::<()>(park_after_handler("daily-review"))
///     .handler(|ctx: WorkflowCtx| async move { /* steps */ Ok(Value::Null) });
/// ```
pub fn park_after_handler(key: impl Into<String>) -> Middleware {
    let key = key.into();
    Middleware::new().wrap(move |ctx: WorkflowCtx, next| {
        let key = key.clone();
        Box::pin(async move {
            // 先跑完 handler，再泊车。
            let out = next.await?;
            let _ = ctx.wait_for_event(key, INTERIM_GOAL_SIGNAL).await?;
            Ok(out)
        })
    })
}

/// 别名：语义上就是「按目标停车」，但 Rust 侧只能做到「跑完再停」。
/// 保留了 TS 侧的命名，便于对照；**行为不同**，见模块文档。
pub fn interim_goals() -> Middleware {
    park_after_handler("__interim-goal-0")
}
