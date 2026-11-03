//! `aa_workflow_middlewares` —— **内置中间件清单**。
//!
//! 定位与 TS 侧 `@wf-demo/workflow-middlewares` 一致：这不是「中间件机制」的
//! 实现（机制在 [`aa_workflow_core::middleware`]），而是在它之上提供几个
//! **现成的策略中间件**。
//!
//! # 能力边界（与 TS 的差异只剩两处，均已论证）
//!
//! 中间件形状已经**与 TS 对齐**：一个 `server` 函数，最内层是 handler，外层
//! 靠 `next()` 推进（洋葱）。TS 的 `server` 能做三件事，Rust 一一对应：
//!
//! | TS `server` 里做的事 | Rust 对应 |
//! | --- | --- |
//! | `next({ context: {...} })` 扩展 ctx | [`MiddlewareServerFnArgs::next`]`(ext)`（一次调用，同构） |
//! | 在 `next` 前后加行为（计时 / 日志） | `server` 闭包里 `sc.next(..).await` 前后 |
//! | `ctx.step = wrapped` 拦截 step | [`MiddlewareServerFnArgs::set_step_interceptor`] |
//!
//! 剩下的差异是**语言必然**，不是设计取舍：
//!
//! 1. `TCtxExt` 的类型交集：TS 编译期 `UnionToIntersection`，Rust 运行期
//!    serde 反序列化（同 axum extensions）。
//! 2. `ctx.step` 是方法不是字段，所以「拦截」被收成显式 API
//!    （`set_step_interceptor`），而不是赋值覆盖。
//!
//! Rust 侧还**多**一项能力：拦截器能返回 `Park`（在 step 边界停下整条
//! drive），`server` 也能 `take_over`（跳过 handler 直接出结果）。见
//! `docs/decisions/0002-rust-step-hooks.md`。
//!
//! # 使用者不用写 `<Ext>`（内置中间件固定 `Middleware<(), ()>`）
//!
//! 三个内置中间件都是**不写扩展**的策略（只读 `meta` / `input`），所以它们的构造函数
//! 返回 `Middleware<(), ()>` 而不是泛型未知的 `Middleware`。好处是调用点
//! **完全不用写类型参数**：
//!
//! ```rust,ignore
//! .middleware([interim_goals(vec!["b".into()], get_events)])
//! .middleware([reseed(plan, load_results)])
//! .middleware([dag_policies(shape, get_events)])
//! ```
//!
//! **给中间件作者的建议**：如果你的中间件不写 `ctx.ext`，函数返回值就写成
//! `Middleware<(), ()>`，使用者就能省掉 `.middleware::<Ext>(..)`。
//! 只有「确实要写扩展」的中间件才需要使用者手写 `<Ext>`——`Ext` 是 handler 侧
//! `ctx.ext` 的类型，Rust 没有 TS 的 `UnionToIntersection`（见 `docs/decisions/0013`）。
//!
//! # 策略参数走 `RunWorkflowOptions.meta`
//!
//! 三个中间件都从 [`ctx.meta`](aa_workflow_core::define::BaseCtx) 读策略参数
//! （`__interimGoals` / `__dagPlan`），而不是往引擎加字段——`interim_goals` /
//! `reseed_run` 这类**策略**不该长在引擎上（见 `docs/decisions/0001`）。
mod dag;
mod interim_goals;
mod reseed;

pub use dag::{DAG_PLAN_FIELD, GetEvents, INTERIM_GOAL_SIGNAL, dag_policies};
pub use interim_goals::{INTERIM_GOALS_FIELD, interim_goals};
pub use reseed::{LoadResults, ReseedPlan, reseed};

/// 供文档链接用：把常用名字提上来，避免调用方再导一遍 core。
///
/// ```ignore
/// use aa_workflow_middlewares::prelude::*;
/// ```
pub mod prelude {
    pub use aa_workflow_core::middleware::{
        Middleware, MiddlewareServerFn, MiddlewareServerFnArgs, StepAction, StepEvent, StepInterceptor,
    };
}
