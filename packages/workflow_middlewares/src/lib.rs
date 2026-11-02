//! `aa_workflow_middlewares` —— **内置中间件清单**。
//!
//! 定位与 TS 侧 `@wf-demo/workflow-middlewares` 一致：这不是「中间件机制」的
//! 实现（机制在 [`aa_workflow_core::middleware`]），而是在它之上提供几个
//! **现成的策略中间件**。
//!
//! # ⚠️ Rust 侧的能力边界（与 TS 不同，必须先读）
//!
//! TS 的中间件能**替换 `ctx.step`**——它只是 `ctx` 对象上的一个函数属性，
//! `ctx.step = wrapped` 即可。这就是 TS 版 `interimGoals` / `reseed` 能做到
//! 「逐 step 拦截」的原因。
//!
//! **Rust 做不到**：
//!
//! | | TS | Rust |
//! | --- | --- | --- |
//! | `ctx.step` | 对象上的函数属性，可赋值覆盖 | `&self` 上的**方法**，不可替换 |
//! | 中间件能碰的 | 整个 `ctx` 对象引用 | [`Middleware::produce`]（`ctx.ext`）+ [`Middleware::wrap`]（整体包裹） |
//! | 引擎内部 | JS 闭包，中间件够不着 | `pub(crate) engine`，**外部 crate 访问不到** |
//! | 拦截「第 N 次 step 调用」 | ✅ | ❌ |
//!
//! 因此本包提供的是 **Rust 能表达的等价形态**，语义与 TS 版**不完全相同**
//! （见每个中间件的文档）。
//!
//! # 更推荐的路径
//!
//! Rust 侧**引擎本身就有** `interim_goals` / `reseed_run`（`RunWorkflowOptions`
//! 字段）。它们比中间件更直接、语义更完整。中间件的价值只在：
//!
//! - 想在**不改引擎**的前提下表达同样策略；
//! - 或者只想在 handler 前后加行为（`wrap` 擅长这个）。
//!
//! [`Middleware::produce`]: aa_workflow_core::middleware::Middleware::produce
//! [`Middleware::wrap`]: aa_workflow_core::middleware::Middleware::wrap

mod interim_goals;

pub use interim_goals::{
    interim_goals, park_after_handler, INTERIM_GOAL_SIGNAL, INTERIM_GOALS_FIELD,
};
