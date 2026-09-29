//! Host-agnostic example layer for `aa_workflow_core`（**位置**对齐 TanStack
//! `examples/deployment-pocs/shared`，但面向 Rust + 本地 host）。
//!
//! 注意只对齐位置，**不对齐代码写法**：上游那份 examples 用的是过时写法
//! （`ctx.input as T` 手动断言、裸 `Date.now()`），而 guide 用的是受推断契约
//! 保护的新写法（`input: z.object(...)` + `ctx.input.field`）。我们跟 guide
//! 一致。差异细节见 `docs/tanstack-alignment.md` 的「参考上游时看哪边」。
//!
//! # ⚠️ 本 crate 的 `FileRunStore` 实现的是**旧契约** `RunStore`
//!
//! 上游 `docs/api/store-adapters.md` 开篇规定的是：
//!
//! > Store adapters implement `WorkflowExecutionStore` from
//! > `@tanstack/workflow-runtime`.
//!
//! 本 crate 里的 [`FileRunStore`] 实现的是 core 的 `aa_workflow_core::RunStore`
//! ——那是**旧形状**（由 `create_run_store_adapter` 从新契约降格得到）。**写新的
//! store adapter 请勿照它**，参考这两个：
//!
//! - [`examples/store_file`](../../store_file) — 文件落盘，实现新契约（单进程）
//! - [`aa_workflow_store_sqlx_postgres`](../../../packages/workflow_store_sqlx_postgres)
//!   — Postgres，实现新契约（生产级 / 多 worker）
//!
//! 保留本 crate 的原因：它对齐 TanStack `examples/deployment-pocs/shared` 的位置，
//! 有与上游对账的价值。所以两者分工是**契约**，不是新旧替代。
//!
//! 三个可复用件：
//! - [`file_run_store::FileRunStore`] — 一个独立的落盘 [`aa_workflow_core::RunStore`]（**旧契约**）实现（`run.json` 信封 +
//!   append-only `events.jsonl`，CAS append + subscribe），证明 store 契约可插拔；
//! - [`workflows`] — 示例 workflow 定义（handler 形态，代码即 DAG），场景集对齐
//!   wf-demo：并行/retry/分支、resume/`continue_from`、approval、双定时器 sleep、
//!   连续事件等待、信号→定时→审批→步骤混合链；输入走
//!   `create_workflow(CreateWorkflowConfig::new(id).input::<T>())` typed schema
//!   （serde 校验），输出从返回值推断；
//! - [`runtime`] — host 无关的薄壳辅助：`drive` 把 start / resume / continue_from
//!   收成一个调用，外加 publisher → tracing；
//! - [`registry`] — 按 id 取示例 workflow（对齐 wf-demo 的 `WORKFLOWS` map）。
//!
//! ## 强约束（与 LocalDub 的分界）
//!
//! - 依赖只允许 `aa_workflow_core` + tokio，**不依赖 LocalDub**；落盘 store 是独立
//!   最小实现（LocalDub 的 `FsRunStore` 语义可回指它）。
//! - 引擎已有 pause 语义（`ctx.approve` / `ctx.sleep`，唤醒走 `signal_run`）：
//!   事件等待用 `ctx.approve` 表达（reason 即事件名），
//!   定时闸门用 `ctx.sleep`（引擎自动唤醒）。
//! - 不做 serverless host 前端（Cloudflare / Netlify / Vercel 对 Rust 无意义）；
//!   需求出现时的薄前端是本地 `cli` / 最小 HTTP server。

pub mod file_run_store;
pub mod registry;
pub mod runtime;
pub mod workflows;

pub use file_run_store::FileRunStore;
pub use registry::{all as all_workflows, get as get_workflow};
pub use runtime::{drive, tracing_publisher};
pub use workflows::{
    approval_order, approval_review, compliance, email_digest, fulfillment, fulfillment_saga,
    invoice, payment_gateway, refund,
};
