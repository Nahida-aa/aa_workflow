//! Host-agnostic example layer for `workflow-core`（对齐 TanStack
//! `examples/deployment-pocs/shared` 的位置，但面向 Rust + 本地 host）。
//!
//! 三个可复用件：
//! - [`file_run_store::FileRunStore`] — 一个独立的落盘 [`workflow_core::RunStore`] 实现（`run.json` 信封 +
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
//! - 依赖只允许 `workflow-core` + tokio，**不依赖 LocalDub**；落盘 store 是独立
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
