//! Postgres durable execution store for aa-workflow.
//!
//! 实现 [`WorkflowExecutionStore`](aa_workflow_runtime::run_store_adapter::WorkflowExecutionStore)
//! ——即上游 `docs/api/store-adapters.md` 规定的那个契约：
//!
//! > Store adapters implement `WorkflowExecutionStore` from
//! > `@tanstack/aa-workflow-runtime`.
//!
//! **不是** core 的 `aa_workflow_core::RunStore`（那个是旧形状，由
//! [`create_run_store_adapter`](aa_workflow_runtime::run_store_adapter::create_run_store_adapter)
//! 从本 trait 降格得到）。所以本 crate **不写** `impl RunStore`——那会变成两套
//! 几乎相同的方法且有漂移风险。

pub mod migrations;
pub mod store;

pub use store::{SqlxPostgresStore, sqlx_postgres_store};
