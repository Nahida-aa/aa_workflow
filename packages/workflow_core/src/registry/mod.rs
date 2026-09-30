//! WorkflowDefinition 选择层：对齐 TanStack `workflow-core/src/registry/`。
//!
//! 上游此目录只有 `select-version.ts`（外加一个 `createWorkflowRegistry`，
//! 我们用 `.previous_versions` 替代，见
//! `docs/runtime-design.md` 与 PARITY #11）。

pub mod select_version;

pub use select_version::select_workflow_version;
