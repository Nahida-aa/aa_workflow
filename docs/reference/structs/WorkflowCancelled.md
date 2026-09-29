---
id: WorkflowCancelled
title: WorkflowCancelled
---

# Struct: WorkflowCancelled

Defined in: [`packages/workflow-core/src/engine/mod.rs:69`](../../../packages/workflow-core/src/engine/mod.rs#L69)

Internal sentinel: [`cancel_run`](../functions/cancel_run.md) set the run to `Aborted` while the
handler was driving. Step closures are not interruptible mid-`await`,
so the error surfaces at the next engine boundary (step entry / attempt
boundary). Becomes an `Aborted` run, never `Errored`.

## Trait Implementations

- `impl Borrow for WorkflowCancelled`
- `impl BorrowMut for WorkflowCancelled`
- `impl Into for WorkflowCancelled`
- `impl From for WorkflowCancelled`
- `impl TryInto for WorkflowCancelled`
- `impl TryFrom for WorkflowCancelled`
- `impl Any for WorkflowCancelled`
- `impl ToString for WorkflowCancelled`
- `impl Debug for WorkflowCancelled`
- `impl Display for WorkflowCancelled`
- `impl Error for WorkflowCancelled`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

