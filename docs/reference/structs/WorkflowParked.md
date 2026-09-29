---
id: WorkflowParked
title: WorkflowParked
---

# Struct: WorkflowParked

Defined in: `packages/workflow-core/src/engine/mod.rs:54`

Internal sentinel: the engine parked the run at a durable wait point
([`exec_pause`]). The pause checkpoint and the `Paused` projection were
already written; this only unwinds the handler so the drive can end
`Paused` without appending a terminal event. Never `Errored`.

Mirrors TanStack's `engine.paused` flag + `throw new WorkflowPaused()`
(`run-workflow.ts:970`), whose catch does `if (engine.paused) return`.

## Trait Implementations

- `impl Borrow for WorkflowParked`
- `impl BorrowMut for WorkflowParked`
- `impl Into for WorkflowParked`
- `impl From for WorkflowParked`
- `impl TryInto for WorkflowParked`
- `impl TryFrom for WorkflowParked`
- `impl Any for WorkflowParked`
- `impl ToString for WorkflowParked`
- `impl Debug for WorkflowParked`
- `impl Display for WorkflowParked`
- `impl Error for WorkflowParked`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

