---
id: StepStatus
title: StepStatus
---

# Enum: StepStatus

Defined in: [`packages/workflow_core/src/event.rs:25`](../../../packages/workflow_core/src/event.rs#L25)

Per-step lifecycle status, derived from the event log via
[`fold_step_states`](../functions/fold_step_states.md). This is a *projection* of the log, never stored
independently.

## Variants

### Pending

Defined in: [`packages/workflow_core/src/event.rs:27`](../../../packages/workflow_core/src/event.rs#L27)


***

### Running

Defined in: [`packages/workflow_core/src/event.rs:28`](../../../packages/workflow_core/src/event.rs#L28)


***

### Success

Defined in: [`packages/workflow_core/src/event.rs:29`](../../../packages/workflow_core/src/event.rs#L29)


***

### Failed

Defined in: [`packages/workflow_core/src/event.rs:30`](../../../packages/workflow_core/src/event.rs#L30)


***

### Paused

Defined in: [`packages/workflow_core/src/event.rs:33`](../../../packages/workflow_core/src/event.rs#L33)

Derived from a `StepPaused` checkpoint: the step is parked at a durable
wait (approval / sleep) until a matching `StepResume` arrives.

## Trait Implementations

- `impl Borrow for StepStatus`
- `impl BorrowMut for StepStatus`
- `impl CloneToUninit for StepStatus`
- `impl Into for StepStatus`
- `impl From for StepStatus`
- `impl TryInto for StepStatus`
- `impl TryFrom for StepStatus`
- `impl Any for StepStatus`
- `impl ToOwned for StepStatus`
- `impl Debug for StepStatus`
- `impl Clone for StepStatus`
- `impl Copy for StepStatus`
- `impl StructuralPartialEq for StepStatus`
- `impl PartialEq for StepStatus`
- `impl Eq for StepStatus`
- `impl Default for StepStatus`
- `impl Serialize for StepStatus`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

