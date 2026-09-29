---
id: StepStatus
title: StepStatus
---

# Enum: StepStatus

Defined in: `packages/workflow-core/src/event.rs:25`

Per-step lifecycle status, derived from the event log via
[`fold_step_states`]. This is a *projection* of the log, never stored
independently.

## Variants

### `Pending`

### `Running`

### `Success`

### `Failed`

### `Paused`

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

