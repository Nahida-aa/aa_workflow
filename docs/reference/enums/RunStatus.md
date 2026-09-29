---
id: RunStatus
title: RunStatus
---

# Enum: RunStatus

Defined in: `packages/workflow-core/src/event.rs:10`

Overall lifecycle status of a run.

## Variants

### `Running`

### `Finished`

### `Errored`

### `Aborted`

### `Paused`

The run is parked at a durable wait point (`StepPaused`), e.g. an
approval or a sleep; it resumes when a `StepResume` arrives.

## Trait Implementations

- `impl Borrow for RunStatus`
- `impl BorrowMut for RunStatus`
- `impl CloneToUninit for RunStatus`
- `impl Into for RunStatus`
- `impl From for RunStatus`
- `impl TryInto for RunStatus`
- `impl TryFrom for RunStatus`
- `impl Any for RunStatus`
- `impl ToOwned for RunStatus`
- `impl DeserializeOwned for RunStatus`
- `impl Debug for RunStatus`
- `impl Clone for RunStatus`
- `impl Copy for RunStatus`
- `impl StructuralPartialEq for RunStatus`
- `impl PartialEq for RunStatus`
- `impl Eq for RunStatus`
- `impl Serialize for RunStatus`
- `impl Deserialize for RunStatus`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

