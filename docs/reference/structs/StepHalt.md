---
id: StepHalt
title: StepHalt
---

# Struct: StepHalt

Defined in: [`packages/workflow_core/src/engine/mod.rs:36`](../../../packages/workflow_core/src/engine/mod.rs#L36)

Internal sentinel: the engine stopped driving the handler after
`target_step` succeeded. Becomes a `Finished` run, never `Errored`.

## Trait Implementations

- `impl Borrow for StepHalt`
- `impl BorrowMut for StepHalt`
- `impl Into for StepHalt`
- `impl From for StepHalt`
- `impl TryInto for StepHalt`
- `impl TryFrom for StepHalt`
- `impl Any for StepHalt`
- `impl ToString for StepHalt`
- `impl Debug for StepHalt`
- `impl Display for StepHalt`
- `impl Error for StepHalt`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

