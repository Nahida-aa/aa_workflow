---
id: WorkflowDefinition
title: WorkflowDefinition
---

# Struct: WorkflowDefinition

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:347`](../../../packages/workflow_core/src/define/define_workflow.rs#L347)

A workflow whose `TInput`/`TOutput`/`TState`/`TCtxExt` are statically known at the
declaration site. `Deref<Target = Workflow>` lets it be handed to
[`crate::engine::run_workflow`](../functions/run_workflow.md) / the registry directly; explicit erasure is
[`into_workflow`](WorkflowDefinition.md).

_（存在非公开字段）_

## Implementations

### into_workflow()

```rust
pub fn into_workflow(self) -> Workflow
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:357`](../../../packages/workflow_core/src/define/define_workflow.rs#L357)

Type-erase back to the engine's [`Workflow`](Workflow.md) view.

#### Returns

[`Workflow`](Workflow.md)

## Trait Implementations

- `impl Borrow for WorkflowDefinition`
- `impl BorrowMut for WorkflowDefinition`
- `impl CloneToUninit for WorkflowDefinition`
- `impl Into for WorkflowDefinition`
- `impl From for WorkflowDefinition`
- `impl TryInto for WorkflowDefinition`
- `impl TryFrom for WorkflowDefinition`
- `impl Receiver for WorkflowDefinition`
- `impl Any for WorkflowDefinition`
- `impl ToOwned for WorkflowDefinition`
- `impl Clone for WorkflowDefinition`
- `impl Deref for WorkflowDefinition`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

