---
id: WorkflowDefinition
title: WorkflowDefinition
---

# Struct: WorkflowDefinition

Defined in: `packages/workflow-core/src/define/define_workflow.rs:326`

A workflow whose `In`/`Out`/`St`/`Ext` are statically known at the
declaration site. `Deref<Target = Workflow>` lets it be handed to
[`crate::engine::run_workflow`] / the registry directly; explicit erasure is
[`into_workflow`](Self::into_workflow).

_（存在非公开字段）_

## Implementations

### `into_workflow`

```rust
pub fn into_workflow(self) -> Workflow
```

Type-erase back to the engine's [`Workflow`] view.

Defined in: `packages/workflow-core/src/define/define_workflow.rs:336`

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

