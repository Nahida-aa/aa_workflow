---
id: WorkflowBuilder
title: WorkflowBuilder
---

# Struct: WorkflowBuilder

Defined in: `packages/workflow-core/src/define/define_workflow.rs:181`

The typed builder — `TInput`/`TOutput`/`TState` mirror the workflow's schema
at build time, `TCtxExt` is the ctx extension type declared by the last
[`middleware`](Self::middleware) call (`()` when none).

_（存在非公开字段）_

## Implementations

### `middleware`

```rust
pub fn middleware<PExt>(self, m: Middleware) -> WorkflowBuilder<TInput, TOutput, TState, PExt>
```

Attach a runtime middleware and re-key `TCtxExt` to `PExt` — the type of
`ctx.ext` the handler reads (`produce`'s JSON output is deserialized
into it; [`Default`] is used when the middleware has no `produce`).
TanStack's intersection of extension types collapses to this single
bundle; extra `wrap`s still compose in registration order.

Defined in: `packages/workflow-core/src/define/define_workflow.rs:194`

### `previous_versions`

```rust
pub fn previous_versions(self, v: Vec<Workflow>) -> Self
```

Older versions of the same workflow to route resumed runs to (see
[`select_workflow_version`](crate::engine::select_workflow_version)).

Defined in: `packages/workflow-core/src/define/define_workflow.rs:209`

### `handler`

```rust
pub fn handler<F, Fut, AOut>(self, handler: F) -> WorkflowDefinition<TInput, AOut, TState, TCtxExt>
```

Finalize with the orchestrating closure. `AOut` is inferred from the
handler's return value; the engine stores it serialized as JSON, so on
resume the handler re-runs from scratch with re-parsed input/state and a
freshly `produce`d extension (same as TanStack).

Defined in: `packages/workflow-core/src/define/define_workflow.rs:218`

## Trait Implementations

- `impl Borrow for WorkflowBuilder`
- `impl BorrowMut for WorkflowBuilder`
- `impl Into for WorkflowBuilder`
- `impl From for WorkflowBuilder`
- `impl TryInto for WorkflowBuilder`
- `impl TryFrom for WorkflowBuilder`
- `impl Any for WorkflowBuilder`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

