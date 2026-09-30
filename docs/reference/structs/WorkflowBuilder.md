---
id: WorkflowBuilder
title: WorkflowBuilder
---

# Struct: WorkflowBuilder

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:214`](../../../packages/workflow_core/src/define/define_workflow.rs#L214)

The typed builder — `TInput`/`TOutput`/`TState` mirror the workflow's schema
at build time, `TCtxExt` is the ctx extension type declared by the last
[`middleware`](WorkflowBuilder.md) call (`()` when none).

_（存在非公开字段）_

## Implementations

### middleware()

```rust
pub fn middleware<PExt>(self, m: Middleware) -> WorkflowBuilder<TInput, TOutput, TState, PExt>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:227`](../../../packages/workflow_core/src/define/define_workflow.rs#L227)

Attach a runtime middleware and re-key `TCtxExt` to `PExt` — the type of
`ctx.ext` the handler reads (`produce`'s JSON output is deserialized
into it; `Default` is used when the middleware has no `produce`).
TanStack's intersection of extension types collapses to this single
bundle; extra `wrap`s still compose in registration order.

#### Parameters

##### m

[`Middleware`](Middleware.md)

#### Returns

[`WorkflowBuilder<TInput, TOutput, TState, PExt>`](WorkflowBuilder.md)


***

### previous_versions()

```rust
pub fn previous_versions<impl Into<AnyWorkflowDefinition>: Into, impl IntoIterator<Item = impl Into<AnyWorkflowDefinition>>: IntoIterator>(self, v: impl ?) -> Self
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:242`](../../../packages/workflow_core/src/define/define_workflow.rs#L242)

Older versions of the same workflow to route resumed runs to (see
[`select_workflow_version`](../functions/select_workflow_version.md)).

#### Parameters

##### v

`impl ?`

#### Returns

`Self`


***

### handler()

```rust
pub fn handler<F, Fut, AOut>(self, handler: F) -> WorkflowDefinition<TInput, AOut, TState, TCtxExt>
where
    TInput: DeserializeOwned + Send + Sync + 'static,
    TState: DeserializeOwned + Serialize + Send + Sync + 'static,
    TCtxExt: DeserializeOwned + Default + Send + Sync + 'static,
    AOut: Serialize + Send + Sync + 'static,
    F: Fn(BaseCtx<TInput, TState, TCtxExt>) -> Fut + Send + Sync + 'static,
    Fut: Future + Send + 'static
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:254`](../../../packages/workflow_core/src/define/define_workflow.rs#L254)

Finalize with the orchestrating closure. `AOut` is inferred from the
handler's return value; the engine stores it serialized as JSON, so on
resume the handler re-runs from scratch with re-parsed input/state and a
freshly `produce`d extension (same as TanStack).

#### Parameters

##### handler

`F`

#### Returns

[`WorkflowDefinition<TInput, AOut, TState, TCtxExt>`](WorkflowDefinition.md)

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

