---
id: WorkflowBuilder
title: WorkflowBuilder
---

# Struct: WorkflowBuilder

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:222`](../../../packages/workflow_core/src/define/define_workflow.rs#L222)

The typed builder — `TInput`/`TOutput`/`TState` mirror the workflow's schema
at build time, `TCtxExt` is the ctx extension type declared by the last
[`middleware`](WorkflowBuilder.md) call (`()` when none).

_（存在非公开字段）_

## Implementations

### middleware()

```rust
pub fn middleware<PExt>(self, m: Middleware) -> WorkflowBuilder<TInput, TOutput, TState, PExt>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:247`](../../../packages/workflow_core/src/define/define_workflow.rs#L247)

Attach a runtime middleware and re-key `TCtxExt` to `PExt` — the type of
`ctx.ext` the handler reads. Every middleware's `produce` runs and its
JSON output shallow-merges into the bundle in registration order (later
keys win, matching TanStack's `Object.assign` accumulation), so with
several producing middlewares each returns its own part and the *last*
`.middleware::<PExt>()` call fixes the bundle type covering all parts;
`Default` applies when no middleware has a `produce`. TanStack's
intersection of extension types collapses to this single bundle, and a
`produce` cannot read earlier middlewares' extensions (upstream can, via
the shared ctx); extra `wrap`s still compose in registration order.

The collapse is deliberate: middleware lists are dynamic, so the wrap
chain is erased and extension-data flow is validated at runtime — the
same tradeoff as axum's `Router::layer` over erased `Route` services
(and axum's own extension injection is runtime-checked too). TS's
compile-time `UnionToIntersection` has no Rust equivalent worth
building; see `crate::middleware` for the full rationale.

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

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:262`](../../../packages/workflow_core/src/define/define_workflow.rs#L262)

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

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:274`](../../../packages/workflow_core/src/define/define_workflow.rs#L274)

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

