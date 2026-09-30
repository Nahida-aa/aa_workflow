---
id: AnyWorkflowDefinition
title: AnyWorkflowDefinition
---

# Struct: AnyWorkflowDefinition

Defined in: [`packages/workflow_core/src/define/mod.rs:684`](../../../packages/workflow_core/src/define/mod.rs#L684)

The erased workflow, and the type every run site takes — TanStack's
`AnyWorkflowDefinition` (`types.ts:466`).

# Why this is a newtype and not an alias

Upstream that name **is** an alias: `WorkflowDefinition<any, any, any>`.
TypeScript's `any` is bidirectionally assignable, so a single type serves
both the typed declaration site and the erased run site. Rust has no such
assignability — `WorkflowDefinition<ChargeInput, Draft>` is a *different
type* from `WorkflowDefinition<Value, Value>` and neither coerces to the
other — so the erased form has to be a type of its own. That is this.

Conversion is explicit, lossless, and free (both directions go through an
`Arc`, so no deep clone):

```
use aa_workflow_core::{AnyWorkflowDefinition, CreateWorkflowConfig, WorkflowDefinition, create_workflow};

// the erased form — what a run site takes
let erased: AnyWorkflowDefinition = WorkflowDefinition::new("charge").into();
assert_eq!(erased.id, "charge");

// a typed declaration site erases just as cheaply
let typed = create_workflow(CreateWorkflowConfig::new("charge"))
    .handler(|_ctx| async { Ok(()) });
let erased: AnyWorkflowDefinition = typed.into();
assert_eq!(erased.id, "charge");
```

# Why it holds an `Arc`

`WorkflowDefinition` owns `Vec`s and several `Arc`s, so a by-value clone on
every run would deep-copy the middleware and previous-version lists. `Arc`
makes clone a refcount bump and keeps the struct lifetime-parameter-free
(lifetimes would make the builder chain very unpleasant to write).

## Trait Implementations

- `impl Borrow for AnyWorkflowDefinition`
- `impl BorrowMut for AnyWorkflowDefinition`
- `impl CloneToUninit for AnyWorkflowDefinition`
- `impl Into for AnyWorkflowDefinition`
- `impl From for AnyWorkflowDefinition`
- `impl TryInto for AnyWorkflowDefinition`
- `impl TryFrom for AnyWorkflowDefinition`
- `impl Receiver for AnyWorkflowDefinition`
- `impl Any for AnyWorkflowDefinition`
- `impl ToOwned for AnyWorkflowDefinition`
- `impl Clone for AnyWorkflowDefinition`
- `impl Deref for AnyWorkflowDefinition`
- `impl From for AnyWorkflowDefinition`
- `impl From for AnyWorkflowDefinition`
- `impl From for AnyWorkflowDefinition`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

