---
id: WorkflowError
title: WorkflowError
---

# Enum: WorkflowError

Defined in: [`packages/workflow_core/src/error.rs:150`](../../../packages/workflow_core/src/error.rs#L150)

Errors produced by the engine.

## Variants

### Validation

```rust
(String)
```

Defined in: [`packages/workflow_core/src/error.rs:152`](../../../packages/workflow_core/src/error.rs#L152)

The workflow graph is invalid (duplicate ids, unknown `needs`, cycle).


***

### Step

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/error.rs:154`](../../../packages/workflow_core/src/error.rs#L154)

A workflow step failed after exhausting its retry policy.


***

### Finalize

```rust
(String)
```

Defined in: [`packages/workflow_core/src/error.rs:156`](../../../packages/workflow_core/src/error.rs#L156)

The finalize step failed.


***

### Store

```rust
(StoreError)
```

Defined in: [`packages/workflow_core/src/error.rs:158`](../../../packages/workflow_core/src/error.rs#L158)

Transport/persistence error while talking to the store.


***

### RunNotFound

```rust
(String)
```

Defined in: [`packages/workflow_core/src/error.rs:160`](../../../packages/workflow_core/src/error.rs#L160)

No `RunState` exists for the requested run id.


***

### SignalLost

```rust
(String)
```

Defined in: [`packages/workflow_core/src/error.rs:163`](../../../packages/workflow_core/src/error.rs#L163)

A delivered signal could not be matched to a parked wait point (from
[`crate::engine::signal_event`](../functions/signal_event.md): nothing was paused on that event name).


***

### Internal

```rust
(String)
```

Defined in: [`packages/workflow_core/src/error.rs:165`](../../../packages/workflow_core/src/error.rs#L165)

An internal invariant was violated (deadlock guard, channel closed).

## Trait Implementations

- `impl Borrow for WorkflowError`
- `impl BorrowMut for WorkflowError`
- `impl Into for WorkflowError`
- `impl From for WorkflowError`
- `impl TryInto for WorkflowError`
- `impl TryFrom for WorkflowError`
- `impl Any for WorkflowError`
- `impl ToString for WorkflowError`
- `impl Debug for WorkflowError`
- `impl Display for WorkflowError`
- `impl Error for WorkflowError`
- `impl From for WorkflowError`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

