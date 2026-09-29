---
id: WorkflowError
title: WorkflowError
---

# Enum: WorkflowError

Defined in: [`packages/workflow-core/src/error.rs:147`](../../../packages/workflow-core/src/error.rs#L147)

Errors produced by the engine.

## Variants

### Validation

```rust
(String)
```

Defined in: [`packages/workflow-core/src/error.rs:149`](../../../packages/workflow-core/src/error.rs#L149)

The workflow graph is invalid (duplicate ids, unknown `needs`, cycle).


***

### Step

```rust
{ .. }
```

Defined in: [`packages/workflow-core/src/error.rs:151`](../../../packages/workflow-core/src/error.rs#L151)

A workflow step failed after exhausting its retry policy.


***

### Finalize

```rust
(String)
```

Defined in: [`packages/workflow-core/src/error.rs:153`](../../../packages/workflow-core/src/error.rs#L153)

The finalize step failed.


***

### Store

```rust
(StoreError)
```

Defined in: [`packages/workflow-core/src/error.rs:155`](../../../packages/workflow-core/src/error.rs#L155)

Transport/persistence error while talking to the store.


***

### RunNotFound

```rust
(String)
```

Defined in: [`packages/workflow-core/src/error.rs:157`](../../../packages/workflow-core/src/error.rs#L157)

No `RunState` exists for the requested run id.


***

### SignalLost

```rust
(String)
```

Defined in: [`packages/workflow-core/src/error.rs:160`](../../../packages/workflow-core/src/error.rs#L160)

A delivered signal could not be matched to a parked wait point (from
[`crate::engine::signal_event`](../functions/signal_event.md): nothing was paused on that event name).


***

### Internal

```rust
(String)
```

Defined in: [`packages/workflow-core/src/error.rs:162`](../../../packages/workflow-core/src/error.rs#L162)

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

