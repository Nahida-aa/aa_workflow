---
id: WorkflowError
title: WorkflowError
---

# Enum: WorkflowError

Defined in: `packages/workflow-core/src/error.rs:147`

Errors produced by the engine.

## Variants

### `Validation`

```rust
(String)
```

The workflow graph is invalid (duplicate ids, unknown `needs`, cycle).

### `Step`

```rust
{ .. }
```

A workflow step failed after exhausting its retry policy.

### `Finalize`

```rust
(String)
```

The finalize step failed.

### `Store`

```rust
(StoreError)
```

Transport/persistence error while talking to the store.

### `RunNotFound`

```rust
(String)
```

No `RunState` exists for the requested run id.

### `SignalLost`

```rust
(String)
```

A delivered signal could not be matched to a parked wait point (from
[`crate::engine::signal_event`]: nothing was paused on that event name).

### `Internal`

```rust
(String)
```

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

