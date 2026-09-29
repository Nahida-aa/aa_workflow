---
id: StepState
title: StepState
---

# Struct: StepState

Defined in: [`packages/workflow-core/src/event.rs:234`](../../../packages/workflow-core/src/event.rs#L234)

Derived per-step state reconstructed from an event log.

## Fields

### status

```rust
status: StepStatus
```

Defined in: [`packages/workflow-core/src/event.rs:235`](../../../packages/workflow-core/src/event.rs#L235)


***

### result

```rust
result: Option<Value>
```

Defined in: [`packages/workflow-core/src/event.rs:236`](../../../packages/workflow-core/src/event.rs#L236)


***

### error

```rust
error: Option<RunError>
```

Defined in: [`packages/workflow-core/src/event.rs:238`](../../../packages/workflow-core/src/event.rs#L238)

与 `StepAttempt::error` 同构（都是 `RunError`）。


***

### started_at

```rust
started_at: Option<i64>
```

Defined in: [`packages/workflow-core/src/event.rs:239`](../../../packages/workflow-core/src/event.rs#L239)


***

### finished_at

```rust
finished_at: Option<i64>
```

Defined in: [`packages/workflow-core/src/event.rs:240`](../../../packages/workflow-core/src/event.rs#L240)

## Trait Implementations

- `impl Borrow for StepState`
- `impl BorrowMut for StepState`
- `impl CloneToUninit for StepState`
- `impl Into for StepState`
- `impl From for StepState`
- `impl TryInto for StepState`
- `impl TryFrom for StepState`
- `impl Any for StepState`
- `impl ToOwned for StepState`
- `impl Debug for StepState`
- `impl Clone for StepState`
- `impl Default for StepState`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

