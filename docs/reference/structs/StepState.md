---
id: StepState
title: StepState
---

# Struct: StepState

Defined in: [`packages/workflow_core/src/event.rs:236`](../../../packages/workflow_core/src/event.rs#L236)

Derived per-step state reconstructed from an event log.

## Fields

### status

```rust
status: StepStatus
```

Defined in: [`packages/workflow_core/src/event.rs:237`](../../../packages/workflow_core/src/event.rs#L237)


***

### result

```rust
result: Option<Value>
```

Defined in: [`packages/workflow_core/src/event.rs:238`](../../../packages/workflow_core/src/event.rs#L238)


***

### error

```rust
error: Option<RunError>
```

Defined in: [`packages/workflow_core/src/event.rs:240`](../../../packages/workflow_core/src/event.rs#L240)

与 `StepAttempt::error` 同构（都是 `RunError`）。


***

### started_at

```rust
started_at: Option<i64>
```

Defined in: [`packages/workflow_core/src/event.rs:241`](../../../packages/workflow_core/src/event.rs#L241)


***

### finished_at

```rust
finished_at: Option<i64>
```

Defined in: [`packages/workflow_core/src/event.rs:242`](../../../packages/workflow_core/src/event.rs#L242)

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

