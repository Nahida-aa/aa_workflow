---
id: StepState
title: StepState
---

# Struct: StepState

Defined in: [`packages/workflow_core/src/event.rs:364`](../../../packages/workflow_core/src/event.rs#L364)

Derived per-step state reconstructed from an event log.

## Fields

### status

```rust
status: StepStatus
```

Defined in: [`packages/workflow_core/src/event.rs:365`](../../../packages/workflow_core/src/event.rs#L365)


***

### result

```rust
result: Option<Value>
```

Defined in: [`packages/workflow_core/src/event.rs:366`](../../../packages/workflow_core/src/event.rs#L366)


***

### error

```rust
error: Option<RunError>
```

Defined in: [`packages/workflow_core/src/event.rs:368`](../../../packages/workflow_core/src/event.rs#L368)

与 `StepAttempt::error` 同构（都是 `RunError`）。


***

### started_at

```rust
started_at: Option<i64>
```

Defined in: [`packages/workflow_core/src/event.rs:369`](../../../packages/workflow_core/src/event.rs#L369)


***

### finished_at

```rust
finished_at: Option<i64>
```

Defined in: [`packages/workflow_core/src/event.rs:370`](../../../packages/workflow_core/src/event.rs#L370)

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

