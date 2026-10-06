---
id: StepState
title: StepState
---

# Struct: StepState

Defined in: [`packages/workflow_core/src/event.rs:294`](../../../packages/workflow_core/src/event.rs#L294)

Derived per-step state reconstructed from an event log.

## Fields

### status

```rust
status: StepStatus
```

Defined in: [`packages/workflow_core/src/event.rs:295`](../../../packages/workflow_core/src/event.rs#L295)


***

### result

```rust
result: Option<Value>
```

Defined in: [`packages/workflow_core/src/event.rs:296`](../../../packages/workflow_core/src/event.rs#L296)


***

### error

```rust
error: Option<RunError>
```

Defined in: [`packages/workflow_core/src/event.rs:298`](../../../packages/workflow_core/src/event.rs#L298)

与 `StepAttempt::error` 同构（都是 `RunError`）。


***

### started_at

```rust
started_at: Option<i64>
```

Defined in: [`packages/workflow_core/src/event.rs:299`](../../../packages/workflow_core/src/event.rs#L299)


***

### finished_at

```rust
finished_at: Option<i64>
```

Defined in: [`packages/workflow_core/src/event.rs:300`](../../../packages/workflow_core/src/event.rs#L300)

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

