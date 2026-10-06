---
id: StepState
title: StepState
---

# Struct: StepState

Defined in: [`packages/workflow_core/src/event.rs:317`](../../../packages/workflow_core/src/event.rs#L317)

Derived per-step state reconstructed from an event log.

## Fields

### status

```rust
status: StepStatus
```

Defined in: [`packages/workflow_core/src/event.rs:318`](../../../packages/workflow_core/src/event.rs#L318)


***

### result

```rust
result: Option<Value>
```

Defined in: [`packages/workflow_core/src/event.rs:319`](../../../packages/workflow_core/src/event.rs#L319)


***

### error

```rust
error: Option<RunError>
```

Defined in: [`packages/workflow_core/src/event.rs:321`](../../../packages/workflow_core/src/event.rs#L321)

与 `StepAttempt::error` 同构（都是 `RunError`）。


***

### started_at

```rust
started_at: Option<i64>
```

Defined in: [`packages/workflow_core/src/event.rs:322`](../../../packages/workflow_core/src/event.rs#L322)


***

### finished_at

```rust
finished_at: Option<i64>
```

Defined in: [`packages/workflow_core/src/event.rs:323`](../../../packages/workflow_core/src/event.rs#L323)

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

