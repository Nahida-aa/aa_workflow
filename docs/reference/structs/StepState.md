---
id: StepState
title: StepState
---

# Struct: StepState

Defined in: `packages/workflow-core/src/event.rs:234`

Derived per-step state reconstructed from an event log.

## Fields

### `status`

```rust
status: StepStatus
```

### `result`

```rust
result: Option<Value>
```

### `error`

```rust
error: Option<RunError>
```

与 [`StepAttempt::error`] 同构（都是 `RunError`）。

### `started_at`

```rust
started_at: Option<i64>
```

### `finished_at`

```rust
finished_at: Option<i64>
```

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

