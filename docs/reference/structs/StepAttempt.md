---
id: StepAttempt
title: StepAttempt
---

# Struct: StepAttempt

Defined in: [`packages/workflow_core/src/event.rs:38`](../../../packages/workflow_core/src/event.rs#L38)

A single execution attempt of one step (used for retry bookkeeping).

## Fields

### attempt

```rust
attempt: usize
```

Defined in: [`packages/workflow_core/src/event.rs:40`](../../../packages/workflow_core/src/event.rs#L40)

1-based attempt number.


***

### started_at

```rust
started_at: i64
```

Defined in: [`packages/workflow_core/src/event.rs:41`](../../../packages/workflow_core/src/event.rs#L41)


***

### finished_at

```rust
finished_at: i64
```

Defined in: [`packages/workflow_core/src/event.rs:42`](../../../packages/workflow_core/src/event.rs#L42)


***

### result

```rust
result: Option<Value>
```

Defined in: [`packages/workflow_core/src/event.rs:43`](../../../packages/workflow_core/src/event.rs#L43)


***

### error

```rust
error: Option<RunError>
```

Defined in: [`packages/workflow_core/src/event.rs:46`](../../../packages/workflow_core/src/event.rs#L46)

结构化错误（对齐 TanStack `StepAttempt.error?: SerializedError`，
`types.ts:261`）。

## Trait Implementations

- `impl Borrow for StepAttempt`
- `impl BorrowMut for StepAttempt`
- `impl CloneToUninit for StepAttempt`
- `impl Into for StepAttempt`
- `impl From for StepAttempt`
- `impl TryInto for StepAttempt`
- `impl TryFrom for StepAttempt`
- `impl Any for StepAttempt`
- `impl ToOwned for StepAttempt`
- `impl DeserializeOwned for StepAttempt`
- `impl Debug for StepAttempt`
- `impl Clone for StepAttempt`
- `impl Serialize for StepAttempt`
- `impl Deserialize for StepAttempt`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

