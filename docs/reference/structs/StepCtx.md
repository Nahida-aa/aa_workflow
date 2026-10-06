---
id: StepCtx
title: StepCtx
---

# Struct: StepCtx

Defined in: [`packages/workflow_core/src/define/mod.rs:42`](../../../packages/workflow_core/src/define/mod.rs#L42)

Per-invocation view handed step closures. Mirrors TanStack's `StepContext`
member-for-member: `id` / `attempt` / `runtime` helpers, with the per-attempt
`signal` exposed as a cooperative [`is_cancelled`](StepCtx.md) poll.

## Fields

### id

```rust
id: String
```

Defined in: [`packages/workflow_core/src/define/mod.rs:46`](../../../packages/workflow_core/src/define/mod.rs#L46)

Deterministic step ID. Stable across retries *and* replays of the same
run — the idempotency-key candidate for external systems (`id`).


***

### attempt

```rust
attempt: usize
```

Defined in: [`packages/workflow_core/src/define/mod.rs:48`](../../../packages/workflow_core/src/define/mod.rs#L48)

Current attempt number (1-indexed).

_（存在非公开字段）_

## Implementations

### run_id()

```rust
pub fn run_id(&self) -> &str
```

Defined in: [`packages/workflow_core/src/define/mod.rs:52`](../../../packages/workflow_core/src/define/mod.rs#L52)

#### Returns

`&str`


***

### input()

```rust
pub fn input(&self) -> &Value
```

Defined in: [`packages/workflow_core/src/define/mod.rs:55`](../../../packages/workflow_core/src/define/mod.rs#L55)

#### Returns

`&Value`


***

### progress()

```rust
pub fn progress(&self, value: f64)
```

Defined in: [`packages/workflow_core/src/define/mod.rs:58`](../../../packages/workflow_core/src/define/mod.rs#L58)

#### Parameters

##### value

`f64`


***

### is_cancelled()

```rust
pub fn is_cancelled(&self) -> bool
```

Defined in: [`packages/workflow_core/src/define/mod.rs:65`](../../../packages/workflow_core/src/define/mod.rs#L65)

Whether this run was cancelled via [`cancel_run`](../functions/cancel_run.md).
Cooperative poll counterpart to TanStack's per-attempt `signal` — Rust
cannot interrupt an in-flight future, so long steps must self-check on
their own cadence (same granularity as `BaseCtx::is_cancelled`).

#### Returns

`bool`


***

### deadline()

```rust
pub fn deadline(&self) -> Option<i64>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:76`](../../../packages/workflow_core/src/define/mod.rs#L76)

Absolute UTC ms runtime budget for this drive (TanStack
`runtime.deadline`); `None` when unbudgeted.

#### Returns

`Option<i64>`


***

### time_remaining()

```rust
pub fn time_remaining(&self) -> u64
```

Defined in: [`packages/workflow_core/src/define/mod.rs:80`](../../../packages/workflow_core/src/define/mod.rs#L80)

Ms of runtime budget left (`u64::MAX` when no deadline).

#### Returns

`u64`


***

### should_yield()

```rust
pub fn should_yield(&self) -> bool
```

Defined in: [`packages/workflow_core/src/define/mod.rs:89`](../../../packages/workflow_core/src/define/mod.rs#L89)

True once the budget is nearly exhausted (`time_remaining() <
min_yield_remaining_ms`); the step should `?` fast or orchestrate
yielding via `WorkflowCtx::yield_`.

#### Returns

`bool`

## Trait Implementations

- `impl Borrow for StepCtx`
- `impl BorrowMut for StepCtx`
- `impl CloneToUninit for StepCtx`
- `impl Into for StepCtx`
- `impl From for StepCtx`
- `impl TryInto for StepCtx`
- `impl TryFrom for StepCtx`
- `impl Any for StepCtx`
- `impl ToOwned for StepCtx`
- `impl Clone for StepCtx`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

