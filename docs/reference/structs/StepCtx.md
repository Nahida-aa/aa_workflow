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

Defined in: [`packages/workflow_core/src/define/mod.rs:49`](../../../packages/workflow_core/src/define/mod.rs#L49)

Current attempt number (1-indexed). 0 only for internal probes (e.g.
the `up_to_date` make-check).

_（存在非公开字段）_

## Implementations

### run_id()

```rust
pub fn run_id(&self) -> &str
```

Defined in: [`packages/workflow_core/src/define/mod.rs:53`](../../../packages/workflow_core/src/define/mod.rs#L53)

#### Returns

`&str`


***

### input()

```rust
pub fn input(&self) -> &Value
```

Defined in: [`packages/workflow_core/src/define/mod.rs:56`](../../../packages/workflow_core/src/define/mod.rs#L56)

#### Returns

`&Value`


***

### progress()

```rust
pub fn progress(&self, value: f64)
```

Defined in: [`packages/workflow_core/src/define/mod.rs:59`](../../../packages/workflow_core/src/define/mod.rs#L59)

#### Parameters

##### value

`f64`


***

### is_cancelled()

```rust
pub fn is_cancelled(&self) -> bool
```

Defined in: [`packages/workflow_core/src/define/mod.rs:66`](../../../packages/workflow_core/src/define/mod.rs#L66)

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

Defined in: [`packages/workflow_core/src/define/mod.rs:77`](../../../packages/workflow_core/src/define/mod.rs#L77)

Absolute UTC ms runtime budget for this drive (TanStack
`runtime.deadline`); `None` when unbudgeted.

#### Returns

`Option<i64>`


***

### time_remaining()

```rust
pub fn time_remaining(&self) -> u64
```

Defined in: [`packages/workflow_core/src/define/mod.rs:81`](../../../packages/workflow_core/src/define/mod.rs#L81)

Ms of runtime budget left (`u64::MAX` when no deadline).

#### Returns

`u64`


***

### should_yield()

```rust
pub fn should_yield(&self) -> bool
```

Defined in: [`packages/workflow_core/src/define/mod.rs:90`](../../../packages/workflow_core/src/define/mod.rs#L90)

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

