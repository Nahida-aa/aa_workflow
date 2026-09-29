---
id: StepCtx
title: StepCtx
---

# Struct: StepCtx

Defined in: `packages/workflow-core/src/define/mod.rs:43`

Per-invocation view handed step closures. Mirrors TanStack's `StepContext`
member-for-member: `id` / `attempt` / `runtime` helpers, with the per-attempt
`signal` exposed as a cooperative [`is_cancelled`](Self::is_cancelled) poll.

## Fields

### `id`

```rust
id: String
```

Deterministic step ID. Stable across retries *and* replays of the same
run — the idempotency-key candidate for external systems (`id`).

### `attempt`

```rust
attempt: usize
```

Current attempt number (1-indexed). 0 only for internal probes (e.g.
the `up_to_date` make-check).

_（存在非公开字段）_

## Implementations

### `run_id`

```rust
pub fn run_id(&self) -> &str
```

Defined in: `packages/workflow-core/src/define/mod.rs:54`

### `input`

```rust
pub fn input(&self) -> &Value
```

Defined in: `packages/workflow-core/src/define/mod.rs:57`

### `progress`

```rust
pub fn progress(&self, value: f64)
```

Defined in: `packages/workflow-core/src/define/mod.rs:60`

### `is_cancelled`

```rust
pub fn is_cancelled(&self) -> bool
```

Whether this run was cancelled via [`cancel_run`](crate::engine::cancel_run).
Cooperative poll counterpart to TanStack's per-attempt `signal` — Rust
cannot interrupt an in-flight future, so long steps must self-check on
their own cadence (same granularity as `BaseCtx::is_cancelled`).

Defined in: `packages/workflow-core/src/define/mod.rs:67`

### `deadline`

```rust
pub fn deadline(&self) -> Option<i64>
```

Absolute UTC ms runtime budget for this drive (TanStack
`runtime.deadline`); `None` when unbudgeted.

Defined in: `packages/workflow-core/src/define/mod.rs:78`

### `time_remaining`

```rust
pub fn time_remaining(&self) -> u64
```

Ms of runtime budget left (`u64::MAX` when no deadline).

Defined in: `packages/workflow-core/src/define/mod.rs:82`

### `should_yield`

```rust
pub fn should_yield(&self) -> bool
```

True once the budget is nearly exhausted (`time_remaining() <
min_yield_remaining_ms`); the step should `?` fast or orchestrate
yielding via `WorkflowCtx::yield_`.

Defined in: `packages/workflow-core/src/define/mod.rs:91`

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

