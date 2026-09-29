---
id: RetryPolicy
title: RetryPolicy
---

# Struct: RetryPolicy

Defined in: `packages/workflow-core/src/define/mod.rs:390`

## Fields

### `max_attempts`

```rust
max_attempts: usize
```

Total attempts, including the first. 1 == no retry.

### `backoff`

```rust
backoff: Backoff
```

## Implementations

### `new`

```rust
pub fn new(max_attempts: usize, backoff: Backoff) -> Self
```

Defined in: `packages/workflow-core/src/define/mod.rs:397`

## Trait Implementations

- `impl Borrow for RetryPolicy`
- `impl BorrowMut for RetryPolicy`
- `impl CloneToUninit for RetryPolicy`
- `impl Into for RetryPolicy`
- `impl From for RetryPolicy`
- `impl TryInto for RetryPolicy`
- `impl TryFrom for RetryPolicy`
- `impl Any for RetryPolicy`
- `impl ToOwned for RetryPolicy`
- `impl Clone for RetryPolicy`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

