---
id: RetryPolicy
title: RetryPolicy
---

# Struct: RetryPolicy

Defined in: [`packages/workflow_core/src/define/mod.rs:450`](../../../packages/workflow_core/src/define/mod.rs#L450)

## Fields

### max_attempts

```rust
max_attempts: usize
```

Defined in: [`packages/workflow_core/src/define/mod.rs:452`](../../../packages/workflow_core/src/define/mod.rs#L452)

Total attempts, including the first. 1 == no retry.


***

### backoff

```rust
backoff: Backoff
```

Defined in: [`packages/workflow_core/src/define/mod.rs:453`](../../../packages/workflow_core/src/define/mod.rs#L453)

## Implementations

### new()

```rust
pub fn new(max_attempts: usize, backoff: Backoff) -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:457`](../../../packages/workflow_core/src/define/mod.rs#L457)

#### Parameters

##### max_attempts

`usize`

##### backoff

[`Backoff`](../enums/Backoff.md)

#### Returns

`Self`

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

