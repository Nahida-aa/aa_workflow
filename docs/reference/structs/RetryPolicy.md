---
id: RetryPolicy
title: RetryPolicy
---

# Struct: RetryPolicy

Defined in: [`packages/workflow_core/src/define/mod.rs:490`](../../../packages/workflow_core/src/define/mod.rs#L490)

## Fields

### max_attempts

```rust
max_attempts: usize
```

Defined in: [`packages/workflow_core/src/define/mod.rs:492`](../../../packages/workflow_core/src/define/mod.rs#L492)

Total attempts, including the first. 1 == no retry.


***

### backoff

```rust
backoff: Backoff
```

Defined in: [`packages/workflow_core/src/define/mod.rs:493`](../../../packages/workflow_core/src/define/mod.rs#L493)


***

### durable

```rust
durable: bool
```

Defined in: [`packages/workflow_core/src/define/mod.rs:496`](../../../packages/workflow_core/src/define/mod.rs#L496)

耐久重试（#19 `retry.durable`）：attempt/失败/下次截止时间持久化，
重启后经 sweep 续跑。false = 内存级重试（现状）。

## Implementations

### new()

```rust
pub fn new(max_attempts: usize, backoff: Backoff) -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:500`](../../../packages/workflow_core/src/define/mod.rs#L500)

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

