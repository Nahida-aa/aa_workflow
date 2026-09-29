---
id: Backoff
title: Backoff
---

# Enum: Backoff

Defined in: [`packages/workflow-core/src/define/mod.rs:370`](../../../packages/workflow-core/src/define/mod.rs#L370)

Fixed | exponential | custom backoff. `attempt` is 1-based: after attempt
#N fails, we wait `delay_ms(N)` before attempt N+1 (exponential: base * 2^(N-1)).

## Variants

### Fixed

```rust
{ .. }
```

Defined in: [`packages/workflow-core/src/define/mod.rs:371`](../../../packages/workflow-core/src/define/mod.rs#L371)


***

### Exponential

```rust
{ .. }
```

Defined in: [`packages/workflow-core/src/define/mod.rs:372`](../../../packages/workflow-core/src/define/mod.rs#L372)


***

### Custom

```rust
(Arc<dyn Fn(usize) -> u64 + Send + Sync>)
```

Defined in: [`packages/workflow-core/src/define/mod.rs:373`](../../../packages/workflow-core/src/define/mod.rs#L373)

## Implementations

### delay_ms()

```rust
pub fn delay_ms(&self, attempt: usize) -> u64
```

Defined in: [`packages/workflow-core/src/define/mod.rs:377`](../../../packages/workflow-core/src/define/mod.rs#L377)

#### Parameters

##### attempt

`usize`

#### Returns

`u64`

## Trait Implementations

- `impl Borrow for Backoff`
- `impl BorrowMut for Backoff`
- `impl CloneToUninit for Backoff`
- `impl Into for Backoff`
- `impl From for Backoff`
- `impl TryInto for Backoff`
- `impl TryFrom for Backoff`
- `impl Any for Backoff`
- `impl ToOwned for Backoff`
- `impl Clone for Backoff`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

