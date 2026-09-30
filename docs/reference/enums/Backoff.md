---
id: Backoff
title: Backoff
---

# Enum: Backoff

Defined in: [`packages/workflow_core/src/define/mod.rs:429`](../../../packages/workflow_core/src/define/mod.rs#L429)

Fixed | exponential | custom backoff. `attempt` is 1-based: after attempt
#N fails, we wait `delay_ms(N)` before attempt N+1 (exponential: base * 2^(N-1)).

## Variants

### Fixed

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/define/mod.rs:430`](../../../packages/workflow_core/src/define/mod.rs#L430)


***

### Exponential

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/define/mod.rs:431`](../../../packages/workflow_core/src/define/mod.rs#L431)


***

### Custom

```rust
(Arc<dyn Fn(usize) -> u64 + Send + Sync>)
```

Defined in: [`packages/workflow_core/src/define/mod.rs:432`](../../../packages/workflow_core/src/define/mod.rs#L432)

## Implementations

### delay_ms()

```rust
pub fn delay_ms(&self, attempt: usize) -> u64
```

Defined in: [`packages/workflow_core/src/define/mod.rs:436`](../../../packages/workflow_core/src/define/mod.rs#L436)

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

