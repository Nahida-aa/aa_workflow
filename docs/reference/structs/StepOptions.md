---
id: StepOptions
title: StepOptions
---

# Struct: StepOptions

Defined in: [`packages/workflow_core/src/define/mod.rs:517`](../../../packages/workflow_core/src/define/mod.rs#L517)

Per-step configuration. `retry`/`timeout` match upstream `StepOptions`
(upstream passes them as inline `step(id, fn, opts)`); `retry.durable`
maps to upstream `StepRetryOptions.durable`. `resource` is a local
extension. Everything here is orthogonal to the durable log.

## Fields

### retry

```rust
retry: Option<RetryPolicy>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:518`](../../../packages/workflow_core/src/define/mod.rs#L518)


***

### timeout

```rust
timeout: Option<Duration>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:519`](../../../packages/workflow_core/src/define/mod.rs#L519)


***

### resource

```rust
resource: Option<String>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:522`](../../../packages/workflow_core/src/define/mod.rs#L522)

Resource key this step contends on (e.g. `"gpu:0"`). Steps with the
same key are serialized (capacity-1 gate, sibling of a mutex).

## Implementations

### new()

```rust
pub fn new() -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:526`](../../../packages/workflow_core/src/define/mod.rs#L526)

#### Returns

`Self`


***

### retry()

```rust
pub fn retry(self, retry: RetryPolicy) -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:530`](../../../packages/workflow_core/src/define/mod.rs#L530)

#### Parameters

##### retry

[`RetryPolicy`](RetryPolicy.md)

#### Returns

`Self`


***

### timeout()

```rust
pub fn timeout(self, timeout: Duration) -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:535`](../../../packages/workflow_core/src/define/mod.rs#L535)

#### Parameters

##### timeout

`Duration`

#### Returns

`Self`


***

### resource()

```rust
pub fn resource<impl Into<String>: Into>(self, resource: impl ?) -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:540`](../../../packages/workflow_core/src/define/mod.rs#L540)

#### Parameters

##### resource

`impl ?`

#### Returns

`Self`

## Trait Implementations

- `impl Borrow for StepOptions`
- `impl BorrowMut for StepOptions`
- `impl CloneToUninit for StepOptions`
- `impl Into for StepOptions`
- `impl From for StepOptions`
- `impl TryInto for StepOptions`
- `impl TryFrom for StepOptions`
- `impl Any for StepOptions`
- `impl ToOwned for StepOptions`
- `impl Clone for StepOptions`
- `impl Default for StepOptions`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

