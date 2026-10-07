---
id: StepOptions
title: StepOptions
---

# Struct: StepOptions

Defined in: [`packages/workflow_core/src/define/mod.rs:505`](../../../packages/workflow_core/src/define/mod.rs#L505)

Per-step configuration. `retry`/`timeout` match upstream `StepOptions`
(upstream passes them as inline `step(id, fn, opts)`); `retry.durable`
maps to upstream `StepRetryOptions.durable`. `resource` is a local
extension. Everything here is orthogonal to the durable log.

## Fields

### retry

```rust
retry: Option<RetryPolicy>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:506`](../../../packages/workflow_core/src/define/mod.rs#L506)


***

### timeout

```rust
timeout: Option<Duration>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:507`](../../../packages/workflow_core/src/define/mod.rs#L507)


***

### resource

```rust
resource: Option<String>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:510`](../../../packages/workflow_core/src/define/mod.rs#L510)

Resource key this step contends on (e.g. `"gpu:0"`). Steps with the
same key are serialized (capacity-1 gate, sibling of a mutex).

## Implementations

### new()

```rust
pub fn new() -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:514`](../../../packages/workflow_core/src/define/mod.rs#L514)

#### Returns

`Self`


***

### retry()

```rust
pub fn retry(self, retry: RetryPolicy) -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:518`](../../../packages/workflow_core/src/define/mod.rs#L518)

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

Defined in: [`packages/workflow_core/src/define/mod.rs:523`](../../../packages/workflow_core/src/define/mod.rs#L523)

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

Defined in: [`packages/workflow_core/src/define/mod.rs:528`](../../../packages/workflow_core/src/define/mod.rs#L528)

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

