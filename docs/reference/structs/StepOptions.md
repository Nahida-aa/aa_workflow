---
id: StepOptions
title: StepOptions
---

# Struct: StepOptions

Defined in: [`packages/workflow-core/src/define/mod.rs:409`](../../../packages/workflow-core/src/define/mod.rs#L409)

Per-step configuration. `retry`/`timeout`/`resource`/`up_to_date` are
per-call-site options (v2), whereas TanStack passes them as inline
`step(id, fn, opts)`. Everything here is orthogonal to the durable log.

## Fields

### label

```rust
label: Option<String>
```

Defined in: [`packages/workflow-core/src/define/mod.rs:410`](../../../packages/workflow-core/src/define/mod.rs#L410)


***

### retry

```rust
retry: Option<RetryPolicy>
```

Defined in: [`packages/workflow-core/src/define/mod.rs:411`](../../../packages/workflow-core/src/define/mod.rs#L411)


***

### timeout

```rust
timeout: Option<Duration>
```

Defined in: [`packages/workflow-core/src/define/mod.rs:412`](../../../packages/workflow-core/src/define/mod.rs#L412)


***

### resource

```rust
resource: Option<String>
```

Defined in: [`packages/workflow-core/src/define/mod.rs:415`](../../../packages/workflow-core/src/define/mod.rs#L415)

Resource key this step contends on (e.g. `"gpu:0"`). Steps with the
same key are serialized (capacity-1 gate, sibling of a mutex).


***

### up_to_date

```rust
up_to_date: Option<Arc<dyn Fn(&StepCtx, &StepState) -> bool + Send + Sync>>
```

Defined in: [`packages/workflow-core/src/define/mod.rs:420`](../../../packages/workflow-core/src/define/mod.rs#L420)

Make-style freshness check against the *derived* step state. `Ok(true)`
while a successful checkpoint exists ⇒ the step short-circuits; if it
reports stale a step still re-executes on resume. Defaults to the
durable checkpoint only.

## Implementations

### new()

```rust
pub fn new() -> Self
```

Defined in: [`packages/workflow-core/src/define/mod.rs:424`](../../../packages/workflow-core/src/define/mod.rs#L424)

#### Returns

`Self`


***

### retry()

```rust
pub fn retry(self, retry: RetryPolicy) -> Self
```

Defined in: [`packages/workflow-core/src/define/mod.rs:428`](../../../packages/workflow-core/src/define/mod.rs#L428)

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

Defined in: [`packages/workflow-core/src/define/mod.rs:433`](../../../packages/workflow-core/src/define/mod.rs#L433)

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

Defined in: [`packages/workflow-core/src/define/mod.rs:438`](../../../packages/workflow-core/src/define/mod.rs#L438)

#### Parameters

##### resource

`impl ?`

#### Returns

`Self`


***

### up_to_date()

```rust
pub fn up_to_date<impl Fn(&StepCtx, &StepState) -> bool + Send + Sync + 'static: Fn(&StepCtx, &StepState) -> bool + Send + Sync + 'static>(self, up_to_date: impl ? + ? + ? + ?) -> Self
```

Defined in: [`packages/workflow-core/src/define/mod.rs:443`](../../../packages/workflow-core/src/define/mod.rs#L443)

#### Parameters

##### up_to_date

`impl ? + ? + ? + ?`

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

