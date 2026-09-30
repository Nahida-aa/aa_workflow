---
id: StepOptions
title: StepOptions
---

# Struct: StepOptions

Defined in: [`packages/workflow_core/src/define/mod.rs:468`](../../../packages/workflow_core/src/define/mod.rs#L468)

Per-step configuration. `retry`/`timeout`/`resource`/`up_to_date` are
per-call-site options (v2), whereas TanStack passes them as inline
`step(id, fn, opts)`. Everything here is orthogonal to the durable log.

## Fields

### label

```rust
label: Option<String>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:469`](../../../packages/workflow_core/src/define/mod.rs#L469)


***

### retry

```rust
retry: Option<RetryPolicy>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:470`](../../../packages/workflow_core/src/define/mod.rs#L470)


***

### timeout

```rust
timeout: Option<Duration>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:471`](../../../packages/workflow_core/src/define/mod.rs#L471)


***

### resource

```rust
resource: Option<String>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:474`](../../../packages/workflow_core/src/define/mod.rs#L474)

Resource key this step contends on (e.g. `"gpu:0"`). Steps with the
same key are serialized (capacity-1 gate, sibling of a mutex).


***

### up_to_date

```rust
up_to_date: Option<Arc<dyn Fn(&StepCtx, &StepState) -> bool + Send + Sync>>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:479`](../../../packages/workflow_core/src/define/mod.rs#L479)

Make-style freshness check against the *derived* step state. `Ok(true)`
while a successful checkpoint exists ⇒ the step short-circuits; if it
reports stale a step still re-executes on resume. Defaults to the
durable checkpoint only.

## Implementations

### new()

```rust
pub fn new() -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:483`](../../../packages/workflow_core/src/define/mod.rs#L483)

#### Returns

`Self`


***

### retry()

```rust
pub fn retry(self, retry: RetryPolicy) -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:487`](../../../packages/workflow_core/src/define/mod.rs#L487)

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

Defined in: [`packages/workflow_core/src/define/mod.rs:492`](../../../packages/workflow_core/src/define/mod.rs#L492)

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

Defined in: [`packages/workflow_core/src/define/mod.rs:497`](../../../packages/workflow_core/src/define/mod.rs#L497)

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

Defined in: [`packages/workflow_core/src/define/mod.rs:502`](../../../packages/workflow_core/src/define/mod.rs#L502)

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

