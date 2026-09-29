---
id: StepOptions
title: StepOptions
---

# Struct: StepOptions

Defined in: `packages/workflow-core/src/define/mod.rs:409`

Per-step configuration. `retry`/`timeout`/`resource`/`up_to_date` are
per-call-site options (v2), whereas TanStack passes them as inline
`step(id, fn, opts)`. Everything here is orthogonal to the durable log.

## Fields

### `label`

```rust
label: Option<String>
```

### `retry`

```rust
retry: Option<RetryPolicy>
```

### `timeout`

```rust
timeout: Option<Duration>
```

### `resource`

```rust
resource: Option<String>
```

Resource key this step contends on (e.g. `"gpu:0"`). Steps with the
same key are serialized (capacity-1 gate, sibling of a mutex).

### `up_to_date`

```rust
up_to_date: Option<Arc<dyn Fn(&StepCtx, &StepState) -> bool + Send + Sync>>
```

Make-style freshness check against the *derived* step state. `Ok(true)`
while a successful checkpoint exists ⇒ the step short-circuits; if it
reports stale a step still re-executes on resume. Defaults to the
durable checkpoint only.

## Implementations

### `new`

```rust
pub fn new() -> Self
```

Defined in: `packages/workflow-core/src/define/mod.rs:424`

### `retry`

```rust
pub fn retry(self, retry: RetryPolicy) -> Self
```

Defined in: `packages/workflow-core/src/define/mod.rs:428`

### `timeout`

```rust
pub fn timeout(self, timeout: Duration) -> Self
```

Defined in: `packages/workflow-core/src/define/mod.rs:433`

### `resource`

```rust
pub fn resource<impl Into<String>>(self, resource: impl ?) -> Self
```

Defined in: `packages/workflow-core/src/define/mod.rs:438`

### `up_to_date`

```rust
pub fn up_to_date<impl Fn(&StepCtx, &StepState) -> bool + Send + Sync + 'static>(self, up_to_date: impl ? + ? + ? + ?) -> Self
```

Defined in: `packages/workflow-core/src/define/mod.rs:443`

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

