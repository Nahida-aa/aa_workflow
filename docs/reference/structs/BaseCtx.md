---
id: BaseCtx
title: BaseCtx
---

# Struct: BaseCtx

Defined in: [`packages/workflow_core/src/define/mod.rs:115`](../../../packages/workflow_core/src/define/mod.rs#L115)

The handler's argument — mirrors TanStack's `BaseCtx<TInput, TState>`
interface member-for-member. Public surface is exactly `Self::run_id`
(`runId`), `Self::input` (`input`) and `Self::state` (`state`), plus
the durable primitives as methods (`step` / `sleep` / `sleep_until` /
`wait_for_event` / `approve` / `now` / `uuid` / `emit`), a
[`is_cancelled`](BaseCtx.md) `signal` and the runtime budget
helpers ([`deadline`](BaseCtx.md) / [`time_remaining`](BaseCtx.md)
/ [`should_yield`](BaseCtx.md) / [`yield_`](BaseCtx.md)).

The engine handle is *not* part of the public API: TanStack holds it in JS
closure scope (invisible on `ctx`), so here it lives in a
`#[doc(hidden)]` crate-private field — reachable but never nameable by
callers.

State semantics: `state` is the handler's working copy, seeded from the
engine's live image at drive start and flushed back to it before every
durable primitive. `ctx.input` / `ctx.state` are plain owned fields —
clone (or move) values as needed across `await` points.

## Fields

### run_id

```rust
run_id: String
```

Defined in: [`packages/workflow_core/src/define/mod.rs:117`](../../../packages/workflow_core/src/define/mod.rs#L117)

`runId: string`


***

### input

```rust
input: TInput
```

Defined in: [`packages/workflow_core/src/define/mod.rs:119`](../../../packages/workflow_core/src/define/mod.rs#L119)

`input: TInput` — frozen run input (typed or `Value`)


***

### state

```rust
state: StateHandle<TState>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:122`](../../../packages/workflow_core/src/define/mod.rs#L122)

`state: TState` — 共享可变的 typed state（对齐 TS 的
`ctx.state === engine.state`）；见 `state_handle` 模块文档。


***

### ext

```rust
ext: TCtxExt
```

Defined in: [`packages/workflow_core/src/define/mod.rs:127`](../../../packages/workflow_core/src/define/mod.rs#L127)

`TExtensions` — ctx extension bundle, the `{...context}` accumulated by
middleware. `()` (the default) when no middleware declares one. Built by
the middleware's `produce` and re-deserialized on every drive, so it is
deterministic across resume.

_（存在非公开字段）_

## Implementations

### step()

```rust
pub async fn step<F, Fut>(&self, step_id: &str, run: F) -> Result<Value>
where
    F: FnOnce(StepCtx) -> Fut + Clone + Send + 'static,
    Fut: Future + Send + 'static,
    TState: Serialize
```

Defined in: [`packages/workflow_core/src/define/mod.rs:182`](../../../packages/workflow_core/src/define/mod.rs#L182)

Runs `run` durably under `step_id`. On replay (resume) a previously
succeeded step short-circuits to its cached result *without* calling
`run` again; a previously failed step rethrows the stored error.

The step closure is async so that concurrent steps compose via
`tokio::try_join!` — parallel durable execution, no custom primitive.

#### Parameters

##### step_id

`&str`

##### run

`F`

#### Returns

`Result<Value>`


***

### approve()

```rust
pub async fn approve<impl Into<String>: Into, impl AsRef<str>: AsRef>(&self, key: impl ?, reason: impl ?) -> Result<Value>
where
    TState: Serialize
```

Defined in: [`packages/workflow_core/src/define/mod.rs:195`](../../../packages/workflow_core/src/define/mod.rs#L195)

Durable approval wait: pauses the run until [`signal_run`](../functions/signal_run.md)
delivers a decision for `key`. `reason` is persisted in the `StepPaused`
checkpoint for approvers. Resolves to the signal's payload on success;
on replay the already-recorded `StepResume` is served from the log.

#### Parameters

##### key

`impl ?`

##### reason

`impl ?`

#### Returns

`Result<Value>`


***

### sleep()

```rust
pub async fn sleep<impl Into<String>: Into>(&self, key: impl ?, dur: Duration) -> Result<Value>
where
    TState: Serialize
```

Defined in: [`packages/workflow_core/src/define/mod.rs:219`](../../../packages/workflow_core/src/define/mod.rs#L219)

Durable sleep: pauses the run until `dur` elapses. `key` is the
deterministic pause identity. An external timer (the runtime's sweep,
indexing on `RunState.waiting_for.deadline`) delivers the resume via
[`signal_run`](../functions/signal_run.md) once the deadline passes; on
replay, a previously delivered resume short-circuits immediately.

#### Parameters

##### key

`impl ?`

##### dur

`Duration`

#### Returns

`Result<Value>`


***

### sleep_until()

```rust
pub async fn sleep_until<impl Into<String>: Into>(&self, key: impl ?, ts_ms: i64) -> Result<Value>
where
    TState: Serialize
```

Defined in: [`packages/workflow_core/src/define/mod.rs:238`](../../../packages/workflow_core/src/define/mod.rs#L238)

Durable absolute-time wait: pauses until wall-clock `ts_ms` (equivalent
to TanStack's `sleepUntil`). The timestamp is stored **verbatim** —
a timestamp already in the past parks with an expired deadline, which a
timer sweep claims on its next pass. `key` is the deterministic pause
identity.

#### Parameters

##### key

`impl ?`

##### ts_ms

`i64`

#### Returns

`Result<Value>`


***

### wait_for_event()

```rust
pub async fn wait_for_event<impl Into<String>: Into, impl AsRef<str>: AsRef>(&self, key: impl ?, event_name: impl ?) -> Result<Value>
where
    TState: Serialize
```

Defined in: [`packages/workflow_core/src/define/mod.rs:255`](../../../packages/workflow_core/src/define/mod.rs#L255)

Durable named wait: pauses the run until [`signal_event`](../functions/signal_event.md)
delivers a payload for `event_name`. `key` is the deterministic pause
identity (must not collide with step ids); `event_name` is the channel
a host signals on (`RunState.waiting_for.signal_name`). On replay a
previously delivered resume short-circuits from the log.

#### Parameters

##### key

`impl ?`

##### event_name

`impl ?`

#### Returns

`Result<Value>`


***

### emit()

```rust
pub fn emit<impl AsRef<str>: AsRef>(&self, name: impl ?, value: Value)
```

Defined in: [`packages/workflow_core/src/define/mod.rs:271`](../../../packages/workflow_core/src/define/mod.rs#L271)

Emit an observability event to the publisher. Never appended to the
log, so it is outside replay — `fold_step_states` and resume ignore it
(mirrors TanStack's `emit` / `CUSTOM`).

#### Parameters

##### name

`impl ?`

##### value

`Value`


***

### now()

```rust
pub fn now(&self) -> Result<i64>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:283`](../../../packages/workflow_core/src/define/mod.rs#L283)

Deterministic wall-clock (TanStack `ctx.now`): records the call's
timestamp as a checkpoint; replay serves the recorded value, so a run
sees the same clock across resumes. Returns `Err` only on store failure.

#### Returns

`Result<i64>`


***

### uuid()

```rust
pub fn uuid(&self) -> Result<String>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:290`](../../../packages/workflow_core/src/define/mod.rs#L290)

Deterministic id (TanStack `ctx.uuid`): records a generated UUIDv4 as a
checkpoint; replay serves the recorded id, so the same value is seen
across resumes. Returns `Err` only on store failure.

#### Returns

`Result<String>`


***

### is_cancelled()

```rust
pub fn is_cancelled(&self) -> bool
```

Defined in: [`packages/workflow_core/src/define/mod.rs:298`](../../../packages/workflow_core/src/define/mod.rs#L298)

Whether this run was cancelled via [`cancel_run`](../functions/cancel_run.md).
Polled at step boundaries only — the engine cannot interrupt a step's
in-flight `await` (same granularity as JS `AbortSignal`: the closure
must check cooperatively).

#### Returns

`bool`


***

### deadline()

```rust
pub fn deadline(&self) -> Option<i64>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:310`](../../../packages/workflow_core/src/define/mod.rs#L310)

Absolute UTC ms runtime budget for this drive (TanStack `deadline`);
`None` when the host set no budget.

#### Returns

`Option<i64>`


***

### time_remaining()

```rust
pub fn time_remaining(&self) -> u64
```

Defined in: [`packages/workflow_core/src/define/mod.rs:315`](../../../packages/workflow_core/src/define/mod.rs#L315)

Ms of runtime budget left (`u64::MAX` when no deadline).

#### Returns

`u64`


***

### should_yield()

```rust
pub fn should_yield(&self) -> bool
```

Defined in: [`packages/workflow_core/src/define/mod.rs:324`](../../../packages/workflow_core/src/define/mod.rs#L324)

True once the budget is nearly exhausted: `time_remaining() <
min_yield_remaining_ms` (default 1000ms).

#### Returns

`bool`


***

### yield_()

```rust
pub async fn yield_(&self) -> Result<Value>
where
    TState: Serialize
```

Defined in: [`packages/workflow_core/src/define/mod.rs:332`](../../../packages/workflow_core/src/define/mod.rs#L332)

Cooperative hand-back of the runtime budget (TanStack `yield`): durably
parks the run on a `"__timer"` wait until [`RunWorkflowOptions`](RunWorkflowOptions.md) 的 `yield_resume_at`
(or now+1ms), so a host can re-invoke with a freshly extended deadline.
Deterministic id `__yield-{n}` (per-invocation counter), replay-safe.

#### Returns

`Result<Value>`


***

### step_with()

```rust
pub async fn step_with<F, Fut>(&self, step_id: &str, opts: StepOptions, run: F) -> Result<Value>
where
    F: FnOnce(StepCtx) -> Fut + Clone + Send + 'static,
    Fut: Future + Send + 'static,
    TState: Serialize
```

Defined in: [`packages/workflow_core/src/define/mod.rs:351`](../../../packages/workflow_core/src/define/mod.rs#L351)

[`step`](BaseCtx.md) with per-step options (retry policy, timeout,
resource gate, `up_to_date` make-check).

#### Parameters

##### step_id

`&str`

##### opts

[`StepOptions`](StepOptions.md)

##### run

`F`

#### Returns

`Result<Value>`

## Trait Implementations

- `impl Borrow for BaseCtx`
- `impl BorrowMut for BaseCtx`
- `impl CloneToUninit for BaseCtx`
- `impl Into for BaseCtx`
- `impl From for BaseCtx`
- `impl TryInto for BaseCtx`
- `impl TryFrom for BaseCtx`
- `impl Any for BaseCtx`
- `impl ToOwned for BaseCtx`
- `impl Clone for BaseCtx`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

