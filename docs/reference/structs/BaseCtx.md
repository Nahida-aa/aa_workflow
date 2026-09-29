---
id: BaseCtx
title: BaseCtx
---

# Struct: BaseCtx

Defined in: `packages/workflow-core/src/define/mod.rs:115`

The handler's argument — mirrors TanStack's `BaseCtx<TInput, TState>`
interface member-for-member. Public surface is exactly [`Self::run_id`]
(`runId`), [`Self::input`] (`input`) and [`Self::state`] (`state`), plus
the durable primitives as methods (`step` / `sleep` / `sleep_until` /
`wait_for_event` / `approve` / `now` / `uuid` / `emit`), a
[`is_cancelled`](Self::is_cancelled) `signal` and the runtime budget
helpers ([`deadline`](Self::deadline) / [`time_remaining`](Self::time_remaining)
/ [`should_yield`](Self::should_yield) / [`yield_`](Self::yield_)).

The engine handle is *not* part of the public API: TanStack holds it in JS
closure scope (invisible on `ctx`), so here it lives in a
`#[doc(hidden)]` crate-private field — reachable but never nameable by
callers.

State semantics: `state` is the handler's working copy, seeded from the
engine's live image at drive start and flushed back to it before every
durable primitive. `ctx.input` / `ctx.state` are plain owned fields —
clone (or move) values as needed across `await` points.

## Fields

### `run_id`

```rust
run_id: String
```

`runId: string`

### `input`

```rust
input: TInput
```

`input: TInput` — frozen run input (typed or `Value`)

### `state`

```rust
state: StateHandle<TState>
```

`state: TState` — 共享可变的 typed state（对齐 TS 的
`ctx.state === engine.state`）；见 `state_handle` 模块文档。

### `ext`

```rust
ext: TExt
```

`TExtensions` — ctx extension bundle, the `{...context}` accumulated by
middleware. `()` (the default) when no middleware declares one. Built by
the middleware's `produce` and re-deserialized on every drive, so it is
deterministic across resume.

_（存在非公开字段）_

## Implementations

### `step`

```rust
pub async fn step<F, Fut>(&self, step_id: &str, run: F) -> Result<Value>
```

Runs `run` durably under `step_id`. On replay (resume) a previously
succeeded step short-circuits to its cached result *without* calling
`run` again; a previously failed step rethrows the stored error.

The step closure is async so that concurrent steps compose via
`tokio::try_join!` — parallel durable execution, no custom primitive.

Defined in: `packages/workflow-core/src/define/mod.rs:182`

### `approve`

```rust
pub async fn approve<impl Into<String>, impl AsRef<str>>(&self, key: impl ?, reason: impl ?) -> Result<Value>
```

Durable approval wait: pauses the run until [`signal_run`](crate::engine::signal_run)
delivers a decision for `key`. `reason` is persisted in the `StepPaused`
checkpoint for approvers. Resolves to the signal's payload on success;
on replay the already-recorded `StepResume` is served from the log.

Defined in: `packages/workflow-core/src/define/mod.rs:195`

### `sleep`

```rust
pub async fn sleep<impl Into<String>>(&self, key: impl ?, dur: Duration) -> Result<Value>
```

Durable sleep: pauses the run until `dur` elapses. `key` is the
deterministic pause identity. An external timer (the runtime's sweep,
indexing on `RunState.waiting_for.deadline`) delivers the resume via
[`signal_run`](crate::engine::signal_run) once the deadline passes; on
replay, a previously delivered resume short-circuits immediately.

Defined in: `packages/workflow-core/src/define/mod.rs:219`

### `sleep_until`

```rust
pub async fn sleep_until<impl Into<String>>(&self, key: impl ?, ts_ms: i64) -> Result<Value>
```

Durable absolute-time wait: pauses until wall-clock `ts_ms` (equivalent
to TanStack's `sleepUntil`). The timestamp is stored **verbatim** —
a timestamp already in the past parks with an expired deadline, which a
timer sweep claims on its next pass. `key` is the deterministic pause
identity.

Defined in: `packages/workflow-core/src/define/mod.rs:238`

### `wait_for_event`

```rust
pub async fn wait_for_event<impl Into<String>, impl AsRef<str>>(&self, key: impl ?, event_name: impl ?) -> Result<Value>
```

Durable named wait: pauses the run until [`signal_event`](crate::engine::signal_event)
delivers a payload for `event_name`. `key` is the deterministic pause
identity (must not collide with step ids); `event_name` is the channel
a host signals on (`RunState.waiting_for.signal_name`). On replay a
previously delivered resume short-circuits from the log.

Defined in: `packages/workflow-core/src/define/mod.rs:255`

### `emit`

```rust
pub fn emit<impl AsRef<str>>(&self, name: impl ?, value: Value)
```

Emit an observability event to the publisher. Never appended to the
log, so it is outside replay — `fold_step_states` and resume ignore it
(mirrors TanStack's `emit` / `CUSTOM`).

Defined in: `packages/workflow-core/src/define/mod.rs:271`

### `now`

```rust
pub fn now(&self) -> Result<i64>
```

Deterministic wall-clock (TanStack `ctx.now`): records the call's
timestamp as a checkpoint; replay serves the recorded value, so a run
sees the same clock across resumes. Returns `Err` only on store failure.

Defined in: `packages/workflow-core/src/define/mod.rs:283`

### `uuid`

```rust
pub fn uuid(&self) -> Result<String>
```

Deterministic id (TanStack `ctx.uuid`): records a generated UUIDv4 as a
checkpoint; replay serves the recorded id, so the same value is seen
across resumes. Returns `Err` only on store failure.

Defined in: `packages/workflow-core/src/define/mod.rs:290`

### `is_cancelled`

```rust
pub fn is_cancelled(&self) -> bool
```

Whether this run was cancelled via [`cancel_run`](crate::engine::cancel_run).
Polled at step boundaries only — the engine cannot interrupt a step's
in-flight `await` (same granularity as JS `AbortSignal`: the closure
must check cooperatively).

Defined in: `packages/workflow-core/src/define/mod.rs:298`

### `deadline`

```rust
pub fn deadline(&self) -> Option<i64>
```

Absolute UTC ms runtime budget for this drive (TanStack `deadline`);
`None` when the host set no budget.

Defined in: `packages/workflow-core/src/define/mod.rs:310`

### `time_remaining`

```rust
pub fn time_remaining(&self) -> u64
```

Ms of runtime budget left (`u64::MAX` when no deadline).

Defined in: `packages/workflow-core/src/define/mod.rs:315`

### `should_yield`

```rust
pub fn should_yield(&self) -> bool
```

True once the budget is nearly exhausted: `time_remaining() <
min_yield_remaining_ms` (default 1000ms).

Defined in: `packages/workflow-core/src/define/mod.rs:324`

### `yield_`

```rust
pub async fn yield_(&self) -> Result<Value>
```

Cooperative hand-back of the runtime budget (TanStack `yield`): durably
parks the run on a `"__timer"` wait until [`RunWorkflowOptions`](crate::engine::RunWorkflowOptions) 的 `yield_resume_at`
(or now+1ms), so a host can re-invoke with a freshly extended deadline.
Deterministic id `__yield-{n}` (per-invocation counter), replay-safe.

Defined in: `packages/workflow-core/src/define/mod.rs:332`

### `step_with`

```rust
pub async fn step_with<F, Fut>(&self, step_id: &str, opts: StepOptions, run: F) -> Result<Value>
```

[`step`](Self::step) with per-step options (retry policy, timeout,
resource gate, `up_to_date` make-check).

Defined in: `packages/workflow-core/src/define/mod.rs:351`

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

