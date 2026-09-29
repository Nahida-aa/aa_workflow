---
id: exec_now
title: exec_now
---

# Function: exec_now()

```rust
pub fn exec_now(inner: &Arc<EngineRuntime>) -> Result<i64>
```

Defined in: [`packages/workflow-core/src/engine/mod.rs:458`](../../../packages/workflow-core/src/engine/mod.rs#L458)

Deterministic wall-clock behind [`WorkflowCtx::now`](../structs/BaseCtx.md)
(TanStack `ctx.now`): the call's timestamp, recorded as a `NowRecorded`
checkpoint. On replay the recorded value is served from the log — a run
sees the **same** clock across resumes. The checkpoint id uses a
per-invocation atomic counter (`__now-0`, `__now-1`, …) so concurrent
calls stay unique; replay permutes them to the same ids and reads the cache.

## Parameters

### inner

`&Arc<EngineRuntime>`

## Returns

`Result<i64>`

