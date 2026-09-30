---
id: exec_uuid
title: exec_uuid
---

# Function: exec_uuid()

```rust
pub fn exec_uuid(inner: &Arc<EngineRuntime>) -> Result<String>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:559`](../../../packages/workflow_core/src/engine/mod.rs#L559)

Deterministic id behind [`WorkflowCtx::uuid`](../structs/BaseCtx.md)
(TanStack `ctx.uuid`): a generated UUIDv4, recorded as a `UuidRecorded`
checkpoint. On replay the recorded id is served — stable across resumes.

## Parameters

### inner

`&Arc<EngineRuntime>`

## Returns

`Result<String>`

