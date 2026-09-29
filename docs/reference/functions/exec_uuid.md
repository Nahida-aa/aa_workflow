---
id: exec_uuid
title: exec_uuid
---

# Function: exec_uuid()

Defined in: [`packages/workflow-core/src/engine/mod.rs:485`](../../../packages/workflow-core/src/engine/mod.rs#L485)

Deterministic id behind [`WorkflowCtx::uuid`](../structs/BaseCtx.md)
(TanStack `ctx.uuid`): a generated UUIDv4, recorded as a `UuidRecorded`
checkpoint. On replay the recorded id is served — stable across resumes.

```rust
pub fn exec_uuid(inner: &Arc<EngineRuntime>) -> Result<String>
```

## Parameters

### inner

`&Arc<EngineRuntime>`

## Returns

`Result<String>`

