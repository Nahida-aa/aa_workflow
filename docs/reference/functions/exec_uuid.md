---
id: exec_uuid
title: exec_uuid
---

# Function: exec_uuid

Defined in: `packages/workflow-core/src/engine/mod.rs:485`

Deterministic id behind [`WorkflowCtx::uuid`](crate::define::WorkflowCtx::uuid)
(TanStack `ctx.uuid`): a generated UUIDv4, recorded as a `UuidRecorded`
checkpoint. On replay the recorded id is served — stable across resumes.

## Signature

```rust
pub fn exec_uuid(inner: &Arc<EngineRuntime>) -> Result<String>
```

