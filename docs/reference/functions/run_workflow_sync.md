---
id: run_workflow_sync
title: run_workflow_sync
---

# Function: run_workflow_sync()

```rust
pub fn run_workflow_sync(opts: RunWorkflowOptions) -> Result<RunOutcome, WorkflowError>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:914`](../../../packages/workflow_core/src/engine/run_workflow.rs#L914)

Sync convenience over a local multi-thread runtime for callers that are
not async themselves (e.g. LocalDub's CLI entrypoint).

事件流在这里被**排干**：本函数的契约是「跑到终态并交出 outcome」，中间事件
不外泄。要逐条看事件就用 async 的 [`run_workflow`](run_workflow.md)。`publish` 回调仍然逐条
触发（它在 `RunEventStream` 里被 await，与 async 路径同一处）。

## Parameters

### opts

[`RunWorkflowOptions`](../structs/RunWorkflowOptions.md)

## Returns

`Result<RunOutcome, WorkflowError>`

