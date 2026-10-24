---
id: signal_approval
title: signal_approval
---

# Function: signal_approval()

```rust
pub fn signal_approval(store: &dyn RunStore, run_id: &str, approval_id: &str, approved: bool, feedback: Option<&str>) -> Result<(), WorkflowError>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:1160`](../../../packages/workflow_core/src/engine/mod.rs#L1160)

Appends an `ApprovalResolved` for the run parked on approval `approval_id`
— the external side of
[`WorkflowCtx::approve`](../structs/BaseCtx.md).

**过闸**：只在该审批确实泊着时才落盘；否则报
[`WorkflowError::SignalLost`](../enums/WorkflowError.md)（上游对应 `approval_lost`：run 没在等这个
审批）。事件形状逐字对齐上游 `APPROVAL_RESOLVED`——**没有 payload**，审批是
结构化决定（`approved` + `feedback`）。

## Parameters

### store

`&dyn RunStore`

### run_id

`&str`

### approval_id

`&str`

### approved

`bool`

### feedback

`Option<&str>`

## Returns

`Result<(), WorkflowError>`

