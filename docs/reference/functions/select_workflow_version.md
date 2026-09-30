---
id: select_workflow_version
title: select_workflow_version
---

# Function: select_workflow_version()

```rust
pub fn select_workflow_version<'a>(workflow: &'a WorkflowDefinition, persisted: Option<&str>) -> Option<&'a WorkflowDefinition>
```

Defined in: [`packages/workflow_core/src/registry/select_version.rs:31`](../../../packages/workflow_core/src/registry/select_version.rs#L31)

在 `[current] + current.previous_versions` 中按持久化版本选定义。

返回 `None` = 版本化 run 在候选里找不到对应定义（由调用方报错）。

## Parameters

### workflow

`&'a WorkflowDefinition`

### persisted

`Option<&str>`

## Returns

`Option<&'a WorkflowDefinition>`

