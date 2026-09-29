---
id: select_workflow_version
title: select_workflow_version
---

# Function: select_workflow_version()

Defined in: [`packages/workflow-core/src/registry/select_version.rs:31`](../../../packages/workflow-core/src/registry/select_version.rs#L31)

在 `[current] + current.previous_versions` 中按持久化版本选定义。

返回 `None` = 版本化 run 在候选里找不到对应定义（由调用方报错）。

```rust
pub fn select_workflow_version<'a>(workflow: &'a Workflow, persisted: Option<&str>) -> Option<&'a Workflow>
```

## Parameters

### workflow

`&'a Workflow`

### persisted

`Option<&str>`

## Returns

`Option<&'a Workflow>`

