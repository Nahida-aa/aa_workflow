---
id: diff_state
title: diff_state
---

# Function: diff_state()

```rust
pub fn diff_state(prev: &Value, next: &Value) -> Vec<Operation>
```

Defined in: [`packages/workflow_core/src/engine/state_diff.rs:35`](../../../packages/workflow_core/src/engine/state_diff.rs#L35)

产出 `prev` → `next` 的 RFC 6902 JSON Patch；无变化返回空数组。

递归 diff 对象与数组；数组长度不同时发一个整体 `replace` 而非 splice 式
ops（线格式更简单，对状态观测足够——上游注释原话）。

## Parameters

### prev

`&Value`

### next

`&Value`

## Returns

`Vec<Operation>`

