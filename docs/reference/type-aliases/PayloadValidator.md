---
id: PayloadValidator
title: PayloadValidator
---

# Type Alias: PayloadValidator

Defined in: [`packages/workflow_core/src/define/mod.rs:524`](../../../packages/workflow_core/src/define/mod.rs#L524)

恢复 payload 的形状校验器（对齐 TanStack `WaitForEventOptions.schema`，
`types.ts:276-277`）。**代码而非数据**：不落进 `StepPaused` checkpoint，
replay 时同一个 `wait_for_event_with(..)` 调用点重新装上同一个校验器。

## Definition

```rust
pub type PayloadValidator = Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>
```

