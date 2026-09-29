---
id: snapshot_state
title: snapshot_state
---

# Function: snapshot_state

Defined in: `packages/workflow-core/src/engine/state_diff.rs:27`

给 state 拍快照，供之后 diff（对齐上游 `snapshotState`）。

TS 版要 `structuredClone` 防引用共享；Rust 的 `Value` 是拥有的值，
`.clone()` 即等价。

## Signature

```rust
pub fn snapshot_state(state: &Value) -> Value
```

