---
id: RunStore
title: RunStore
---

# Trait: RunStore

Defined in: `packages/workflow-core/src/run_store/mod.rs:209`

The store contract, modelled after TanStack Workflow's two-surface design:

1. **Metadata surface** — `get/set/delete` a small [`RunState`] envelope.
2. **Append-only event-log surface** — `append_event` with a CAS
   `expected_next_index`, `get_events` for resume, and `subscribe` for live
   fan-out.

The engine never mutates events; state is always derived by replaying the
log (see [`crate::event::fold_step_states`]).

## 与 TanStack 的对应

上游有**三个**相关名字，其中两个只差一个 `Store` 后缀，容易看错：

| 上游 | 位置 | 是什么 |
| ---- | ---- | ------ |
| `RunStore` | `aa-workflow-core/src/types.ts:599` | core 的接口，引擎用；**就是本 trait** |
| `WorkflowRunStoreAdapter = RunStore` | 同上 `:305` 在 runtime 侧 | 一行 `export type` 真别名 |
| `WorkflowRunStoreAdapterStore` | `aa-workflow-runtime/src/types.ts:290` | **独立 interface，不是别名** |
| `WorkflowExecutionStore extends …AdapterStore` | `:307` | runtime 的扩展契约 |

后两个本 crate 与 `aa-workflow-runtime` 都**不设对应物**：

- `WorkflowRunStoreAdapter` 是 TS 的 `type` 别名，零成本。Rust 的 trait
  别名要 `#![feature(trait_alias)]`（仍 unstable），`trait A: B {}` 则是新
  trait——每个实现者都得多写空 `impl`。它的唯一用途是标注「适配器的输出
  类型」，而适配器本身我们也不需要（见下条）。
- `WorkflowRunStoreAdapterStore` **不是 `RunStore` 的重复**：它在事件读写上
  有真实差别（批量 `appendEvents`、返回带 `eventIndex` / `createdAt` 的
  `StoredWorkflowEvent`）。它存在是因为**上游正在迁移中**——core 还在用旧的
  `RunStore`，runtime 已用新的，`createRunStoreAdapter` 负责把新的降格成
  旧的。

于是 runtime 的扩展 trait 继承的是 `WorkflowRunStoreAdapterStore`，
**不经过本 trait**（`WorkflowExecutionStore: WorkflowRunStoreAdapterStore`）。
两者是**平行的两套方法**：`load_run_state` vs `get_run_state`、
`append_events` vs `append_event`，没有自动转换关系。

## 本 trait 里哪些方法是**本地扩展**（上游没有）

对照上游时按这张表看，**不必**为下列方法去找对端——找不到是正常的：

| 方法 | 上游有吗 | 说明 |
| ---- | -------- | ---- |
| `get_run_state` / `set_run_state` / `delete_run` | ✅ | 一一对应 |
| `append_event` / `get_events` / `subscribe` | ✅ | 一一对应 |
| [`truncate_log_at_step`](Self::truncate_log_at_step) | ❌ **本地扩展** | 支撑 `continue_from`；上游连 `continueFrom` 都没有 |

详见 `docs/tanstack-alignment.md` 的「保留了分歧（本地扩展 / 一等公民）」。
这也是为什么 runtime 的新契约 `WorkflowExecutionStore` 没有截断能力——它对齐的
是上游，而截断是本仓独有的。

⚠️ **要喂给 core 的 `run_workflow`，实现者必须额外 `impl RunStore`**：
该函数的入参是 `Arc<dyn RunStore>`（具体 trait 对象），而 supertrait 只保证
`WorkflowRunStoreAdapterStore` 的父关系，跟本 trait 无关。
完整论证见 `docs/runtime-design.md` 的 D1。

