# 0003 — 事件分类按上游拆分：StepPaused/StepResume → 四类型

**状态**：已接受（2026-11-02，无用户包袱故直接对齐）
**关联**：`docs/tanstack-alignment.md`、`packages/workflow_core/src/event.rs`

## 背景

本仓原把上游的四个事件合并成两个：

| 上游 | 本仓原状 |
| --- | --- |
| `SIGNAL_AWAITED` + `APPROVAL_REQUESTED` | `StepPaused` |
| `SIGNAL_RESOLVED` + `APPROVAL_RESOLVED` | `StepResume` |

合并省了两个类型，但丢了**结构化字段**：审批的 `approvalId`/`approved`/`feedback`
塞在 `signal_name`/`payload` 里；且 `signalId`（幂等令牌）无法进日志。

## 决策

拆成四个、字段逐字对齐基线 `0971b4c` 的 `types.ts`：

| 事件 | 关键字段 |
| --- | --- |
| `SignalAwaited` | `name` / `deadline` / `meta` |
| `ApprovalRequested` | `approval_id` / `title` / `description` / `meta` |
| `SignalResolved` | `name` / `signal_id` / `payload` / `meta` |
| `ApprovalResolved` | `approval_id` / `approved` / `feedback` / `meta` |

**`ApprovalResolved` 不带 payload**（与上游一致）：审批是结构化决定，任意 payload
只属于 `SignalResolved`。

## 保留的有意差异（记档）

- 序列化**字段名是 snake_case**：`rename_all` 只作用于类型 tag。本仓不做 TS 互操作，
  按 Rust 实践；代价是逐字段对照上游时键名对不上（`signal_id` vs `signalId`）。
- 保留本仓独有事件：`StepProgress`、`CompensationCompleted` / `CompensationFailed`
  （可观测功能，删除是净损失）。

## 后果

- 官方消费者（ai-orchestration 的 `events.ts`）消费的正是这四个类型与字段名，
  对齐后 grep 日志与对照上游源码零映射；
- `signal_event` 增加 `signal_id` 参数，幂等令牌**第一次进日志**；
- 全仓 41 文件改动，`docs/reference` 同步重生成（当时还健在）。
