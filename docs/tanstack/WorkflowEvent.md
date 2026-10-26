# WorkflowEvent
**整个引擎的"通用语法"——所有 durability 与 observability 消费面都只约定这一种**事件 shape

```ts
// ============================================================
// Workflow event stream (unified log entry + transport event)
// ============================================================

/**
 * The shape of every event the engine appends to a run's log.
 *
 * Two consumers, one shape:
 *
 *   - **Durability**: the engine appends events to the run's log.
 *     Replay reads the log and short-circuits primitives that have
 *     a matching CHECKPOINT event by `stepId`.
 *   - **Observability**: the engine emits the same events through
 *     `runWorkflow`'s `AsyncIterable<WorkflowEvent>` and (if wired)
 *     through stream subscribers. A browser/UI subscribes to the
 *     same log a Durable Streams URL would expose.
 *
 * Events fall into two categories internally:
 *
 *   - **Checkpoint events** — replay uses these to skip already-
 *     completed work. Indexed by `stepId`. STEP_FINISHED,
 *     STEP_FAILED, SIGNAL_RESOLVED, APPROVAL_RESOLVED, NOW_RECORDED,
 *     UUID_RECORDED, RUN_FINISHED, RUN_ERRORED.
 *
 *   - **Coordination events** — persisted so hosts and resume calls
 *     can identify the pending wait. SIGNAL_AWAITED,
 *     APPROVAL_REQUESTED.
 *
 *   - **Observability events** — engine emits but replay ignores.
 *     RUN_STARTED, STEP_STARTED, STATE_DELTA, CUSTOM.
 *
 * The optional `audience` field is engine-ignored. Adapters/views
 * (e.g., a Durable Streams projection layer) may filter on it to
 * produce internal vs client vs admin views of the same log.
 */
export type WorkflowEvent =
  // ── Run lifecycle ─────────────────────────────────────────────
  | {
      type: 'RUN_STARTED'
      ts: number
      runId: string
      threadId?: string
      audience?: string
    }
  | {
      type: 'RUN_FINISHED'
      ts: number
      runId: string
      output: unknown
      audience?: string
    }
  | {
      type: 'RUN_ERRORED'
      ts: number
      runId: string
      error: SerializedError
      code: string
      audience?: string
    }
  | {
      type: 'STEP_ATTEMPT_STARTED'
      ts: number
      stepId: string
      attempt: number
    }
  | { type: 'COMPENSATION_REGISTERED'; ts: number; stepId: string }
  | { type: 'COMPENSATION_STARTED'; ts: number; error: SerializedError }
  | {
      type: 'STEP_RETRY_SCHEDULED'
      ts: number
      stepId: string
      attempt: number
      nextAttemptAt: number
      error: SerializedError
      attempts: ReadonlyArray<StepAttempt>
    }
  // ── Step (durable side-effect via ctx.step) ────────────────────
  | {
      type: 'STEP_STARTED'
      ts: number
      stepId: string
      meta?: WorkflowMetadata
      audience?: string
    }
  | {
      type: 'STEP_FINISHED'
      ts: number
      stepId: string
      result: unknown
      attempts?: ReadonlyArray<StepAttempt>
      meta?: WorkflowMetadata
      audience?: string
    }
  | {
      type: 'STEP_FAILED'
      ts: number
      stepId: string
      error: SerializedError
      attempts?: ReadonlyArray<StepAttempt>
      meta?: WorkflowMetadata
      audience?: string
    }
  // ── Signal (ctx.waitForEvent, ctx.sleep) ──────────────────────
  | {
      type: 'SIGNAL_AWAITED'
      ts: number
      stepId: string
      name: string
      deadline?: number
      meta?: Record<string, unknown>
      audience?: string
    }
  | {
      type: 'SIGNAL_RESOLVED'
      ts: number
      stepId: string
      name: string
      /** Host-supplied idempotency token. Same `signalId` at the
       *  same `stepId` is a no-op (idempotent retry); different
       *  `signalId` is a lost race. */
      signalId?: string
      payload: unknown
      meta?: WorkflowMetadata
      audience?: string
    }
  // ── Approval (ctx.approve) ────────────────────────────────────
  | {
      type: 'APPROVAL_REQUESTED'
      ts: number
      stepId: string
      approvalId: string
      title: string
      description?: string
      meta?: WorkflowMetadata
      audience?: string
    }
  | {
      type: 'APPROVAL_RESOLVED'
      ts: number
      stepId: string
      approvalId: string
      approved: boolean
      feedback?: string
      meta?: WorkflowMetadata
      audience?: string
    }
  // ── Deterministic recording (ctx.now, ctx.uuid) ────────────────
  | {
      type: 'NOW_RECORDED'
      ts: number
      stepId: string
      value: number
      meta?: WorkflowMetadata
      audience?: string
    }
  | {
      type: 'UUID_RECORDED'
      ts: number
      stepId: string
      value: string
      meta?: WorkflowMetadata
      audience?: string
    }
  // ── State + custom ────────────────────────────────────────────
  | {
      type: 'STATE_DELTA'
      ts: number
      delta: ReadonlyArray<Operation>
      audience?: string
    }
  | {
      type: 'CUSTOM'
      ts: number
      name: string
      value: Record<string, unknown>
      audience?: string
    }

/** Durable completion and recovery checkpoints used during replay. */
export type CheckpointEvent = Extract<
  WorkflowEvent,
  {
    type:
      | 'STEP_ATTEMPT_STARTED'
      | 'STEP_RETRY_SCHEDULED'
      | 'COMPENSATION_REGISTERED'
      | 'COMPENSATION_STARTED'
      | 'STEP_FINISHED'
      | 'STEP_FAILED'
      | 'SIGNAL_RESOLVED'
      | 'APPROVAL_RESOLVED'
      | 'NOW_RECORDED'
      | 'UUID_RECORDED'
      | 'RUN_FINISHED'
      | 'RUN_ERRORED'
  }
>
```

---

## 附录：为什么 WorkflowEvent 是 append-only / 一个 run 的 jsonl 长什么样

（补充说明；与 `core-runtime-map.md` §1/§4 对齐。）

### 1. `WorkflowEvent` 是干嘛的

`WorkflowEvent` 是整个引擎**通用的语法**：所有 durability 与 observability
消费面都只约定这一种事件 shape，so no second schema for "store row" vs "wire shape"。

<|message|> consumer | 怎么用 | 不能做的事 |
| --- | --- | --- |
| Durability 引擎 | `appendEvent(runId, index, event)` 写入 run log；replay 时 `fold_step_states` 按 stepId 短路 | 把 `ctx.state` 持久化成 STATE_DELTA（STATE_DELTA **不进日志**，emit-only） |
| Observability/UI | 异步 iterable `runWorkflow`、Durable Streams URL、browser subscribe | 把 RUN_STARTED 当成 persisted 证据（**不落盘**、也不占下标） |

三家族（`Checkpoint` / `Coordination` / `Observability`）+ `meta`/`audience` 扩展槽见 §2
与 `core-runtime-map.md` §2。

### 2. append-only 事件日志 = 什么样子

- **只能追加，不能改、不能删**；
- `index ≡ log.len()`（CAS 单调序），两 writer 靠 CAS 裁决；
- 每条 event immutable；
- 三种 family 决定"落盘 vs 只 emit"；
- `RUN_STARTED` 不占下标，`importedFrom.index` 以持久化日志为基准。

### 3. 一个 run 的 jsonl 例（JS/camelCase 风格，真实 JSONL 无注释）

```jsonl
{"type":"RUN_STARTED","ts":1729891000000,"runId":"r-abc","threadId":"T1"}
{"type":"STEP_STARTED","ts":1729891000010,"stepId":"analyze","meta":{"doc":"x.pdf"}}
{"type":"STEP_FINISHED","ts":1729891050000,"stepId":"analyze","result":{"words":42,"risk":"high"},"meta":{"doc":"x.pdf"}}
{"type":"STEP_STARTED","ts":1729891050012,"stepId":"writeReport"}
{"type":"STEP_FINISHED","ts":1729891051000,"stepId":"writeReport","result":{"md":"# Report..."}}
{"type":"SIGNAL_AWAITED","ts":1729891051010,"stepId":"cooldown","name":"__timer","deadline":1729891111000,"meta":{"reason":"rate-limit"}}
```

<|message|>
