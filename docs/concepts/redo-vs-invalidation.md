# redo 与 INVALIDATED：为什么本库放弃持久化失效 primitive

> 配套阅读：
> `feature-requirements.md` F1 / F2 / F6 · `core-runtime-map.md` §4 · `tanstack-alignment.md` §完整分歧清单
> 本页面是 "redo 优先" 的 decisions 记录与 rationale。

## 0. TL;DR（工程师 30 秒版）

| 你想做什么 | 应该的路 | 不应做的路 |
| --- | --- | --- |
| 同一个 run 里“把某几步作废，再跑几步” | **F1：drive 参数 `invalidateSteps`** (same run, no persistence) | — |
| 换整个世界 (new input, immutably audit) | **F2：`reexecuteFrom` 开新 run，seeding 幸存 checkpoint** | — |
| run 完成后 retroactively “作废某 fact 并使作废持久” | **application 层 externalize**（记录 sourceref + downstream 只读） | `INVALIDATED` 耐久原语（F6: 明确不做） |

结论：拖入 INVALIDATED 作为耐久 event 需要新 machinery；**新 run + seed survivors** 不动 engine core，把 decision 放在 drive boundary。

## 1. 三种需求，不要混在一起

### 1.1 same run, few steps (F1)

- 触发场景：raise + resume after interim goal, or a checkpoint event 是错误的。
- 解法：drive 启动时把 lives 中对应的 entries 删掉 (in-memory filter)→step re-runs →
  append 新 StepFinished → replay 时 later-wins（coverage write in fold_step_states）。
- 机制 anchors: engine/run_workflow.rs:876 (`fold_step_states`)，892-902 (`lives`/`pending_retries`
  filter)，902 (`goals_done_at_drive_start` 必须 post-filter 算)。
- 语义：drive params only, 不落盘；下次 resume 不传 = 不失效。see F1 §.

### 1.2 new world, clean audit (F2)

- 解法：显式 `reexecuteFrom(sourceRunId, unsafeInvalidate)` → 全新 run id,
  ingest survivors, new StepFinished append `meta.importedFrom` 作为 sourcing.
- old log immutable, new log 从 `RUN_STARTED` 开始; list two runs to audit.

### 1.3 "invalidated 作废 fact durable" (F6, out of scope)

- 上游 fn-orchestration 也没有 INVALIDATED event; `reexecuteFrom` 公开 doc 也说
  "only usable on the start path of a fresh run"。本库的 F6 = "已评估：不做"。
- 需要时 externalize at app layer（SKU mapping, versioned pointer）。

## 2. 为什么 INVALIDATED 会 complicated core

<|constrain|> aspect | cost |
| --- | --- |
| new event kind | WorkflowEvent union + Rust enum expand; replay matcher updates in two surfaces |
| replay semantics | invalidated step projection & propagation to successor + compensation undo? |
| persistence | RunState needs representation? store migrations + idempotency |
| idempotency | same run repeated INVALIDATE 是否 collide? |
| interaction with F1 | 两条 near-identical APIs in engine vocabulary |

Note that compensation (F5) already adds `CompensationRegistered/Started/Completed/Failed`;
adding INVALIDATED multiplies cross-effects (compensation after invalidation = re-undo?).

Aggregate result: INVALIDATED makes every discipline in AGENTS.md
(hydration / replay-identity / store contract /  MUST perf) +1 new member across both binaries.

## 3. Redo pattern (the right default):

1. call `reexecuteFrom({ sourceRunId, unsafeInvalidate })` with fresh `runId`;
2. survivors (`STEP_FINISHED`) ingest as seed checkpoints (append with `meta.importedFrom`);
3. FAILed steps not seeded → re-run automatically;
4. old log stays immutable, new log provides clean audit line.

Invariant: old run logs remain queryable by `sourceRunId`; application layer holds
`newRunId ← oldRunId` mapping for external references (chat thread, ticket, UI link).

### 3.1 code sketch (TS flavor):

```
await runWorkflow({
  workflow, runStore,
  reexecuteFrom: { sourceRunId: oldRun, unsafeInvalidate: ['step-b'] },
});
```

### 3.2 replay identity

`fold_step_states` applies override: when the same `stepId` gets a second `STEP_FINISHED`,
the later one wins. So survivors + freshly re-run results are always later events,
replay converges to expected.

## 4. F1 `invalidateSteps` is the key co-existing primitive

- survives INVALIDATED as the "cheap same-run flip" alternative:
  - same run id、no new log、no new identity;
  - replay-identity by coverage-write semantics;
  - no compensations / no log split.
- call sequence: user passes `invalidateSteps: ['step-b']` (same shape both langs);
  engine removes life/pending_retry entries before drive, recalcs goals_done_at_drive_start.
- invalidates at the _single drive_ scope; next run re-drives if needed.

## 5. trade-offs of redo (be honest)

1. **identity cost:** external pointers must be re-linked in app layer (cost: app code). Mitigate with standard mapping table.
2. **input reprint:** re-execution needs full input; abort+partial doesn't apply. but freeze-runners should snapshot input at schedule time (they do).
3. **audit continuity:** two logs concatenated conceptually; demonstrate with `meta.importedFrom` and run_id pairing in dashboards.

None of these trade-offs violate AGENTS constraints; they just live downstream.

## 6. Decision tree (use in docs)

```
mutate a few steps inverse run?
 ├─ want same run id / no log split → F1 invalidateSteps (drive param)
 ├─ want clean audit / new identity → F2 reexecuteFrom (redo, seeding)
 └─ want durable fact "this step is invalidated universally" → no engine prim; app externalize
```

## 7. upstream signals (supporting no INVALIDATED)

- TanStack `f2-lite-feature-points.md` §6: explicit "INVALIDATED 不做", matches F6;
- ai-orchestration official uses same-run resume + redo split per F2-lite note §1
  (all continuations on same run_id; new run only for reexecuteFrom-class need);
- upstream `reexecuteFrom` public doc also says start-path-only.
- more: reexecuteFrom still inored by TanStack as "future" public API in their docs
  (contract arms shipped, internal docs point out "future-proof schema primitives").

## 8. doc obligations (what must be published to avoid repeated questions)

1. `docs/quick-start.md` get a "redo recipe" with importedFrom fields annotated.
2. `feature-requirements.md`: F6 explicit positioning "别(ld validity) persistence primitive".
3. `tanstack-alignment.md`: append to divergence清单 a small entry "INVALIDATED = 0,
   INSTEAD F2 covers".
4. New `docs/concepts/redo-vs-invalidation.md` (this file).
5. `docs/core-runtime-map.md`: §3 line adds no extra primitives; no change needed.
6. Tests: one E2E scenario "redo round trip preserves audit refs + new run result".

## 9. pointers

- F1 mechanism: engine/run_workflow.rs:876-876 / 902  (see command above)
- F2 path: commit 057a0c6 + runtime 91111f3
- F4/F5 alignment: commit c536e03 / 0971b4c
EOF
echo done