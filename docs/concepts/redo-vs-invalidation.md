# redo 与 INVALIDATED：为什么本库放弃持久化失效 primitive

> 配套阅读：
> `feature-requirements.md` F1 / F2 / F6 · `core-runtime-map.md` §4 · `tanstack-alignment.md` §完整分歧清单
> 本页面是 "redo 优先" 的 decisions 记录与 rationale。

## 0. TL;DR（工程师 30 秒版）

> 先分清两件常被混成一件事的：
>
> - **redo（seed）**：开**新 run** + 把源 run 的幸存 checkpoint **播种**进去，失效名单
>   里的 step 重跑。机制轻——零新事件类型、零 store 契约变更（f2-lite 引擎已落地）。
> - **INVALIDATED（持久失效原语）**：让"作废决定"本身成为**耐久事实**（跨 drive 仍生效、
>   日志可独立读出"这步被作废过"）。需要新事件 + 重放规则 + 状态转换，重。
>
> 本仓立场：**redo（seed）可以做、且是失败续跑的正确默认路径；INVALIDATED 明确不做**
> （`feature-requirements.md` F6）。"redo 不进 core" 这句话实际否的是 INVALIDATED，
> 别把它误读成"seed 也不要做"。

| 你想做什么 | 应该的路 | 不应做的路 |
| --- | --- | --- |
| 同一个 run 里“把某几步作废，再跑几步” | **F1：drive 参数 `invalidateSteps`** (same run, no persistence) | — |
| 换整个世界 (new input, immutably audit) | **F2：`reseedRun` 开新 run，seeding 幸存 checkpoint** | — |
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

- 解法：显式 `reseedRun(sourceRunId, { redoSet })` → 全新 run id,
  ingest survivors, new StepFinished append `meta.importedFrom` 作为 sourcing.
- old log immutable, new log 从 `RUN_STARTED` 开始; list two runs to audit.

### 1.3 "invalidated 作废 fact durable" (F6, out of scope)

- 上游 fn-orchestration 也没有 INVALIDATED event; **f2-lite 引擎的** `reseedRun`
  也限定 "only usable on the start path of a fresh run"（start-path-only，上游主线
  本无此 API）。本库的 F6 = "已评估：不做"。
- 需要时 externalize at app layer（SKU mapping, versioned pointer）。

## 2. 为什么 INVALIDATED 会 complicated core

| 维度 | 代价 |
| --- | --- |
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

1. call `reseedRun({ sourceRunId, { redoSet } })` with fresh `runId`;
2. survivors (`STEP_FINISHED`) ingest as seed checkpoints (append with `meta.importedFrom`);
3. FAILed steps not seeded → re-run automatically;
4. old log stays immutable, new log provides clean audit line.

Invariant: old run logs remain queryable by `sourceRunId`; application layer holds
`newRunId ← oldRunId` mapping for external references (chat thread, ticket, UI link).

### 3.1 code sketch (TS flavor):

```
await runWorkflow({
  workflow, runStore,
  reseedRun: { sourceRunId: oldRun, redoSet: ['step-b'] },
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

## 6. 命名：`reseedRun`（原 `reexecuteFrom`，已改名）

原 `reexecuteFrom` 这个名字**没体现"落到一个新 run 里"**——"re-execute from" 字面只说了"从哪重放"，没说结果是一个新 run。读代码的人容易误以为是在**同一个 run 里**重放。**已改名 `reseedRun`**（f2-lite 引擎 + aa-workflow Rust 两侧，0.x 阶段直接换新名、不留 alias）。

**建议改名 `reseedRun(sourceRunId, { redoSet })`**，并保留 `re-` 前缀：

| 候选 | 体现了什么 | 漏了什么 |
| --- | --- | --- |
| `reexecuteFrom(src, …)` | 源 run、失效名单 | **新 run** 这件事 |
| `seedRun(src, …)` | 新 run + 播种 | "第二次播种"的增量语义 |
| **`reseedRun(src, { redoSet })`** | **新 run + 从源 run 幸存 checkpoint 选择性重新播种** | — |

`re-` 不可省：

1. **语义**：`seed` 单独用在编程圈几乎都指"第一次初始化一个新实例"（`db seed` /
   `rand::seed()`）——隐含"从零种"。但这里做的是"**从源 run 已有的 checkpoint 重新种**"
   （幸存的 seed、失效/失败的重跑），是**增量**播种。省 `re-` 会丢"第二次"的信息。
2. **词族**：`redo` / `re-execute` / **reseed** 同属"redo 一族"的 `re-` 词族标志，
   读的人一眼归到"redo"；丢 `re-` 变 `seedRun` 就滑到"初始化族"，认知跑偏。
3. **不撞术语**：`reseed` 在 PostgreSQL / Kafka / Rails 都没被占；分布式圈偶见
   `reseed follower`（从已有数据重同步 follower），语义同向不冲突。

> 落地范围：f2-lite 引擎（`run-workflow.ts` 的 `reseedRun` 字段 + `runtime` 透传）
> 与 aa-workflow Rust（`ReseedRun` struct + `RunWorkflowOptions.reseed_run` +
> `workflow_dag` 的 `reseed_plan`）**两侧同步、直接删旧名**（0.x 未稳定，
> 用户 2026-10 明确不留向后兼容）。机制不变，只换名字。

## 6.1 参数命名：`redoSet`（原 `unsafeInvalidate`，已改名）

`reseedRun` 的入参原叫 `unsafeInvalidate`，**已改名 `redoSet`**，理由两层：

1. **词族方向**。`reseed` 是"重新做"的方向，`invalidate`（作废）是反方向，两个词族
   打架——读者容易飘到 INVALIDATED（持久失效原语）那一侧。`redoSet` 与 `reseedRun`
   同词族（redo 族），方向一致。
2. **`unsafe` 前缀强度偏重**。它暗示"用了就违反引擎不变量"（Rust `unsafe` 的硬
   味道），但实际语义是 **advisory**：引擎**不校验**名单对依赖方向封闭，**不封闭
   可能**让下游拿到旧 checkpoint——是否可接受取决于你的业务，引擎既不替你校验、也
   不假设你不封。这是个软提示，不是硬 UB。用 `redoSet`（无 `unsafe`）+ 注释里的
   advisory 说明，强度才匹配。

**注释结构**（引擎 `run-workflow.ts` + Rust `run_workflow.rs` 两侧同形）：

- **先理论**：一个 step 被重执行 iff 它的 checkpoint 没被导入；引擎只导入源 run 里
  stepId **不在** `redo_set` 的 `STEP_FINISHED`；`STEP_FAILED` 永不导入（所以失败
  step 默认重跑）。
- **再注意事项**（由理论引出，引擎不替你校验）：若你**没**列进 `redo_set` 的 step
  消费了**列了**的 step 的结果，它保留旧缓存值。例：链 A→B→C→D、`redo_set = [B, D]`：
  C 不在名单 → 导入 C 旧 checkpoint、C 不重跑；D 重跑时读到 C 的旧结果。要强制封闭，
  在 DAG 层用 `descendants(from)` 展开整个下游闭包。

## 7. Decision tree (use in docs)

```
mutate a few steps in a run?
 ├─ want same run id / no log split → F1 invalidateSteps (drive param)
 ├─ want clean audit / new identity → F2 reseedRun (redo: 新 run + seed 幸存 checkpoint，见 §6)
 └─ want durable fact "this step is invalidated universally" → no engine prim; app externalize
```

## 8. upstream signals (supporting no INVALIDATED)

- TanStack `f2-lite-feature-points.md` §6: explicit "INVALIDATED 不做", matches F6;
- ai-orchestration official uses same-run recovery + redo split per F2-lite note §1
  (all continuations resume the same runId — driven by the `recover`/delivery params
  into `resumeRun`; a new run only for `reseedRun`-class needs);
- **f2-lite 引擎的** `reseedRun` 文档限定 start-path-only（上游主线无此 API——
  `b928717` / `0971b4c` 两个 base 都没有 redo/reseed，它是本仓 f2-lite 引擎的 diff；
  故 upstream 语境里见到的 `reexecuteFrom` 等旧名，都是引用本仓 f2-lite 侧）。

## 9. doc obligations (what must be published to avoid repeated questions)

1. `docs/quick-start.md` get a "redo recipe" with importedFrom fields annotated.
2. `feature-requirements.md`: F6 explicit positioning "别(ld validity) persistence primitive".
3. `tanstack-alignment.md`: append to divergence清单 a small entry "INVALIDATED = 0,
   INSTEAD F2 covers".
4. New `docs/concepts/redo-vs-invalidation.md` (this file).
5. `docs/core-runtime-map.md`: §3 line adds no extra primitives; no change needed.
6. Tests: one E2E scenario "redo round trip preserves audit refs + new run result".

## 10. pointers

- F1 mechanism: engine/run_workflow.rs:876-876 / 902  (see command above)
- F2 path: commit 057a0c6 + runtime 91111f3
- F4/F5 alignment: commit c536e03 / 0971b4c