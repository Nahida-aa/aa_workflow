# redo：新 run 播种幸存 checkpoint（TS 版本实现）

> 基线：`learn_ls/workflow` **f2-lite 分支** HEAD = `b711b51`。
> ⚠️ **不在 `taren/durable-recovery`（`0971b4c`）上**——与 interim goals 同批
> 引入（`b9eebed`），是 **f2-lite 自有设计**。核验：
>
> ```bash
> cd /home/aa/repos/lib_ls/learn_ls/workflow
> git grep -c "reseedRun" 0971b4c -- packages/      # 空 = 基线上没有
> git log --oneline -S "reseedRun" f2-lite | tail -1
> ```
>
> 曾用名 `reexecuteFrom` / `unsafeInvalidate`（`b711b51` 改名，参数 `redoSet`
> 见 `1f55afa`）。0.x 阶段不留 alias。

> **约定**（同 `_01`/`_02`）：代码块从真实实现裁剪，不改语义；每块下方给对端
> 测试，冲突时以测试为准。跑法见 `_01` 顶部（**用 `node` 跑 vitest**）。

## 0. 一句话总览

redo = **开一个新 run，把源 run 的幸存 `STEP_FINISHED` 抄进它的日志**，
抄的时候给每条打上「来自哪」的戳（`meta.importedFrom`）。

```text
startRun(新 runId, reseedRun: { sourceRunId, redoSet })
  ├ 建 run 状态（status: running）、emit RUN_STARTED（不落盘）
  ├ history = []
  └ if (reseedRun) {
      读源 run 全量日志
      for 每条 STEP_FINISHED:
        stepId ∈ redoSet ? 跳过 : 抄进新日志（index 从 0 连续）+ 打 importedFrom 戳
      history = 抄进来的这些
    }
  → driveHandler(history)   ← handler 照跑，原语各自查这份「伪造的过去」
```

**零新事件类型、零 store 契约变更**——靠的是「新 run 的日志从空开始」这个前提：
`appendEvent` 的 CAS（index 必须等于当前长度）天然成立。

---

## 1. 声明（`run-workflow.ts:72-104`）

```ts
/** F2-lite re-execute plan (redo side): start a new run with the
 *  source run's surviving checkpoints imported into its log — every
 *  `STEP_FINISHED` whose stepId is NOT in `redoSet` (failed steps are
 *  re-executed by design). Provenance rides the event's
 *  `meta.importedFrom`. Only usable on the start path of a fresh run.
 *
 *  `redoSet` — the theory:
 *    A step is re-executed iff its checkpoint was NOT imported. The
 *    engine imports only the source run's `STEP_FINISHED` events for
 *    stepIds absent from `redoSet`; a `STEP_FAILED` step is never
 *    imported, so it re-runs by default. Everything the source run
 *    succeeded on — except the ones you list — carries over cached.
 *  …
 *  Named `reseedRun` because it lands in a *new* run ("re-seed a fresh
 *  run from the source run's surviving checkpoints"). The `re-` prefix
 *  is load-bearing: it's the second seed (incremental), not a fresh
 *  init, and it joins the redo word-family. */
reseedRun?: {
  sourceRunId: string
  redoSet?: ReadonlyArray<string>
}
```

三条从注释里读出来的关键约束：

1. **只能用在 fresh run 的 start 路径**（`Only usable on the start path of a
   fresh run`）。resume 路径忽略它——重放短路已经覆盖旧 checkpoint，再导入就是双写。
2. **`redoSet` 是「要重跑的」，不是「要保留的」**。语义是反着的：没列进来的
   → 有 checkpoint 就抄、不重跑；列进来的 → 不抄、重跑。空集/缺省 = 全部抄 = 零重跑。
3. **封闭性引擎不校验**（`the engine neither verifies the set is closed … nor
   assumes it isn't`）。列 `A→B→C→D` 的 `redoSet: [B, D]`：C 没列、抄了旧结果、
   不重跑；D 重跑时读到的是 C 的**旧**值。这是 advisory，不是错误——是否可接受
   由业务决定。

---

## 2. 实现：`startRun` 里的 20 行循环（`run-workflow.ts:286-315`）

```ts
// F2-lite: import the source run's surviving checkpoints into this
// run's log before the first drive (re-execute plan, redo side).
// Only STEP_FINISHED events are imported — failed steps are
// re-executed by design. Provenance rides the existing `meta` slot
// (meta.importedFrom), so no event type changes; the log's CAS guard
// still applies (a fresh run's log starts empty).
let history: ReadonlyArray<WorkflowEvent> = []
const reseed = options.reseedRun
if (reseed) {
  const redoSet = new Set(reseed.redoSet ?? [])
  const sourceLog = await runStore.getEvents(reseed.sourceRunId)
  const imported: WorkflowEvent[] = []
  for (let i = 0; i < sourceLog.length; i++) {
    const e = sourceLog[i]!
    if (e.type !== 'STEP_FINISHED' || redoSet.has(e.stepId)) continue
    const ev: WorkflowEvent = {
      ...e,
      meta: {
        ...(e.meta ?? {}),
        importedFrom: {
          runId: reseed.sourceRunId,
          index: i,
        },
      },
    }
    await emitAndAppend(runStore, runId, imported.length, emit, ev)
    imported.push(ev)
  }
  history = imported
}
```

四处值得注意：

| 位置 | 细节 |
| --- | --- |
| `:300` 过滤 | 只抄 `STEP_FINISHED`。`STEP_FAILED` **永不**导入 → 失败 step 默认重跑 |
| `:302-310` 打戳 | `...e` 展开整条事件（含 `ts`/`result`/`attempts`），只覆写 `meta`；`meta` 先展开旧的再叠 `importedFrom`，不丢原 meta |
| `:311` index | `imported.length` —— **新日志的下标**（从 0 连续），不是源日志的 `i` |
| `:290` 注释 | `the log's CAS guard still applies (a fresh run's log starts empty)` |

**`importedFrom.index` 记的是源日志下标**（`i`），不是新日志下标。测试
`engine.rerun.test.ts:64-73` 专门钉了这点，注释还说明了为什么不能从事件流取：

> Index is relative to the source's persisted log, not the event stream —
> the stream prepends the unpersisted RUN_STARTED.

`RUN_STARTED` 是 observability-only、**不落盘**（`_01` §4 已记），所以事件流
的第 0 条在 store 里不存在——从流里算下标会整体错一位。

---

## 3. 为什么必须是新 run（不是同 run 重跑）

事件日志是 **append-only + CAS**。要让一个已成功的 step 重跑，只有两条路：

1. **删旧 checkpoint**（截断日志）→ 破坏 append-only，且崩溃重放时历史分叉；
2. **开新日志，导入时排除它** → 就是 redo。

所以 redo 的「新 run」不是实现取巧，是存储模型决定的。代价也由此而来（都是
**显式**开销，不是隐藏成本）：

- 新 run id → 外部引用（工单号、UI 链接）要自己维护映射；
- input 要重喂（或由库层从源 run 拷）；
- 审计要跨两个 run 拼（`importedFrom` 提供了拼的线索）。

> 对照：想**同 run 内**换目标继续，用的是 interim goals（`_02`）+ 失效名单——
> 那条线见 `docs/feature-requirements.md` F1，尚未实现。
>
> 两种轴各有一份「不进引擎时怎么做」的记录：临时目标见
> `_04_interim-goals-without-engine.md`；redo 的库层形态（消费点查日志、
> 引用传递而非值拷贝）也在该文 §2。

### 3.1 另一种形态：不导入，消费点查日志（wf-demo `main` 分支）

`reseedRun` 不是唯一做法。wf-demo 的 `main` 分支（引擎 `0971b4c`，**无** f2-lite
改动）在库层这样实现同一需求：

```ts
// 消费点取值：从 sourceRun 日志提取存活 checkpoint（排除 unsafeInvalidate）
if (plan.sourceRunId) {
  const invalidate = new Set(plan.unsafeInvalidate ?? [])
  const events = await store.readEvents({ runId: plan.sourceRunId })
  lookup = new Map()
  for (const rec of events) {
    if (rec.event.type === 'STEP_FINISHED' && !invalidate.has(rec.event.stepId)) {
      lookup.set(rec.event.stepId, rec.event.result)
    }
  }
}
// …
return ctx.step(id, async () => {
  if (cached !== undefined) return cached   // 保留节点：零业务副作用
  return node.run(depVals, user)
})
```

**引用传递，不是值拷贝**：

| | 引擎 `reseedRun` | 库 `lookup` |
| --- | --- | --- |
| 源结果存在哪 | 新 run 的日志里（真拷贝） | 源 run 的日志里（每次读） |
| 新 run 的日志 | 含全部导入的 `STEP_FINISHED` | 只有真正执行的那几条 |
| 源 run 被删 | 新 run 不受影响 | **新 run 会重跑那些 step** |
| 溯源 | `meta.importedFrom` 自带 | 无（要自己从两个 run 拼） |

完整对照见 `_04_interim-goals-without-engine.md` §2。

---

## 4. 与 interim goals 的关系：两个正交的轴

`b9eebed` 一个提交同时引入两者，但它们是**独立的两件事**：

| | redo（`reseedRun`） | interim goals（`interimGoals`） |
| --- | --- | --- |
| 作用 | **哪些结果作废**（数据面） | **跑到哪停下**（控制面） |
| 载体 | 新 run 的**日志内容** | 一次 drive 的**参数** |
| 是否落盘 | 是（导入的 checkpoint 进日志） | 否（per-drive，不复用） |
| 可以叠加 | 同时给：新 run 导入幸存 checkpoint，跑到目标停 | 同左 |

代码上的交互点只有一个：`goalsDoneAtDriveStart` 在 drive 开始时查 `history`——
而 `history` 正是 redo 导入的那份。所以**如果目标 step 被导入（不在 `redoSet`），
该目标算「开局已达成」**，不会因为你设了这个目标就重跑它。想让它跑，必须列进
`redoSet`。

这个交互也是 core 的一个**静默失效点**（目标被导入 → 泊车整块跳过）。
详见 `_04` §4.2。

---

## 5. 库层的封装：`rerunDag`（wf-demo，两个分支语义不同）

引擎层的 `redoSet` 是**裸名单**（谁列谁重跑，不校验封闭性）。库层
`@wf-demo/workflow-dag` 的 `rerunDag` 在它之上加了图知识，但两个分支的
`interimGoals` 语义不同：

| | `f2-lite` 分支 | `main` 分支 |
| --- | --- | --- |
| `from` | `descendants(from)` ∪ `redoSet` | 同（缺省 = 上一版未成功节点） |
| `to`（`interimGoals`） | **跑到这里停**（引擎泊车） | **截断重跑区**：`to` 的严格后代推迟 |
| 实现 | 透传给引擎 | `region = full ＼ descendants(to)` |
| 闭包校验 | `to ∉ closure` → throw | `to ∉ full` → throw |

`main` 的注释（`index.ts:444-446`）：

```ts
// 临时目标（停止边界）：interimGoals 的严格后代推迟；
// 重跑区域 R = full ＼ beyond。
```

**两个分支都做闭包校验**，因为 `to` 落在片外时，无论叫「停」还是叫「截断」，
都不会按调用方想的发生。差别只在「停」vs「不调度」。

---

## 6. 代码位置索引（f2-lite `b711b51`）

| 主题 | 文件 : 行 |
| --- | --- |
| 参数声明 + `redoSet` 理论 + 命名理由 | `packages/workflow-core/src/engine/run-workflow.ts:72-104` |
| 导入循环（全部实现） | `run-workflow.ts:286-315` |
| `history` 成员（`history: []` → `history`） | `run-workflow.ts:292` / `:301` |
| 从 `drive` 到 `startRun` 的分流 | `run-workflow.ts:165-182`（见 `_01` §6 的陷阱） |

## 7. 测试位置索引

| 主题 | 测试 : 行（测试名） |
| --- | --- |
| 导入幸存 checkpoint + 重跑失效集 + provenance | `engine.rerun.test.ts:38` "imports surviving STEP_FINISHED events and re-executes the invalidated set" |
| 只给 `sourceRunId` → 全导入、零重跑 | `engine.rerun.test.ts:84` "imports everything (no invalidate list) when only sourceRunId is given" |

第一条测试钉了四件事：① `redoSet: ['a','b']` → a/b 重跑、c 零执行；
② 导入的 c 带 `meta.importedFrom = { runId, index }` 且 **index 是源日志下标**；
③ a/b 是新鲜执行、**没有** provenance 戳；④ 最终 status = finished。

跑法：

```bash
cd /home/aa/repos/lib_ls/learn_ls/workflow   # f2-lite 分支，HEAD b711b51
node node_modules/.bin/vitest run packages/workflow-core/tests/engine.rerun.test.ts
```

---

## 8. 与 Rust 侧（aa-workflow）的对应

`aa-workflow` 有同一机制（`ReseedRun { source_run_id, redo_set }` +
`with_redo_set`，runtime `reseed_run` 透传）。**语义对齐、实现风格不同**：
Rust 侧的对应设计文档是 `aa-workflow/docs/feature-requirements.md` F2 与
`docs/concepts/redo-vs-invalidation.md`。两边都在 0.x，**改一边要同时改另一边**
（配对纪律：`workflow` f2-lite ↔ `wf-demo` f2-lite ↔ `aa-workflow`）。
