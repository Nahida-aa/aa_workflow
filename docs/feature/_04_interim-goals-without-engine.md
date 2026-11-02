# 临时目标的替代实现：不让引擎知道「目标」

> 配套文档：`_02_interim-goals.md`（引擎原生实现）、`_03_redo.md`（redo）。
> 本文回答：**如果 `interimGoals` / `reseedRun` 不进入 core 的 API，同样的
> 需求怎么实现？** 参考实现在 `learn_ls/wf-demo` 的 **`main` 分支**
> （对应引擎基线 `0971b4c`，**不含** f2-lite 的引擎改动）。
>
> | 分支 | 引擎 | 目标怎么实现 |
> | --- | --- | --- |
> | `main` | `0971b4c`（无 f2-lite 改动） | **库层**：DAG 库把图算成节点子集，用普通 `ctx.step` 调度 |
> | `f2-lite` | `4cc7a46`（含引擎改动） | **引擎层**：`interimGoals` 参数 + 保留信号泊车 |

## 0. 两种实现的本质差别：谁持有「图」

| | 引擎原生（f2-lite） | 库层替代（main） |
| --- | --- | --- |
| 谁知道 DAG 形状 | **没人**（引擎无图） | **库**（`buildStaticDag`） |
| 「停」怎么表达 | 保留信号 `__interim-goal` + 一次真泊车 | **根本不泊车**——只调度子集，跑完自然结束 |
| run 状态 | `paused`（可续驱） | `finished`（子集跑完就是跑完） |
| 续驱 | `recover: true` / `deliverSignal` | 同 run 续驱走 `continueDagRun`（但 main 上它通常只处理 yield）；「下一段」用 `rerunDag`，`to` 在那里是**截断重跑区**（见 §3.1） |
| 引擎改动 | +47 行（声明 + 检查块 + 3 字段） | **0 行** |

关键洞察：**「停住」不是必须的**。如果宿主的意图是「这一段先跑这些，我看看再决定」，那么「只跑这些、run 正常结束」也能满足——代价是续跑要新开 run（redo），而不是同 run resume。

---

## 1. 替代实现：把目标翻译成节点子集

`wf-demo` main 分支 `packages/workflow-dag/src/index.ts` 的 `dagToWorkflow`：

```ts
// 临时目标（停止边界）：只调度祖先闭包内的节点
const goals =
  plan.interimGoals === undefined
    ? []
    : Array.isArray(plan.interimGoals)
      ? plan.interimGoals
      : [plan.interimGoals]
const active =
  goals.length > 0 ? new Set(goals.flatMap((t) => [...graph.ancestors(t)])) : null
const ids = active ? [...active] : Object.keys(spec.nodes)
// …
await Promise.all(ids.map((id) => ensure(id)))
```

**`ids` 就是本次要跑的节点集**。`ensure(id)` 递归确保依赖先完成，所以：

- 没设目标 → `ids` = 全部节点 → 跑全图；
- `interimGoals: ['merge']` → `ids` = `ancestors(merge)` = `{load, enrich, translate, merge}` → `publish` **根本不进调度**。

于是「停在 merge」= 「publish 压根没被 ensure」。**不需要泊车，因为压根没有要停的东西**。

> 对端测试：`wf-demo` main 分支
> `packages/workflow-dag/tests/dag-pipeline.test.ts` + `dag-plan.test.ts`
> （`main` 分支上跑：`bun test packages/workflow-dag/tests/`）。

### 目标语义在这里是「祖先闭包」

`ancestors(t)` 而不是 `t` 本身——因为要跑到 t，必须先把它的上游跑完。这与引擎实现的
`interimGoalsDone` 累积是同一个语义，但**算的地方不同**：引擎是运行时逐个 step
检查「是否都达成了」，库是开跑前一次算好「需要哪些节点的集合」。

**这个差别决定了校验能力**：

| | 引擎（运行时） | 库（开跑前） |
| --- | --- | --- |
| 目标不在图里 | 不知道（无图），永不达成 → 静默跑完 | **throw**（`:412-416` 查 declared） |
| 目标不可达 | 只能 best-effort | 静态可达性可算 |

---

## 2. redo 的替代：在消费点查日志

同一文件里，`sourceRunId + unsafeInvalidate` 也是库层实现：

```ts
// 消费点取值：从 sourceRun 日志提取存活 checkpoint（排除 unsafeInvalidate）
let lookup: Map<string, unknown> | undefined
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

**引用传递，不是值拷贝**：不把源 checkpoint 抄进新日志，而是每次 `ctx.step` 前查一下
`lookup`，有就**直接返回**、不调业务函数。

两种做法的对照：

| | 引擎 `reseedRun`（f2-lite） | 库 `lookup`（main） |
| --- | --- | --- |
| 源结果存在哪 | **新 run 的日志里**（真导入） | **源 run 的日志里**（每次读） |
| 新 run 的日志 | 含全部导入的 `STEP_FINISHED` | **只有真正执行的那几条** |
| 审计 | 新日志自包含 + `meta.importedFrom` 溯源 | 要看两个 run |
| 读成本 | 一次性（导入时） | 每 step 一次查表（已建好 Map） |
| 源 run 被删 | 新 run 不受影响（已拷贝） | **新 run 会重跑那些 step** |

最后一行是真实差别：**值拷贝 vs 引用传递**决定了「源 run 的生命周期」是不是新 run 的依赖。

---

## 2.1 ⚠️ 两套 `to` 语义不一样（最容易误读的地方）

同样是 `interimGoals`，`main` 和 `f2-lite` **指的完全是两件事**：

| | `main`（库层，`rerunDag` `:443-456`） | `f2-lite`（引擎，`_02`） |
| --- | --- | --- |
| 语义 | **截断重跑区**：`to` 的严格后代**推迟**，本次不跑 | **跑到这里停**：`to` 达成后泊车 |
| 实现 | `region = full ＼ descendants(to)` | `interimGoals.every(done)` → `waitForEvent` |
| 目标之外 | 增量完成：`to` 之前跑完，`to` 之后**不调度** | 增量**挂起**：等宿主决定 |
| run 结果 | `finished`（output 只含 region 内的节点） | `paused`（可被 `recover` 续） |
| 校验 | `to ∉ full` → **throw**（`:444-449`） | 无（引擎无图，见 §4） |

`main` 的注释写得很清楚（`index.ts:444-446`）：

```ts
// 临时目标（停止边界）：interimGoals 的严格后代推迟；
// 重跑区域 R = full ＼ beyond。
```

**所以「自动忽略」在 main 上本来就不成立**：`to` 在重跑区外时它直接 throw，
而不是忽略——因为忽略会让「重跑区」变成一个调用方没预期的形状。

---
## 3. 为什么后来还是进了引擎（f2-lite 的选择）

既然库层能做，为什么 f2-lite 还要改引擎？两个原因，都不是「库层做不了」：

**1. 「停」和「跑完」是不同的事实。**
库层方案里，停在 merge 的 run 状态是 `finished`——从这个状态**看不出**「用户还想继续」。
引擎方案泊成 `paused`，`waitingFor.signalName = '__interim-goal'`，是可观测、可查询、
可被 sweep 看见的活的状态。

**2. 续跑不需要新 revision。**
库层方案要下一段就得 `rerunDag` 开新 run（新 id、审计跨 run）。引擎方案
`recover: true` 就接着跑——同一个 run，日志连续。

这就是「**人想要明确干预时的行为**」：如果只是「这一段先跑这些」的批处理语义，
库层足够；如果是「我现在停下来，人看完再决定」的交互语义，需要一个真的暂停态。

---

## 4. core 不校验的三个问题（机制事实，不是 bug）

core 没有 step 清单（`WorkflowDefinition` 只有 `id`/`version`/`previousVersions`/
`defaultStepRetry`/`middlewares`/`handler`，没有 `steps`），所以引擎**判不了**以下三种误用。
这些都是机制层面的事实，用 `interimGoals` 的人必须知道：

### 4.1 目标拼写错误 → 静默跑完

`interimGoals: ['publsh']`：永远没有 step 叫 `publsh`，`interimGoalsDone` 永不含它，
`every(...)` 永假 → **不泊车，run 跑到底**，无任何提示。

**替代实现也一样能错**（库层的 `ancestors('publsh')` 会返回空集或 throw，取决于实现），
但库层**有图所以能查**（`rerunDag` 的 `:412-416` 就是这么做的）。

### 4.2 目标在 reseed 闭包外 → 泊车机制整体失效

`reseedRun: { redoSet: ['translate'] }` + `interimGoals: ['load']`：

- `load` 不在 `redoSet` → 它的 `STEP_FINISHED` 被**导入**新日志（`_03` §2）；
- drive 开始 → `goalsDoneAtDriveStart = true`（`load` 已在 `history` 里）；
- **整块检查跳过**（`run-workflow.ts:1126` 的闸）→ run 一路跑到底。

后果不是「忽略了这个目标」，而是「**这个目标让本次 drive 的泊车完全失效**」。

> ⚠️ **core 其实有区分「导入」与「本地执行」的信息，只是没用**：
> 导入的 checkpoint 带 `meta.importedFrom`（`_03` §2），而
> `goalsDoneAtDriveStart` 的判断（`run-workflow.ts:542-544`）**只看 `type` 和
> `stepId`，没看 `meta`**。所以「导入的目标算不算达成」在 core 里是**未定义**的——
> 当前行为是「算达成（不泊车）」，理由是续驱时的合法场景（上次已停在目标上）也需要它。

### 4.3 目标运行时不可达 → 静默跑完

目标 step 因条件分支走开而永不执行。引擎的 `every(...)` 永假 → 不泊车。
**这个补不了**：静态可达 ≠ 运行时可达（条件分支、`fail()` 短路径），需要图 + 运行时信息。

---

## 5. 能力对照：谁挡得住哪种误用

| 误用 | 引擎（core，f2-lite） | 库层 `rerunDag`（f2-lite） | 库层 `rerunDag`（main） |
| --- | --- | --- | --- |
| 节点名拼错 | 静默跑完 | **throw**（未声明） | **throw**（`to ∉ full`） |
| `to` 不在重跑闭包内 | 静默跑完（泊车失效） | **throw**（`outside the rerun closure`） | **throw**（`not inside the rerun closure`） |
| `to` 运行时不可达 | 静默跑完（best-effort） | 静默 | **不适用**（截断重跑区，不是「停」） |
| `to` 恰好是最后一步 | 停成 `paused`，需再驱一次 | 同上（引擎语义） | 正常 `finished`（region 内最后一个） |
| `to` 存在但无后代 | — | 停在那里 | region = `full`（`beyond` 为空），等同不设 |

两个库层实现**都**做了闭包校验，措辞不同但判定一致：`to` 必须在「这次要重跑的
那一片」里。理由相同：`to` 若在片外，无论叫「停」还是叫「截断」，都不会按调用方
想的发生。

「`to` 恰好是最后一步」这行最能体现两套语义的分野：**引擎方案里「停」是额外动作**
（step 完成后还要主动泊车），所以要处理「停在终点」这个边界；`main` 的库层方案里
根本没有「停」这个动作——`to` 的后代不调度，run 跑完 region 自然 `finished`。

---

## 6. 结论：机制说明该写在哪

| 内容 | 位置 |
| --- | --- |
| `interimGoals` 的语义（全齐才停、软围栏、per-drive） | 引擎 API 文档（`_02` §1-§6） |
| **三种误用 + core 判不了的原因** | **本文 §4**（机制事实） |
| 库层能查什么、为什么能查 | `_02` §6 对照表 + 本文 §5 |
| 「不泊车」的替代形态 | 本文 §1-§3 |

**给用 core 的人的一句话**：`interimGoals` 是**兜底**语义，不是断言——
它会尽力在目标达成时停下，但**不保证**停（拼错、不可达、被导入，都是静默的）。
要「一定停」，就得在库层用图校验（f2-lite 与 main 的 `rerunDag` 都这么做了），
或者采用「只调度子集」的形态（main 的 `to` 语义——这时没有「停」这回事，只有
「这段跑完了」）。
