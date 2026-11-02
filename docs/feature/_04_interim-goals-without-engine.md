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
三条都是**已确定**的机制事实：4.1 / 4.3 源于「引擎无图」，4.2 源于
「reseed 是部分重跑」这一设计意图。

### 4.1 目标拼写错误 → 静默跑完

`interimGoals: ['publsh']`：永远没有 step 叫 `publsh`，`interimGoalsDone` 永不含它，
`every(...)` 永假 → **不泊车，run 跑到底**，无任何提示。

**替代实现也一样能错**（库层的 `ancestors('publsh')` 会返回空集或 throw，取决于实现），
但库层**有图所以能查**（`rerunDag` 的 `:412-416` 就是这么做的）。

### 4.2 导入的目标算「已达成」→ 不重跑、不泊车（**这是设计，不是缺陷**）

`reseedRun` 的目的是**部分重跑**，不是从头重跑。导入的 checkpoint 就是「已完成」，
所以目标若被导入：

- **不重跑**（导入即完成，符合 reseed 意图）；
- **不泊车**（停在已完成的目标上，意味着「为了停在那里先把它重跑一遍」，
  与 reseed 相反）。

> 实测钉死：`engine.rerun.test.ts` "an imported goal counts as reached: no park,
> no re-execution"——`redoSet: ['b']` + `interimGoals: ['a']` ⇒ `exec == {b: 1}`、
> 无 `SIGNAL_AWAITED`、status `finished`。对照测试 "a goal that is not imported
> still parks normally" 证明这不是「目标永远不泊」。

推论（用的人要知道）：**`redoSet` 才是「哪些目标该真跑」的开关**。想让目标真跑，
就把它列进 `redoSet`；不列，导入会满足它。

例：`reseedRun: { redoSet: ['translate'] }` + `interimGoals: ['load']`——
`load` 是 `translate` 的上游、不在 `redoSet` 里，被导入；目标指向它 ⇒ 开局即达成
⇒ 不泊车、run 跑到底。想让 `load` 真跑并停在它上面，就把它列进 `redoSet`。

⚠️ **别把这理解成「泊车坏了」**：泊车只在「目标还没达成」时才有意义。
若调用方确实希望「重跑到 load 并停在 load 之后」，那 `redoSet` 里就该有 load。
库层 `rerunDag` 把这个矛盾变成硬校验（`outside the rerun closure`），因为
在它那种「from 决定重跑区」的模型里，闭包外的目标一定是**写错了**。

### 4.3 目标运行时不可达 → 静默跑完

目标 step 因条件分支走开而永不执行。引擎的 `every(...)` 永假 → 不泊车。
**这个补不了**：静态可达 ≠ 运行时可达（条件分支、`fail()` 短路径），需要图 + 运行时信息。

---

## 5. 能力对照：谁挡得住哪种误用

| 误用 | 引擎（core，f2-lite） | 库层 `rerunDag`（f2-lite） | 库层 `rerunDag`（main） |
| --- | --- | --- | --- |
| 节点名拼错 | 静默跑完 | **throw**（未声明） | **throw**（`to ∉ full`） |
| `to` 被 reseed 导入 | **静默跑完**（导入=已达成，符合设计） | **throw**（`outside the rerun closure`） | **throw**（`not inside the rerun closure`） |
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

---

## 7. 中间件方案（2026-11-02 实测）：不用引擎改动也能做到

### 7.1 形态

中间件给 `ctx` 加东西是上游**支持的路径**（`next({ context })`），
`ai-orchestration` 的 `ctx.ai` 就是这么来的。临时目标不需要新字段，只需要
**包装两个已有原语**：

```ts
function interimGoalsMiddleware() {
  return createMiddleware().server(async ({ ctx, next }) => {
    let goals: string[] = (ctx.input as any).__interimGoals ?? []   // 首驱初值
    const origStep = ctx.step
    const origWait = ctx.waitForEvent
    const done = new Set<string>()

    // ① 唤醒时更新目标：payload 带 __interimGoals
    ;(ctx as any).waitForEvent = async (name: string, o?: any) => {
      const r = await origWait(name, o)
      if (name === '__interim-goal' && Array.isArray((r as any)?.__interimGoals)) {
        goals = (r as any).__interimGoals
      }
      return r
    }

    // ② 每次 step 完成后检查目标是否达成
    ;(ctx as any).step = (id: string, fn: any, o?: any) =>
      (async () => {
        const r = await origStep(id, fn, o)
        if (goals.includes(id)) done.add(id)
        if (goals.length > 0 && goals.every((g) => done.has(g))) {
          done.clear()                                  // 开始新的一段
          await (ctx.waitForEvent as any)('__interim-goal', {
            id: `__interim-goal-${id}`,                 // 每段泊位 id 唯一
            meta: { __interimGoals: goals },
          })
        }
        return r
      })()

    return next({ context: {} })
  })
}
```

用法与续驱：

```ts
createWorkflow({ id, input: z.object({ __interimGoals: z.array(z.string()).optional() }) })
  .middleware([interimGoalsMiddleware()])
  .handler(...)

// 首驱：目标来自 input
runWorkflow({ workflow, runStore, runId, input: { __interimGoals: ['b'] } })
// 续驱：每次可换目标（空数组 = 跑到终点）
runWorkflow({ workflow, runStore, runId,
  signalDelivery: { name: '__interim-goal', signalId, payload: { __interimGoals: ['d'] } } })
```

实测三段（`workflow-core/tests/engine.interim-middleware.test.ts`）：

```text
D1: {a:1, b:1}                paused      input.__interimGoals = ['b']
D2: {a,b,c,d}                 paused      payload.__interimGoals = ['d']
D3: 全部完成                   finished    payload.__interimGoals = []
```

**零引擎改动、零 store 依赖、零额外状态字段。**

### 7.2 三个必须做对的地方（每个都实测验证过）

| 坑 | 后果 | 正确做法 |
| --- | --- | --- |
| 在 `ctx.step` 的**闭包内部**泊车 | `WorkflowPaused` 被记成 `STEP_FAILED`，续驱时 rethrow | 在 `orig()` **返回之后**再泊 |
| 泊完不清 `done` | 目标累积只增不减，本 drive 内不再停，**换目标也无效** | 泊完 `done.clear()` |
| 泊位 id 不带区分 | 多段泊车时 `findCheckpoint` 分不清 | id 带 step 名或计数器 |

另外两条与前面 §4 呼应：

- **续驱必须投信号**（`signalDelivery`），不能用 `recover: true`——后者不产生
  `SIGNAL_RESOLVED`，重放时 `waitForEvent` 匹配不到，会再泊一次。
- **目标从 payload 更新**而不是从 input 改：`resumeRun` 用 `persistedState.input`
  （`run-workflow.ts:432`），续驱传新 input 会被忽略。

### 7.3 它继承了 §4 的三条局限，但没有引入新的

| §4 的问题 | 中间件方案 |
| --- | --- |
| 目标拼写错 → 静默跑完 | **同样**（无图），但 `goals` 就在用户手里，可以自己先校验 |
| 目标被 reseed 导入 → 不泊车 | **同样**（导入的 `STEP_FINISHED` 也会让 `done` 命中），语义一致 |
| 目标运行时不可达 → 静默跑完 | **同样**（分支走开，`every(...)` 永不成立） |

**中间件方案不是「绕过问题」，是「同一个软围栏，换个地方实现」**——所以它的
能力边界与引擎版一致，区别只在谁来维护（中间件 vs 引擎）和依赖什么（守卫未覆盖
的行为 vs 自己维护的参数）。

### 7.4 风险：依赖一个未承诺的实现细节

§7.1 的 `ctx.step = ...` 能生效，靠的是两件事：

**① 中间件拿到的是同一个 `ctx` 对象引用**

```text
composeMiddlewares (run-workflow.ts:1468)
  await m.server({ ctx, next })     ← ctx 是 baseCtx 本体（:606），不是拷贝
```

所以直接属性赋值对后续中间件与 handler 可见。

**② 保留字段的守卫只覆盖 `next({ context })` 这条路径**

```ts
// :1488-1499（next() 内部）
for (const key of Object.keys(ext)) {
  if (reservedCtxFields.has(key)) throw new Error('may not shadow reserved ctx field')
}
Object.assign(ctx, ext)             // 只检查 ext 的 key
```

`reservedCtxFields`（`:1451-1466`）**包含 `step` 与 `waitForEvent`**，但只挡
`next({ context: { step } })`。全仓没有 `Object.freeze(ctx)` / `defineProperty`。

**上游先例核查**（`b711b51`，grep `packages/ examples/ docs/ research/`）：

```text
ctx.step = ...            → 零命中
origStep / wrapStep       → 零命中
Object.assign(ctx, ...)   → 只有引擎自己那一处（:1499）
```

`ai-orchestration` 的 `agentMiddleware` 也只是**调用** `ctx.step`（在 `ctx.ai.agent`
内部），**没有包装或改写**。

**所以准确的说法是**：这不是「另一条合法路径」，而是**守卫未覆盖的行为**。
上游文档明说「Middleware extensions cannot shadow reserved ctx fields」——语境虽
指 `next({context})`，但意图是保留字段不被动。因此：

- 现在能用（有测试背书），适合**原型与自研工具**；
- 上游一旦 `Object.freeze(ctx)` 或补上直接赋值的守卫，**中间件会静默失效**（不报错）；
- 要作为长期契约，最终应落到 **drive 参数**（`RunWorkflowOptions.interimGoals`，
  见 `_02`）或**新原语**（如 `ctx.park()`），而不是中间件包装。

### 7.5 与引擎方案的取舍

| | 中间件方案 | 引擎参数（f2-lite `interimGoals`） |
| --- | --- | --- |
| core 改动 | 0 | 47 行 + 3 字段 |
| 首驱指定目标 | `input.__interimGoals` | `RunWorkflowOptions.interimGoals` |
| 续驱换目标 | `signalDelivery.payload.__interimGoals` | 同参数 per-drive |
| 需要 `inputSchema` 声明 | **是**（否则被剥离） | 否 |
| 续驱通道 | **必须 `signalDelivery`** | `recover` 或 `signalDelivery` 均可 |
| 依赖上游承诺 | ❌（守卫未覆盖的行为） | ✅（自己维护） |
| 适合 | 原型、自研工具、不想动 core | 长期契约 |

