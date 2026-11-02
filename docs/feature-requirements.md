# 功能需求清单

本文件是**唯一的功能需求真相源**：记录「我要的功能」——场景、为什么现有手段不够、
自然形态是什么、两侧（f2-lite TS / aa-workflow Rust）状态。动源码前先读这里；
新增需求先在这里登记，再进任一线实现。

配对纪律：`workflow` 仓 f2-lite 分支（TS 实验线）↔ `aa-workflow`（Rust 产线）。
新原语一般先在 f2-lite 验语义，再镜像到 Rust。

## 状态总表

| # | 功能 | f2-lite (TS) | aa-workflow (Rust) | 状态 |
| --- | --- | --- | --- | --- |
| F1 | 同 run 继续：resume + 失效名单 + 临时目标 | ❌ | ❌ | **设计中，未实现** |
| F2 | redo：新 run 播种幸存 checkpoint | ✅ | ✅ | 已实现（原 `reexecuteFrom`，已改名 `reseedRun`） |
| F3 | 临时目标泊车（interim goals） | ✅ | ✅ | 已实现 |
| F4 | 信号收件箱（buffer_signals） | ✅（基线 taren 分支） | ✅ | 已实现（Rust 侧 `c536e03` 等） |
| F5 | 耐久重试 + 补偿 | ✅ | ✅ | 已实现 |
| F6 | INVALIDATED 耐久失效原语 | ❌ | ❌ | **已评估：不做**（区别于 F2 redo/seed，见 `docs/concepts/redo-vs-invalidation.md`） |

## F1. 同 run 继续：resume + 失效名单 + 临时目标

**场景**：一个已经跑完 / 已停下（到达 interim goal 或 error）的 workflow，
resume 时可以选择：

- **失效名单**：哪些 step 作废重跑；
- **临时目标**：跑到哪些 step 停；**目标为空 = 跑到终点**；
- 同一个 run 继续，**不新开 run**。

**为什么现有手段不够**：

- `interim_goals`（F3）只管「停在哪」，不管「哪些重跑」；
- `reseed_run`（F2）能表达失效，但要**开新 run**：新 run id、外部引用断裂、
  input 重喂、审计跨两个 run 拼。为「换目标继续」付这些代价，收益是零——是 hack。

**自然形态**：drive 参数 `invalidate_steps: Option<Vec<String>>`，与
`interim_goals` 并列（core 的 `RunWorkflowOptions` 与 runtime 的
`WorkflowRuntimeStartRunArgs` 各加一处，两侧同名同形）。语义与
`reseed_run.redo_set` 是**同一个 advisory 语义**（引擎**不**校验名单对依赖方向
封闭——不封闭**可能**让下游拿到旧 checkpoint，是否可接受由调用者业务判断：
失效 B 不失效下游 C，C 会拿旧结果短路），只是作用对象是一次 drive 而非新 run 导入。

**机制**（已对 Rust 代码核实）：drive 启动时从折叠出的 `lives` 删掉被点名的
entry，step 回 `Pending` → 重跑 → 追加新 `StepFinished` → 重放时**后者胜**
（`fold_step_states` 覆盖写）。不碰事件系统、不碰 store 契约、不开新 run。

**两个必须处理的细节**：

1. 同时清 `pending_retries`——耐久重试的到期检查在 replay 短路**之前**，
   不清会先在 `__timer` 泊车、根本不重跑；
2. `goals_done_at_drive_start` 用失效**后**的 lives 算——否则「失效的 step 恰好
   是 goal」会被误判为已达成。

**设计细节与判据**：`workflow` 仓 `docs/internal/f2-lite-feature-points.md`
（含官方 ai-orchestration 用法对照、「隐式新开 run」的四条评估标准）。

**落地顺序**：f2-lite 先行加 `invalidateSteps` 并写测试 → Rust 镜像 → 回本文件
更新状态。

## F2. redo：新 run 播种幸存 checkpoint（已实现，已改名 `reseedRun`）

`reseed_run(source_run_id, redo_set)`：全新 run 首驱前导入源 run
幸存的 `STEP_FINISHED`（失败 step 按设计重跑），溯源 `meta.importedFrom`。
两侧都有（f2-lite `reseedRun` / Rust `057a0c6`、runtime 透传 `91111f3`）。

**已改名 `reseedRun`**（原 `reexecuteFrom`）：旧名没体现"落到一个新 run 里"，
且 `re-` 前缀保留（增量播种 + redo 词族）。0.x 阶段两侧同步、直接换新名、
不留 alias。论证见 `docs/concepts/redo-vs-invalidation.md` §6。

**定位**：显式新开 run = 常规手段，服务于「换世界重来 + 旧日志原样不动」，
不是继续手段。日志型代价（新 id / 引用断 / 审计跨 run）由它的问题域承担。

> **注意**：F2 的 redo（seed）≠ F6 的 INVALIDATED。redo/seed 机制轻（零新事件、
> 零 store 契约变更，已落地），**保留**；INVALIDATED 持久失效原语才是不做的对象。
> "redo 不进 core" 若被听成"seed 也不做"是误读——seed 是失败续跑的正确默认路径。

## F3. 临时目标泊车（已实现）

`interim_goals`：名单内 step 全部 checkpoint 后泊车（保留信号 `__interim-goal`），
宿主续驱 = 同名信号投递（Rust）或 `recover: true` drive（f2-lite core）。
per-drive 参数，不落盘。转移检查防重挂。

## F4. 信号收件箱（已实现）

早到信号：`buffer_signals` 开关（默认关，对齐上游）+ store 收件箱
（signalId 去重、终态与 `__timer` 拒收、一次性消费）+ core 等待点消费后落账。
Rust 侧四个事件类型、`signal_id` 进日志、投递过闸均已对齐基线 `0971b4c`。
TS 侧基线 `taren/durable-recovery` 原生有 `bufferSignals`。

## F5. 耐久重试 + 补偿（已实现）

`retry.durable`（`StepRetryScheduled` 泊车 + sweep 续驱 + `StepAttemptStarted`
事件）与 `ctx.compensate`（`CompensationRegistered/Started/Completed/Failed` +
逆序 undo + 重复 id 报错）。两侧对齐基线。

## F6. INVALIDATED 耐久失效原语（已评估：不做）

与 F1 的区别：F1 的失效是 **drive 参数**（不传就不失效）；INVALIDATED 要求
失效**持久生效**（下次 resume 不传参也作废）且日志可独立读出「这步被作废过」，
需要新事件 + 重放规则 + 状态转换。

**不做**的理由：当前没有「跨 drive 持久失效」的场景；F1 的 drive 期过滤 +
日志两条 `StepFinished` 后者胜已覆盖需求。若将来出现该场景（例：审计要求
「作废决定本身是耐久事实」），按配对纪律两遍实现，**先写设计文档**。

更详尽 rationale（engine complexity、redo trade-offs、decision tree、upstream
supporting points）见 `docs/concepts/redo-vs-invalidation.md`（ADR 式，编号与本仓
F1/F2/F6 对齐）。

## 通用约束（本轮明确过的，新增功能都要过）

1. **隐式新开 run 默认禁止**。引擎在 resume 时内部创建新 run 而调用方不直接
   指定，会让 attach / sweep / lease 全部无法回答「操作的是哪个 run」。只有当
   全部满足才可考虑：(a) 调用方可见 id 不变或映射落 store；(b) 新旧日志可拼接；
   (c) attach 旧 run 与驱动新 run 事实一致；(d) 穷尽同 run 手段后仍无法表达。
2. **语义对齐优先于代码移植**（本仓既有原则）：能用自己的机制达成上游可观察
   行为时，不照搬机制；但**结构差异要写进文档**，不留在暗处。
3. **命名 / 行为对齐上游**是为降低长期维护成本（用户 2026-10 明确）：每次上游
   改动都要重新翻译、每个新人要先学分歧，是会复利的成本。与上游不同的地方，
   在 `docs/tanstack-alignment.md` 记档。
4. **功能以自然形态落地**：如果实现需要调用方做「本来不该由它做」的事
   （如手工拼新 run 续跑），先回来改引擎，不要在上层堆补丁。

---

## 设计取舍记录：临时目标与 redo 该放哪一层

**决策日期**：2026-11-02。**状态**：临时目标 → core；redo → core 给机制、图校验留 dag。

讨论过三个选项（「都进 core」/「都只在 dag」/「都不做，教用户手写」）。下面是
结论与**被否决的理由**，以及一次论证的推翻过程——后者比结论更值得留。

### 结论

| 功能 | 放哪 | 理由 |
| --- | --- | --- |
| **临时目标**（`interimGoals`） | **core** | 是 workflow 的**控制语义**，与图无关；手写代价高（见下） |
| **redo 机制**（`reseedRun` 导入循环） | **core**（20 行） | 与图无关：读源日志、按名单过滤、抄进新日志 |
| **redo 的闭包校验** | **dag 层**（`rerunDag`） | 只有图能算 `descendants` / 闭包；core 只能给裸名单 `redoSet`（advisory） |
| **「停」的替代形态**（不调度子集） | dag 层可选 | main 分支的做法，run 正常 `finished`，无续驱 |

### 中间件方案：可行，且实测与引擎版等价（2026-11-02）

用户提出：ai-orchestration 就是用中间件（`.middleware([agentMiddleware()])`）扩展
`ctx`，而不是改 core——临时目标能不能也这么办？

**能，实测通过。** 中间件拿到的是同一个 `ctx` 引用（`composeMiddlewares`
`run-workflow.ts:1490`），虽然 `step` 在 `reservedCtxFields` 里（不能用
`next({ context: { step } })` 覆盖），但可以**直接改写 `ctx.step`**。

关键有两处，踩错任一处就退化成有缺陷的版本：

```ts
function interimGoals(store, goals) {
  return createMiddleware().server(async ({ ctx, next }) => {
    const orig = ctx.step
    // ① 转移检查：开局前目标已全达成 ⇒ 本次 drive 不泊（否则续驱会重复泊）
    const hist = await store.getEvents(ctx.runId)
    const doneAtStart = goals.every(g =>
      hist.some(e => e.type === 'STEP_FINISHED' && e.stepId === g))

    const done = new Set<string>()
    let parks = 0, parked = false

    ctx.step = (id, fn, opts) => (async () => {
      const r = await orig(id, fn, opts)          // ② step 先正常收尾
      if (doneAtStart || parked) return r
      if (goals.includes(id)) done.add(id)
      if (goals.every(g => done.has(g))) {        // ③ 在闭包之外泊
        parked = true
        await ctx.waitForEvent('__interim-goal', {
          id: `__interim-goal-${parks++}`, meta: { goals },
        })
      }
      return r
    })()

    return next({ context: {} })
  })
}
```

实测结果（`a → b → c`，`goals: ['b']`；测试
`packages/workflow-core/tests/engine.interim-middleware.test.ts`，含一条反向
测试证明「去掉 `doneAtStart` 闸就会重复泊车」）：

| | 首驱 | 续驱（`recover: true`） |
| --- | --- | --- |
| exec | `{a:1, b:1}`（c 未跑） | `{a:1, b:1, c:1}` |
| status | `paused` | `finished` |
| 日志里的泊车点 | 1 | **仍 1**（不重复泊） |
| 假 `STEP_FAILED` | 无 | 无 |

与引擎版逐项一致。踩过的三个坑（都实测确认过）：

1. **在 `ctx.step` 的 fn 内部泊** → `WorkflowPaused` 被记成 `STEP_FAILED(a)`，
   续驱时 rethrow、run 无法继续。所以必须 `await orig(...)` 之后再泊。
2. **中间件 `done` 每次 drive 新建** → 续驱重放时 a/b 短路返回、`done` 又齐了
   → **重复泊车**。需要 `doneAtStart` 那道闸（对应引擎的 `goalsDoneAtDriveStart`）。

   两点澄清（2026-11-02 实测）：

   - **换续驱方式没用。** `recover: true` 与 `signalDelivery` 进的是同一个
     `resumeRun`（`run-workflow.ts:228` 与 `:232`），都会走到 `driveHandler`
     重跑 handler。用 `signalDelivery` 续驱，实测**同样重复泊车**（日志尾部又出现
     一条 `SIGNAL_AWAITED(__interim-goal)`，run 仍是 `paused`）。
   - **根因在 replay，不在续驱通道。** 栈不能持久化，所以「恢复运行」= 重跑 handler
     + 已完成的 step 短路返回。短路之后控制权**回到包装层**，包装层无从得知「这次
     是短路返回的」，于是再判一次「目标齐了」。引擎能在短路分支直接 `return`
     （`engineStep` 的 `cached` 分支），所以天然绕开。
3. 泊车 id 必须带计数器，否则多次泊车时 `findCheckpoint` 分不清。

### 中间件方案 vs 引擎方案

| | 中间件（用户或库提供） | 引擎内建 |
| --- | --- | --- |
| core 改动 | **0 行** | 47 行 + 3 个字段 |
| 用户成本 | `.middleware([interimGoals(store, ['b'])])` | `.interimGoals(['b'])` 参数 |
| 语义 | 实测等价 | — |
| 需要 `store` | **是**（`ctx` 上不暴露；工厂多一个参数） | 否（引擎内部就有） |
| 谁维护 | 提供中间件的人 | 引擎 |
| 是否进公开 API | 否（组合式，可被替换） | 是（一等参数） |

**关于 `ctx` 上有没有 store（已核实）**：没有。

| 来源 | 含 store / history |
| --- | --- |
| `BaseCtx`（中间件拿到的） | ❌ 只有 `runId`/`input`/`state`/`signal`/`runtime` + 9 个原语 |
| `ctx.runtime` | ❌ 只有 deadline / timeRemaining / shouldYield / yield |
| `StepContext`（step 闭包内） | ❌ 只有 `id`/`attempt`/`runtime`/`signal` |
| `engine` 对象 | ❌ 被闭包锁在 `ctx.step` 等函数里（`run-workflow.ts:571-601`），无处泄露 |

所以要读日志（做转移检查、或实现 redo 的 lookup），中间件只能**从外部闭包捕获
store**：`interimGoalsMiddleware(store, goals)`。

**但这基本不是负担**：装配这个中间件的地方，本来就要拿到 store 去建 runtime
（`defineWorkflowRuntime({ store, ... })`）。写成工厂 `makeWorkflow(store, goals)`
即可——而这是测试里**已经在用**的形状（`makeWf(exec)` 到处是）。已实测：

```ts
function makeWorkflow(store, goals) {
  const mw = interimGoalsMiddleware(store, goals)   // store 从闭包进
  return createWorkflow({ id }).middleware([mw]).handler(...)
}
```

跑通，`paused` 正常。**代价是一次函数签名的改动，不是架构问题。**

> 对照：ai-orchestration 的 `agentMiddleware()` 不需要 store（它只包 step 调用、
> 映射事件），所以没这个问题。需要读日志的中间件多一个参数而已。

### 为什么临时目标该进 core

一度认为「dag 库调 `ctx.waitForEvent` 只需 2 行，不必进引擎」。**该论证错了**：

`waitForEvent` 那一行只是**引擎内部**的样子。用户要手写等价物，需要：

```ts
const done = new Set<string>()
let parks = 0
const wrapped = (id: string, run: () => Promise<T>) => ctx.step(id, async () => {
  const r = await run()
  if (goals.includes(id)) done.add(id)
  if (goals.every(x => done.has(x))) {
    await ctx.waitForEvent('__interim-goal', {
      id: `__interim-goal-${parks++}`,   // 不计数则 findCheckpoint 分不清第几次泊车
      meta: { goals },
    })
  }
  return r
})
```

即：**约 10 行 + 改写所有 `ctx.step` 调用点**，还要自己想到「id 要带计数器」
这类细节。绝大多数用户直接写 `ctx.step('a', run)`，不会包装它。

**而且上面这个写法是错的**（2026-11-02 实测）。在 `ctx.step` 的闭包**内部**调
`waitForEvent`，`WorkflowPaused` 会被当成 step 的业务失败记进日志：

```
原始日志：RUN_STARTED, STEP_STARTED, SIGNAL_AWAITED, STEP_FAILED(a, WorkflowPaused)
```

后果：续驱重放时 a 是 `STEP_FAILED` 而不是 `STEP_FINISHED` → 被 rethrow，
整个 run 无法继续。**泊车必须发生在 step 成功结束之后**：

```ts
const a = await ctx.step('a', run)                 // step 先正常收尾
if (goals.includes('a') && /* 全齐 */) {
  await ctx.waitForEvent('__interim-goal', { ... })  // 再在 step 之外泊
}
```

但这样**就没法写成一个包住 `ctx.step` 的通用 wrapper 了**——每个 step 调用点
后面都要就地加检查。引擎版之所以能做成「对用户透明」，正是因为它在
`engineStep` 的**成功返回之前、闭包之外**做判断（`run-workflow.ts:1135-1155`）。

所以省的不是「一行」，是**一次结构性改造 + 一个不容易自己发现的坑**。
这是「功能以自然形态落地」（通用约束 #4）的直接应用。

### 为什么 redo 的校验不能进 core

core 没有 step 清单（`WorkflowDefinition` 不含 `steps`），step 是 handler 运行时
通过 `ctx.step` 动态出现的。于是 core **判不了**：

- 目标/名单里的名字**是否存在**
- 名单**是否封闭**（被作废的 step 的下游有没有跟着作废）

它能做的只有「按名单过滤」——所以 `redoSet` 是 **advisory**：引擎既不校验也不假设。
图校验必须在上层做（`rerunDag` 的 `descendants(from)` + 闭包 throw）。

> 引擎侧已把这个差异写进注释：`run-workflow.ts` 的 `interimGoals` 文档
> （目标可能永远达不到、与 `reseedRun` 的交互）与 `reseedRun` 文档（封闭性不校验）。

### 为什么不「只在 dag 里做」

- **临时目标**：dag 层只能「不调度」（run 正常 finished），那样就没有「停住等宿主
  决定」的能力，也没有同 run 续驱。要保留泊车就得在 handler 里手写上面那 10 行。
- **不用 DAG 的用户**：手写 workflow 同样需要「跑到某步停下」。这不是图能力，
  是 workflow 的控制能力。

### 被否决的选项

| 选项 | 否决理由 |
| --- | --- |
| 都只在 dag | 临时目标退化成「不调度」，丢失泊车/续驱；非 DAG 用户没有 |
| 都不做，教用户手写 | 手写代价是 10 行 + 改写所有 step 调用点（见上），不是 1 行 |
| 把闭包校验也塞进 core | core 无图，只能做名字存在性检查，做不到封闭性检查；做了会给人「已校验」的错觉 |

### 已知的 core 局限（机制事实，不是缺陷）

三条，细节见 `docs/feature/_04_interim-goals-without-engine.md` §4：

1. 目标拼写错 → 静默跑完（引擎不知道有哪些 step）
2. 目标被 `reseedRun` 导入 → 算已达成、不泊车（**符合 reseed「部分重跑」的意图**；
   `redoSet` 才是「哪些目标该真跑」的开关）
3. 目标运行时不可达 → 静默跑完（静态可达 ≠ 运行时可达）

---

## 机制事实：中间件改不了 `inputSchema`（2026-11-02 实测）

问：「中间件能不能改 inputSchema？」**不能**，三处证据：

| 实测 | 结果 |
| --- | --- |
| 中间件能否拿到 `workflow` | ❌ `ctx` 的 14 个键里没有（`runId`,`input`,`state`,`signal`,`runtime` + 9 原语），`'workflow' in ctx === false` |
| 校验失败时中间件是否运行 | ❌ **不运行**——`startRun` 在 `run-workflow.ts:252` 校验后直接 `RUN_ERRORED(validation_error)` 返回 |
| 运行期改 `workflow.inputSchema` | 字段可写，但校验已发生，只影响下一次 drive（且跨 run 污染） |

时序（`run-workflow.ts`）：

```text
drive → startRun
  :252  validateWorkflowInput(workflow, options.input)   ← 校验
  :253  buildInitialState
  :301  driveHandler
          :610  composeMiddlewares                        ← 中间件在这里
                  → handler
```

**中间件在校验的下游**，结构上不可能影响它。唯一能定义 `inputSchema` 的时机是
`createWorkflow({ input })` 构建期。想在运行时换 schema 只能换一个 workflow 对象，
但同 `id` 的两个对象在 runtime 注册表里会互相覆盖——**不是「按 run 换 schema」**。

---

## 方案：`input.__interimGoals`（中间件读 input，用户自己传）

既然中间件改不了 schema，那就**在定义期就把它写进 schema**，由用户自己带值。
实测可行：

```ts
const schema = z.object({
  doc: z.string(),
  __interimGoals: z.array(z.string()).optional(),   // ← 定义时声明
})

const mw = createMiddleware().server(async ({ ctx, next }) => {
  const goals = (ctx.input as any).__interimGoals ?? []
  if (goals.length === 0) return next({ context: {} })
  // …包 ctx.step，达标后 waitForEvent('__interim-goal', { id: '__interim-goal-0' })
})

const wf = createWorkflow({ id: 'x', input: schema })
  .middleware([mw])
  .handler(...)

// 首次 drive 带目标
runWorkflow({ workflow: wf, runStore, runId, input: { doc, __interimGoals: ['b'] } })
// 续驱：投 __interim-goal（不是 recover）
runWorkflow({ workflow: wf, runStore, runId, signalDelivery: { name: '__interim-goal', signalId } })
```

实测结果：`D1 {a:1,b:1} paused` → `D2 {a:1,b:1,c:1} finished`，泊车恰好一次。

### 对用户的四条约定（使用这个中间件就要遵守）

| # | 约定 | 违反的后果 |
| --- | --- | --- |
| ① | 在 `input` schema 里**声明** `__interimGoals?: string[]` | 被 schema 校验拒绝或剥离，中间件读不到 |
| ② | 目标**只能在 run 创建时**通过 input 给 | 续驱时传新 input 不生效（`resumeRun` 用 `persistedState.input`，`run-workflow.ts:432`） |
| ③ | 续驱投 `__interim-goal` 信号，**不用** `recover: true` | 没有 `SIGNAL_RESOLVED`，重放时长匹配不到，会**重复泊车** |
| ④ | `__` 前缀避开业务字段 | 与用户自己的字段重名 |

### ⚠️ 悬而未决：「续驱传新 input 不生效」（TODO，待讨论）

实测：run 建好后 `persistedState.input` 是 `{doc, __interimGoals:['b']}`；续驱时传
`input: { __interimGoals: ['c'] }` **被忽略**，仍按 `['b']` 执行（`resumeRun` 用
`persistedState.input`，`run-workflow.ts:432`）。

**待讨论**（2026-11-02 用户标记）：

- 这是设计（input 是 run 身份的一部分，换了就是另一个 run）还是要松动的约束？
- 若松动，影响面：`buildInitialState` 每次 resume 重新执行（`state` 从
  `initialize(input)` 重建）——换 input 会让**已重放的 state 与历史不一致**。
  这条是核心难点，不是简单放开参数就行。
- 绕开方式现状：换目标走 `RunWorkflowOptions.interimGoals`（per-drive 不落盘），
  但那是引擎参数，中间件层拿不到。

**在此之前不依赖「续驱换 input」**。

| 载体 | 每次 resume 能换目标 |
| --- | --- |
| `input.__interimGoals` | ❌ 固定在 run 的 input 里 |
| 中间件闭包传 goals | ❌ 构建期固定 |
| 外部可变对象（`let goals`） | ⚠️ 能读到新值，但**不可重放**（重放与日志不一致） |
| **`RunWorkflowOptions.interimGoals`**（引擎参数） | ✅ per-drive，不落盘 |

最后一行是引擎参数方案的**独有价值**：目标是「本次 drive 的参数」，不是「run 的属性」。
`_02` §1 记的正是这一点。所以两者不是替代关系——`input.__interimGoals` 适合
「目标在 run 生命周期内固定」，引擎参数适合「每次续驱可能换目标」。
