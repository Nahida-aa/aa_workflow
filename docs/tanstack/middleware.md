# 中间件能力边界探索

> 基线：`learn_ls/workflow` f2-lite 分支 HEAD `b711b51`（行号对 `run-workflow.ts`）。
> 结论全部来自实测探针，跑法见 `workflow` 仓 `docs/internal/test-runner.md`。

## 0. 问题

上游 `@tanstack/ai-orchestration` 用中间件扩展 `ctx`（`.middleware([agentMiddleware()])`
加 `ctx.ai`），而不是改 core。本篇回答：**中间件能承载到什么程度**——临时目标、
redo 这类「策略」能否完全放在中间件里，core 保持不动。

## 1. 中间件能拿到什么、在什么位置跑

```text
runWorkflow                          :123
└ drive                              :200   四模式分流
   ├ resumeRun / startRun            :230 / :332
   │  └ driveHandler                 :517
   │     ├ engine.history = [...history]      :529   ← 快照（拷贝）
   │     ├ engine.nextLogIndex = history.length :530   ← 固定
   │     ├ const ctx = baseCtx               :606
   │     └ composeMiddlewares(ctx, handler)   :610   ← 中间件在这里
   │        └ m.server({ ctx, next })         :1500
   │           └ next({ context }) 合并进同一个 ctx 引用  :1509-1519
```

`ctx`（`BaseCtx`，`types.ts:347`）实际字段：

```
runId, input, state, signal, runtime,
compensate, step, sleep, sleepUntil, waitForEvent, approve, now, uuid, emit
```

**没有 `workflow`、没有 `runStore`、没有 `history`、没有 `engine`。**
`ctx.runtime` 也只有 `deadline` / `timeRemaining` / `shouldYield` / `yield`。

### 保留字段

`reservedCtxFields`（`:1451`）禁止 `next({ context: { step } })` 覆盖：
`runId`/`input`/`state`/`signal`/`runtime`/`step`/`compensate`/`sleep`/
`sleepUntil`/`waitForEvent`/`approve`/`now`/`uuid`/`emit`。

**但可以直接改写 `ctx.step`**——中间件拿的是同一个 `ctx` 对象引用
（`composeMiddlewares` 传 `ctx` 原对象，`:1468`）。这是所有「包装 step」
方案的基础。

## 2. 能做的

| 能力 | 做法 | 实测 |
| --- | --- | --- |
| 扩展 ctx 命名空间 | `next({ context: { ai } })` | 上游 ai-orchestration 的做法 |
| **包装 `ctx.step`** | `ctx.step = (id, fn, o) => orig(id, fn, o)` | ✅ |
| 在 step 之间插入逻辑 | 包装层在 `orig()` 返回后执行 | ✅ |
| 调 `waitForEvent` / `sleep` 等原语 | 直接用 `ctx.*` | ✅ |
| 读 run 的 input | `ctx.input` | ✅ |
| 读 run 的 state | `ctx.state` | ✅ |

### 已验证的完整用例：临时目标

```ts
function interimGoalsMiddleware(goals: string[]) {
  return createMiddleware().server(async ({ ctx, next }) => {
    const orig = ctx.step
    const done = new Set<string>()
    let parked = false                       // 本次 drive 只泊一次
    ctx.step = (id, fn, opts) => (async () => {
      const r = await orig(id, fn, opts)     // ① step 先收尾
      if (parked) return r
      if (goals.includes(id)) done.add(id)
      if (goals.every(g => done.has(g))) {
        parked = true
        await ctx.waitForEvent('__interim-goal', {
          id: '__interim-goal-0',            // 稳定 id
          meta: { goals },
        })
      }
      return r
    })()
    return next({ context: {} })
  })
}
```

续驱必须投 `__interim-goal` 信号（不是 `recover: true`）——见 §4。

## 3. 不能做的

| 能力 | 为什么 | 实测 |
| --- | --- | --- |
| **改 `inputSchema`** | 校验在 `startRun:252`，早于中间件；`ctx` 上也没有 `workflow` | 校验失败时中间件**根本不运行** |
| **改 `engine.history`** | 它是 `[...history]` 快照（`:529`），且不在 `ctx` 上 | — |
| **往 store 写事件让引擎短路** | 快照已完成、`nextLogIndex` 已固定 | **CAS 冲突 → `RUN_ERRORED`** |
| 拿 `runStore` | `ctx` 上没有；`engine` 被闭包锁在 `ctx.step` 等函数里 | — |
| 替换 handler | 中间件在 handler **外层包裹**，不能替换它 | — |

### 「先写日志再让引擎短路」为什么不可行（实测）

中间件在 `ctx.step` 里先 append 一条 `STEP_FINISHED(a)`，再调 `orig()`：

```text
EXEC: {"a": 1}                          ← 闭包仍然执行了（没短路）
LOG:  STEP_FINISHED(a), RUN_ERRORED     ← 而且 run 报错
STATUS: errored
```

两个独立原因：

1. **`findCheckpoint` 扫 `engine.history`**（`:879`），那是 `[...history]` 副本，
   外部追加的内容不在里面 → 短路不生效。
2. **`engine.nextLogIndex` 在快照时固定**（`:530`），store 长度变了之后引擎的
   下一条 `appendEvent(index)` CAS 失败 → `RUN_ERRORED`。

而引擎版 `reseedRun` 之所以可行，是因为它在 **`driveHandler` 之前**写日志并把
`history` 作为入参传进去（`startRun:292-315` → `driveHandler:317`）：

```text
startRun
  ├ 导入循环 emitAndAppend → store + history = imported   :292-315
  └ driveHandler({ history })                              :317
      └ engine.history = [...history]   ← 快照时导入的已在里面   :529
```

中间件在 `driveHandler` **内部**，物理上晚了一步。

## 4. 关键：replay 让「包装层」需要显式状态

「恢复运行」= **重跑 handler** + 已完成的 step 短路返回（栈不能持久化）。
短路发生在 `engineStep` 内部（`:879-902`，定义见 `findCheckpoint:1601`），**短路后的控制权回到包装层**，
包装层无从得知「这次是短路返回的」。

### 临时目标：`parked` 布尔解决

同一 drive 内，目标达成后不再检查。**不加这个标记的后果**（实测）：

```text
LOG: STEP_FINISHED(a), STEP_FINISHED(b), SIGNAL_AWAITED,
     SIGNAL_RESOLVED, STEP_FINISHED(c), SIGNAL_AWAITED   ← 多泊一次
STATUS: paused（应 finished）
```

### 续驱通道影响结果

| 续驱方式 | 日志里有 `SIGNAL_RESOLVED` | 结果 |
| --- | --- | --- |
| `signalDelivery: { name: '__interim-goal' }` | ✅ | 重放到 `waitForEvent` 时短路命中 |
| `recover: true` | ❌ | 匹配不到任何 `SIGNAL_RESOLVED`，再泊一次 |

`recover` 与 `signalDelivery` 进**同一个 `resumeRun`**（`:210` / `:214`），区别只是
有没有 seed delivery。**泊车是等待原语，续驱就该投对应信号**——`recover` 是给
「进程崩了、栈没了、需要重放」用的。

### redo：`meta.importedFrom` 溯源丢失

中间件只能做 lookup 短路（`ctx.step` 里查源 run 的结果直接返回），**不能写日志**：

| | 引擎 `reseedRun` | 中间件 lookup |
| --- | --- | --- |
| 结果复用 | ✅ | ✅ |
| 重放能短路 | ✅ | ✅（引擎自己写了 `STEP_FINISHED`） |
| **`meta.importedFrom` 溯源** | ✅ | ❌ |
| 源 run 删除后 | ✅ 不受影响（已拷贝） | ❌ 重跑 |
| 改 `redoSet` 时机 | run 创建时 | 同（中间件参数构建期固定） |

## 5. 结论

| 策略 | 能否放中间件 |
| --- | --- |
| 扩展 ctx（`ctx.ai` 这类） | ✅ 上游已这么做 |
| **临时目标（泊车）** | ✅ **行为完全等价**，需 ②`parked` 布尔 + 稳定 id + ③续驱投信号 |
| **redo（结果复用）** | ⚠️ 行为等价，**日志不等价**（无 `importedFrom`、依赖源 run 存活） |
| redo 的**日志等价**（真导入） | ❌ 必须发生在 `driveHandler` 之前 |
| 改 `inputSchema` | ❌ 结构上不可能 |

**中间件的价值**：core 不动、可独立发包、可组合（与 `agentMiddleware()` 叠加）。

**中间件的代价**（用户需知道的约定）：

1. 目标/源 run 等参数走 `input.__xxx`，需在 schema 里声明，且 **run 创建后不可改**
2. 续驱必须投信号，不能用 `recover`
3. 包装层需要自己的 replay 感知状态（`parked`），不能依赖引擎的快照
4. redo 拿不到事件级溯源
