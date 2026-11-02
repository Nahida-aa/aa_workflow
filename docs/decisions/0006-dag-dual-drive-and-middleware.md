# 0006 — DAG 双驱动（core/runtime）+ DAG 作为中间件

**状态**：已接受（2026-11-02，实验分支 `f2-lite-middleware-dag`）
**关联**：`feature/_04_interim-goals-without-engine.md` §7、
`wf-demo/packages/workflow-dag/src/core-driver.ts`、
`wf-demo/packages/workflow-middlewares/src/dag.ts`

## 背景

DAG 封装原先只走 runtime 驱动（`createDagRuntime` + `rt.startRun`）。两个需求推动
拆分：

1. 宿主已有调度框架、或只想依赖 core（不要 lease/sweep/timer 自动化）；
2. 引擎参数（`interim_goals` / `reseed_run`）按 [0001](0001-strategy-vs-mechanism.md)
   要删除，DAG 的策略需要由中间件承载。

## 决策

**驱动 × 策略解耦为两个独立维度**：

### 驱动：两个同构入口

| | runtime 驱动 `createDagRuntime` | core 驱动 `createCoreDagRuntime` |
| --- | --- | --- |
| 启动 | `runtime.startRun` | `runWorkflow` + drain |
| 续驱 | `runtime.deliverSignal` | `runWorkflow({ signalDelivery })` |
| 读日志/状态 | `runtime.store.readEvents` / `loadRunState` | `runStore.getEvents` / `getRunState` |
| 自动 sweep | 有 | **无**（宿主自己记「该恢复谁」） |
| 依赖 | `workflow-runtime` | **只有 `workflow-core`** |

core 的 `RunStore` 已具备全部所需方法，core 驱动**没有缺口**。

### 策略：中间件承载

| 策略 | 中间件 | 对应引擎参数（计划删除） |
| --- | --- | --- |
| 跑到某节点停 | `interimGoals()` | `interim_goals` |
| 子图重跑（复用+重跑） | `reseed({ getEvents })` | `reseed_run` |
| **DAG 调度本身** | `dag(spec)`（中间件接管 handler，用户不写） | — |

三种用法用户自选：

```ts
// A. 工厂（最简）
dagWorkflowWithPolicies(spec, { getEvents })

// B. handler 由库给、策略中间件自己挂
createWorkflow({ ... }).middleware([...dagPolicies(spec, { getEvents }), dag(spec)])
  .handler((ctx) => ctx.dagResults)

// C. 只要 handler（自己控制 id/input/middleware 全部元数据）
createWorkflow({ ... }).handler(dagToHandler(spec))
```

### 组合顺序约束

**`dag(spec)` / `dagToHandler` 必须在策略中间件之后（最内层）**。洋葱模型下外层
先执行「前置」：若 `dag` 在外层，它的调度会调用**还没被 `reseed` 包装的
`ctx.step`**，复用静默失效（实测踩过）。详见 [0007](0007-middleware-compose-order.md)。

## 语言差异（实测）

Rust：中间件不调 `next` 即完全接管——handler 不执行、返回值就是 output。
TS：`composeMiddlewares` **丢弃** `m.server()` 的返回值，只有 `next()` 的返回值
成为 output——所以 TS 的 `dag(spec)` 必须调 `next`，结果经 `ctx.dagResults` 转手，
用户 handler 一定会执行（但只是转手）。

## 后果

- 「DAG 作为插件」不成立：插件只能观察/拦截，DAG 要**发起** step 调用——
  调度是 handler 的职责；策略才是中间件/插件的事；
- `meta.importedFrom` 溯源在中间件版缺失（引擎版在 `driveHandler` 之前真导入，
  中间件物理上晚一步），可从 `input.__dagPlan` + 源日志**推算**（实测完全一致）；
- 「新 run 跑过之后源被删」两种实现都不受影响（重放走自己的日志）——唯一的
  差异是「源在首驱前就没」这一种操作顺序（引擎已拷贝照常、中间件查不到会真跑）；
- DAG 中间件的引擎日志顺序为**执行顺序**（a,b,c），引擎参数版为「导入在前、
  重跑在后」（a,c,b）——审计视角不同，均为有意行为。
