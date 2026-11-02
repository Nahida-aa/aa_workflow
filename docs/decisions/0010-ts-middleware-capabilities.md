# 0010 — TS 中间件的能力事实与参数载体（`input.__interimGoals`）

**状态**：已接受（2026-11-02，全部实测）
**关联**：[0001](0001-strategy-vs-mechanism.md)、`feature/_04_interim-goals-without-engine.md` §4

## 能力事实（三条，全部实测）

1. **中间件改不了 `inputSchema`**：校验在 `startRun`（`run-workflow.ts:252`），
   早于 `composeMiddlewares`；校验失败时中间件**根本不运行**；且 `ctx` 上没有
   `workflow`（14 个键里没有）。
2. **`input` 对 run 是一对一不可变**：`resumeRun` 用 `persistedState.input`
   （`run-workflow.ts:432`），续驱传新 input **被忽略**——「每次 resume 换目标」
   在 input 载体下做不到。
3. **中间件能读 `input`**：目标藏在 input 信封里（`__dagPlan.to`）时用
   `goalsFromInput` 直读。

## 决策：`input.__interimGoals` 作为 TS 中间件的参数载体

- 定义期就把 `__interimGoals?: string[]` 写进 input schema（用户自己声明）；
- 首驱目标来自 input；续驱换目标走 `signalDelivery.payload.__interimGoals`
  （唤醒时更新，作用于下一段）；
- 泊完 `done.clear()`（不清则换目标无效）；泊位 id 带区分。

## 对用户的四条约定（违反即静默失效）

| # | 约定 | 违反后果 |
| --- | --- | --- |
| ① | schema 里声明 `__interimGoals?` | 被剥离，中间件读不到 |
| ② | 目标只能在 run 创建时给 | 续驱传新 input 被忽略 |
| ③ | 续驱投 `__interim-goal` 信号，不用 `recover` | 无 `SIGNAL_RESOLVED` → 重复泊车 |
| ④ | `__` 前缀避开业务字段 | 重名冲突 |

## 悬而未决（TODO）：「续驱传新 input 不生效」

- 是设计（input 是 run 身份的一部分）还是要松动？
- 松动的核心难点：`buildInitialState` 每次 resume 从 `initialize(input)` 重建，
  换 input 会让已重放的 state 与历史不一致——不是放开参数就行。
- 松动前不依赖该行为。

## 与引擎参数的关系

`RunWorkflowOptions.interim_goals` 是 **per-drive 参数**（每次 resume 可换目标、
不落盘）——这是引擎参数相对 input 载体的**独有价值**。两者不是替代关系：
input 载体适合「目标在 run 生命周期内固定」，引擎参数适合「每次续驱可能换目标」。
