# 0007 — 中间件组合顺序约束：调度器必须在最内层

**状态**：已接受（2026-11-02，实测踩过）
**关联**：[0006](0006-dag-dual-drive-and-middleware.md)、
`wf-demo/packages/workflow-middlewares`（`dag` / `reseed` / `interimGoals`）

## 背景

`dag(spec)`（调度）与 `reseed`（复用）、`interimGoals`（泊车）都通过中间件承载。
`composeMiddlewares` 是洋葱模型：**数组靠前 = 外层 = 前置代码先执行**。

## 决策

**`dag(spec)` 必须放在策略中间件之后（数组最后、链最内层）**：

```ts
.middleware([...dagPolicies(spec, { getEvents }), dag(spec)])
//           ↑ 策略在外层                ↑ 调度最内层
```

即：**reseed 先包 `ctx.step` → interimGoals 先包 `ctx.waitForEvent` → dag 才开始
调度**。策略中间件的前置（包装）必须先于调度执行，调度调到的才是包装后的原语。

## 为什么是约束而不是「库自动处理」

中间件组合出问题本就是使用者的责任（与插件模型同）；库的职责是**把规则写清楚**
（本文 + `dagPolicies` 文档注释），而不是替用户重排数组——重排会破坏用户对顺序
的预期（例如故意把某策略放外层）。

## 实测的后果（顺序写错时）

`[dag, ...policies]`（dag 在外层）的后果链：

1. dag.server 的**前置**（调度）先执行——此刻 `reseed` 还没包装 `ctx.step`；
2. 调度调用的是**原始** `ctx.step` → `reseed` 的复用**静默失效**（全部真跑）；
3. `interimGoals` 同理——`ctx.waitForEvent` 未包装，泊车事件收不到、不泊；
4. 日志表现为：所有节点都重跑、`to` 不生效——**静默**，无任何报错。

这个坑在 `dag-policies.test.ts` 第一版实测踩到（`lookup keys` 为空 + 全部真跑），
靠逐步加日志定位。

## 附：TS 与 Rust 的返回值差异（同属组合细节）

- **Rust**：中间件不调 `next` 即完全接管——handler 不执行、中间件返回值就是
  run 的 output（实测 `handlerRan: 0`、`output` 为中间件的返回值）。
- **TS**：`composeMiddlewares` 丢弃 `m.server()` 的返回值（`returned` 只在
  `next()` 内赋值），只有 `next()` 的返回值成为 output——所以 `dag(spec)` 必须
  调 `next`，结果经 `ctx.dagResults` 由 handler 转手，用户 handler **一定执行**
  （但只负责转手）。

这也意味着「中间件完全接管」在 Rust 是一等能力，在 TS 是**借道 `ctx` 传值**的
受限能力。
