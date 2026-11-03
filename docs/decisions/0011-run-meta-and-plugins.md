# 0011 — Run 级参数通道：`RunWorkflowOptions.meta` + 插件在构建期挂载

**状态**：已接受（2026-11-02）
**关联**：[0001](0001-strategy-vs-mechanism.md)、[0002](0002-rust-step-hooks.md)、
[0006](0006-dag-dual-drive-and-middleware.md)、`feature/_09_plugin.md` §3.4

## 背景

中间件承载策略需要**参数通道**。此前 TS 侧用 `input.__dagPlan` 信封——input 被迫
混入策略参数，而 input 本该是纯业务数据（handler 消费、resume 不可变）。

用户同时确定了两件事：
1. **插件在构建期挂载**（`.middleware()` 即现状）是合适的位置；
2. 策略参数需要一条**不占用 input** 的通道。

## 决策

### `RunWorkflowOptions.meta: Option<Value>`——run 级参数通道

透传链：`opts.meta` → `EngineRuntime.meta` → `BaseCtx.meta`（wrap 前置可读）→
`BeforeStepEvent.run_meta` / `StepFinishedEvent.run_meta`（钩子可读）。

与 `input` 的分工：

| | `input` | `meta` |
| --- | --- | --- |
| 语义 | 业务数据 | 策略/扩展参数 |
| 消费者 | handler | 中间件（server 前置、step 拦截器） |
| 落 `RunState` | 是 | 否 |
| resume 可变 | 否 | 是（每次 drive 可换） |

### 插件挂载：构建期

中间件（及其经 `set_step_interceptor` 装的拦截器）随 `Middleware` 在
`createWorkflow(...).middleware([...])` 时挂载。**不挂在 `RunWorkflowOptions`**——
保持 [0009](0009-no-implicit-new-run.md) 的分层纪律：引擎 options 不出现策略形态
的字段；插件列表若上游将来出（`feature/_09` §3.4 预测「定义期+运行期都支持」），
迁移只涉及收集点。

## 被否决的选项

| 选项 | 否决理由 |
| --- | --- |
| 继续用 `input.__dagPlan` 信封 | input 被迫混入策略参数；且 Rust 中间件读 input 要额外透传，通道绕 |
| meta 加进 `StepOptions`（per-step） | 策略是 run 级的，per-step 通道粒度错 |
| 环境变量 / 全局注册表 | 隐式依赖、测试不可隔离 |

## 后果

- `input` 回归纯业务数据；策略参数有了**每次 drive 可换、钩子可读**的正式通道；
- Rust 的 `meta` 是**本地扩展**（上游无此字段）——记入
  `docs/tanstack-alignment.md`；若上游将来加 `meta`/`plugins`，对齐成本低
  （字段名/位置可能不同，语义一致）；
- `dag_policies` 已切到 `ctx.meta.__dagPlan`；`input.__dagPlan` 通道废弃。
