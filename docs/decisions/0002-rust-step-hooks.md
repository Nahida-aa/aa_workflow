# 0002 — Rust 中间件能力钩子（on_step_finished / on_before_step）——有意扩展

**状态**：已接受（2026-11-02）
**关联**：[0001](0001-strategy-vs-mechanism.md)、`docs/tanstack/middleware.md`、
`feature/_09_plugin.md`

## 背景

TS 的中间件能把 `ctx.step` 当**函数属性**覆盖（`ctx.step = wrapped`），因此能在
「每个 step 完成后」插入策略逻辑。Rust 的 `ctx.step` 是 `&self` 方法、不可替换，
`Middleware::wrap` 只能包裹整个 handler future，且 `ctx` 上没有 store/history，
`engine` 是 `pub(crate)`——外部 crate 够不着。

不做钩子的后果：Rust 中间件只能「跑完再停」（实测），与 TS 的「达成即停」不等价。

## 决策

给 `Middleware` 增加**两个 step 边界钩子**（机制，不绑定具体策略）：

| 钩子 | 时机 | 能力 |
| --- | --- | --- |
| `on_before_step` | step 闭包执行**前**（取消检查后、重试泊车前） | `Skip { result }`——不跑闭包、直接提交缓存结果（redo 需要）；`Park`——边界泊车 |
| `on_step_finished` | `STEP_FINISHED` 已 append、live 状态已更新后 | `Park`——在目标 step 边界停下（临时目标需要） |

EngineRuntime 构造时从 `definition.middlewares` 收集，**收集点唯一**
（`run_workflow.rs`）——上游将来若出 plugins（定义期/运行期/两者），只改收集来源。

## 有意的越界（必须承认）

上游现有的唯一用户回调是 `telemetry.mapStepMeta`——**纯观察**（只返回 attributes）。
我们的钩子返回 `Park`/`Skip`，**能改变执行**。这是功能需要（没有 `Park` 做不出
临时目标；没有 `Skip` 做不出 redo），且 TS 侧通过覆盖 `ctx.step` 事实上也能改变
执行。若上游将来把插件定为只读，本仓**保留更宽的版本**，在
`docs/tanstack-alignment.md` 记档为有意差异。

## 归属是暂时的

钩子挂在 `Middleware` 上，但上游可能把「插件」与「中间件」分成两个概念
（telemetry 写死 → 迟早抽象出 `plugins` 列表，见 `feature/_09_plugin.md` §3）。
类型签名（`StepFinishedEvent` / `StepHookOutcome`）**独立于挂载点**，搬家成本低。

## 后果

- Rust 中间件与 TS 中间件语义等价（`interim_goals` 达成即停、`reseed` 复用，
  均有测试）；
- 重放短路的 step **不触发**钩子（它们不产生新 checkpoint）——策略需要
  drive 期状态时从日志推导（与引擎 `goals_done_at_drive_start` 同法）；
- `reseed` 的 `Skip` 需要在**执行前**介入，故落在 `on_before_step` 而非
  给 `on_step_finished` 加变体。
