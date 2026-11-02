# 0001 — 策略与机制分层：引擎不加业务参数，策略走中间件包

**状态**：已接受（2026-11-02）
**关联**：`feature-requirements.md` F1/F2/F6、`feature/_02_interim-goals.md`、
`feature/_03_redo.md`、`feature/_04_interim-goals-without-engine.md`、
`feature/_09_plugin.md`

## 背景

「跑到某几个 step 就停」（临时目标）与「作废某些 step 重跑」（redo）是我们需要的
能力，但上游（TanStack workflow，基线 `0971b4c`）三个 ref 对相关概念**零命中**
——`research/`、`docs/` 里连近义概念（partial rerun / stop at / checkpoint reuse）
都没有。把它们做成 `RunWorkflowOptions` 参数（`interim_goals` / `reseed_run`）
等于维护一份永不合并的 fork。

## 决策

1. **引擎只提供机制**：step 边界钩子（见 [0002](0002-rust-step-hooks.md)）、
   可覆盖的中间件原语——不出现任何具体策略名。
2. **策略放中间件包**：TS `@wf-demo/workflow-middlewares` 的 `interimGoals` /
   `reseed` / `dagPolicies`；Rust `aa_workflow_middlewares` 的 `interim_goals` /
   `reseed`。
3. **引擎参数 `interim_goals` / `reseed_run` 计划删除**，前提是中间件实现达到
   语义等价（TS 已等价；Rust `interim_goals` 已等价、`reseed` 已等价）。

## 被否决的选项

| 选项 | 否决理由 |
| --- | --- |
| 都只在 DAG 库 | 临时目标退化成「不调度」（run 正常 finished），丢失泊车/续驱；非 DAG 用户没有 |
| 都不做，教用户手写 | 手写等价物需要包 `ctx.step` + 改写所有调用点 + 自己处理泊位 id 唯一性（TS 实测约 10 行且有一个不易发现的坑：闭包内泊车会留假 `STEP_FAILED`） |
| 闭包校验也塞进 core | core 无图，只能查名字存在性，做不到封闭性检查；做了会给人「已校验」的错觉 |

## 后果

- 引擎公开面与上游保持一致，升级无冲突；
- 中间件包独立演进，基线（`0971b4c`）也能用；
- TS 与 Rust 的中间件**能力不对称**（TS 可覆盖 `ctx.step`，Rust 需要钩子）——
  见 [0002](0002-rust-step-hooks.md)；
- 已知的软围栏局限（拼写错/被导入/运行时不可达 → 静默跑完）在两种实现里一致，
  见 `feature/_04` §4。
