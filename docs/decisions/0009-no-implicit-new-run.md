# 0009 — 隐式新开 run 默认禁止（四条放行标准）

**状态**：已接受（2026-11-02，用户明确）
**关联**：`feature-requirements.md` 通用约束 #1、
`packages/workflow_core/src/engine/run_workflow.rs` 的 `attach_decision` 文档、
`docs/feature/_04_interim-goals-without-engine.md` §7

## 背景

「让一个已成功的 step 重新执行」或「换目标继续」有两种实现路径：
**同 run 继续**（resume / drive 参数）与**开新 run**（reseedRun / 新 runId）。
前者是自然形态；后者作为显式手段已实现（F2）。风险在于第三种：**引擎在 resume
时内部创建新 run 而调用方不直接指定**——那会让 attach / sweep / lease 全部无法
回答「操作的是哪个 run」。

## 决策

**隐式新开 run 默认禁止。** 引擎永远不自动创建新 run；调用方要么复用 runId
（resume），要么显式给新 runId（reseedRun）。

引擎若要在未来支持隐式新开，必须**同时满足**四条放行标准：

1. 调用方可见的 run id 不变，或新旧 id 的映射关系明确落 store；
2. 新 run 的日志能引用/拼接旧 run 的日志（审计不断链）；
3. `attach` 旧 run 与驱动新 run 得到一致的事实；
4. 穷尽同 run 手段后仍无法表达该语义。

## 实证依据

- 上游 `startRun` 的**隐式幂等 redirect**（已存在 runId → 转 attach）在本仓被
  拒绝过一次：本仓没有 `signalDelivery` 标志位区分 start/resume，照抄会把每次
  resume 都变成 attach（见 `run_workflow.rs` 的 `attach` 参数文档）；
- 「resume 时换目标/换 input」的需求已由 per-drive 参数（`interim_goals`）或
  reseedRun（显式新 run）覆盖，无需隐式路径。

## 后果

- `reseedRun` 的所有代价（新 run id、外部引用、input 重喂、审计跨 run 拼）都是
  **显式**开销，由它的问题域（换世界重来 + 旧日志保真）承担；
- 需求文档（`feature-requirements.md` F1）明确「同 run 换目标继续」不走新 run，
  用 drive 参数 / 中间件实现。
