# 0004 — 信号收件箱：可选能力方法（A'），core 投递过闸

**状态**：已接受（2026-11-02）
**关联**：上游 PR #19（`taren/durable-recovery` = `0971b4c`）的 `bufferSignals`、
`docs/durable-recovery-alignment.md` §3

## 背景

上游 PR #19 为信号收件箱加了两个**可选** store 方法（`bufferSignal` /
`consumeBufferedSignal`）+ runtime 配置开关。本仓原状是 core 层「无条件收件箱」
（`signal_run` 不检查是否在等，早到信号直接落日志）。

## 决策

走 **A'（机制 + 语义都对齐）**：

1. store 契约加可选的 `buffer_signal` / `consume_buffered_signal`
   （对齐上游 `bufferSignal?` / `consumeBufferedSignal?` 的可选能力形态）；
2. runtime 加 `buffer_signals` 配置开关（默认关，对齐上游；开了但 store 不支持
   就构造期报错）；
3. core 的投递原语**过闸**：`signal_run` / `signal_approval` / `signal_event`
   只在目标确实泊着时落盘，否则报 `SignalLost` 且不留日志——core 层不再有
   「无条件收件箱」，早到信号由 runtime 的可选收件箱承接。

## 被否决的选项

| 选项 | 否决理由 |
| --- | --- |
| A（真导入上游机制：store 双方法 + 下标定位消费） | 要把 `find_resume` 的全日志扫描改成下标定位，动 replay 地基 |
| B（只做语义对齐、零机制） | 达不到「命名/行为一致以降低维护成本」的目标——用户要的是机制也对齐 |

## 后果

- store 契约 28 = 28（与基线一致）；
- 收件箱行为三处差异均为有意设计：恒开 vs opt-in、滞留 vs 一次性、无 `__timer`
  豁免 vs 双闸豁免（记在 `durable-recovery-alignment.md` §3）；
- 三个 adapter（in_memory / file / postgres）全部实现，共享契约测试 4 条 +
  postgres 真库验证。
