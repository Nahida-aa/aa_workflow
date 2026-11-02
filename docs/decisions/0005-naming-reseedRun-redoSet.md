# 0005 — 命名对齐：reseedRun / redoSet，不留 alias

**状态**：已接受（2026-10-28）
**关联**：`docs/concepts/redo-vs-invalidation.md` §6、`feature-requirements.md` F2

## 背景

redo 的原参数名 `reexecuteFrom`（`unsafeInvalidate`）有两个问题：
1. 没体现「落到一个**新** run 里」——`re-` 前缀本意是「第二次播种」（增量），
   不是「重新执行」；
2. `unsafe` 前缀把一个**正当行为**（开集失效——下游保留旧值有时正是想要的，
   demo 场景 3 就故意这么用并断言它）标成了 footgun。

## 决策

- `reexecuteFrom` → **`reseedRun`**（`re-` 前缀承重：第二次播种、增量、加入
  redo 词族）；
- `unsafeInvalidate` → **`redoSet`**：注释改为「先理论、后注意事项」的 advisory
  措辞 + A→B→C→D 列 `[B,D]` 的具体反例，替代 `unsafe` 硬不变量措辞；
- 0.x 阶段两侧（f2-lite TS / aa-workflow Rust）同步改名、**不留 alias**。

## 后果

- 词族统一：`reseedRun { sourceRunId, redoSet }` / `reseed_run` + `with_redo_set`；
- 与 INVALIDATED（另一种失效语义，见 F6）的区分不再被命名模糊化；
- 旧名只存在于历史提交与历史叙事文档（如 `dag-strategies.md` §2 标注
  「提案形状（非现状 API）」的段落）——**历史不改写**。

## 注意

`docs/durable-recovery-alignment.md` 引用的 `run-workflow.ts:71-84` 等行号是改名
**前**的快照；f2-lite HEAD 已到 `b711b51`+，现行号在 `:72-104` 一带。
