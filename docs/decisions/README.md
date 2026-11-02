# 决策记录（Decisions）

本目录收录**架构与 API 决策**的结论性记录（ADR 风格）。每条记录：状态、背景、
决策、理由、后果——**论证过程与实测细节留在链接的原文件里**，这里只放结论。

阅读顺序建议：先看 `feature-requirements.md`（需求与状态总表），需要知道
「为什么这样做」时再查对应 ADR。

| ADR | 标题 | 状态 |
| --- | --- | --- |
| [0001](0001-strategy-vs-mechanism.md) | 策略与机制分层：引擎不加业务参数，策略走中间件包 | 已接受 |
| [0002](0002-rust-step-hooks.md) | Rust 中间件能力钩子（on_step_finished / on_before_step）——有意扩展 | 已接受 |
| [0003](0003-event-taxonomy.md) | 事件分类按上游拆分：StepPaused/StepResume → 四类型 | 已接受 |
| [0004](0004-signal-inbox.md) | 信号收件箱：可选能力方法（A'），core 投递过闸 | 已接受 |
| [0005](0005-naming-reseedRun-redoSet.md) | 命名对齐：reseedRun / redoSet，不留 alias | 已接受 |
| [0006](0006-dag-dual-drive-and-middleware.md) | DAG 双驱动（core/runtime）+ DAG 作为中间件 | 已接受 |
| [0007](0007-middleware-compose-order.md) | 中间件组合顺序约束：调度器必须在最内层 | 已接受 |
| [0008](0008-drop-reference-generator.md) | 删除 docs/reference 生成器与生成物 | 已接受 |
| [0009](0009-no-implicit-new-run.md) | 隐式新开 run 默认禁止（四条放行标准） | 已接受 |
| [0010](0010-ts-middleware-capabilities.md) | TS 中间件的能力事实与参数载体（`input.__interimGoals`） | 已接受 |

## 什么时候写新 ADR

- 决定**采纳或拒绝**某个方案（含被否决的选项与理由）；
- 决定**有意偏离**上游（必须写偏离点与代价）；
- 决定**删除**某个已存在的东西。

不写：机制分析（→ `feature/`）、实测数据（→ 对应 ADR 的链接目标）、
需求状态（→ `feature-requirements.md`）。
