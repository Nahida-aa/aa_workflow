# 0008 — 删除 docs/reference 生成器与生成物

**状态**：已接受（2026-10-30，提交 `67034cc`）
**关联**：`scripts/generate-docs.py`（已删）、`docs/reference/`（已删，73 文件 −7192 行）

## 背景

仓库曾维护「rustdoc JSON → markdown」生成器（`scripts/generate-docs.py`），
把 core 公开面渲染成 `docs/reference/` 的 markdown 页入库，对齐上游 typedoc
的做法。实践中暴露的问题：

1. 每次**任何** core 公开面改动都要重跑（nightly 依赖），漏跑就会出现
   「文档落后于代码」的静默漂移；
2. 生成页是**机器翻译**的签名堆砌，信息量低于源码里的手写 rustdoc；
3. 与「文档必须与代码同步」的目标相比，入库产物增加了大量无意义 diff 噪音。

## 决策

- 删除 `scripts/generate-docs.py` 与 `docs/reference/` 全部生成物；
- **源码里的 rustdoc 注释全部保留**——`cargo doc` 照常用；
- justfile 里两个悬空目标（`generate-reference` / `open-reference`）一并删除。

## 被否决的选项

| 选项 | 否决理由 |
| --- | --- |
| 保留但加 CI 强制重跑 | 增加每次改动的摩擦，收益只有「开箱即读的 md 页」 |
| 改成 CI 产物（不入库） | 无上游 CI 消费方，成本无回报 |

## 后果

- 「文档落后」类问题从根上消失（不再有可漂移的生成物）；
- 查公开面 API 用 `cargo doc --workspace --open`；
- 注意：历史提交信息里出现的「docs/reference 已重生成」在当时是事实，
  不可据此在当前树里找这些文件。
