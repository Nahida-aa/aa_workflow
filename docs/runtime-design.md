# workflow-runtime 设计意图

> **状态**：D5 已调查完，结论**否定了原定的方向**——LocalDub 已自有 queue
> （等价于上游 runtime），真正缺的是**并行编排表达力**（即 core 的能力）。
> 详见 D5 附。**在拍板 D5 前不要动手写 runtime。**

> 本文只写**意图与取舍**，不写 API 签名——签名会随设计变化，写在意图之前
> 只会造成文档与实现不一致。

## 为什么要先写这份文档

上游 TanStack 在实现 runtime 之前，先写了一篇
`docs/research/SCHEDULING.md — cron landscape + future package shape`
（`30e46ba`，2026-05-21），**7 天后**才由 `5d05fa8` 实现 runtime。先写清
「要什么」再动手。

我们的处境比上游更需要在动手前定清楚，因为下面几个决策**会反向影响 core**
（core 刚理顺：59 个引擎级测试、契约清晰），不能边写边定。

## 起点：core 刻意不做的事

`packages/workflow-core` 现在的边界（与上游 core 一致）：

- replay 引擎 + 耐久原语 + `RunStore` 契约
- **不做**：调度、队列、worker 协调、timer 索引、schedule

上游 core 的原话（`docs/guide/runtime-model.md:33`）：

> The core engine is intentionally not a scheduler, queue, database adapter, or
> deployment adapter.

runtime 就是来补这块执行所有权层。上游 runtime 的职责（`runtime-model.md:60`）：

- `startRun` / `deliverSignal` / `deliverApproval` / `sweep`
- **不要求某个进程永久拥有一个 run**：每次调用 claim 一个 run、驱到下一个
  pause 或终态、释放 lease、返回
- 依赖 `WorkflowExecutionStore`——它比 core 的 `RunStore` **更宽**，含
  timers / schedules / 原子 claim / lease / 陈旧 run 恢复 / list 与 timeline

---

## 决策点

### D1. `WorkflowExecutionStore` 是新 trait，还是扩 `RunStore`？

上游选择了**新 trait**，理由是 serverless / 多 worker 需要的「比 replay 更多」。

| 选项 | 代价 | 倾向 |
| --- | --- | --- |
| **A. 新 trait `ExecutionStore`，`RunStore` 不动** | 需要两层抽象，runtime 依赖 `ExecutionStore`，`FileRunStore` 之类要同时实现（或加桥接） | ✅ |
| B. 把 timers / leases 塞进 `RunStore` | **破坏性**：每个 store 实现都要改；且 core 的契约面被污染——core 只用得上 CAS + 元数据 | ❌ |

**倾向 A**。理由：核心是「core 不该知道自己不需要的东西」。core 只需要
「append + 读 + 元数据」；timer 索引和 lease 是 runtime 的关注点。上游也是
这个切分。

**待定**：`ExecutionStore` 是继承 `RunStore`（supertrait）还是组合？
- supertrait：`trait ExecutionStore: RunStore`，实现者只需一个类型
- 组合：`ExecutionStore` 内含 `RunStore` 的值

倾向前者（Rust 里 `dyn` 组合会多一层间接），但要看 lease 的 API 形状再定。

### D2. lease 放 store 还是放 runtime？

上游把 lease 交给 store（`leaseOwner` / `leaseMs`，runtime 负责续租）。

| 选项 | 代价 | 倾向 |
| --- | --- | --- |
| **A. lease 在 store（原子 claim 由 store 保证）** | 每个 store 实现要正确处理并发 claim；`FileRunStore` 这种单机实现要伪造成「总能 claim 成功」 | ✅ |
| B. lease 在 runtime（进程内锁） | 多进程无效——runtime 的整个存在意义就是跨进程，进程内的锁解决不了 | ❌ |

**倾向 A**。**但注意**：这一条**不影响 core**——只要 lease 不塞进 `RunStore`
（见 D1），core 完全无感。

**关键待定**：我们**真的需要 lease 吗**？见 D5。

### D3. timer 投递：谁来唤醒 sleep？

现状：`exec_pause` 在挂起点自轮询（`engine/mod.rs` 的 `RESUME_POLL_MS = 25ms`），
到期后引擎自己 `signal_run` 投递。

| 选项 | 代价 | 倾向 |
| --- | --- | --- |
| **A. runtime 的 sweep 认领到期 timer 并投递** | core 要提供「查询哪些 run 在等、deadline 是什么」的入口；或 runtime 自己扫 `RunState.waiting_for` | ✅ 与上游一致 |
| B. 保持 core 自轮询 | 每个挂起的 run 占一个存活的任务 + 25ms 轮询；进程一退，timer 就没人投递（除非有人重放） | ⚠️ 见下 |
| C. core 提供 `subscribe` 等待 | 已有 `RunStore::subscribe`，但 `FileRunStore` 之外未必有 | — |

现在其实是 **B**。它能工作是因为「drive 期间进程活着」；一旦把 run 交给运行时
边界（进程退出），**没有东西会来投递 timer**——这正是 runtime 存在的理由。

**但这个切换有代价**（已实测，见下）：从 B 到 A 意味着 `exec_pause` 不再自己等，
而是「写 `STEP_PAUSED` → 返回」。这是**行为变更**。

### D3 附：影响面实测

依赖「同一次 drive 内自动 resume」的测试共 **7 个**：

| 位置 | 测试 |
| --- | --- |
| `engine/mod.rs:1555` | `sleep_pauses_then_auto_resumes` —— 断言 `task.await` 在整个 sleep 期间不返回，**且引擎自己 append 了 `StepResume`**（`:1612-1620`） |
| `engine/mod.rs:1808` | `sleep_until_past_resolves_immediately` |
| `engine/mod.rs:1841` | `sleep_until_schedules_timer` |
| `examples/.../workflows.rs:1068` | `invoice_double_sleep_auto_resumes` |
| `examples/.../workflows.rs:1184` | `refund_approved_disburses_after_timer` |
| `examples/.../workflows.rs:1614` | `event_gate_emit_wait_then_sleep_until` |
| `examples/.../runtime.rs:265` | `invoice_double_sleep_survives_restart`（跨进程重启，本身就不依赖单次 drive） |

**结论**：切换到 A 是**行为变更但非破坏性**——没有测试断言「引擎绝不会自己
投递 timer」，只是它们**顺带**依赖了这个行为。改写方式是把
`task.await` 换成「等 pause → 手动投递 timer → 再 drive」，与
`approval` 类测试的写法一致（那类测试已经是这个模式）。

**但这会损失一个能力**：现在「一次 `run_workflow` 调用就能跑完含 sleep 的
workflow」是成立的，切到 A 之后就需要外部驱动器。如果 LocalDub 的调用方
（`packages/cli/run-task.ts`）依赖这个便利，得先确认。

**可能的折中**：core 保留自投递（B），runtime 的 sweep 作为**可选的**
外部驱动器（A）——两者不冲突，`exec_pause` 的行为不变，runtime 只是多了一条
「不依赖进程存活」的路径。这样零测试改动。**这一条值得优先考虑。**

### D4. sweep 的边界怎么定？

上游：`sweep({ maxRecoveredRuns, maxScheduledRuns, maxTimers, maxDurationMs })`，
边界是为了「塞进一次 host 执行」（serverless 函数超时）。

我们本地没有 host 超时概念。

| 选项 | 代价 | 倾向 |
| --- | --- | --- |
| **A. 保留有界 sweep（`max_*` + `maxDurationMs`），只是默认值更大** | 多几个参数，但接口与上游同构；将来接 serverless 时不用改 | ✅ |
| B. 无界 sweep（跑到没有活干为止） | 简单，但将来接 host 时要重设计 | ❌ |

**倾向 A**。边界参数即使本地用不上，也是**接口形状**的一部分——现在省掉，
将来就要破坏性改。

### D5. 我们真的需要哪些能力？（最重要的一条）

上游 runtime 有 8 项能力（`runtime-model.md:75`）：幂等创建 / 事件日志 /
run state / timers / signal 与 approval 投递 / schedules / 原子 claim 与 lease /
陈旧 run 恢复 / list 与 timeline。

**结论：LocalDub 已经自己长出了一套等价物，不应重复建设。**

调查对象：`packages/server/src/feat/workflows/queue/`（642 行）。

| 上游 runtime 能力 | LocalDub 已有 | 实现 |
| --- | --- | --- |
| sweep（有界后台单元） | ✅ 等价 | `run_worker()` 常驻循环（`queue/mod.rs:328`），一次一个 |
| append-only event log | ✅ | `events.ndjson`，crash 兜底靠重放 `[consumed_offset, EOF)` |
| run state | ✅ | `checkpoint.json`，原子重写，正常路径零重放 |
| **lease** | ✅（**进程内**） | `INFLIGHT: Mutex<HashSet<String>>`（`workflows/mod.rs:29`）防同一任务并发续跑 |
| execution store | ✅ | `data/queue/` |
| run status | ✅ | Queued / Running / Done / Failed |
| `startRun` | ✅ | `enqueue` + `wait_next()` 唤醒 worker |
| schedules / cron | ❌ 不需要 | 任务由 CLI/桌面显式 `enqueue`，非自定时 |
| timers 投递 | ❌ 不需要 | 见下 |

**timer 那条尤其关键**：`execute_entry(&input)` 在 `spawn_blocking` 里跑**同步**
pipeline，整条一次跑完。所以「挂起 → 返回 → 以后再来」的 `ctx.sleep` 模式对它
不适用——它根本不需要跨进程投递 timer。

**对 D2（lease）的直接影响**：LocalDub **明确选择了不做跨进程 lease**（用进程内
`HashSet`），因为它假设 worker 是单进程常驻的。我们若做 lease，是在解决一个它
没打算解决的问题。

**对 D4（sweep 边界）的直接影响**：LocalDub 的 `run_worker` 是**常驻循环**而非
有界单元，`maxDurationMs` 对它没有意义。

### D5 附：LocalDub 真正缺的是什么

不是 runtime，是**同一进程内的并行编排**。

`get_steps`（`steps/utils/steps.rs:134`）返回一个**扁平串行列表**，
`DUB_SF_OCR_STEPS`：

```
separate → separate_after → sf_ocr_pre → sf_ocr → sf_ocr_fix
  → translate → split_audio → tts → mix_audio → mix_video
```

其中 `separate` / `separate_after`（语音分离）与 `sf_ocr_pre` / `sf_ocr`
（关键帧 OCR）**没有数据依赖**，可以并行——这正是用户指出的点。而
`asr_ocr` 序列里 `separate*` 与 `asr` / `asr_ocr*` 同理。

**这说明我们真正需要的可能是 workflow-core 而非 workflow-runtime**：

- core 的 `tokio::try_join!` 正好表达这种并行（`fulfillment-saga` 示例就是）
- core 的 `continue_from` 对应 LocalDub 已有的 `continue_workflow`
- core 的 append-only 日志 + CAS 对应 LocalDub 的 `events.ndjson` + checkpoint

**而 runtime 那层（lease / sweep / schedules）LocalDub 已经有了自己的实现**，
且是针对「单机串行常驻 worker」这个形态优化过的——换成我们的通用实现未必更好。

**待你拍板**：

- **选项 A**：只把 core 接进 LocalDub，用 `try_join!` 解决并行；runtime 暂不做。
  代价：核心能力（重放短路、continue_from）与 LocalDub 现有队列有重叠，要决定
  谁让位。
- **选项 B**：做 runtime，但**只做 core 缺的那部分**——即不碰 LocalDub 已有的
  队列/持久化，只补「并行编排」的表达能力（其实就是 core）。
- **选项 C**：照上游做完整 runtime，然后**替换** LocalDub 的队列实现。
  代价最大，且要用通用实现换掉一个已经贴合场景的实现。

**倾向 A 或 B**（两者接近），**明确不倾向 C**。理由：LocalDub 的队列是
「单机串行常驻 worker」这个具体形态的最优解，通用 runtime 在这个场景下是过度
设计——上游做 runtime 是因为 serverless 需要「无进程常驻」，而 LocalDub 恰恰
是常驻进程。

### D6. core 的 25ms 轮询要不要顺带修？

现状：`exec_pause` 每 25ms `store.get_events()` 全量重读 + `find_resume` 线性扫，
是 O(n²)。这是 README「已知边界」里记的唯一性能硬伤。

| 选项 | 代价 | 倾向 |
| --- | --- | --- |
| **A. 先不动，等 runtime 定了再说** | 硬伤留着 | ✅ 见下 |
| B. 现在换成 `subscribe` | `subscribe` 返回阻塞 `mpsc::Receiver`，async 上下文里要 `spawn_blocking`；且 store 可以不给（`None`） | ⚠️ |

**倾向 A 暂时不动**，但要在 D3 的讨论里一并考虑——因为「谁来唤醒」这个问题的
答案，会决定轮询要留还是要改。

---

## 明确不做的事

- **不做托管控制面**：上游 `comparison.md:49` 把「Managed control plane
  included」列为 Intentional non-goal。
- **不做 host adapter**：Netlify / Vercel / Cloudflare 层的薄壳对 Rust + 本地
  部署无意义。将来若要，是「最小的本地 HTTP server 或 CLI」。
- **不下沉进 core**：D1 的 A 选项就是这个意思。

---

## 下一步（按顺序）

1. ~~查 D3 的影响面~~ —— **已实测**，见 D3 附。
2. ~~查 D5（LocalDub 需要什么）~~ —— **已调查，结论推翻了原定方向**，见 D5 附：
   LocalDub 已有等价 runtime（`server/src/feat/workflows/queue/`），真正缺的是
   并行编排表达力（core 的能力）。
3. **待你拍板 D5 的 A / B / C**（倾向 A 或 B），再决定 D1 / D2 / D4 是否还需要。
   ——**如果选 A，D1/D2/D4 全部作废**，因为它们都是「做 runtime」才需要回答的问题。
