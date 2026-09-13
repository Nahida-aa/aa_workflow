# workflow-runtime 设计意图

> **状态**：草案，未拍板。本文只写**意图与取舍**，不写 API 签名——签名会随
> 设计变化，写在意图之前只会造成文档与实现不一致。

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

**我们唯一已知的消费场景是 LocalDub pipeline。** 需要先问它要什么，而不是
照抄全集。

粗略归类：

| 能力 | LocalDub 需要吗？ | 判断依据 |
| --- | --- | --- |
| timers 投递 | **很可能需要** | pipeline 里有 sleep / 定时闸门 |
| 陈旧 run 恢复 | **很可能需要** | 「机器重启后接着跑」是 LocalDub 的核心诉求 |
| 幂等创建 | 可能需要 | 防止同一 pipeline 起两个 run |
| **lease / 原子 claim** | **待定** | 如果只有一个进程在跑，lease 是纯开销 |
| schedules / cron | **待定** | pipeline 是被触发的还是自定时的？ |
| list / timeline | 待定 | 取决于有没有 UI |

**如果 lease 和 schedules 都不需要**，runtime 的工作量比上游小一个数量级——
可能只有一个 `sweep`（扫陈旧 run + 投递到期 timer）+ `start_run`。

**这条不查清就动手，风险最大**——要么白做一半，要么做小了将来重做。

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

1. ~~查 D3 的影响面~~ —— **已实测**，见 D3 附：7 个测试依赖自动 resume，都在
   `task.await` 层面，切换是行为变更但非破坏性；且有「core 保留自投递 +
   runtime 作可选外部驱动器」的折中方案，零测试改动。
2. **查 D5（唯一未查的）**：看 LocalDub 的 pipeline 实际需要什么。
   具体要回答：
   - 有没有多个 worker 会同时 drive 同一个 run？（决定 lease 要不要）
   - pipeline 是被外部触发的，还是自定时的？（决定 schedules 要不要）
   - 有没有 UI 要列 run？（决定 list / timeline 要不要）
   这一步的产出决定 runtime 的规模——**如果 lease 和 schedules 都不要，
   工作量比上游小一个数量级**。
3. **据 2 的结果拍板 D1 / D2 / D4**，然后才写代码。

第 2 步是**只读调查**，不是实现。它需要看 LocalDub 侧的调用方：
`packages/cli/run-task.ts`（派发器）与 `packages/core/cmd/tasks/task.ts`。
