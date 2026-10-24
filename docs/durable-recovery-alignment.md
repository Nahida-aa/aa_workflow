# 耐久恢复原语对齐设计（对齐 TanStack workflow `taren/durable-recovery` / PR #19）

> 目标：把 PR #19 的三个恢复原语（bufferSignals / durable retry /
> ctx.compensate）按语义对齐到 aa-workflow 的 Rust 引擎，并预留 redo 轴
> （INVALIDATED）的原生位置。语义依据 = 上游 `taren/durable-recovery` 分支的
> 官方实现与 `docs/guide/durable-recovery.md`（209 行语义文档），不是凭 PR
> 描述猜测。
>
> **原则（对齐 = 语义对齐，不是代码移植）**：TS 侧的实现细节（await 链、
> meta 槽、CAS 位置）不照抄；照抄的是**事件形态、重放语义、失败路径、
> 范围限制**——这些是可测试的行为承诺。

## 0. Rust 现状盘点（2026-10-02 实测）

| 能力 | TS（taren 分支） | Rust（aa-workflow） | 差距 |
| --- | --- | --- | --- |
| runtime 四方法面 | startRun/deliverSignal/deliverApproval/sweep | ✅ 已对齐（runtime_driver.rs 同名四方法） | 无 |
| store 契约 | WorkflowExecutionStore 19 方法 | 23 方法（store_contract.rs） | 形态差异已对齐 |
| 信号 | bufferSignals 收件箱（**runtime 层 opt-in**，早到持久化 + 原子消费 + signalId 去重；需 store `bufferSignal`/`consumeBufferedSignal`，`__timer` 双闸豁免） | core 层收件箱 ✅（零新机制——信号即事件，`early_signal_run_acts_as_inbox` 钉死）；runtime 层与上游同构且更严（未等待即 `NotWaiting`） | 差距三项（core 层恒开不可关 / 只积累不去重 / 无 `__timer` 豁免，见 Phase 3 含层对应表） |
| step 重试 | `retry.durable`（attempt/deadline 持久化，sweep 恢复） | ✅ 已实现（`RetryPolicy.durable` + STEP_RETRY_SCHEDULED 泊车/续驱，c1c7ddf） | 无 |
| 补偿 | `ctx.compensate` + COMPENSATION_STARTED + 逆序 undo | ✅ 已实现（`ctx.compensate` 内存登记 + 重放重建 + 逆序 undo，c1c7ddf） | 无 |
| checkpoint 导入/泊车 | reexecuteFrom / interimGoals（**f2-lite 自有，上游未采纳**；`f2-lite` 分支 `run-workflow.ts:71-84`） | ✅ 已实现（`reexecute_from` + `StepFinished.meta.imported_from` 溯源；`interim_goals` 泊车 a9a7424） | 无 |
| 图原语 | ——（引擎涌现图） | aa_graph（图中性原语库） | Rust 侧反而领先 |

> **溯源更正（2026-10-15）**：表中「checkpoint 导入/泊车」一行曾写作
> 「上游已原生采纳，`run-workflow.ts:71-84`」——**这是错的**。
>
> **先明确「上游」指谁**。本仓的对齐基线是 `0971b4c`（= `origin/taren/durable-
> recovery`，PR #19，**不是** `origin/main`）——这正是本文档标题所写的对齐目标。
> 推导链（已逐个 hash 核验）：
>
> ```text
> origin/main              b928717  2026-07-21  上游 main（已停滞）
>   └─ taren/durable-recovery  0971b4c  2026-09-22  **本仓基线**（PR #19）
>        ├─ fork/taren/...     0971b4c            fork 与基线同 hash，零分叉
>        └─ f2-lite            498a882  2026-10-04  基线 + 6 个本地提交
> ```
>
> `0971b4c` 是 `f2-lite` 的祖先、反向差 0 提交；fork 那支与基线**同 hash**。
> 所以「f2-lite 里凡是 `0971b4c` 没有的，必是本地工作」这条判据成立。
> 对 `reexecuteFrom` / `interimGoals` / `__interim-goal` 三个关键词核验：
> `origin/main`（`b928717`）**零命中**、`0971b4c`（基线本身）**也零命中**——
> 不是"main 还没合"，是**基线上根本不存在**。只存在于 f2-lite 那 6 个提交。
> （`run-workflow.ts:80` / `:91`，runtime 侧 `runtime/types.ts:425` / `:430`），
> 是本仓**自有设计**。成因：当时工作区 checkout 在 `f2-lite`，读本地文件时
> 把「自己的注释」当成了上游（`f2-lite` 的注释字面写着 `F2-lite re-execute
> plan`，被反向读成「上游引用 f2-lite」）。
>
> **实际影响**：redo / 泊车两轴**没有上游背书**——API 形状（`unsafe_invalidate`
> 命名、`meta.imported_from` 的 index 语义）归本仓所有，可自由改；同时也没有
> 上游测试兜底，`engine.rerun.test.ts` 是 `f2-lite` 自己的测试。
> 上游至今**只有 undo 轴**（`taren/durable-recovery` 的 compensate），
> 「上游两轴都有」的说法不成立。

确认：Rust 引擎**零补偿机制**（`grep -rn compensate packages/workflow_core/src`
零命中）；事件系统 `WorkflowEvent` 已有 StepFailed{error, attempts}、
StepFinished{result, attempts}——耐久重试与补偿的持久化挂载点现成。

## 1. Phase 1 — `ctx.compensate`（undo 轴）

### 1.1 语义规格（逐条对齐 #19 官方文档）

1. **登记先于效果**：`ctx.compensate(id, undo_fn, opts)` 在受保护步骤**之前**
   调用；登记在 `ctx.compensate` 返回前持久化（防"效果成功但 worker 死于记录
   结果之前"的缺口）。
2. **触发**：仅未捕获的工作流失败。不覆盖：取消、进程终止、已被 handler 处理
   的错误、成功返回。
3. **执行顺序**：逆登记序、一次一个；每个补偿是**独立的耐久 step**（自己的
   checkpoint + 自己的 retry 预算）。
4. **崩溃恢复**：进程死于补偿执行中 → 先走既有 recovery（running+lease 过期
   → sweep）→ 恢复后的 run 若仍失败才继续补偿；已完成的补偿不重复执行
   （幂等键 = 补偿 id，中断的补偿可用同键重入）。
5. **耗尽即停**：某个补偿的 retry 耗尽 → 补偿链就地停止；run 同时记录
   **原始失败 + 补偿失败**；更早登记的补偿保持未执行，留给运营处置。
6. **终态**：即使全部补偿成功，run 仍以 **errored** 结束（携带原始失败）——
   补偿修复外部世界，不修改 run 的失败定性。
7. **确定性纪律**：登记、id、捕获值必须确定性（重放重建 handler 时按代码重
   登记，**回调不序列化进存储**）；补偿 id 唯一；`__compensation:` /
   `__retry:` 为保留 id 前缀；补偿 handler 内**禁止调用工作流原语**（直接用
   传入的 step context 执行外部动作）。

### 1.2 Rust 设计

**API**（对齐 #19 的签名）：

```rust
// engine/compensate.rs
impl<BaseInput, BaseState> BaseCtx<BaseInput, BaseState> {
    pub async fn compensate(
        &self,
        id: &str,                     // 唯一；__compensation: 前缀保留
        undo: impl Fn(StepContext) -> BoxFuture<'static, anyhow::Result<()>> + Send + 'static,
        opts: CompensateOptions,      // retry: { max_attempts, base_ms }
    ) -> anyhow::Result<()>
}
```

**持久形态（设计决策：事件 + 引擎内存双轨）**：

| 时刻 | 机制 | 说明 |
| --- | --- | --- |
| 登记 | 内存 `Vec<Compensation{id, undo_fn}>`（EngineRuntime 扩展） | handler 重放时按代码重登记——回调不序列化（#19 同款纪律） |
| 登记*记录* | 可选：`COMPENSATION_REGISTERED` 观测事件（**不持久化**，对齐 RunStarted 先例） | 跨 drive 的登记恢复不靠它 |
| 触发 | 未捕获失败时：**先落 `COMPENSATION_STARTED { error }`（持久 checkpoint）**，再逆序执行 | 崩溃后恢复的驱动从该事件得知"补偿进行中" |
| 每个补偿完成 | `COMPENSATION_COMPLETED { id }`（持久 checkpoint） | 重启后已完成的跳过（幂等键 = id） |
| 补偿失败 | `COMPENSATION_FAILED { id, error, attempts }`（持久）+ 耐久重试（见 Phase 2 的 next_attempt 机制复用） | 耗尽即停 |

**执行路径**（run_workflow.rs 的失败 catch 处）：handler 抛错 → 判定非
pause/非 abort → 若 `engine.compensations` 非空 → 逆序执行补偿链 → 完成后
照常落 RunErrored（原始错误 + 补偿结果摘要）。

**与既有机制的交互（必须写测试）**：
- 补偿链执行期间 `engine.paused`（不可能——补偿内禁止原语，但防御性断言）；
- 补偿期间再失败 → 记 COMPENSATION_FAILED、继续下一个？**否**——#19 语义：
  耗尽即停，后续补偿留给人工（对齐"Earlier handlers remain unexecuted"）；
- 与 abort 的竞争：abort 优先于补偿（补偿可被 signal 打断 → 记录中断，run
  aborted）。

**Rust 特有权（比 TS 更强的保证）**：undo 闭包的 `StepContext` 由引擎注入，
`attempt`/`id` 原生可得；类型系统保证补偿 handler 拿不到 `&mut BaseState`
（对齐 dub_sf_ocr 的"只读 state 类型级保证"传统）。

### 1.3 测试网（移植 #19 语义文档的每一条承诺）

1. 登记 → 失败 → 逆序执行（顺序断言）；
2. 补偿自身失败 → 停链 + 双失败记录（原始 + 补偿）；
3. 补偿耗时重试（durable）→ 重启边界模拟（store 重建）后从断点续；
4. 已完成的补偿跨重启不重复；
5. 无登记的失败 → 行为与现状完全一致（回归）；
6. handler 正常返回/被处理错误 → 补偿不触发（范围限制）；
7. `__compensation:` 保留前缀拒绝。

## 2. Phase 2 — `retry.durable`（重试跨重启）

- 现状：attempts 已入 StepFailed/StepFinished（`Vec<StepAttempt>`）——缺
  **下次重试截止时间**的持久化与 sweep 捞取。
- 设计：失败且 `retry.durable` 且未耗尽 → run 转 `paused`（复用既有挂起机制）
  + 记录 `next_attempt_at` → **sweep**（已存在）到期捞取 → 同 runId 重驱 →
  重放短路已完成、失败步骤重试（attempt 计数从持久化的 attempts 恢复）。
- 与崩溃恢复的关系：driver 崩溃在重试等待期 = 同一恢复路径（sweep），无新机制。
- 测试：重试等待期杀驱动 → sweep → 同 attempt 号重入（幂等键稳定）→ 耗尽后
  终态。

## 3. Phase 3 — 信号收件箱（评估先行）

- Rust D3 现状：`signal_run`/`signal_event` **先经 store 契约持久化再驱动**
  （D3 形态）——需评估：run 尚未等待时的投递，当前是拒绝还是已按契约持久
  化？core 只区分"哪些需要持久化、哪些是运行时数据"，介质（磁盘与否）不在
  它的词汇里。
- 若已按契约持久化 + 原子消费：Phase 3 = 语义对照文档（bufferSignals ≈ D3
  已有行为的正式化），零代码；
- 若被拒：store 契约加收件箱方法（对齐 #19 的 `bufferSignal` /
  `consumeBufferedSignal` 双方法 + `0001_signal_inbox.sql` 形态：按 run 的
  插入序、signalId 去重、消费与 SIGNAL_RESOLVED 同一原子写），in-memory
  store 同步实现。
- 明确不做（对齐 #19 的范围声明）：无自动过期/大小限制；finished/errored/
  aborted 的信号返回 not-waiting；`__timer` 不可缓冲。

**评估结论（2026-10-15 补记）**：core 层**已经是收件箱，零新机制**。

- `signal_run`（`engine/mod.rs:1033`）**无条件** CAS append `SignalResolved`——
  不检查 run 是否在等待，早到信号照常落盘；消费点是 replay 的
  `find_resume`（`engine/mod.rs:899`），从日志短路拾取**首条**匹配。
- 所以 #19 的核心行为（早到持久化 + 等待点 payload 过墙）由**事件日志天然
  获得**——信号投递本就是一次普通 CAS append，而上游需要新机制是因为它的
  信号投递不在事件日志里。`early_signal_run_acts_as_inbox` 已钉死：信号先于
  run 落盘为日志第一条，驱动到 wait 点时过墙、全程无挂起事件。
- **层对应（先对齐，别跨层比）**：

  | 层 | 上游 | Rust |
  | --- | --- | --- |
  | runtime API | `runtime-driver.ts:49`/`:200` | `runtime_driver.rs:531` |
  | store 契约 | `runtime-driver.ts:272` `store.deliverSignal` | `in_memory_store.rs:575` |
  | 引擎消费点 | `run-workflow.ts:1095` `consumeBufferedSignal` | `engine/mod.rs:899` `find_resume` |
  | core 投递原语 | **不存在**（core 只收 `signalDelivery` 作 resume 入参，`run-workflow.ts:42`） | `signal_run` `:1033` / `signal_event` `:1061` |

  Rust 的 runtime 层与上游同构且**更严**：`deliver_signal` 先过 store 闸
  （`is_run_waiting_for_signal`，`in_memory_store.rs:188`），未等待即
  `NotWaiting`；只有 `Delivered` 才桥到 core 的 `signal_event`
  （`runtime_driver.rs:548-559`）。

- **与 #19 的真实差距**（都是设计差，不是缺功能）：
  1. **多一层恒开的投递面**：#19 的开关在 runtime（`bufferSignals: true`），
     Rust 的 runtime 也有闸——但 `signal_run` / `signal_event` 是 **core 直达
     API，绕过 store 闸也能用**，这一层上游根本没有，也无从谈开关。宿主写错
     `step_id` 会静默污染日志。
  2. **只积累、不消费、不去重**：`find_resume` 读首条后条目**滞留日志**
     （且**不能删**——`SignalResolved` 本身就是让 replay 短路的日志条目，删了崩溃
     重放就找不到 resume）。真正缺的是上游那层 **signalId 去重**
     （`in-memory-store.ts:165` 返回 `'duplicate'`）。
  3. **无 `__timer` 豁免**：#19 在 core
     （`run-workflow.ts:1095` `name !== '__timer'`）与 store
     （`in-memory-store.ts:165`）**双闸**；Rust 两边都没有——`is_run_waiting_
     for_signal` 只比对 `signal_name`，`__timer` 泊车的 run 照样能投递。
- 注意分层：runtime 的 `deliver_signal` 对没在等的 run 返回 `NotWaiting`
  （`in_memory_store.rs:584`）是**名字投递路径的既有拒绝**，与 core 的恒开
  收件箱是两层，别用它论证「core 会拒绝早到信号」。
- **排期建议**：不必为 Phase 3 写新机制。若要补齐，方向是「加开关 + 消费
  语义」，**不是**「加收件箱」——收件箱已经有了。

## 4. Phase 4 — redo / 临时目标（名单透传原语，graph-agnostic）

**核心定位修正**：core 是自由涌现代码——处理器里的步骤不必然构成 DAG（可以
有环、可以条件重入），core **不知道也不需要知道**步骤间的依赖结构。所以：

- `unsafeInvalidate` / redo 名单 = **使用者自己提供的步骤列表，原样透传**；
  "unsafe" 前缀的含义正是：core 无法校验封闭性、无法推导消费者，后果由
  调用者承担；
- **不依赖 DAG 算法**：失效的是步骤列表，不是图节点；aa_graph / 声明式层的
  闭包计算只是**可选的便利**（帮使用者算"建议名单"），core 的原语永远是
  纯名单语义；
- 临时目标（interimGoals / 泊车）同理：core 只知道"这些 stepId checkpoint
  后挂起"，不知道也不关心它们在什么"图"上。

**现状**（2026-10）：redo / 临时目标两半都已落地——

- `reexecute_from(source_run_id, unsafe_invalidate)`：全新 run 首驱前导入
  源 run 幸存 checkpoint（只导 `STEP_FINISHED`、失败 step 按设计重跑），
  溯源 `meta.imported_from`（index 以源 run 持久化日志为基准）。参考
  `f2-lite` 分支 `run-workflow.ts:269-295`（**本仓自有，上游未采纳**）；
  `StepFinished`
  因此新增 `meta` 槽（缺省省略，日志形状前后兼容）。resume 驱动忽略、
  `source_run_id == run_id` 报 validation、源缺失报 `run_lost`（f2-lite
  in-memory 对缺失源静默导零，我们显式报错——attach 同款约定）；
- `interim_goals` 泊车/续驱（a9a7424）：goal 全 checkpoint 后泊车，
  续驱补齐缺席 step。

四个原语各有一个 demo（`packages/workflow_dag/tests/dag_recovery_demo.rs`），
引擎侧三个场景测试镜像 `f2-lite` `engine.rerun.test.ts`。
**长期**：INVALIDATED 原语（失效事件 + 重放规则 + 状态转换）——Rust 引擎
自有，无上游兼容包袱；undo（Phase 1）+ redo（本 Phase）两轴齐备后，
agent 流水线的失败语义完整。

## 5. store_sqlx_postgres 对齐

- Phase 1/2：复用既有 append/state 方法，无新迁移（补偿/重试状态走事件与
  RunState）；
- Phase 3（若需收件箱）：新增迁移对齐 `0001_signal_inbox.sql` 形态（插入序
  索引、signalId 唯一、消费原子性）。

## 6. 与 TS 侧的对照纪律

- 每落地一个 Phase，在 `learn_ls/wf-demo/docs/dag-strategies.md` §8 追加
  undo/redo 对照行；
- TS 侧 f2-lite 的 rebase（等 #19 合并）以本文的语义规格为差异清单；
- 命名对齐：`__compensation:` / `__retry:` / `__interim-goal` 保留前缀在
  两侧含义一致。
