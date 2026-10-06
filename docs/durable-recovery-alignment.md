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
| 信号 | bufferSignals 收件箱（早到持久化+原子消费） | `signal_run`/`signal_event` 先经 store 契约持久化再驱动（D3 形态） | **需评估**（见 Phase 3） |
| step 重试 | `retry.durable`（attempt/deadline 持久化，sweep 恢复） | 内存级 retry（`default_step_retry`，attempts 记入 StepFailed） | **缺耐久化**（Phase 2） |
| 补偿 | `ctx.compensate` + COMPENSATION_STARTED + 逆序 undo | **无**（grep 零命中） | **缺整轴**（Phase 1） |
| checkpoint 导入/泊车 | reexecuteFrom / interimGoals（f2-lite 引擎 diff） | 无 | Phase 4 |
| 图原语 | ——（引擎涌现图） | aa_graph（图中性原语库） | Rust 侧反而领先 |

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

**现状**：早停已有（`target_step` 命中即停 → Finished）；泊车形态（跑到
目标后挂起等宿主扩展，TS f2-lite 的 `waitingFor.kind: 'goal'`）未实现——
但泊车原语的材料已齐（本次 retry.durable 的 exec_pause 泊车 + sync_timer
登记链路可直接复用）。

**实现**：`reexecuteFrom`（checkpoint 导入 + meta.importedFrom 溯源）/
`interimGoals`（goal 全 checkpoint 后经保留信号泊车，转移检查防重挂）——
照 TS f2-lite 引擎 diff 语义移植，词汇保持图中性。
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
