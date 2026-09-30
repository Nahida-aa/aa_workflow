- 需要有批判性思维(可以质疑)
- api 未稳定, 可以修改测试
- 另外文件可以放项目下的 tmp/ 目录

## Store adapter 实现哪个契约：`WorkflowExecutionStore`（不是 `RunStore`）

上游 `docs/api/store-adapters.md` 开篇就是这条规定：

> **Store adapters implement `WorkflowExecutionStore` from `@tanstack/workflow-runtime`.**

所以**写 store adapter 一律实现 [`WorkflowExecutionStore`]**，不是 core 的 `RunStore`。
两个都叫「store 契约」，差一个后缀，极易看错。

层次（`packages/workflow_runtime/src/run_store_adapter.rs`）：

```text
aa_workflow_core::RunStore            引擎 replay 用（旧形状）
  ↕ 形状相近但无关（没有继承关系）
WorkflowRunStoreAdapterStore       本仓 runtime 的存储基础：元数据信封 + 事件日志（6 个方法）
  └── WorkflowExecutionStore       要实现的**就是它**：+ lease / timer / schedule / 查询（共 19 个）
```

**实现者只写一套方法**。要喂给 core 的 `run_workflow`（入参是 `Arc<dyn RunStore>`），
用现成的 [`create_run_store_adapter`](packages/workflow_runtime/src/run_store_adapter.rs) 降格转换，
**不要在 store 里另写一份 `impl RunStore`**——那会变成两套几乎相同的方法，且有漂移风险
（改一套忘另一套 → runtime 与 core 看到的状态不一致）。

`RunStore` 是**旧的那一层**：core 还在用它，但对外发布 / 新增的 adapter 不再是它。
判断依据是上游自己也在迁移——`createRunStoreAdapter` 的存在就是为了把新形状降格成旧的。

### ⚠️ 新契约**没有** `truncate_log_at_step`，所以 `continue_from` 用不了

先说清来源：**这个方法上游 TanStack 没有**。上游 `RunStore`
（`types.ts:600-627`）只有 6 个方法，**既没有它，也没有 `continueFrom`**——
两者都是本仓的本地扩展（见 `docs/tanstack-alignment.md` 的「保留了分歧」）。
上游整个 monorepo 里跟 "truncate" 有关的只有 `tanstack.workflow.events_truncated`
这个**遥测属性名**，不是能力。

两个契约**不是包含关系，是各有各的**：

| 能力                                   | core 的 `RunStore`（旧）      | `WorkflowExecutionStore`（新）      |
| -------------------------------------- | ----------------------------- | ----------------------------------- |
| `truncate_log_at_step`（**本地扩展**） | ✅ 有 —— `continue_from` 靠它 | ❌ **没有**（它对齐上游，上游没有） |
| lease / timer / schedule / 查询        | ❌ 没有                       | ✅ 有                               |

所以**走新契约的 store，`continue_from` 一定失败**。适配器如实报错而非静默 no-op
（`run_store_adapter.rs:451`），失败信息指向 `truncate_log_at_step`。实测见
`examples/store_file/tests/dub_sf_ocr.rs` 的
`dub_sf_ocr_continue_from_is_unsupported_on_new_contract`。

**要 `continue_from` 就得**：继续用 core 的 `RunStore`（但拿不到 lease/timer），
或者在 store 上**加非契约方法**并自行截断（这等于扩大契约，需明确决定，别默认做）。

## `continue_from` 的机制：日志是「短路索引」

理解这一点，才知道上面那个缺口为什么是结构性的。

`continue_from` 做两件事（`engine/run_workflow.rs:135-139`）：

1. **跑 handler 之前**，调 `store.truncate_log_at_step(run_id, step_id)`；
2. 然后照常**从头重放 handler**。

`truncate_log_at_step` 本身极简（`run_store/in_memory.rs:106-124`）：

```rust
// 找该 step 的**最后一个**终态 checkpoint（StepFinished 或 StepFailed）
let cut = log.iter().rposition(|ev| match ev {
    StepFinished { step_id: id, .. } | StepFailed { step_id: id, .. } => id == step_id,
    _ => false,
});
if let Some(i) = cut {
    log.truncate(i);   // 丢掉 i 及之后的一切
}
```

### 为什么剪日志就等于「让那些 step 重跑」

因为**引擎没有「step 是否执行过」这种独立状态**——它只认日志里有没有该 step 的终态
事件。`ctx.step(id)` 在重放时的行为完全由日志决定：

| 日志里                               | `ctx.step(id)` 的行为                       |
| ------------------------------------ | ------------------------------------------- |
| **有** `StepFinished/StepFailed(id)` | **短路** —— 返回缓存结果，闭包**不执行**    |
| **没有**                             | 真正执行闭包，跑完 append 一条新 checkpoint |

所以：

> **日志 = 「哪些 step 可以短路」的索引。截断 = 把索引从某处切断，
> 使那段重放时不再短路。**

这也解释了 `continue_from` 为什么放在 **store 层**而不是引擎里
（`run_workflow.rs:135` 原注释：_"continue_from lives at the store layer"_）——
它是**日志操作**，不是引擎操作。

### 三个常被忽略的推论

1. **剪的是「该 step 及其之后」，不只是那一个 step**。因为下游 checkpoint 是在上游
   结果之上产生的，上游要重跑，下游的旧结果就不能信。见测试
   `continue_from_resets_downstream`。
2. **副作用会真的再发生一次**（闭包被重新调用）。所以 `continue_from` 的语义是
   **重跑**，不是"续命"。有真实副作用的 step 必须用 `stepCtx.id` 做外部系统的幂等键。
3. **对没有终态 checkpoint 的 step，它是 no-op 且不报错**
   （`run_store/mod.rs:214-218`：_"there is nothing to cut — resume would re-run it
   anyway"_）。逻辑自洽：没 checkpoint 的本来就会重跑。

### 没有任何东西被「回滚」

容易误解成"回滚状态"。实际上全程只有一个动作：**删日志事件**。

- `ctx.state` 靠 replay 重建，不需要回滚（见上面「`RunState` ≠ `ctx.state`」一节）
- `RunState` 信封（status/output/error）在下次 drive 时被重新投影

## Adapter 实现清单（上游 `docs/api/store-adapters.md`）

生产级 store adapter 应满足：

- [ ] 事件的 compare-and-swap 追加语义
- [ ] 幂等的 run 创建
- [ ] 幂等的 signal / approval 投递
- [ ] 原子认领与 lease 语义
- [ ] 到期 timer 与 schedule 的索引
- [ ] 陈旧 lease 的恢复（`claim_stale_runs`）
- [ ] timeline / list API
- [ ] 迁移策略（package-owned SQL migration，见 `SCHEMA_MIGRATIONS.md` 体例）
- [ ] 跑通共享契约测试套件（`packages/workflow_runtime/src/store_contract.rs`）

## `RunState` ≠ `ctx.state`（同名，但毫无关系）

这是本仓**最容易混淆**的一对名字，混淆后会把「持久化的 run 元数据」和「handler 的
业务状态」当成一回事。区分：

|          | `RunState<TInput, TOutput>`                                                                      | `ctx.state: TState`                                   |
| -------- | ------------------------------------------------------------------------------------------------ | ----------------------------------------------------- |
| 是什么   | run 的**持久化元数据信封**（store 存它）                                                         | workflow 的**业务状态**（handler 用）                 |
| 定义处   | `packages/workflow_core/src/run_store/mod.rs`（上游 `types.ts:540`）                             | `BaseCtx<TInput, TState>` 的 `state` 字段             |
| 怎么声明 | 固定结构，无 schema                                                                              | `.state::<T>()` + `.initialize(...)`                  |
| 谁消费   | **store**（路由 / 恢复 / 审计）                                                                  | **handler**（`ctx.state.count += 1`）                 |
| 存哪     | **持久化**（表 `workflow_run_states` / `workflow_runs`）                                         | **不持久化**，每次 resume 由 `initialize(input)` 重建 |
| 装什么   | `run_id` / `status` / `input` / `output` / `error` / `waiting_for` / `pending_approval` / 时间戳 | 任意业务字段                                          |

**上游专门写了一句注释来排斥这种混淆**（`workflow-core/src/types.ts:534-536`）：

> Persisted run metadata. **State is intentionally NOT stored here** — it is
> reconstructed from `initialize(input)` + log replay on every resume.

也就是说 `RunState` 的职责边界**就是**「不放 `ctx.state`」。

### 三个衍生结论（都不是可选项）

1. **store 的 schema 里不该有业务 state 的列**。`workflow_runs` 与
   `workflow_run_states` 两张表存的都是 `RunState` 那一族（前者是 runtime 的
   `WorkflowExecution` 投影，后者是 core 的 `RunState` 信封），**都不存 `ctx.state`**。
2. **`STATE_DELTA` 不是 `ctx.state` 的持久化**。它是 emit-only：只投 publisher 做
   可观测，**不进日志、不参与 replay**。所以 `ctx.state` 在整条链上**没有任何持久化
   形态**。
3. **`ctx.state` 的正确性完全依赖 handler 的确定性**——因为它靠 replay 重建，而
   replay 会短路已 checkpoint 的 step。所以：**step 闭包内对 state 的修改会丢**
   （见 `state_step_closure_mutation_lost_on_resume`）；**并行分支通过
   `ctx.clone()` 各持快照、互不可见**（见 `state_parallel_steps_snapshot_then_driver_flush`）。
   要跨分支传递可变数据，走 **step 的返回值**（durable 结果），不要走 state。

## `ctx.state` 的写入位置准则（串行 + 并行）

**完整论证、串行反例、上游实测见 [`docs/concepts/ctx-state.md`](docs/concepts/ctx-state.md)。**
本页只留必须记住的机械准则。

**核心：workflow 是对副作用的编排，需要「跳过已执行的副作用」+「复用其结果」。满足
这两条的是事件日志 + step 原语 —— 所以 step 闭包唯一耐久的输出通道是返回值。**

机制：replay 时已 checkpoint 的 step **直接返回日志里的结果，闭包不被调用**
（上游 `replay-and-resume.md:31`：`fn` is NOT called）。它把代码分成两类：

|           | step 闭包 `ctx.step(id, fn)` | handler 体内 |
| --------- | ---------------------------- | ------------ |
| replay 时 | **被跳过**                   | **重跑**     |
| 副作用    | 只能通过**返回值**输出       | 会被重新施加 |

所以 **step 闭包不要写 `ctx.state`**：那是「未被记录的副作用」—— 不在返回值里，
日志里也没有，而 state 每次 resume 都由 `initialize(input)` 重建
（`STATE_DELTA` emit-only、不进日志）。数据流走 **step 返回值 → 局部变量 → 下个 step
的入参**。

补充一点容易误读的：state 的写**是自动发 `STATE_DELTA` 的**，不用手动发
（`define/mod.rs:166-174` 的 `flush_state` → `state.sync()` + `emit_state_delta()`），
只是攒到耐久边界才 flush（step / wait / approve / sleep_until / yield / drive 收尾）。
最容易踩的坑是：**首次 drive 的 delta 看着完全正常，resume 时闭包被短路 → 一条 delta
都不发 → 终态是另一个值，且永远不会被纠正。** 所以判别信号是「**恢复期有没有重新发出
`STATE_DELTA`**」，不是终态值本身。

> 精确化：step 闭包**不是**严格纯函数 —— 上游例子 `ctx.step('flag', fetchFlag)` 就有
> 外部副作用。区别在**副作用有没有被记录**：`fetchFlag()` 的效果进了
> `STEP_FINISHED.result`，所以只真实执行一次，那是允许的。要求的是「无隐藏副作用」。

> 串行时随手改 `ctx.state` 是自然行为，一旦并行就必须显式设计。这个不对称是坑的
> 来源，所以并行时再加一条：
>
> **`ctx.state.x = ...` 必须出现在「执行顺序由词法决定」的位置。**
> 只需自问：**这个赋值在 `try_join!` 的哪一侧？**
>
> - 在 join **之内**（分支闭包内部）→ **违规**，改成把值 `return` 出去
> - 在 join **之后**（汇合点）→ **安全**

### 为什么不是「加锁就好」

并行分支对 `ctx.state` 的两次写入之间**没有确定的先后**（由调度器决定），于是
**同一份日志重放两次可能得到不同的 state**——replay 地基直接塌掉。锁能消除竞写，
但消除不了不确定性（谁先谁后变成「谁先拿到锁」，仍依赖调度）。

更彻底的是：`ctx.state` **没有任何持久化形态**（见上面第 2 条），所以连「事后按日志
合并」这条退路都没有。

两边的语言差异会让症状不同，但都不安全：

- **TS**：`ctx.state` 是同一个对象引用（`run-workflow.ts:480`），并行写**会互相看见**
  → 真竞写，last-write-wins、非确定
- **Rust**：`StateHandle::clone` 是快照分裂（`state_handle.rs:37-40`），并行写**不会被
  对方看见** → 静默丢弃

### 两种需求的正确写法

**A. 各产出、最后合成** —— 合成放在汇合点：

```rust
let (ra, rb) = tokio::try_join!(branch_a, branch_b)?;   // 分支内只 return
ctx.state.merged = merge(ra, rb);                        // 汇合点：词法顺序，安全
```

**B. 累积（计数 / 集合）** —— 把「改 state」换成「产出增量」：

```rust
// 分支内：只产出增量，不碰 state
let delta_a = ctx_a.step("count_a", ..).await?;   // -> 3
let delta_b = ctx_b.step("count_b", ..).await?;   // -> 5

// 汇合点：统一归并（串行、确定性）
ctx.state.total += delta_a + delta_b;
```

B 的关键不只是「避免竞写」：**增量作为 step 返回值进了日志、参与 replay**，所以归并
逻辑可重放。这是它比「并行写 state」强的地方。

### `ctx.state` 该用来干什么

⚠️ **以下是推论，不是上游表述。** 上游从未说明 `ctx.state` 的用途 —— `docs/` 里
`ctx.state` 出现 0 次，`guide/observability.md` 完全没提 state。唯一挂钩的一句是生成
文档 `reference/type-aliases/Operation.md:28`（讲 diff 机制本身）："…for workflow
state **observability**"。

可以确定的是它**不是**结果通道：上游 5 处 state schema（作者均为 maintainer）
无一例外是 status / phase / counter；唯一写下来的规范用法
（`engine.smoke.test.ts:42-43`）方向是 **返回值 → state**。

所以它更像**给观察者看的进度投影**或 handler 草稿区，而非跨步骤的数据通道。
这条推论**不参与**上面的写入规则（规则只依赖「闭包不被重放」+「state 每次重建」
两条已证实事实）。参考用法见上游 `engine.smoke.test.ts:37`（最小）与
`examples.kyle-durable-agent.test.ts:100`（agent 的虚拟 FS，跨迭代累积）。

本仓示例 `dub_sf_ocr`（`examples/store_file/src/dub_sf_ocr.rs`）是正例：两个并行分支
的 step **只读** state，一处都不写。

## 参考实现

- 上游 TS：`learn_ls/workflow/packages/workflow-store-drizzle-postgres`（全量实现 19 个方法，
  含 lease/timer/schedule/查询）与 `workflow-store-cloudflare-d1`
- 本仓 `packages/workflow_runtime/src/in_memory_store.rs`（`InMemoryExecutionStore`）

写新 adapter 时**照 TS 参考实现逐方法对照**，schema 也逐列对齐它的 `migrations/*.sql`。
