- 需要有批判性思维(可以质疑)

## Store adapter 实现哪个契约：`WorkflowExecutionStore`（不是 `RunStore`）

上游 `docs/api/store-adapters.md` 开篇就是这条规定：

> **Store adapters implement `WorkflowExecutionStore` from `@tanstack/workflow-runtime`.**

所以**写 store adapter 一律实现 [`WorkflowExecutionStore`]**，不是 core 的 `RunStore`。
两个都叫「store 契约」，差一个后缀，极易看错。

层次（`packages/workflow-runtime/src/run_store_adapter.rs`）：

```text
workflow_core::RunStore            引擎 replay 用（旧形状）
  ↕ 形状相近但无关（没有继承关系）
WorkflowRunStoreAdapterStore       本仓 runtime 的存储基础：元数据信封 + 事件日志（6 个方法）
  └── WorkflowExecutionStore       要实现的**就是它**：+ lease / timer / schedule / 查询（共 19 个）
```

**实现者只写一套方法**。要喂给 core 的 `run_workflow`（入参是 `Arc<dyn RunStore>`），
用现成的 [`create_run_store_adapter`](packages/workflow-runtime/src/run_store_adapter.rs) 降格转换，
**不要在 store 里另写一份 `impl RunStore`**——那会变成两套几乎相同的方法，且有漂移风险
（改一套忘另一套 → runtime 与 core 看到的状态不一致）。

`RunStore` 是**旧的那一层**：core 还在用它，但对外发布 / 新增的 adapter 不再是它。
判断依据是上游自己也在迁移——`createRunStoreAdapter` 的存在就是为了把新形状降格成旧的。

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
- [ ] 跑通共享契约测试套件（`packages/workflow-runtime/src/store_contract.rs`）

## `RunState` ≠ `ctx.state`（同名，但毫无关系）

这是本仓**最容易混淆**的一对名字，混淆后会把「持久化的 run 元数据」和「handler 的
业务状态」当成一回事。区分：

| | `RunState<TInput, TOutput>` | `ctx.state: TState` |
| --- | --- | --- |
| 是什么 | run 的**持久化元数据信封**（store 存它） | workflow 的**业务状态**（handler 用） |
| 定义处 | `workflow-core/src/run_store/mod.rs`（上游 `types.ts:540`） | `BaseCtx<TInput, TState>` 的 `state` 字段 |
| 怎么声明 | 固定结构，无 schema | `.state::<T>()` + `.initialize(...)` |
| 谁消费 | **store**（路由 / 恢复 / 审计） | **handler**（`ctx.state.count += 1`） |
| 存哪 | **持久化**（表 `workflow_run_states` / `workflow_runs`） | **不持久化**，每次 resume 由 `initialize(input)` 重建 |
| 装什么 | `run_id` / `status` / `input` / `output` / `error` / `waiting_for` / `pending_approval` / 时间戳 | 任意业务字段 |

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

## 参考实现

- 上游 TS：`learn_ls/workflow/packages/workflow-store-drizzle-postgres`（全量实现 19 个方法，
  含 lease/timer/schedule/查询）与 `workflow-store-cloudflare-d1`
- 本仓 `packages/workflow-runtime/src/in_memory_store.rs`（`InMemoryExecutionStore`）

写新 adapter 时**照 TS 参考实现逐方法对照**，schema 也逐列对齐它的 `migrations/*.sql`。
