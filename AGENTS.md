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

## 参考实现

- 上游 TS：`learn_ls/workflow/packages/workflow-store-drizzle-postgres`（全量实现 19 个方法，
  含 lease/timer/schedule/查询）与 `workflow-store-cloudflare-d1`
- 本仓 `packages/workflow-runtime/src/in_memory_store.rs`（`InMemoryExecutionStore`）

写新 adapter 时**照 TS 参考实现逐方法对照**，schema 也逐列对齐它的 `migrations/*.sql`。
