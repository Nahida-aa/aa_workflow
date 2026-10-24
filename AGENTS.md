- 需要有批判性思维(可以质疑)
- api 未稳定, 可以修改测试
- 另外文件可以放项目下的 tmp/ 目录

## 验证命令

```bash
cargo check --workspace --all-targets
cargo test --workspace --no-fail-fast          # 本地有 Postgres 时就这样跑（全绿 265）
cargo test --workspace --exclude aa_workflow_store_sqlx_postgres --no-fail-fast   # 没有 Postgres 时
```

**Postgres 侧别再默认跳过。** `aa_workflow_store_sqlx_postgres` 的两个测试
（`--test contract` 共享契约 + `run_store_e2e`）连的是**本地 Postgres**，连接串取
`DATABASE_URL`，缺省 `postgres:///workflow_test`（走 unix socket + peer 认证，
本机通常直接可用）。先探一下再决定跑不跑：

```bash
pg_isready                       # 接受连接就说明能跑
psql -d workflow_test -c 'select 1'
```

> 曾经这里写着「E2E 会挂住（要真 Postgres）」于是把该包整个排除——**这句是错的**。
> 挂住的原因不是"没有 Postgres"，而是 `tests/contract.rs` 用
> `new_current_thread()` 建 runtime：store 的同步方法内部走 `block_in_place`，
> 占住唯一 worker，sqlx 连接池归还连接又需要 runtime 继续驱动后台任务，于是自己
> 卡死自己，30s 后报 `pool timed out while waiting for an open connection`。
> 约束（worker ≥2）早就写在 `SqlxPostgresStore::block` 的文档注释里，
> `run_store_e2e.rs` 也一直是 `new_multi_thread().worker_threads(4)`，只有
> contract 漏了。2026-10-15 已修（`73d2253`），修完即全绿。
> 新增 store 实现若也包 sync/async 桥，照这个教训办：**runtime 至少 2 个 worker**。

`SCHEMA_MIGRATIONS.md` 里有首次建库命令（`createdb -T template0 --lc-collate=C
--lc-ctype=C workflow_test`）。**不跑 rustfmt**——基线本身就不是 rustfmt 过的，
跑一次就是几百行无关 diff。

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

### `continue_from` / `truncate_log_at_step` 已删除

原 `RunWorkflowOptions::continue_from(step_id)` 与 `RunStore::truncate_log_at_step`
**已从 core 删除**，`examples/shared/src/file_run_store.rs`（旧 `FileRunStore`）一并删除。
删除理由：它**不是上游能力**——上游 `RunStore` 只有 6 个方法，既没有 `truncate_log_at_step`
也没有 `continueFrom`（上游全仓与 truncate 相关的只有 `tanstack.workflow.events_truncated`
这个**遥测属性名**）。详见 `docs/tanstack-alignment.md`。

曾经它是「本地扩展」：`continue_from` = 先把日志裁到该 step 最后一个终态 checkpoint
（`StepFinished` / `StepFailed`）的**前一条**，再从头重放 handler；剪掉的 step 因为
「没有日志」而重跑，其余 step 靠 replay 短路跳过。所以它本质是**日志后缀删除**，
不是回滚——`ctx.state` 由 replay 重建，`RunState` 信封下次 drive 时重新投影。

现在**重试只有一条路：新开一个 run**。别再加截断类 API，需要时先对齐上游。


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

---

# Rust Skill Reference

This section provides a quick lookup index for Rust development rules.


# Layer 1 — Lookup: Rust Rule Index

Quick-reference rule index for Rust development. Use `cargo skill lookup <prefix>` to filter.

Priority: CRITICAL > HIGH > MEDIUM > LOW > REF

---

## **own-** — Ownership & Borrowing (CRITICAL)

- **own-01** `borrow-over-clone` — Prefer `&T` over `.clone()`; clone only when ownership is required
- **own-02** `slice-over-vec` — Accept `&[T]` not `&Vec<T>`; accept `&str` not `&String`
- **own-03** `cow-conditional` — Use `Cow<'a, T>` when data is sometimes owned, sometimes borrowed
- **own-04** `arc-shared` — Use `Arc<T>` for thread-safe shared ownership across threads
- **own-05** `rc-single-thread` — Use `Rc<T>` for single-threaded shared ownership
- **own-06** `refcell-interior` — Use `RefCell<T>` for interior mutability in single-threaded code
- **own-07** `mutex-interior` — Use `Mutex<T>` for interior mutability in multi-threaded code
- **own-08** `rwlock-readers` — Use `RwLock<T>` when reads significantly outnumber writes
- **own-09** `copy-small` — Derive `Copy` for small, trivially-copyable types
- **own-10** `clone-explicit` — Make `Clone` explicit; never rely on implicit copies
- **own-11** `move-large` — Move large data instead of cloning it
- **own-12** `lifetime-elision` — Rely on lifetime elision; annotate only when compiler requires it

---

## **err-** — Error Handling (CRITICAL)

- **err-01** `thiserror-lib` — Use `thiserror` for library error types
- **err-02** `anyhow-app` — Use `anyhow` for application-level error handling
- **err-03** `result-over-panic` — Return `Result<T, E>` for expected errors; never `panic!` on recoverable conditions
- **err-04** `context-chain` — Add context with `.context()` or `.with_context()` at every error boundary
- **err-05** `no-unwrap-prod` — Never use `.unwrap()` in production code paths
- **err-06** `expect-bugs-only` — Use `.expect("msg")` only for programming errors that should never occur
- **err-07** `question-mark` — Use `?` operator for clean error propagation; avoid nested `match` on `Result`
- **err-08** `from-impl` — Use `#[from]` for automatic `From` conversion between error types
- **err-09** `source-chain` — Use `#[source]` to preserve the underlying error in the chain
- **err-10** `lowercase-msg` — Error messages: lowercase, no trailing punctuation
- **err-11** `doc-errors` — Document all error conditions under `# Errors` in rustdoc
- **err-12** `custom-type` — Define custom error types; never use `Box<dyn Error>` as a return type

---

## **mem-** — Memory Optimization (CRITICAL)

- **mem-01** `with-capacity` — Use `Vec::with_capacity(n)` and `String::with_capacity(n)` when size is known
- **mem-02** `smallvec` — Use `SmallVec<[T; N]>` for collections usually smaller than N elements
- **mem-03** `arrayvec` — Use `ArrayVec<T, N>` for strictly bounded-size collections
- **mem-04** `box-large-variant` — Box large enum variants to keep the enum size small
- **mem-05** `boxed-slice` — Use `Box<[T]>` instead of `Vec<T>` for fixed-length sequences
- **mem-06** `thinvec` — Use `ThinVec` for frequently-empty vectors (1 pointer vs 3)
- **mem-07** `clone-from` — Use `dst.clone_from(&src)` to reuse allocations instead of `dst = src.clone()`
- **mem-08** `reuse-collections` — Reuse collections across iterations with `.clear()` instead of reallocating
- **mem-09** `avoid-format` — Avoid `format!()` when a string literal or `write!()` suffices
- **mem-10** `write-over-format` — Use `write!(buf, ...)` into an existing buffer instead of `format!(...)`
- **mem-11** `arena-allocator` — Use arena allocators (`bumpalo`) for batch allocations with shared lifetime
- **mem-12** `zero-copy` — Use zero-copy patterns with slices and `bytes::Bytes` instead of copying data
- **mem-13** `compact-string` — Use `CompactString` or `SmolStr` for strings usually under 24 bytes
- **mem-14** `smaller-integers` — Use the smallest integer type that fits the domain (`u8`, `u16`, etc.)
- **mem-15** `assert-type-size` — Use `static_assertions::assert_eq_size!` to catch hot-type size regressions

---

## **api-** — API Design (HIGH)

- **api-01** `builder-pattern` — Use Builder pattern for structs with more than 3 optional fields
- **api-02** `builder-must-use` — Add `#[must_use]` to builder types and their final build method
- **api-03** `newtype-safety` — Use newtypes to encode domain distinctions in the type system
- **api-04** `typestate` — Use typestate pattern for compile-time state machine enforcement
- **api-05** `sealed-trait` — Seal traits with a private supertrait to prevent external implementations
- **api-06** `extension-trait` — Use extension traits to add methods to foreign types
- **api-07** `parse-dont-validate` — Parse unvalidated input into validated types at system boundaries
- **api-08** `impl-into` — Accept `impl Into<T>` for ergonomic string and conversion inputs
- **api-09** `impl-asref` — Accept `impl AsRef<Path>` / `impl AsRef<str>` for borrowed inputs
- **api-10** `must-use` — Add `#[must_use]` to all `Result`-returning and pure functions
- **api-11** `non-exhaustive` — Use `#[non_exhaustive]` on public enums/structs to allow future fields
- **api-12** `from-not-into` — Implement `From<T>`, never `Into<T>` directly (auto-derived)
- **api-13** `default-impl` — Implement `Default` for all types with a sensible zero/empty state
- **api-14** `common-traits` — Eagerly derive `Debug`, `Clone`, `PartialEq`, `Eq`, `Hash` where valid
- **api-15** `serde-optional` — Gate `Serialize` / `Deserialize` behind a `serde` feature flag

---

## **async-** — Async/Await (HIGH)

- **async-01** `tokio-runtime` — Use Tokio as the production async runtime
- **async-02** `no-lock-await` — Never hold a `Mutex` or `RwLock` guard across an `.await` point
- **async-03** `spawn-blocking` — Use `tokio::task::spawn_blocking` for CPU-bound or blocking I/O work
- **async-04** `tokio-fs` — Use `tokio::fs` instead of `std::fs` in async contexts
- **async-05** `cancellation-token` — Use `CancellationToken` from `tokio-util` for graceful shutdown
- **async-06** `join-parallel` — Use `tokio::join!` to run independent futures in parallel
- **async-07** `try-join` — Use `tokio::try_join!` for parallel fallible futures; fails fast on first error
- **async-08** `select-racing` — Use `tokio::select!` for racing futures or implementing timeouts
- **async-09** `bounded-channel` — Use bounded channels (`mpsc`) to apply backpressure
- **async-10** `mpsc-queue` — Use `tokio::sync::mpsc` for work queues (multi-producer, single-consumer)
- **async-11** `broadcast-pubsub` — Use `tokio::sync::broadcast` for fan-out pub/sub patterns
- **async-12** `watch-latest` — Use `tokio::sync::watch` for sharing the latest value across tasks
- **async-13** `oneshot-response` — Use `tokio::sync::oneshot` for request/response patterns
- **async-14** `joinset-structured` — Use `JoinSet` for managing a dynamic set of spawned tasks
- **async-15** `clone-before-await` — Clone shared data before the `.await` point; release locks first

---

## **opt-** — Compiler Optimization (HIGH)

- **opt-01** `inline-small` — Use `#[inline]` on small, frequently-called functions in hot paths
- **opt-02** `inline-always-rare` — Use `#[inline(always)]` sparingly; only when profiling confirms benefit
- **opt-03** `inline-never-cold` — Use `#[inline(never)]` on cold/error paths to keep hot code compact
- **opt-04** `cold-unlikely` — Mark cold functions with `#[cold]` to guide branch prediction
- **opt-05** `lto-release` — Enable `lto = "fat"` in release profile for whole-program optimization
- **opt-06** `codegen-units` — Set `codegen-units = 1` in release profile for maximum optimization
- **opt-07** `pgo-profile` — Use Profile-Guided Optimization (PGO) for performance-critical binaries
- **opt-08** `target-cpu` — Set `RUSTFLAGS="-C target-cpu=native"` for local/bench builds
- **opt-09** `bounds-check` — Use iterators and `.get_unchecked()` (with SAFETY comment) to eliminate bounds checks
- **opt-10** `simd-portable` — Use `std::simd` (portable SIMD) for data-parallel operations
- **opt-11** `cache-friendly` — Design hot data structures with SoA (Struct of Arrays) layout for cache locality
- **opt-12** `panic-abort` — Set `panic = "abort"` in release to eliminate unwinding overhead

---

## **type-** — Type Safety (MEDIUM)

- **type-01** `newtype-ids` — Wrap primitive IDs in newtypes: `struct UserId(u64)`
- **type-02** `newtype-validated` — Use newtypes for validated data: `Email`, `NonEmptyString`
- **type-03** `enum-states` — Use enums for mutually exclusive states; exhaustive matching catches bugs
- **type-04** `option-nullable` — Use `Option<T>` for nullable values; never use sentinel values
- **type-05** `result-fallible` — Use `Result<T, E>` for every fallible operation, including constructors
- **type-06** `phantom-marker` — Use `PhantomData<T>` for type-level markers without runtime cost
- **type-07** `never-diverge` — Use `!` (never type) for functions that provably never return
- **type-08** `generic-bounds` — Add trait bounds only at usage sites; keep struct definitions bound-free
- **type-09** `no-stringly` — Never use `String` for structured data; use enums or newtypes
- **type-10** `repr-transparent` — Use `#[repr(transparent)]` for newtypes used in FFI

---

## **perf-** — Performance Patterns (MEDIUM)

- **perf-01** `iter-over-index` — Use iterators over manual index loops; eliminates bounds checks
- **perf-02** `iter-lazy` — Keep iterator chains lazy; call `.collect()` only when a collection is required
- **perf-03** `collect-once` — Never `.collect()` an intermediate iterator; chain adaptors instead
- **perf-04** `entry-api` — Use `HashMap::entry()` for insert-or-update; avoids double lookup
- **perf-05** `drain-reuse` — Use `.drain(..)` to move elements out while reusing the allocation
- **perf-06** `extend-batch` — Use `.extend()` for batch insertions; avoids repeated reallocations
- **perf-07** `chain-avoid` — Avoid `.chain()` in hot loops; prefer manual unrolling or `itertools`
- **perf-08** `collect-into` — Use `.collect_into(&mut vec)` (stable 1.82+) to reuse an existing allocation
- **perf-09** `black-box-bench` — Use `std::hint::black_box()` in benchmarks to prevent dead-code elimination
- **perf-10** `release-profile` — Always benchmark against `--release`; dev builds are not representative
- **perf-11** `profile-first` — Profile with `cargo flamegraph` or `perf` before optimizing any code

---

## **test-** — Testing (MEDIUM)

- **test-01** `cfg-test-module` — Use `#[cfg(test)] mod tests { }` for unit tests in the same file
- **test-02** `use-super` — Use `use super::*;` inside test modules to access private items
- **test-03** `integration-dir` — Put integration tests in the `tests/` directory, not inline
- **test-04** `descriptive-names` — Name tests as full sentences: `fn returns_error_on_empty_input()`
- **test-05** `arrange-act-assert` — Structure every test as Arrange / Act / Assert
- **test-06** `proptest-properties` — Use `proptest` for property-based testing of pure functions
- **test-07** `mockall-mocking` — Use `mockall` for auto-generating trait mocks
- **test-08** `mock-traits` — Design dependencies as traits to make them mockable
- **test-09** `fixture-raii` — Use RAII (`Drop`) for test fixture cleanup; never rely on test order
- **test-10** `tokio-async` — Use `#[tokio::test]` for async test functions
- **test-11** `should-panic` — Use `#[should_panic(expected = "...")]` for panic contract tests
- **test-12** `criterion-bench` — Use `criterion` for statistically rigorous micro-benchmarks
- **test-13** `doctest-examples` — Keep rustdoc `# Examples` blocks as executable doctests

---

## **doc-** — Documentation (MEDIUM)

- **doc-01** `all-public` — Document every public item with `///`; `#![warn(missing_docs)]` in CI
- **doc-02** `module-inner` — Use `//!` for module-level and crate-level documentation
- **doc-03** `examples-section` — Include `# Examples` with runnable code in every public function
- **doc-04** `errors-section` — Include `# Errors` listing all error variants for fallible functions
- **doc-05** `panics-section` — Include `# Panics` documenting all panic conditions
- **doc-06** `safety-section` — Include `# Safety` explaining required invariants for `unsafe fn`
- **doc-07** `question-mark` — Use `?` in doc examples, never `.unwrap()`
- **doc-08** `hidden-setup` — Use `# ` prefix to hide boilerplate setup in doc examples
- **doc-09** `intra-links` — Use intra-doc links `[Vec]`, `[std::io::Error]` instead of raw URLs
- **doc-10** `link-types` — Cross-link related types and functions in documentation
- **doc-11** `cargo-metadata` — Keep `description`, `repository`, `keywords`, `categories` in `Cargo.toml`

---

## **name-** — Naming Conventions (MEDIUM)

- **name-01** `types-camel` — `UpperCamelCase` for types, traits, enums, and enum variants
- **name-02** `funcs-snake` — `snake_case` for functions, methods, modules, and local variables
- **name-03** `consts-screaming` — `SCREAMING_SNAKE_CASE` for constants and statics
- **name-04** `lifetime-short` — Short single-letter lifetimes: `'a`, `'b`; named only for clarity: `'de`, `'src`
- **name-05** `type-param-single` — Single uppercase for type params: `T`, `E`, `K`, `V`, `I`
- **name-06** `as-free` — `as_` prefix: cheap reference conversion (`as_str`, `as_bytes`)
- **name-07** `to-expensive` — `to_` prefix: expensive owned conversion (`to_string`, `to_vec`)
- **name-08** `into-ownership` — `into_` prefix: consumes self and transfers ownership (`into_bytes`)
- **name-09** `no-get-prefix` — No `get_` prefix for simple field accessors; just use the field name
- **name-10** `is-has-bool` — Boolean methods: `is_`, `has_`, `can_` prefixes
- **name-11** `iter-convention` — Iterator methods: `iter()` / `iter_mut()` / `into_iter()`
- **name-12** `acronym-word` — Treat acronyms as words: `Uuid` not `UUID`, `HttpClient` not `HTTPClient`
- **name-13** `crate-no-rs` — Crate names must not have `-rs` suffix; redundant on crates.io

---

## **proj-** — Project Structure (LOW)

- **proj-01** `lib-main-split` — Keep `main.rs` minimal (arg parsing + run); logic lives in `lib.rs`
- **proj-02** `mod-by-feature` — Organize modules by feature, not by type (`auth/` not `models/`)
- **proj-03** `flat-small` — Keep small crates flat; resist premature module hierarchy
- **proj-04** `pub-crate-internal` — Use `pub(crate)` for APIs internal to the crate
- **proj-05** `pub-super-parent` — Use `pub(super)` for APIs internal to a parent module
- **proj-06** `pub-use-reexport` — Use `pub use` to flatten internal paths into a clean public API
- **proj-07** `prelude-module` — Create a `prelude` module for commonly imported items
- **proj-08** `bin-dir` — Put multiple binaries in `src/bin/`; keep each binary thin
- **proj-09** `workspace-large` — Use Cargo workspaces for multi-crate projects
- **proj-10** `workspace-deps` — Use `[workspace.dependencies]` inheritance to deduplicate versions
- **proj-11** `workspace-lints` — Define `[workspace.lints]` once; inherit in all member crates

---

## **lint-** — Clippy & Linting (LOW)

- **lint-01** `deny-correctness` — `#![deny(clippy::correctness)]` — must always pass
- **lint-02** `warn-suspicious` — `#![warn(clippy::suspicious)]` in all crates
- **lint-03** `warn-style` — `#![warn(clippy::style)]` in all crates
- **lint-04** `warn-complexity` — `#![warn(clippy::complexity)]` in all crates
- **lint-05** `warn-perf` — `#![warn(clippy::perf)]` in all crates
- **lint-06** `pedantic-selective` — Enable `clippy::pedantic` selectively for published libraries
- **lint-07** `missing-docs` — `#![warn(missing_docs)]` in library crates
- **lint-08** `unsafe-doc` — `#![warn(clippy::undocumented_unsafe_blocks)]` everywhere
- **lint-09** `cargo-metadata` — `#![warn(clippy::cargo)]` for published crates
- **lint-10** `rustfmt-check` — Run `cargo fmt --check` in CI; enforce on every PR
- **lint-11** `workspace-lints` — Configure all lints at `[workspace.lints]` level

---

## **anti-** — Anti-patterns (REF)

- **anti-01** `unwrap-abuse` — No `.unwrap()` in production; use `?`, `.unwrap_or()`, or proper error handling
- **anti-02** `expect-lazy` — No `.expect()` for recoverable errors; only for invariant violations
- **anti-03** `clone-excessive` — No `.clone()` when borrowing works; profile clone frequency in hot paths
- **anti-04** `lock-across-await` — Never hold a `Mutex`/`RwLock` guard across an `.await` point
- **anti-05** `string-for-str` — Never accept `&String` in function signatures; accept `&str`
- **anti-06** `vec-for-slice` — Never accept `&Vec<T>` in function signatures; accept `&[T]`
- **anti-07** `index-over-iter` — Never index manually when an iterator adapter exists
- **anti-08** `panic-expected` — Never `panic!` on expected/recoverable errors in library code
- **anti-09** `empty-catch` — Never silently discard errors with `let _ = result;`
- **anti-10** `over-abstraction` — Never add generic type parameters without a concrete use case
- **anti-11** `premature-optimize` — Never optimize without profiler evidence
- **anti-12** `type-erasure` — Never use `Box<dyn Trait>` when `impl Trait` in position works
- **anti-13** `format-hot-path` — Never call `format!()` in hot loops; pre-allocate or use `write!()`
- **anti-14** `collect-intermediate` — Never `.collect()` into a `Vec` only to immediately iterate it
- **anti-15** `stringly-typed` — Never use `String` or `&str` for structured data; use enums

---

## Recommended Cargo.toml profiles

```toml
[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1
panic = "abort"
strip = true

[profile.bench]
inherits = "release"
debug = true
strip = false

[profile.dev]
opt-level = 0
debug = true

[profile.dev.package."*"]
opt-level = 3
```
