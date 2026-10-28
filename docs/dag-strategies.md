# dag-strategies：上游往哪走（前瞻雷达）

> 三件套之一：
> `core-runtime-map.md`（是什么）· `tanstack-alignment.md`（两边差什么）· **本页**（上游往哪走）。
> 静态理解、差异对照、前瞻雷达各司其职。本页回答一个唯一的问题：**上游 TanStack Workflow
> 接下来大概会往哪儿 evolve，我仓应对的动议是什么**。

## 0. 读本页前先立的规矩

1. **只认一手证据，不认印象**。每条动议必须能回指到 upstream 仓里可运行的 commit、
   changeset、internal note、release tag 或测试。官方博客里的话术、博文里
   的话术不算——upstream 的 `research/RESEARCH.md` 里有大量 Q4-2026 时间线推测，
   **本页 deliberately 不转载那些推测**，只转载代码与 tag 记录的真实事实。
2. **证据链用本地 mirror**。`../learn_ls/workflow/` 是 tracked mirror base
   `0971b4c`（2026-10），doc 的锚点一律写 `path:line`，不写 SHA behind path。
   上游一 diff 就会失效——那是设计，配合本页 §8 的操作手册。
3. **动议必须落进 5 个动词之一**：`已追上` / `已落地对齐` / `跟进中` / `明确不做` /
   `暂无动作`。没有动议的观察不算条目——把它塞进 §8 关键词表，它只算「灯塔」。
4. **cadence = 两周**，与 `core-runtime-map.md` 对齐；每次上游 diff 直接按 §8 重跑，
   不要攒满 cases 一次做。

---

## 1. 上游已发布轨迹（2026-05 → 2026-10，commit / tag 双锚点）

证据源：`learn_ls/workflow` git log，release tag。

| 时间 | Commit | 发布 tag（若有） | 主要内容 | 本仓状态 |
| --- | --- | --- | --- | --- |
| 2026-05-20 | `57182df` | — | 规划期调研归档到 `research/` | 无 |
| 2026-05-24 | `721d546` / `76bd7b1` | — | 调度 landscape（SCHEDULING.md）+ ai-orchestration 集成 memo | 引用（调度层设施） |
| 2026-05-25 | `e677ca1` | `@tanstack/workflow-core@0.0.1` | 初始 release | 本仓以 mirror 开始对齐 |
| 2026-06 | `5d05fa8` | — | runtime 适配器 + 文档 | 本仓 runtime 适配约定：store adapter trait（见 packages/workflow_runtime/src/run_store_adapter.rs） |
| 2026-06 | `ba9cc31` | `0.0.3-0.0.4` | Cloudflare / Railway / Netlify / Vercel host 适配器 | `明确不做`（host adapter 不属 core；见 §7） |
| 2026-06 | `0b088a5` | — | package-owned store schema | `已落地对齐`（SQL migration 形态） |
| 2026-06 | `9a6f05d` | — | future-proof schema primitives | `已落地对齐` |
| 2026-06 | `be534b3` | — | due schedule bucket claiming 修正 | `已落地对齐`（`schedule_materializer`） |
| 2026-06 | `87340c8` | `#13` | **runtime deadlines + observability** | `跟进中`（deadline 投影一致；observability OTel 本仓 **未做**） |
| 2026-06 | `fa32903` | mergesets | changeset release 分支管理 | 无 |
| 2026-09 | `b928717` | `#15` | **harden runtime lease recovery + event publishing** | **mirror base（旧）** |
| 2026-09 | `0971b4c` | — | **add durable signals, retries, compensation** | **mirror base（当前）** |

Tag 矩阵：`workflow-core@0.0.4` / `workflow-runtime@0.0.4` / `store-drizzle-postgres@0.0.5` /
`store-cloudflare-d1@0.0.4` / netlify & vercel & cloudflare & railway `@0.0.3/0.0.4`（2026-10）。

**强信号 #1**：每次 upstream 一周内做**两件事**——(a) durable primitive 扩展
（retries / compensation / signal buffering / uuid / now）(b) store contract 对齐
（lease / timer / schedule / migration）。我们仓是对齐 (b) 的 adapter 之一，(a) 已在
engine 侧镜像。

**强信号 #2**：upstream 没有发布基于 DSL/显式 DAG 的所有者——release notes 和 docs
`comparison.md` 里把 TanStack Workflow 与 Temporal/Inngest/Trigger.dev 并列，但
只展示 code-as-DAG（handler 重放），不展示 scheduler。**这不是暂未发表的功能，
是底座决定**：replay = code-as-DAG + primitives，与 §4 的 `明确不做` 一致。

### 1.1 上游两个 planning 文档里直接谈 strategy

- `docs/internal/adapter-roadmap.md`（一手）：adapter 矩阵以 Postgres 为 reference，
  D1 为 pressure test；"prove the shape first" 优于 adapter count；migration 包所有；
  `ensureSchema()` 只用于测试/引导——这个体例我们已经 mirrored（
  `packages/workflow_store_sqlx_postgres/SCHEMA_MIGRATIONS.md` + migration 目录）。
- `docs/internal/f2-lite-feature-points.md`（一手）：继续 / 失效 / 目标一族的
  三个方向。我们直接 source 这份文档落地了 F2/F3/F5；F1（invalidate_steps）与
  F6（INVALIDATED）是我们自己的登记结论（`docs/feature-requirements.md` F1/F6）。

---

## 2. 适配器矩阵与 migrations 形态

证据：commit `0b088a5`（package-owned schemas）+ `ba9cc31`（host adapters）+
`docs/internal/adapter-roadmap.md`。

| upstream 形态 | upstream 证据 | 本仓对应 | 动议 |
| --- | --- | --- | --- |
| `@tanstack/workflow-store-drizzle-postgres` reference adapter | adapter-roadmap #1 | `workflow_store_sqlx_postgres`（publish-ready-ish） | `已落地对齐`（review v0.0.5 notes） |
| package-owned SQL migration | `0001_signal_inbox.sql`（our mirror）+ `SCHEMA_MIGRATIONS.md` | `packages/workflow_store_sqlx_postgres/migrations/0000..0001` | `已落地对齐` |
| Host wakeups 是 adapter 职责，不是 core | deployment guide + adapter roadmap | core 无 persistence 词；host 自接 | `明确不做`（core 词汇不出现 persist/lease/deadline） |
| D1 pressure test（SQLite 语义） | `@tanstack/workflow-store-d1@0.0.4` | 无 Rust 版 SQLite adapter | `暂无动作` |
| Cloudflare Queue wakeup | roadmap "Future Adapter Order #4"（**未发布**） | 无 | `暂无动作`（ roadmap 未来，watch 关键字 `Queues`） |
| Redis/LibSQL/MySQL/DynamoDB | roadmap future | 无 | `暂无动作` |
| Adapter authoring doc | roadmap says "write once Postgres+D1 settled"（**未发布**） | 我们已写 `AGENTS.md` 适配 checklist | `跟进中`（把 upstream 的 checklist 数字（有/没有）与 ours 逐条对照） |

**信号弱/中期**：host adapters（`workflow-cloudflare/netlify/vercel/railway`）是 TS-only
——本仓是 core library，不会 bundled host；如果 upstream 未来把 host-wakeup
schema（cron tick-shaped API） baked into core，那会破坏 core host-agnostic 的 invariant
（见 §7 关键字）。

---

## 3. 信号 / 收件 / timer（durability 信号强）

证据：changeset `durable-signals-retries-compensation.md` + commits `0971b4c` +
`9a6f05d` + `0b088a5`。

 upstream 已发布 capabilities（verified in live mirror）：

- **Durable retires**：`retry: { durable, maxAttempts, backoff, baseMs, shouldRetry }` →
  `STEP_ATTEMPT_STARTED` + `STEP_RETRY_SCHEDULED` events；`StepAttempt` record；
  deadline-budget cooperation via `ctx.runtime.yield`。
- **Compensation**：`ctx.compensate(id, fn)` + reverse-order undo + per-handler retry；
  events `COMPENSATION_REGISTERED / STARTED / COMPLETED / FAILED`。
- **Signal inbox**：`bufferSignals`（store side, opt-in）+ `0001_signal_inbox.sql`
  migration + idempotent signalId + `completeTimer` ack only after durable resolution。
- **Timer ack**：宿主只在 durable resolution 完成后 ack — 即使 `SIGNAL_RESOLVED` 已
  append to log。
- **UUID / NOW determinism**：`ctx.now` / `ctx.uuid` as recorded primitives（
  `NOW_RECORDED` / `UUID_RECORDED` checkpoint）。

本仓状态：**已落地对齐** — `F4/F5` delivered；store adapter 已有
`0001_signal_inbox.sql`；rust side names follow convention
（`CompensationCompleted/Failed` 是本仓先行扩展）。

动议 watch item：**upstream 的 `STEP_ATTEMPT_STARTED` / `STEP_RETRY_SCHEDULED`
是 TS vocabulary**——我们 mirror 的 rust events follow naming; if upstream adds a
newest event kind or changes field casing, `docs/tanstack-alignment.md` alias table
must refresh.

---

## 4. 原语缺口盘（upstream 已有哪些、我们还差什么）

Evidence: upstream live doc `primitives.md`; 本仓 `ctx-state.md` + `define/define_workflow.rs`。

### 4.1 Upstream 原语矩阵（live doc `primitives.md` 为底）

| 原语 | upstream `ctx.*` | 本仓 | 状态 |
| --- | --- | --- | --- |
| Step | `ctx.step(id, fn, { retry / timeout / meta })` | `ctx.step` + `RetryOptions` | `已落地对齐` |
| Sleep | `ctx.sleep(ms)/sleepUntil(ts)` → `SIGNAL_AWAITED{name:'__timer'}` | `sleep` / `sleep_until` → `SignalAwaited{name:'__timer', deadline}` | `已落地对齐` |
| Wait for Event | `ctx.waitForEvent(name, { id, schema, meta, deadline })` | `wait_for_event` | `已落地对齐` |
| Approve | `ctx.approve({ id, title, description })` | `approve` | `已落地对齐` |
| Now / UUID | `ctx.now / ctx.uuid`（recorded） | `now` / `uuid` | `已落地对齐` |
| Emit | `ctx.emit(name, value)` — observability-only | `ctx.emit` | `已落地对齐` |
| Succeed / Fail | tagged returns | `succeed` / `fail` | `已落地对齐` |
| Compensate | `ctx.compensate(id, fn)` 逆序 | `compensate` | `已落地对齐` |
| Runtime yield | `ctx.runtime.yield({ reason })` + `timeRemaining()` | `__timer` park（F2-lite，见 `engine/mod.rs`） | `已落地对齐` |
| AbortSignal | `ctx.signal` | `ctx.signal` | `已落地对齐` |

**关键弱信号**：upstream 的 primitives 文档中没有 `ctx.parallel` / `ctx.race` / `ctx.all` 作为一等原语。
并行由用户手写：

```
const [a, b] = await Promise.all([branchA(), branchB()])
```

本仓对应 `tokio::try_join!`（AGENTS.md `ctx-state.md` 有完整论证）。语义同构、形式不同
（JS 共享引用 vs Rust 快照）。

### 4.2 本仓先行扩展（upstream 未做）

| 扩展 | 证据 | 上游可能跟进？ | 对冲 |
| --- | --- | --- | --- |
| `STEP_PROGRESS` | Rust 源 + `tanstack-alignment` | 低（upstream 有 `ctx.emit` observability-only） | rename 时 rename |
| `COMPENSATION_COMPLETED / FAILED` | Rust 源 | 中 | 保留 + doc peer table 注明 |
| `StepStatus` + `StepStatus::Paused` | Rust `event.rs:27-34` | 无（TS 无 StepStatus） | 明确 divergence |
| `StepState` + `fold_step_states` | Rust runtime | 无 | 明确 divergence |
| `buffer_signals` 默认关 | `feature-requirements` F4 | ON/OFF 对齐（opted-in 默认） | 已对齐 |
| `support_signal_inbox()` trait method | `run_store_adapter.rs` | 可选 trait method | 保持兼容 |

---

## 5. 「继续 / 失效 / 目标」一族的 upstream 信号

证据：upstream `f2-lite-feature-points.md`、`721d546` cron landscape、
`0de8e83 ai-orchestration` memo、`feature-requirements.md` F1-F6。

| 能力 | upstream 0971b4c 位置 | 本仓 |
| --- | --- | --- |
| interim goals (`interimGoals`, per-drive, 不落盘) | research memo 指向；core-side equivalent | F3 已实现 |
| redo：`reseedRun` 开新 run + `redoSet` | `engine/run-workflow.ts` reseedRun path | F2 已实现 |
| 同 run resume + 失效名单 (`invalidateSteps`) | **未发布**（0971b4c 无；f2-lite 内部 note 提议） | F1 设计中，未实现 |
| 耐久 INVALIDATED 事件 | **明确不做**（upstream + F6） | F6 已评估：不做 |
| signal 收件箱 (`bufferSignals`) | 0971b4c ships (`0001_signal_inbox.sql`) | F4 已实现 |
| 耐久 retry (`retry.durable`) | 0971b4c ships | F5 已实现 |
| compensation | 0971b4c ships | F5 已实现 |
| `deadline` / `minYieldRemainingMs` | `87340c8` runtime deadlines (#13) | 部分——rust 已 `waiting_for.deadline` 投影 |

**强信号**：upstream 自己把 "继续 = 同 run resume" 当成第一优先
(f2-lite note §1：官方 ai-orchestration 用法全部 continuation 走同一 run_id；
新 run 仅服务 `reseedRun` 那类需求)。因此 **F1 不会落进分歧清单**——跟进上游
自然语义风险低。

动议：

- F1 落地顺序：f2-lite 先加 `invalidateSteps` + 测试 → Rust mirror（per
  `feature-requirements.md` F1）。注意 `goals_done_at_drive_start` 必须在过滤 lives
  **之后**算（Rust 目前 `run_workflow.rs:902` 在前；落地时记得 reorder）。
- `__interim-goal` 与 upstream `interimGoals / interim-goals` 命名 divergent →
  committing 时在 `tanstack-alignment.md` 加一条小分歧条目。

---

## 6. Schedule / Sweep / Lease（host 做事哪里 evolve）

证据：`87340c8` #13 + `b928717` #15 + adapter-roadmap + `scheduling.md`。

已 shipped upstream 形态：

- `runtime.sweep({ maxScheduledRuns, maxTimers, maxDurationMs, includeEvents })`
  bounded — hard ceiling，host 只 wake-up tick。
- lease：atomic claim + heartbeat (lease/3 interval) + release-on-drive-end +
  claim stale。
- timer ack 只在 durable resolution 完成之后。

本仓对齐：`schedule_materializer` + `runtime_driver::driveClaimedRun` + sqlx
adapter 已完整（stale claim contract 测试已 run）。

**keyword watch**：`bounded sweep` / `claim-stale` / `maxDurationMs` / timer ack
latency。本仓 sweep peer 命名与 upstream 对齐（`WorkflowRuntimeSweepArgs`）；如 upstream
改 sweep args 形状，Rust `runtime_driver.rs` 涉及。

---

## 7. Host adapter 与 core 词汇边界

证据：`ba9cc31` + adapter-roadmap + `AGENTS.md` 「core 只区分哪些要持久化」。

- upstream 发布 TS-only host adapters：
  `@tanstack/workflow-cloudflare`、`-railway`、`-netlify`、`-vercel`（plus
  drizzle-postgres / D1 stores）。
- 本仓是 core，不 publish host；host 自接（AGENTS 明文："服务期宿主自接"）。
- 不做 host：core 函数签名里不出现 persist / lease / deadline vocab（`core-runtime-map.md`
  §4 #10 invariant）。
- 本仓 deliverable 并不是 host wrapper，而是 adapter checklist（ AGENTS §配对纪律）。

动议：`暂无动作`。但 watch：若 upstream 把 host-wakeup schema baked into
`workflow-core`（而不是 runtime adapter 里），core/vocabulary purity 的 invariant 将
受威胁——触发 `tanstack-alignment.md` 重新审视。

---

## 8. 雷达操作手册（本页真正价值所在）

### 8.1 trigger

```bash
cd /home/aa/repos/lib_ls/learn_ls/workflow
git log --oneline 0971b4c..HEAD         # 非空 => 上游动了，开始新一轮
```

### 8.2 一整套步骤（mirror base `b928717` 迁 `0971b4c` 已做过一遍，下次照旧）

1. 拉上游（git pull upstream / merge），记新 base SHA 与日期。
2. 全量 diff 一遍 internal docs（`docs/internal/`）+ changesets（`.changeset/`）+
   `docs/concepts/primitives.md` + `docs/api/store-adapters.md`。
3. 逐 commit subject 分类：`feat`（新原语）/ `fix`（bug，可能影响 parity）/
   `chore(migration)`（schema）/ `perf` / `docs`。把 `feat` 和 migration 提列表。
4. 核 `core-runtime-map.md` 所有 `path:line` 锚点（本仓有 §4 复核脚本）；死锚点=上游 diff。
5. `grep -rn "StepPauses\|StepResume\|..."`（按 invariant 项目自定义的 vocabulary watch list）
   ——新词汇意味新概念。
6. 对照 `feature-requirements.md` F1-F6，看 upstream 是否标 F1 `invalidateSteps`
   或新事件 `INVALIDATED` 是否出现——出现就重新评估 F6。
7. 重跑本仓测试（Postgres + mysql/sqlite adapter if shipped）。
8. 更新本页 §1 track table + §8 commit 日期； commit 时 message 以
   "docs(dag-strategies): 雷达重跑，base SHA X"。

### 8.3 关键字 watch list

- **概念桶**：`INVALIDATED` / `invalidate` / `parallel` / `race` / `all(` / `Progress`
- **contract bucket**：`migration` / `lease` / `timer` / `schedule` / `backpressure` /
  `claim-stale` / `signal-inbox` / `host-wakeup` /
  `sweep`
- **vocab bucket**：任何 `workflow-core` public interface 上的 renamed primitive /
  new event type（diff `types.ts` + `WorkflowEvent` union）

### 8.4  verdict ladder（决定动议用）

- **Runtime-level capability shipped**（带拍板的 VERSION bump + changeset + docs +
  tests green） → 审视是否 `已落地对齐`；如需要 implementation plan 加入 feature-requirements.md。
- **Internal note 提议** but 未发布 → `跟进中`，记录 P1 prerequisite；
  不立即 mirror。
- **Published as `0.0.x-y` adapter** → `已落地对齐` 或 `暂无动作` 视 adapter
  identity 定；本仓不 ship host adapter。
- **明确拒绝**（upstream F6-style memo 或 TanStack official position）
  → `明确不做`，并把理由转到来自家写 incidental decision note.
- **强信号 bug fix** in lease/sweep/timer → 本仓 E2E 重跑（Postgres contract suite） +
  逐条 apply to `sqlx_postgres` adapter.

### 8.5 触发 core-runtime-map.md 更新的 rubric

| 本页雷达事实 | 影响 core-runtime-map |
| --- | --- |
| 新 commit 在 `types.ts` 的 `WorkflowEvent` 联合增减事件 | §2 三分类表 + `CheckpointEvent` 清单 |
| 新 primitive 落地 | §1 file map（ engine/run-workflow.ts 行数）+ §3 驱动模式 |
| runtime contract 方法 signatures 变 | §1 runtime types.ts 行数 + 是否增加 helper trait method |
| upstream 改 base 的 commit 日期 | § header mirror base SHA |

### 8.6 触发 tanstack-alignment.md 更新的 rubric

- **分歧符号 lists** (`§完整分歧清单`) must be re-counted when events/typed change.
- **event-to-event mapping** renames on upstream side changes.
- **storage contraction** primitives changes (new collect fields).

---

## 9. 复核记录

**2026-10-26**（本页 debut）：与 `core-runtime-map.md` 7639ed5 和 `feature-requirements.md`
F-state 一致对齐。Upstream intel 源为 mirror on commit `0971b4c` 显式 tag（最高信心 level）；
internal doc excerpts from `adapter-roadmap.md` & `f2-lite-feature-points.md`（ authored
upstream, 是 docs internal planning, 仍 treated as source）；research memo forecast
deliberately excluded per §0.1。

下一轮 upstream re-pull 后按 §8.2 重跑。
