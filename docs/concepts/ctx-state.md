# `ctx.state` 的读写规则

**结论：step 闭包里不要写 `ctx.state`。跨步骤传数据走 step 的返回值。**

这不是风格偏好，是从工作流的本质直接推出来的。

## 工作流的本质：对副作用的编排

一个 workflow 之所以需要「重放」这套机制，是因为它要满足两条基础要求：

1. **跳过已经执行过的副作用** —— 已完成的步骤不能重复做（不能重复扣款、重复发信）
2. **利用已经执行过的副作用的结果** —— 恢复后要能拿到当初那个结果

满足这两条的是**事件日志 + step 原语**，不是 `ctx.state`：

- 副作用的结果记在日志的 `STEP_FINISHED.result`
- replay 时查到记录就**直接返回**它，**不重跑**（上游 `replay-and-resume.md:31`：
  「Found → return the recorded result. **`fn` is NOT called.**」）
- 查不到才执行闭包，执行完把结果追加进日志

于是「跳过 + 复用」直接推出一个约束：

> **step 闭包唯一耐久的输出通道是它的返回值。**
> 因为它一辈子只真实执行一次（首次 drive），之后所有 replay 都只读日志。

闭包里如果还有别的副作用 —— 不在返回值里、日志里也没有的 —— **无处安放**。
`ctx.state` 正好是这种。

## 为什么 `ctx.state` 不是那条通道

两条**已证实**的事实（上游原话）：

| 事实 | 出处 |
| --- | --- |
| state 每次 resume 由 `initialize(input)` + 重放重建 | `types.ts:535-537` |
| `STATE_DELTA` 是 emit-only，**不进日志、不参与 replay** | `overview.md:61` |

### state 的写是自动发 `STATE_DELTA` 的（但攒到耐久边界才 flush）

**不需要手动发。** 每次写 state 都会自动产生 `STATE_DELTA` —— 机制是
`define/mod.rs:166-174` 的 `flush_state()`：

```rust
self.state.sync();                 // 句柄的 st → mirror
self.engine.emit_state_delta();    // mirror 与 prev snapshot 做 diff，非空才 publish
```

diff 的基准是 `prev_state_snapshot`（`engine/mod.rs:205-212`）—— 也就是**只发变化量**，
且已发过的部分不会重复发。

唯一的 nuance 是**时机**：写本身不同步发事件，而是攒着，等到耐久边界才 flush。
共 6 处：

| 边界 | 位置 |
| --- | --- |
| `step_with` | `define/mod.rs:362` |
| `wait_for_event` | `define/mod.rs:264` |
| `approve` | `define/mod.rs:203` |
| `sleep_until` | `define/mod.rs:246` |
| `yield_` | `define/mod.rs:336` |
| **drive 收尾**（尾段 delta） | `engine/run_workflow.rs:322` |

`engine/mod.rs:195` 的注释也写明：「调用点：耐久边界（`flush_state`）与 drive 收尾
（尾段 delta）」。

所以「写完不接任何原语直接 return」也能观测到 —— 靠收尾那次 flush。这也是上游
smoke 测试要额外加一个空 step 的原因：

```ts
ctx.state.counter = v
// A second step so the delta has a flush boundary after the mutation.
await ctx.step('noop', () => null)
```

### 「自动发 delta」与「写在闭包里会丢」不矛盾 —— 而且这正是它难发现的原因

两件事同时成立：

- **首次 drive：delta 正常发出**（反例实测拿到了 `/writtenInsideStep = 1`，因为写完
  紧跟 `waitForEvent` —— 那是个 flush 边界）
- **resume：闭包被短路 → 不执行 → 没有任何 delta**，终态是 0

也就是说**观测流上只有首次那一条，没有「修正」**。订阅方按 `STATE_DELTA` 渲染出来的
值，与 run 真正结束时的值不同，而且**永远不会被纠正**。这比单纯的「值错了」更难发现。

### TS 与 Rust 在这一步分岔：判别信号是「恢复期是否发 delta」

`flush_state` 第一步 `state.sync()` 只同步 **driver 那个句柄**的 `st`。克隆体的
`DerefMut` 改的是它自己的 `st` 字段，永远进不了 mirror → **连 delta 都不会发**。

| | 首次 drive 的 delta | 恢复期 delta | 终态 |
| --- | --- | --- | --- |
| **TS** 闭包写（按引用捕获） | **发**（值 1） | **无** | `0` |
| **Rust** 克隆写（值拷贝） | **不发** | **无** | `0` |

所以最锐利的判别信号不是终态值，而是：

> **恢复期有没有重新发出 `STATE_DELTA`。**
> 体内写会重发（`/writtenInsideStep = 1`）；闭包写一条都不发。

Rust 在首次就暴露（连 delta 都没有），TS 要到 resume 才发现（delta 看着正常）。


### state 究竟是干什么的？（推论，非上游表述）

**上游从未说明 `ctx.state` 的用途。** `docs/` 里 `ctx.state` 出现 **0 次**，
`guide/observability.md` 完全没提 state。唯一把它和用途挂钩的一句是生成文档
`reference/type-aliases/Operation.md:28`（讲的是 diff 机制本身）：

> Minimal JSON Patch (RFC 6902) helpers for workflow state **observability**.

以下是**推论**，不是上游原文：

- 上游 5 处 state schema（作者均为 maintainer）**无一例外**是 status / phase /
  counter，没有一处装副作用结果
- 唯一被写下来的规范用法（`engine.smoke.test.ts:42-43`）方向是
  **返回值 → state**：`const v = await ctx.step('compute', () => 42)` 之后
  `ctx.state.counter = v`

所以 state 更像是**给观察者（UI / 订阅者）看的进度投影**，而不是结果通道。
但这条推论**不参与**上面的规则 —— 规则只依赖上面那两条已证实的事实。
至于是「进度投影」还是「handler 草稿区」，上游没有表述，不影响结论。

## 两条机制的分工

一条机制把代码分成两类，两类的规则完全不同：

| | step 闭包 `ctx.step(id, fn)` | handler 体内 |
| --- | --- | --- |
| replay 时 | **被跳过**，返回日志里的结果 | **重跑** |
| 它的副作用 | 只能通过**返回值**输出 | 会被重新施加 |
| 因此要求 | 保持纯（无隐藏副作用） | 确定性（同原语、同顺序） |

所以：

1. **step 闭包应该保持纯的** —— 唯一耐久输出通道是返回值
2. **handler 体内可以写 `ctx.state`** —— 它会被重跑，副作用会重新施加

### 关于「纯」的一个必要精确化

step 闭包**不是**严格意义上的纯函数 —— 上游自己的例子就是
`ctx.step('flag', fetchFlag)`，它确实有外部副作用（发请求）。

区别在于**副作用有没有被记录进日志**：

- **被记录 → 允许。** `fetchFlag()` 的效果进了 `STEP_FINISHED.result`，所以它只真实
  执行一次，之后所有 replay 都返回那份记录。这正是 step 的设计目的。
- **未被记录 → 丢失。** 写 `ctx.state` 就是这种。

所以这里要求的纯性是「**无隐藏副作用**」—— 返回值 / 日志之外的任何效果都不该有 ——
而不是「不许有外部调用」。

上游没有用「pure / 纯」这个措辞，也没把这条规则写进 Authoring rules
（见文末「上游文档的缺口」），所以本文把它显式记录在此。

## 那 step 内部的进度呢？

常被接着问的一句：**step 跑 10 分钟，外面看得到跑到哪了吗？**

`ctx.state` 看不到，两个**独立**原因叠加：

1. **没有中途 flush 点。** `flush_state()` 只在 5 个耐久边界调用（`define/mod.rs`
   的 222 / 265 / 283 / 355 / 383 行），全部在 step **之外**。step 执行期间
   driver 手上没有可刷新的 mirror。
2. **就算有 flush 点也写不进去。** 闭包拿到的是 `ctx` 的引用（TS）或克隆的
   `StateHandle`（Rust），改不到 driver 那份 —— 就是本文前面讲的那件事。

所以「step 内进度」有个专门的第三条通道：`StepCtx::progress(f64)`
（`engine/mod.rs:60`），发 `WorkflowEvent::StepProgress`。它和上面两个都不同：

| | 即时 | 耐久 |
| --- | --- | --- |
| `StepCtx::progress` | ✅ | ❌ |
| step 内写 `ctx.state` | ❌ | ❌ |
| 把子阶段拆成独立 step | ✅ | ✅ |

**「即时 + 耐久」这个格子是空的，而且是故意空的。** 事件定义上写死了
`/// Observability only (not persisted)`（`event.rs:121`），`publish()` 只调
publisher 回调、从不 `append`。

为什么不耐久、为什么这是对的：假设 step 报了 50% 然后崩了。resume 时闭包**整个
被跳过**（上游 `replay-and-resume.md:31`），那 50% 从没进过日志，恢复后是 0%。
于是「step 内进度」和「step 内写 state」是**同一个错误**，只是写进了另一个载体 ——
都会在 resume 后静默回到 0%。把它做成耐久的，等于造出一个会骗人的半吊子事实。

结论：**实时观测用 `progress`，能拆的阶段拆成 step。** 但要注意这是**两件事，不
是替代关系**：

- 拆分解决的是**耐久** —— 哪些工作崩溃后能被跳过。
- `progress` 解决的是**观测** —— 一次 attempt 期间外面看到什么。

**不是所有长步骤都拆得动。** `ffmpeg -i in.mp4 out.mp4` 里没有能下刀的接缝；
外部 API 轮询、大模型推理同理。硬切只会造出假粒度（切点落在操作中间，切了也未必
能跳）。对这些步骤，「这次 attempt 跑到 62%」虽然不耐久，却是**当下唯一能给用户
看的真实信息** —— 这就是 `progress` 存在的意义。

反过来说：如果每个长步骤都有天然接缝、都拆成了独立 step，那 progress 确实多余。
它不是冗余设计，是**兜底拆不动的那一类**。

### 顺带：哪些事件落盘，哪些不落

查这个问题时容易误以为「Started/Finished 都记着」。实际只有 5 类事件 `append`
进日志（`engine/mod.rs` 里的 5 处 `inner.append`）：

| 落盘 | 仅 `publish`（emit-only） |
| --- | --- |
| `STEP_FINISHED` / `STEP_FAILED` / `STEP_PAUSED` | `RUN_STARTED` / `STEP_STARTED` |
| `NOW_RECORDED` / `UUID_RECORDED` | `STEP_PROGRESS` / `CUSTOM` / `STATE_DELTA` |

后果值得知道：**进程在 step 中途被杀，日志里查不到「当时在跑哪个 step」** ——
最后一条只是上一个 step 的 Finished。这不违反一致性（那个 step 重来即可），但想诊断
「上次崩在哪」不能只靠事件日志。

可执行验证：`examples/store_file/src/progress_report.rs` 及其测试
（`tests/progress_report.rs`，4 个测试钉住「publisher 收到 / 日志里没有」这条对比）。

本文引用分两类，路径前缀区分：

- **上游** `learn_ls/workflow/` —— TanStack Workflow 仓库
- **本仓** `aa-workflow/` —— 本仓库

## 上游原文依据

上游 `docs/concepts/replay-and-resume.md:31`（重放时 step 闭包不被调用）：

> 2. Found → return the recorded result (or rethrow the recorded error).
>    **`fn` is NOT called.**

上游 `docs/concepts/replay-and-resume.md:42`（对 handler 的要求）：

> The handler **must** reach the same primitives in the same order on every replay

上游 `docs/concepts/replay-and-resume.md:56`：

> State mutations re-run on replay. They're reapplied deterministically because
> they depend only on replayed step results.

上游 `docs/overview.md:33`：

> State is **derived** — reconstructed by replaying the log + re-running the
> handler. Never persisted directly.

上游 `docs/reference/interfaces/RunStore.md:16`：

> State the user mutates **inside the handler** is NOT persisted here; it's
> reconstructed from log replay.

注意最后一条的措辞是「**inside the handler**」—— 上游把「handler」和「step 回调」
当作两个不同位置，也正是上表那两类。

本仓的机制与之一致：state 每次 drive 都由 `initialize` 重建
（本仓 `packages/workflow_core/src/engine/run_workflow.rs:265`），日志里不含任何
state 记录（`STATE_DELTA` 是 emit-only，不落盘）。所以「靠重跑 handler 重放」是
两边共有的、也是唯一的 state 持久化途径。

## 上游在这里会静默产生不一致（已实测）

下面这组数字是在**上游仓库实跑验证**的（`workflow-core` vitest，
`inMemoryRunStore` + `simulateRestart`）。handler 读一次 `ctx.state.writtenInsideStep`，
而 s1 的闭包内做过 `ctx.state.writtenInsideStep += 1`：

```text
首次 drive：  after-s1:1     ← 闭包内的写「生效」
resume 后：   after-s1:0     ← 同一段代码、同一位置，读到 0
```

首次 drive 的 `STATE_DELTA` 还会上报 `{"path":"/writtenInsideStep","value":1}`，
而最终 `RUN_FINISHED.output` 里是 `after-s1:0` —— **观测流和终态自相矛盾**。

这是正确性缺陷，不是风格问题：同一段 handler 在「首次」与「恢复后」产出的结果
不同，取决于该 run 是新建的还是 resume 的。上游没有测试覆盖这个组合。

原因在语言层面：上游 `BaseCtx.state`（`packages/workflow-core/src/types.ts:328`）
是普通属性，JS 闭包**按引用捕获** `ctx`，所以闭包内的写改的就是那一个对象。

### 复现件（可跑，8 个测试全绿）

`learn_ls/wf-demo/packages/workflow-shared/tests/state-in-step-closure.test.ts`
—— 用**真实上游源码**（相对路径 import，与该仓其它文件一致）跑三组，每组只改一个
变量：那次 state 写发生在哪里。

```sh
cd learn_ls/wf-demo && bun test packages/workflow-shared/tests
```

实测事件序列（反例组，`inMemoryRunStore` + signal 恢复）：

```text
首次 drive：
  STEP_FINISHED  s1
  STATE_DELTA    {"op":"replace","path":"/writtenInsideStep","value":1}
  SIGNAL_AWAITED go                    ← 挂起（只有读到 1 才会走到这行）

resume drive：
  SIGNAL_RESOLVED go
  RUN_STARTED
  RUN_FINISHED   output {"afterS1":0}  ← 同一段代码读到 0
                 （没有任何 STATE_DELTA）
```

对照组的差异极干净 —— **「恢复期是否重新上报 STATE_DELTA」就是分水岭**：

| 组 | 写在哪 | 恢复期 STATE_DELTA | 终态 |
| --- | --- | --- | --- |
| 反例 | step 闭包内 | **无** | `afterS1: 0` |
| 对照 A | handler 体内 | `/writtenInsideStep = 1` | `afterS1: 1` |
| 对照 B | 走 step 返回值 | `/seen = 7` | `{fromReturn: 7, fromState: 7}` |

另外两点实测结论：

- **失效是静默的**：反例组两次 drive 都不产生 `RUN_ERRORED`，恢复后照常
  `RUN_FINISHED`。
- **重放的事件不会被再次 yield**：恢复期 generator 不吐 `STEP_STARTED` /
  `STEP_FINISHED`（那是日志里已有的记录）。要看完整历史得用 `attach: true`。

## 上游状态（2026-09-30 查证，影响「要不要提 issue」）

| 项                        | 值                                                |
| ------------------------- | ------------------------------------------------- |
| 仓库创建                  | 2026-05-20（4.3 个月）                            |
| `@tanstack/workflow-core` | 0.0.4                                             |
| main 上最后一次代码提交   | 2026-07-21（2.3 个月前）                          |
| open PR                   | 6 个（**PR 未被禁用**）                           |
| 已合并 PR                 | 10 个，**全部**是 maintainer tannerlinsley 本人的 |
| 外部贡献的 PR             | #17（docs，2026-08-03 起）未合并                  |
| open issue                | 2 个（#12 是 maintainer 自己的 RFC）              |
| Discussions               | 启用，2026-09 有 3 个新主题                       |

需要注意的两点：

1. **PR #19**（tannerlinsley，2026-09-23，+3685/-206、84 files）尚未合并，新增
   `ctx.compensate`、durable retry、signal inbox、lease fencing。**但它没有改动
   `replay-and-resume.md` 与 `overview.md`**，且 `ctx.compensate` 的 handler 同样
   是 checkpointed 的 —— 所以本文的规则**自动扩展**到该新原语，不受影响。
2. **方向未定**：#12（first-class XState）里 maintainer davidkpiano 回「We might
   want to retarget this for XState v6 alpha」。`CONTRIBUTING.md` 要求外部开发必须
   是「已被 maintainer 讨论并同意」的问题。

结论：本条属于**本地已闭环**（规则已文档化 + 有测试钉住），不依赖上游修。上游侧
建议走 Discussions（`CONTRIBUTING.md` 明确「问题 → Discussions」，且仓库唯一的
issue 模板只有 `bug_report.yml`），先以「复现 + 提问」形式征询，而不是直接开
issue 或 PR。

## 本仓更早暴露这个错误（但理由不光彩）

|                                | 闭包捕获语义         | 单次 drive 内           | resume 后                 |
| ------------------------------ | -------------------- | ----------------------- | ------------------------- |
| 上游 TS                        | 按**引用**捕获 `ctx` | 写**看得见** → 误以为对 | 静默丢失 → **结果不一致** |
| 本仓 Rust `StateHandle::clone` | **值拷贝** `st`      | 写**看不见** → 立刻发现 | 静默丢失                  |

本仓 `packages/workflow_core/src/define/state_handle.rs:37` 的模块文档写明了
`Clone` 的语义：

> **`Clone` = 快照**（复制 `TState`，共享 mirror），不是 live 共享

具体地（`state_handle.rs:66`）：`clone()` 复制 `st: TState`（`:56`），只共享
`mirror`。克隆体有自己的 `st` 字段，`DerefMut`（`:131`）改的是那一份；driver 原句柄的
`Deref`（`:125`）读自己的 `st` —— 从 clone 那一刻起就分家了。

所以本仓的 step 闭包要碰 state **必须**先 `ctx.state.clone()`，而那个副本的写回不来。
这属于「用户要越过重重障碍才能犯这个错」——比上游好，但**不能当作规则的理由**：
断链在并行下是刻意的（`state_handle.rs:39`：「并行 step 无法竞写 state」），
而那与串行场景无关。正确理由是上面那条纯性要求。

本仓同样是**静默**的：step 不失败，run 照样 `Finished`，且不产生任何
`STATE_DELTA` 观测。

## 串行：体内写 vs 闭包内写

可运行示例：`examples/store_file/src/step_write.rs`（三个 step 逐个摊开，
每个 step 对三种机制各做一次读写）。

| 机制          | 写法                            | 链               | 判定                      |
| ------------- | ------------------------------- | ---------------- | ------------------------- |
| `via_body`    | handler **体内**写 `ctx.state`  | **通** `[0,1,2]` | ✅ 会被重跑，**正解**     |
| `via_arc`     | 闭包内写 `Arc<Mutex<u32>>`      | **通** `[0,1,2]` | ⚠️ 链通但不是 `ctx.state` |
| `via_closure` | 闭包内写 `StateHandle::clone()` | **断** `[0,0,0]` | ❌ 隐藏副作用，破坏纯性   |

实测输出：

```json
{
  "bodyReads": [0, 1, 2],
  "finalBody": 3,
  "arcReads": [0, 1, 2],
  "arcAfter": [1, 2, 3],
  "closureSawClosure": [0, 0, 0],
  "driverAfterClosure": [0, 0, 0],
  "finalClosure": 0
}
```

`via_arc` 那条链是通的（`Arc` 就是 Rust 的引用语义，对应上游的 `ctx` 捕获），
但它**不是 `ctx.state`**：没有 `STATE_DELTA`、不过 state schema 校验、且 resume 时
闭包被短路则不重演。只适合步骤间的活体中间量，不能当持久状态。

## 并行：写必须落在汇合点

纯性要求在并行下**多出一条**：两次写之间必须有词法确定的先后，
否则**同一份日志重放两次可能得到不同的 state**，replay 地基直接塌掉。

机械准则：

> **`ctx.state.x = ...` 必须出现在「执行顺序由词法决定」的位置。**
> 写并行编排时只需自问：**这个赋值在 `try_join!` 的哪一侧？**
>
> - 在 join **之内**（分支闭包内部）→ **违规**，改成把值 `return` 出去
> - 在 join **之后**（汇合点）→ **安全**

**为什么不是「加锁就好」**：锁能消除竞写，但消除不了不确定性——谁先谁后变成
「谁先拿到锁」，仍依赖调度。而 `ctx.state` 没有任何持久化形态（`STATE_DELTA`
emit-only、不进日志），所以连「事后按日志合并」这条退路都没有。

两边的语言差异让症状不同，但都不安全：

- **上游 TS**：`ctx.state` 是同一个对象引用，并行写**会互相看见** → 真竞写，
  last-write-wins、非确定
- **本仓 Rust**：`StateHandle::clone` 是快照分裂（`state_handle.rs:37-40`），
  并行写**不会被对方看见** → 静默丢弃

并行有两种正确需求：

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

反例（正例是只读）：本仓 `dub_sf_ocr`（`examples/store_file/src/dub_sf_ocr.rs`）
两个并行分支的 step **只读** state，一处都不写。

## 正确写法

step 只负责**算**和**返回**，handler 用返回值写 state：

```rust
// ❌ 违反规则：闭包内写 state
let inner = ctx.state.clone();
ctx.step("a", move |_| {
    let mut st = inner;
    async move {
        st.via_closure += 1;    // 静默丢失
        Ok(serde_json::json!({}))
    }
})
.await?;

// ✅ step 闭包不碰 state
let v = ctx.step("a", move |_| async move { compute().await }).await?;
ctx.state.via_body = v;        // 体内写，会被重跑 → 副作用重新施加
```

数据流走 **step 返回值 → 局部变量 → 下个 step 的入参**，不走 state。
如果想在 state 里留痕（`phase`、`draft` 之类），写就完了 —— `STATE_DELTA` 会自动发，
只是攒到下一个耐久边界才 flush（见前文「state 的写是自动发 `STATE_DELTA` 的」）。

上游的 smoke 测试额外加了一个空 step 造边界（`engine.smoke.test.ts:44-46`）——
若删掉那个 `noop` step，尾段 flush 仍能发出 delta，但上游选择显式制造边界。

## 可执行验证

`examples/store_file/tests/step_write.rs`（7 个测试）：

| 测试                                                            | 钉住什么                                    |
| --------------------------------------------------------------- | ------------------------------------------- |
| `body_writes_chain_across_the_three_steps`                      | 体内写的链是通的 `[0,1,2]`                  |
| `arc_shares_across_the_three_steps`                             | `Arc` 共享的链是通的                        |
| `closure_writes_do_not_chain_across_the_three_steps`            | 闭包内的写断链 `[0,0,0]`                    |
| `closure_reads_the_latest_body_value_so_only_the_write_is_lost` | 排除「闭包看到过期快照」的误读              |
| `step_return_values_reach_the_log_for_all_three_steps`          | 返回值进了 `StepFinished.result`（durable） |
| `steps_still_succeed_despite_every_closure_write_being_dropped` | 失效是**静默**的：run 照样 `Finished`       |
| `initialize_seeded_base_is_untouched`                           | 不是「覆盖」，是「没发生」                  |

```sh
cargo test -p example_store_file --test step_write
```

resume 与并行侧在 `examples/shared/src/workflows.rs`：

| 测试                                              | 钉住什么                                           |
| ------------------------------------------------- | -------------------------------------------------- |
| `state_step_closure_mutation_lost_on_resume`      | 闭包被短路 → resume 后写丢失                       |
| `state_parallel_steps_snapshot_then_driver_flush` | 并行分支各持快照互不可见，driver 只 flush 原始句柄 |

## 上游文档的缺口

上游 `docs/overview.md:52` 的 **Authoring rules** 列了 4 条：

1. Side effects go inside `ctx.step(id, fn)`.
2. Use `ctx.now()` / `ctx.uuid()`.
3. Step IDs must be unique per call site.
4. Helpers take `ctx` and call primitives through it.

**没有一条讲 state 的写入位置。** 上游 `docs/concepts/primitives.md:6` 说
「Each primitive has one recipe and one footgun」，而 `ctx.step` 的那个 footgun
是 id 重复（`:50`），不是 state。

第 1 条的措辞还有反向误导：上游语境里 side effect 指**外部调用**
（`replay-and-resume.md` 的 determinism contract 举的例子是 `fetchFlag()` /
`fetch()`），**不含 state 变更**，但这条规则没做区分。把「改 state」算作 side
effect，字面上就会推出「state 写应该放进 step」—— 加上上游的引用语义让这个错误
写法真的看起来能工作，两者叠加就很容易踩进去。

建议上游补一条：

```text
- State mutations go in the handler body, not inside `ctx.step(id, fn)`.
  Step fns are not re-executed on replay, so writes there are lost — silently,
  and they appear to work within a single drive.
```

## 相关

- 本页是 `ctx.state` 写入规则的**权威处**，覆盖串行与并行两侧。
- `examples/store_file/src/step_write.rs` —— 串行三机制的可运行对照
- `examples/store_file/src/dub_sf_ocr.rs` —— 并行分支的 `ctx` 共享（state 只读，正例）
- `docs/tanstack-alignment.md` —— 与上游的采纳 / 分歧总账
- `docs/runtime-design.md` —— runtime 层决策记录（D1–D9）
