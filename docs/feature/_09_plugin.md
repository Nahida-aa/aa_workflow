# 插件（plugin）与钩子：上游走向预测与本仓定位

> 日期 2026-11-02。基线：`learn_ls/workflow` `origin/main` = `b928717`（2026-07-21）、
> `taren/durable-recovery` = `0971b4c`（2026-09-22）。
> 本文件回答：**上游会不会做插件系统？我们该不该先做一个？钩子该挂在哪？**

## 0. 一句话

上游**现在没做插件**，但它的结构已经把自己逼向那条路：**中间件能力不够 → 横切能力写死
→ 写死的东西一多，迟早抽象成插件列表**。本仓的 `Middleware`（单 `server` +
`set_step_interceptor`）是朝那个方向的一小步，**归属暂定在 `Middleware` 上**，等上游真出
`plugins` 再考虑搬家。

> 2026-11-02 订正：本节早期版本把「在 step 边界做策略」写成「中间件 ❌」——那是按
> **TS 中间件的公开契约**判断的（§2 的评判标准不变）。**Rust 侧现已能用中间件做到**
> （拦截器返回 `Skip` / `Park`，有测试），`take_over` 还能整条接管 handler。所以
> §2 的表格对 Rust 不成立，差异见 [0002](../decisions/0002-rust-step-hooks.md)。

---

## 1. 证据：上游的扩展点长什么样

上游目前只有**一个**用户可插的地方，而且它是**配置对象**，不是钩子列表：

```ts
// packages/workflow-core/src/telemetry.ts:29
export interface WorkflowTelemetryOptions {
  enabled?: boolean
  tracer?: Tracer
  spanNamePrefix?: string
  attributes?: Attributes | ((ctx: WorkflowTelemetrySpanContext) => Attributes | undefined)
  recordExceptions?: boolean
  mapStepMeta?: (meta, ctx) => Attributes | undefined   // ← 唯一的用户回调
}
```

三个特征：

| 特征 | 含义 |
| --- | --- |
| 只有 `mapStepMeta` 一个用户回调 | 其余都是配置项；用户**配置**能力，不能**扩展**能力 |
| 回调是**纯函数** | 只能返回 attributes，**不能改变执行** |
| 挂在 `RunWorkflowOptions.telemetry` | 与 `interimGoals` / `reseedRun` 同一种挂载方式（驱动参数） |
| 依赖 `@opentelemetry/api` 的 `Tracer` | **写死具体实现**，换 tracing 后端要换引擎依赖 |

**「写死」说的就是最后两行**：能力（tracing）内建、形态固定、用户只能在给定参数里调。

## 2. 中间件为什么不够（**针对 TS**，不适用于本仓 Rust）

本节判断的是**上游 TS 中间件的公开契约**。中间件能做的和做不到的
（`docs/tanstack/middleware.md` §3）：

| 需求 | TS 中间件 | 本仓 Rust 中间件 |
| --- | --- | --- |
| 扩展 ctx（`ctx.ai`） | ✅ | ✅ `next(ext)` |
| 包裹整个 handler（计时、日志） | ✅ | ✅ `sc.next(..).await` 前后 |
| **在 step 边界做策略** | ❌（`next` 只能整体 await；`ctx` 上没有 store） | ✅ `set_step_interceptor` |
| **改变执行**（跳过某步、在边界停） | ❌（只能靠未承诺的 `ctx.step` 覆盖） | ✅ `Skip` / `Park` / `take_over` |

**上游**之所以把 telemetry 写死，正是因为这类需求在**它的**中间件契约下接不住：
span 必须开在「每个 step 开始/结束」上，而那是 handler 内部的事。

> 佐证：TS 里 `ctx.step` 是可覆盖的函数属性，所以**有人可以**用直接赋值做到；
> 但那是**未承诺的实现细节**（`reservedCtxFields` 明说不能遮蔽）。用它做产品级扩展
> 等于赌上游不加固。

对本仓的含义：「中间件接不住 → 只能等插件」这个推论**不能用**。Rust 侧已经用
中间件实现了逐 step 策略（临时目标 / redo / DAG），只是**写法与 TS 不同**——
这是要付的维护代价，见 [0002](../decisions/0002-rust-step-hooks.md)。

## 3. 预测：上游的演进路径

### 3.1 已走过的路径（可核）

```text
要 tracing  → 写死 WorkflowTelemetryOptions（现在的样子）
要 metrics  → ? 再加一个 options 字段？
要 logging  → ? 再加一个？
要自定义策略 → 做不到（中间件接不住）
```

**每加一个横切能力就多一个 options 字段，这条路走不远。**

### 3.2 因此会走向插件列表

```text
WorkflowTelemetryOptions  →  plugins: Plugin[]
                             ├── telemetryPlugin(opts)
                             ├── metricsPlugin(opts)
                             └── 用户自定义 plugin
```

判断依据：

1. **上游自己承认的缺口**是运维面（`research/COMPETITOR_GAP_ANALYSIS` 的 8 条 takeaway
   全是 search / replay / retry / retention / devtools），这些都需要引擎级钩子；
2. `API_CANDIDATES.md` 的 primitive 清单（`step` / `sleep` / `waitForEvent` / `invoke` /
   `parallel` / `race` / `compensate`）**全是编排原语**，没有一个横切扩展点——
   说明这个维度**尚未设计**，不是已决定不做；
3. `telemetry` 的存在证明了「引擎愿意为用户回调留口子」，只是现在只留了一个。

**置信度**：中高（路径合理、证据一致，但没有上游的明文承诺）。

### 3.3 插件会长什么样（推断）

| 维度 | 推断 | 依据 / 置信度 |
| --- | --- | --- |
| **挂载点** | **定义期与运行期都支持**：`createWorkflow().plugins([...])` 设默认，`runWorkflow({ plugins })` 本次追加/覆盖 | 见 §3.4（**中高**） |
| 合并规则 | 定义期在前、运行期在后（后者覆盖同名），与 middleware 的 `Object.assign` 累加同构 | 中 |
| 粒度 | 引擎事件级（run / step / signal / timer 的开始与结束） | 高（telemetry 的 span 就是这个粒度） |
| 是否可改变执行 | **大概率只读观察**，改动执行仍留给 primitive | 中（`mapStepMeta` 是纯函数） |
| 与 middleware 的关系 | 并存：middleware 扩 ctx，plugin 观察/介入引擎事件 | 高 |

### 3.4 为什么是「两处都能传」（修正）

`telemetry` **只有运行期一处**（`run-workflow.ts:62` 的 `RunWorkflowOptions.telemetry`；
`createWorkflow` 侧完全没有）。按现状外推，应该只有 `RunWorkflowOptions`。

但这个形态**本身就可疑**：tracing 是「这个 workflow 长期都要」的横切关注点，却要
**每次 drive 都传一遍**。合理猜测是早期实现随手挂在了最近的位置。

因此更可能的设计是**两处都支持**：

```ts
// 定义期：默认插件（这个 workflow 长期生效）
createWorkflow({ id }).plugins([metrics(), tracing()])

// 运行期：本次 drive 追加/覆盖（局部调试、一次性采集）
runWorkflow({ workflow, runStore, plugins: [debugTrace()] })
```

| 形态 | 概率（本仓判断） | 说明 |
| --- | --- | --- |
| **定义期 + 运行期** | **最高** | 兼顾「长期默认」与「本次特化」，与 telemetry 的现状不矛盾（后者只是还没补定义期） |
| 仅运行期 | 次之 | 跟 telemetry 现状一致，但形态别扭 |
| 仅定义期 | 较低 | 放弃「本次特殊处理」的能力 |

---

## 4. 本仓的现状与定位

### 4.1 已做：单 `server` 中间件（对齐 TS）

```rust
// packages/workflow_core/src/middleware/create_middleware.rs
pub struct Middleware {
    pub server: Arc<dyn Fn(MiddlewareServerFnArgs) -> BoxFuture<'static, anyhow::Result<Value>> + Send + Sync>,
}

impl MiddlewareServerFnArgs {
    pub fn next(&mut self, ext: impl Into<Option<Value>>) -> BoxFuture<Result<Value>>; // 对齐 next({ context })
    pub fn set_step_interceptor(&self, f: StepInterceptor);    // 对齐 ctx.step = wrapped

    pub fn take_over(&mut self);                               // Rust 特有：不跑 handler
}

pub enum StepAction {
    Skip { result: Value },                       // 不跑闭包，直接落 STEP_FINISHED
    Park { signal_name: String, meta: Option<Value> },  // 边界泊车
}
```

**这是「插件能力」的核心**：中间件能按 step 做决定（复用结果、在边界停），
也能整条接管 handler。所以之前「中间件接不住 telemetry 那种逐 step 需求」的
判断，在 Rust 侧已经不成立——telemetry 完全可以写成中间件。

测试：`packages/workflow_middlewares/tests/{interim_goals,reseed,dag_policies}.rs`
（7 个用例全绿）。

> 早期版本这里是两个钩子（`Middleware::on_step_finished` /
> `on_before_step` + `StepHookOutcome`）。**已删除**，原因是对齐 TS 形状、
> 不做两套写法，见 [0002](../decisions/0002-rust-step-hooks.md)。

### 4.2 归属是**暂时的**

中间件当前由 `createWorkflow(...).middleware(mw)` 挂载（构建期）。理由与风险：

| 理由 | 风险 |
| --- | --- |
| 不凭空造新概念（上游连插件雏形都没有） | 若上游将来把「插件」与「中间件」分成两个概念，我们要搬家 |
| 与既有 API 连续（`.middleware(...)` 已存在） | — |

**搬家成本低**：`StepEvent` / `StepAction` / `StepInterceptor` 的类型签名**独立于
挂载点**。收集点是 `define_workflow.rs` 的折叠循环（唯一一处），无论上游最终选
「定义期 / 运行期 / 两者」，都只是换个来源再合并。

策略参数不挂在中间件上，而是走 `RunWorkflowOptions.meta`（每次 drive 可换），
见 [0011](../decisions/0011-run-meta-and-plugins.md)。

### 4.3 一个自觉的越界

上游的 `mapStepMeta` 是**纯观察**（只返回 attributes）。我们的拦截器返回
`Skip` / `Park`，**能改变执行**。

这是必要的——没有它做不出「目标达成即停」和「redo 复用」。而且从 TS 侧看，
「能影响执行」在语义上并非禁忌：那边用户可以覆盖 `ctx.step`，只是上游没把它
写成契约。

**但这条要在文档里承认**：我们的能力比上游现有扩展点更宽。如果上游最终决定
插件只读，本仓会**保留这个更宽的版本**（临时目标等功能依赖它），并在
`docs/tanstack-alignment.md` 记档为有意差异。

---

## 5. 待定问题

| 问题 | 现状 | 何时需要定 |
| --- | --- | --- |
| 拦截器能否**跳过**某步 | ✅ 已实现（`StepAction::Skip`） | — |
| 拦截器执行顺序（多个中间件都装拦截器） | **后者覆盖前者**——`set_step_interceptor` 是单槽，后装的赢 | 出现「两个策略都要按 step 判断」时改成链 |
| 拦截器与中途取消的先后 | 先查取消，再问拦截器 | 已定 |
| 与 `recover` / 重放的关系 | 重放的 step 短路、不触发拦截器 | 已定（有测试） |

---

## 6. 已解决：redo 需要「跳过」能力

早期记录的问题是「`reseed` 需要不执行闭包、直接落一个现成结果」。现在由
`StepAction::Skip { result }` 提供，`reseed` / `dag_policies` 都基于它实现，
测试见 `tests/reseed.rs`（`redo_reuses_steps_outside_redo_set`）。

---

## 7. 结论

1. **上游迟早会做插件**——证据是「telemetry 写死 + 运维面缺口 + primitive 清单里没有
   横切扩展点」；置信度中高。
2. **本仓现在有自己的中间件能力**，形状与 TS 对齐；**归属暂定**在 `Middleware`，
   类型设计与挂载点解耦。
3. **不加业务参数**（`interim_goals` / `reseed_run` 计划删除）——那是策略，不是机制；
   上游三轮规划文档零提及，留着就是维护一份永不合并的 fork。策略参数走 `meta`。
4. **承认越界**：我们的拦截器能改变执行，比上游现有扩展点宽。这是功能需要，记档为
   有意差异。
