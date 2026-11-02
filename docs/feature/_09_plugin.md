# 插件（plugin）与钩子：上游走向预测与本仓定位

> 日期 2026-11-02。基线：`learn_ls/workflow` `origin/main` = `b928717`（2026-07-21）、
> `taren/durable-recovery` = `0971b4c`（2026-09-22）。
> 本文件回答：**上游会不会做插件系统？我们该不该先做一个？钩子该挂在哪？**

## 0. 一句话

上游**现在没做插件**，但它的结构已经把自己逼向那条路：**中间件能力不够 → 横切能力写死
→ 写死的东西一多，迟早抽象成插件列表**。本仓的 `Middleware::on_step_finished` 是朝那个
方向的一小步，**归属暂定在 `Middleware` 上**，等上游真出 `plugins` 再考虑搬家。

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

## 2. 中间件为什么不够（本仓实测，同样适用于上游）

中间件能做的和做不到的（`docs/tanstack/middleware.md` §3）：

| 需求 | 中间件 |
| --- | --- |
| 扩展 ctx（`ctx.ai`） | ✅ |
| 包裹整个 handler（计时、日志） | ✅ |
| **在 step 边界做策略** | ❌（`wrap` 看不到 handler 内部；`ctx` 上没有 store） |
| **改变执行**（跳过某步、在边界停） | ❌ |

上游之所以把 telemetry 写死，正是因为这类需求**中间件接不住**：span 必须开在
「每个 step 开始/结束」上，而那是 handler 内部的事。

> 佐证：TS 里 `ctx.step` 是可覆盖的函数属性，所以**有人可以**用直接赋值做到；
> 但那是**未承诺的实现细节**（`reservedCtxFields` 明说不能遮蔽）。用它做产品级扩展
> 等于赌上游不加固。

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

从 `WorkflowTelemetryOptions` 的形态外推：

| 维度 | 推断 | 依据 |
| --- | --- | --- |
| 挂载点 | `RunWorkflowOptions.plugins`（与 telemetry 同级） | telemetry 就是这么挂的 |
| 粒度 | 引擎事件级（run / step / signal / timer 的开始与结束） | telemetry 的 span 就是这个粒度 |
| 是否可改变执行 | **大概率只读观察**，改动执行仍留给 primitive | `mapStepMeta` 是纯函数 |
| 与 middleware 的关系 | 并存：middleware 扩 ctx，plugin 观察/介入引擎事件 | 两者正交 |

---

## 4. 本仓的现状与定位

### 4.1 已做：`Middleware::on_step_finished`

```rust
pub enum StepHookOutcome {
    Continue,
    Park { signal_name: String, meta: Option<serde_json::Value> },
}
pub type StepFinishedHook =
    Arc<dyn Fn(&StepFinishedEvent<'_>) -> anyhow::Result<StepHookOutcome> + Send + Sync>;
```

- 调用时机：`STEP_FINISHED` 已 append、live 状态已更新之后；
- **能改变执行**（`Park` 在 step 边界停下）——这一点比上游现有扩展点宽，见 §4.3。

为什么要有它：Rust 的 `ctx.step` 是方法、不可替换，`wrap` 只能包整个 handler，
没有钩子就只能做「跑完再停」。钩子补上这个能力，且它是**机制**——
不绑定任何具体策略。

### 4.2 归属是**暂时的**

钩子当前挂在 `Middleware` 上。理由与风险：

| 理由 | 风险 |
| --- | --- |
| 不凭空造新概念（上游连插件雏形都没有） | 若上游将来把「插件」与「中间件」分成两个概念，我们要搬家 |
| 与既有 API 连续（`.middleware(mw)` 已存在） | — |

**搬家成本低**：`StepFinishedEvent` / `StepHookOutcome` 的类型签名**独立于挂载点**，
真要挪到 `RunWorkflowOptions.plugins` 只是换个收集位置。

### 4.3 一个自觉的越界

上游的 `mapStepMeta` 是**纯观察**（只返回 attributes）。我们的钩子返回 `Park`，
**能中断执行**。

这是必要的——没有它做不出「目标达成即停」。而且从 TS 侧看，「插件能影响执行」在
语义上并非禁忌：那边用户可以覆盖 `ctx.step` 来改变执行，只是上游没把它写成契约。

**但这条要在文档里承认**：我们的钩子在能力上比上游现有扩展点更宽。如果上游最终
决定插件只读，本仓会**保留这个更宽的版本**（因为临时目标等功能依赖它），并在
`docs/tanstack-alignment.md` 记档为有意差异。

---

## 5. 待定问题

| 问题 | 现状 | 何时需要定 |
| --- | --- | --- |
| 钩子是否也要 `on_before_step` | 暂无需求 | 出现「进入某步前拦截」的场景时 |
| 钩子能否**跳过**某步（返回缓存） | 暂无（`Park` 只能停） | 做 `reseed` 中间件时**必须**（见 §6） |
| 钩子顺序与中间件注册顺序 | 按声明顺序 | 已定 |
| 与 `recover` / 重放的关系 | 重放的 step 短路、不触发钩子 | 已定（有测试） |

## 6. 下一步：`reseed` 中间件需要第二种钩子能力

`reseed`（redo）需要「这个 step 不执行，直接用一个现成结果落 checkpoint」。
现有钩子只能 `Park`，不能**替换结果**。缺的能力：

```rust
enum StepHookOutcome {
    Continue,
    Park { signal_name, meta },
    Skip { result: Value },     // ← 需要新增：跳过闭包、直接落 STEP_FINISHED(result)
}
```

但这与「钩子在 step **完成之后**调用」冲突——要跳过就得在**执行之前**介入。
所以真正需要的是 **`on_before_step` 钩子**（可返回 `Skip { result }`），
而不是给 `on_step_finished` 加变体。

**这印证了 §4.2 的判断**：钩子的形状还在演进，现在定死归属不划算。

---

## 7. 结论

1. **上游迟早会做插件**——证据是「telemetry 写死 + 运维面缺口 + primitive 清单里没有
   横切扩展点」；置信度中高。
2. **本仓现在可以有自己的钩子**，但**归属暂定**在 `Middleware`，类型设计保持可与
   挂载点解耦。
3. **不加业务参数**（`interim_goals` / `reseed_run` 计划删除）——那是策略，不是机制；
   上游三轮规划文档零提及，留着就是维护一份永不合并的 fork。
4. **承认越界**：我们的钩子能改变执行，比上游现有扩展点宽。这是功能需要，记档为
   有意差异。
