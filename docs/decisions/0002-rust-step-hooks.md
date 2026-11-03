# 0002 — Rust 中间件的 step 拦截：对齐 TS 的单 `server` 形状

**状态**：已接受（2026-11-02），**取代**最初的「两个钩子」方案
**关联**：[0001](0001-strategy-vs-mechanism.md)、[0007](0007-middleware-compose-order.md)、
[0011](0011-run-meta-and-plugins.md)、`docs/tanstack/middleware.md`

## 背景

TS 的中间件是**一个 `server` 函数**：

```ts
createMiddleware().server(async ({ ctx, next }) => {
  // ① 扩展 ctx
  const out = await next({ context: { user } })
  // ② next 前后 = 洋葱
  return out
})
```

它靠 JS 的动态性同时表达三件事：扩展 ctx、包裹 handler、**拦截 step**
（`ctx.step = wrapped`——`ctx.step` 只是对象上的函数属性）。

Rust 侧最初的实现拆成了 `produce` + `wrap` + 两个 step 边界钩子
（`on_before_step` / `on_step_finished`）。**问题不是能力不够，是形状不同**——
同一份策略在两侧写法差得太远，迁移与对照成本高。

## 决策

**Rust 中间件 = 一个 `server` 函数**，形状与 TS 逐一对应：

| TS `server` 里做的事 | Rust 对应 |
| --- | --- |
| `next({ context: {...} })` | `MiddlewareServerFnArgs::next(ext)`——**一次调用**既合并 context 又推进内层（同构） |
| `await next()` 前后加行为 | `server` 闭包里 `sc.next(..).await` 前后 |
| `ctx.step = wrapped` | `MiddlewareServerFnArgs::set_step_interceptor(f)` |
| 不调 `next` 即接管（TS 丢弃返回值） | `MiddlewareServerFnArgs::take_over()` + 直接 `return` 结果 |

server 函数也有与上游同名的类型（对齐 `types.ts:458` 的
`MiddlewareServerFn<TCtxIn, TExtension>`），**参数个数与名字对齐，语义部分对齐**：

```rust
pub type MiddlewareServerFn<TCtxIn: CtxView = (), TExtension = ()> = Arc<
    dyn Fn(MiddlewareServerFnArgs<TCtxIn, TExtension>) -> BoxFuture<'static, anyhow::Result<serde_json::Value>>
        + Send
        + Sync,
>;

pub struct Middleware<TCtxIn: CtxView = (), TExtension = ()> {
    pub server: MiddlewareServerFn<TCtxIn, TExtension>,
}
```

| 泛型 | 上游 | 本仓 |
| --- | --- | --- |
| `TCtxIn` | server 里 `ctx` 的类型 | **语义对齐**：`CtxView` 决定 `sc.ctx` 是整份擦除 ctx（`()`）还是强类型 `BaseCtx`（`BaseCtx<V, TState, TCtxExt>`，读得到前序中间件写进 `ctx.ext` 的字段） |
| `TExtension` | `UnionToIntersection` 累积进下游 ctx | **纯标注**：handler 侧 `TCtxExt` 由 `.middleware::<PExt>([...])` 决定 |

`TExtension` 不做语义对齐的原因是：Rust 的中间件列表是 `Vec<Middleware>`（泛型已被
默认参数擦除），要让「中间件的类型参数影响 handler」就得让 builder 携带类型级列表
（`HList`），代价远大于收益；而 `.middleware::<PExt>([...])` 已经是现成的真值源，再引入
第二个会漂移。

**能直接传 `Arc<dyn Fn>` 的前提**：`Arc<F>` 实现了 `Fn`（转发到 `&F`）且恒有
`Send + Sync`，所以 `Middleware::server(ext)` 与 `Middleware { server: ext }`
两种写法都通，测试见 `middleware_accepts_a_named_server_fn`。

拦截器签名：

```rust
pub type StepInterceptor = Arc<dyn Fn(&StepEvent<'_>) -> Option<StepAction> + Send + Sync>;

pub struct StepEvent<'a> {
    pub run_id: &'a str,
    pub step_id: &'a str,
    pub run_meta: Option<&'a serde_json::Value>,   // RunWorkflowOptions.meta
}

pub enum StepAction {
    Skip { result: Value },                        // 不跑闭包，直接落 STEP_FINISHED
    Park { signal_name: String, meta: Option<Value> },  // 边界泊车（SIGNAL_AWAITED）
}
```

时机：`exec_step` 入口、**重放短路之后**、取消检查之后、重试泊车前。所以
已 checkpoint 的 step 不会到达拦截器——策略不需要自己做「这条是不是历史」。

旧的 `produce` / `wrap` / 两个钩子**已删除**（不留兼容层）。

## 与 TS 的残留差异（语言必然，非设计取舍）

1. **`TCtxExt` 类型交集**：TS 编译期 `UnionToIntersection`；Rust 在擦除层用
   serde 反序列化进 handler 的 `TCtxExt`（同 axum extensions，运行时报错无静态保证）。
   中间件列表是动态的，静态交集没有值得建的对等物。
2. **`ctx.step` 不可赋值**：它是 `&self` 方法。所以拦截被收成显式 API
   `set_step_interceptor`，而不是「覆盖一个字段」这种未承诺的实现细节。
   TS 那边能不能这么写，取决于实现不把 `ctx` 冻结。

## 多出来的三项（Rust 特有）

- **`Park`**：TS 的 `server` 做不到「在 step 边界停下整条 drive」——它的 `next`
  只能整体 await 完或被丢弃。Rust 的拦截器可以。
- **`take_over`**：TS 的 `m.server()` 返回值被丢弃（上游代码根本不看），
  Rust 里「不调 `next` 直接 return」就是 run 的 output——DAG 中间件
  据此接管整个 handler。
- **`next(ext)` 之外还有 `ctx.meta`**：策略参数走 `RunWorkflowOptions.meta`，
  拦截器经 `StepEvent.run_meta` 直接读到（见 [0011](0011-run-meta-and-plugins.md)）。

这三项都在**机制**层面（任何策略都能用），不是为某个策略开的洞。

## 后果

- `interim_goals` / `reseed` / `dag_policies` 三者的写法与 TS 版一一对应
  （都是 `Middleware::new().server(...)` + `set_step_interceptor`），测试见
  `packages/workflow_middlewares/tests/`；
- 策略状态（`done` / `lookup`）活在 server 闭包捕获的局部变量里，drive 结束
  即丢弃——**与 TS 一致**，不会跨 drive 泄漏；
- 重放短路的 step 不触发拦截器，所以「本段已达成」这类累积天然不会把历史
  重算一遍；
- 上游若将来把「插件」与「中间件」分开（见 `feature/_09_plugin.md`），
  `StepAction` / `StepEvent` 独立于挂载点，搬家成本低。
