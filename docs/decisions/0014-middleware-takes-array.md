# 0014 — `.middleware()` 收数组（对齐上游 `middleware(middlewares: ReadonlyArray<...>)`）

**状态**：已接受（2026-11-02）
**关联**：[0013](0013-middleware-generics.md)、[0002](0002-rust-step-hooks.md)、
[0007](0007-middleware-compose-order.md)

## 背景

早期 Rust 版是**逐个**挂载：

```rust
.middleware::<UserExt>(m1)
.middleware::<AllExt>(m2)
```

上游是**一次传一个数组**（`types.ts:494` 的 `middlewares: ReadonlyArray<AnyMiddleware>`、
`define-workflow.ts:149` 的 `middleware(middlewares)`），并且用
`TCtxExt & AccumulateExtensions<T>` 在类型层累积：

```ts
.middleware([m1, m2])          // TMiddlewares extends ReadonlyArray<AnyMiddleware>
.handler(async (ctx) => ...)   // ctx 是 TCtxExt & AccumulateExtensions<TMiddlewares>
```

用户明确要求「只接受数组才和 TS 最新」（无存量用户，可以重构）。

## 决策

```rust
pub fn middleware<TCtxIn, PExt, MExt>(
    self,
    middlewares: impl IntoIterator<Item = Middleware<TCtxIn, MExt>>,
) -> WorkflowBuilder<TInput, TOutput, TState, PExt>
```

- 一次收一整批，行为与 TS 相同（顺序 = 注册顺序；先注册的是外层，见 0007）；
- 可多次调用叠加，每次重新指定 `PExt`（相当于 TS 的 `TCtxExt &`，只是手写）。

## 三处与上游的差异

### 1. 扩展类型要显式写（`PExt` 在第二个位置）

TS 由类型系统求交集；Rust 的 `next` 收的是 `Value`，中间件的 `TExtension` 与运行期的值
**没有联系**，所以「自动累积」只会产生「编译通过、运行时空值」的假保证（0013 有实测）。

于是 `PExt` 是调用点显式给的「累积后 handler 看到的 `ctx.ext` 长什么样」。

### 2. 数组元素是 `AnyMiddleware`，可以混装（已解决）

上游是 `ReadonlyArray<AnyMiddleware>`（`Middleware<any, any>[]`）。Rust 侧对齐为：

```rust
pub type AnyMiddleware = Middleware<(), ()>;

pub fn middleware<PExt>(
    self,
    middlewares: impl IntoIterator<Item = AnyMiddleware>,
) -> WorkflowBuilder<TInput, TOutput, TState, PExt>
```

`AnyMiddleware` 不是 trait，就是擦除层的**类型别名**：`Middleware<(), ()>` 的
`server` 闭包吃整份擦除 ctx，任何中间件 `erase()` 之后都是这个形状。

```rust
// 泛型各不相同的中间件，erase() 后进同一个数组
let a = Middleware::new().server(|sc: MiddlewareServerFnArgs| { .. });                 // TCtxIn = ()
let b = Middleware::new().server(|sc: MiddlewareServerFnArgs<BaseCtx<.., Prev>>| { .. }); // TCtxIn = BaseCtx
.middleware::<AllExt>(vec![a.erase(), b.erase()])   // ← 混装，同上游
```

所以 `TCtxIn` 差异不再强迫「分两次调用」；只有**想保留类型标注**时才需要关心它。

### 3. 只能「全显式」或「全推断」

Rust 不支持部分显式泛型参数，但现在方法只剩一个泛型 `PExt`，所以写法退化成
`.middleware::<Ext>([...])`——比上一版干净。

## 为什么不是 `Vec<Box<dyn ...>>`

那样数组里可以混装，但每个元素都要 `Box` 且丢掉 `TCtxIn`/`MExt` 的类型信息。
用 `impl IntoIterator<Item = Middleware<TCtxIn, MExt>>` 时，`[m1, m2]` 这种**字面量数组**
零分配直接用；需要动态拼时才走 `Vec`（元素仍是同一类型）。
