# 0015 — `ctx.ext` 按命名空间存取：使用者不需要定义合并类型

**状态**：已接受（2026-11-02）
**取代**：[0013](0013-middleware-generics.md)（`TExtension` 只做标注）、
[0014](0014-middleware-takes-array.md)（`.middleware::<Ext>` 要写扩展类型）
**关联**：[0002](0002-rust-step-hooks.md)、`@tanstack/ai-orchestration` 的 `ctx.ai`

## 问题

上游靠 `UnionToIntersection` 把每个中间件的 `TExtension` 叠成 handler 的 ctx 类型：

```ts
.middleware([m1, m2])
.handler(async (ctx) => ctx.a + ctx.b)   // ctx 上自动多出 a / b
```

之前 Rust 侧要求使用者手写一个「合并后的类型」：

```rust
.middleware::<Combined>([m1, m2])
```

**这不可接受**：中间件可能来自多个外部开发者，使用者既不知道也不该知道
`m1`/`m2` 各自的扩展类型怎么拼成一个新 struct。0013 里论证过「Rust 没有类型交集」，
但那个结论只否定了**自动合并成一个 struct**，没否定「不用合并也能读」。

## 决策

**扩展按命名空间存取，不做类型合并。**

```rust
// 中间件作者（外部 crate）为每个扩展定义一个命名空间
struct AuthNs;
impl ExtNamespace for AuthNs {
    const KEY: &'static str = "auth";
    type Value = AuthExt;
}
impl_ext_accessors!(AuthNs => AuthAccess, auth, try_auth);
// 生成 trait AuthAccess { fn auth(&self) -> AuthExt; fn try_auth(&self) -> Option<AuthExt> }

// 使用者：不写任何类型，也不需要知道「合并后是什么」
use auth_pkg::AuthAccess;
use trace_pkg::TraceAccess;

create_workflow(cfg)
    .middleware([auth(), trace()])        // 不同来源，直接混装
    .handler(|ctx| async move {
        let a = ctx.ext.auth();           // 对齐 TS 的 ctx.auth
        let t = ctx.ext.trace();
        ...
    })
```

`.middleware()` **不再有泛型参数**，`Middleware` / `MiddlewareServerFn` /
`MiddlewareServerFnArgs` 也**都没有泛型**。

## 为什么这解决了「多个来源」

因为**根本没有「合并」这一步**：每个中间件往 JSON 里写自己的键（`{ "auth": ... }`、
`{ "trace": ... }`），handler 按命名空间分别取。加第三个中间件不改任何既有代码——
TS 的交集自动扩大，这里则是「不需要扩大」。

## 与 TS 的差异

| | TS | Rust |
| --- | --- | --- |
| 读取 | `ctx.auth`（字段） | `ctx.ext.auth()`（方法，由 trait 提供） |
| 需要 import | 中间件包 | 中间件包 + 那个 trait |
| 缺失扩展 | `undefined` | **panic**（`try_auth()` 返回 `Option`） |
| 同名键 | 后者覆盖 | 后者覆盖（`Object.assign` 同义） |
| 解析失败 | 不存在这一层 | 只影响该命名空间（旧的多字段 `TCtxExt` 会整份退化） |

读取要写 `ctx.ext.` 前缀、并且要 `use` trait，是 Rust 没有字段级结构类型的直接后果
（`struct` 的字段必须编译期固定，且外部 crate 不能给 `ExtBag` 加固有方法——孤儿规则）。
用 trait + 宏把成本压到「一行 `use`」。

## 删掉的东西

- `BaseCtx` 的 `TCtxExt` 泛型
- `WorkflowDefinition` / `WorkflowBuilder` 的 `TCtxExt` 泛型
- `Middleware` / `MiddlewareServerFn` / `MiddlewareServerFnArgs` 的
  `TCtxIn` / `TExtension` 泛型
- `CtxView`（不再需要「已带前序扩展的强类型 ctx 视图」）
- `.middleware::<Ext>()` 的显式类型参数

## 测试基线

`packages/workflow_core/tests/middleware.rs` 逐条对齐上游
`packages/workflow-core/tests/middleware.test.ts` 的四个用例（用例名保持英文原文）。
