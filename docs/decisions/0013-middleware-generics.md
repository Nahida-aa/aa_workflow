# 0013 — `Middleware<TCtxIn, TExtension>` 的两个泛型：只有 `TCtxIn` 是实参数

**状态**：已接受（2026-11-02）
**关联**：[0002](0002-rust-step-hooks.md)、`docs/tanstack/middleware.ts:458`

## 问题

上游 `Middleware<TCtxIn, TExtension>` 的两个泛型在 TS 里都能推动类型推导：

```ts
const requireUser = createMiddleware()          // TCtxIn = unknown
  .server(async ({ next }) => next({ context: { user } }))
//       TExtension = { user } ──┐
//                               └─→ 下游 ctx 多出 user 字段，handler 里 ctx.user 有类型
```

也就是说 **`TExtension` 是「中间件 → handler」的类型通道**：只要把中间件放进列表，
它写的扩展就自动出现在 handler 的 ctx 类型上，用户**不用再声明一次**。

Rust 侧目前把 `TExtension` 只当标注，真值源在 `.middleware::<PExt>([...])`。本 ADR 记录
为什么不让它成为实参数。

## 候选方案

### A. builder 携带类型级列表（HList）

```rust
struct Builder<L, TIn, TOut, TState> { ... }
// 加中间件：Builder<Cons<M, L>, ..>
// handler 的 ctx 类型：<L as MergeExt>::Out
```

`tmp/hlist_probe.rs` 有可运行的最小实现（3 个测试全过），所以**这是能做的**。
但代价：

| # | 代价 | 能不能靠「builder 与产物解耦」消掉 |
| --- | --- | --- |
| 1 | 每多一种中间件组合，builder 就多一个单态化实例（顺序不同也算两种） | **不能**。它来自「类型里必须携带组合信息」本身，与产物是否擦除无关 |
| 2 | 报错信息里出现 `Cons<M2, Cons<M1, Nil>>` | 能——但这也意味着**报错处看不到真实类型**，等于换了个问题 |
| 3 | `.middleware()` 从 `Vec` 循环加时表达不出来 | 能（拒绝它）——但这是 API 能力缩水，不是免费 |

### A′. 中间件交出**类型化的值** + 类型级列表（能让 Ext 自动推导）

方案 A 被否决的理由是「`server` 交不出 `TExtension` 的值，传播类型只会造假保证」。
如果中间件**自己提供扩展值**，这条理由就消失了：

```rust
pub trait Mw {
    type Ext: Default;
    fn ext(&self) -> Self::Ext;      // ← 强制交出真实的值
}
```

这样 `Cons`/`Nil` 列表 + `MergeExt` 能把 handler 的 `ctx.ext` 类型**自动推出来**，
调用点不用写 `<Ext>`。实测（一次性探针，`rustc --test`）：

| 场景 | 结果 |
| --- | --- |
| 单个中间件 | ✅ 完全自动：`.handler(|ext| ext.user)` 不需要任何标注 |
| 两个中间件 | ✅ 自动，但要有人写 `impl Combine<Trace> for User { type Out = UserAndTrace; }` |
| 漏写合并规则 | ❌ 编译期报 `Trace: Combine<User>` 不满足（**不是静默错误**，这点比手写更好） |

**新代价（这才是真正的否决点）**：

1. **要维护 N² 的合并规则**。`User ⊕ Trace`、`Trace ⊕ User` 各一条（`Cons` 把后注册
   的放表头，方向是反的）。三个中间件就是 6 条，四个是 12 条——每条还要定义一个
   合并后的 struct（`UserAndTrace`）。TS 的 `UnionToIntersection` 是零成本的类型运算，
   Rust 里没有，只能手写。
2. **中间件必须变成 struct，不能是闭包**。`Mw::ext(&self)` 需要「值」，现在的
   `Middleware::new().server(|sc| ...)` 是匿名的，拿不到类型。要么用户为每个中间件
   定义 struct，要么我们提供 `middleware(ext_fn, server_fn)` 之类的新构造器。
3. **与 TS 的写法反而更远**：TS 的扩展是 `next({ context: { user } })` 里顺手给的，
   不需要单独写 `fn ext()`。

### B（当前采用）：`TExtension` 只标注，`Ext` 由调用点写

真值源是 `.middleware::<PExt>([...])`（handler 侧 `TCtxExt` 的实际声明处）。

## 决定性理由（与单态化无关）

**方案 A 的自动推导是「诚实」还是「虚假保证」，取决于中间件是否能交出 `TExtension` 的
值。Rust 侧交不出**：`server` 只能调 `next(Value)`，`TExtension` 从头到尾没有
类型信息进入运行期。

于是 A 的效果是：

```rust
// 声明：M1 的 TExtension = User
// 实际：M1 的 server 里可以不调 next(ext)，或写了别的字段
Builder::new().middleware(M1).handler(|ext: User| ...)  // 编译通过
// 运行时:ext.user == ""（Default），无声失败
```

`tmp/hlist_probe.rs` 的 `t3_declared_ext_needs_no_actual_write` 就是这个场景——
3 个测试里唯一「能过但语义是错的」那个。

**能静态保证的类型才值得传播**。`TExtension` 传下去只会制造「编译器说没问题、
运行时是空值」的假保证，比让用户手写 `.middleware::<PExt>([...])` 更糟：手写至少
是一次显式承诺。

TS 那边不成立的唯一原因：JS 是结构类型 + 运行时对象，`next({ context: { user } })`
**字面量就带着值**，类型与值同源。Rust 的 `next` 把值擦成 `Value` 之后，类型
就与值脱钩了。

## 决策

1. `Middleware<TCtxIn, TExtension>` / `MiddlewareServerFn<TCtxIn, TExtension>` /
   `MiddlewareServerFnArgs<TCtxIn, TExtension>` 保留两个参数，与上游**名字和个数**对齐，方便逐行对照。
2. `TCtxIn` 是实参数：`CtxView` 决定 `sc.ctx` 的类型（`()` → 擦除 ctx；
   `BaseCtx<V, TState, TCtxExt>` → 强类型，读得到前序中间件写进 `ctx.ext` 的字段）。
3. `TExtension` 只是标注，**不参与任何类型推导或检查**。handler 的 `TCtxExt`
   由 `.middleware::<PExt>([...])` 声明。
4. `MiddlewareServerFnArgs::next(ext)` 的形状与上游 `next(opts)` **同构**：一次调用完成
   `Object.assign(ctx, ext)` 与 `compose(index + 1)`，返回 Future；第二次调用
   报错（`Option::take` 承载上游的 `advanced` 标志）。不额外定义
   `Next { f }` 包装结构体。
   - 唯一偏离：参数是 `Option<Value>` —— 上游参数**必填**（`types.ts:460`），
     所以只能写 `next({ context: {} })`；Rust 里 `TCtxExt` 默认是 `()`，
     `json!({})` 反序列化成 `()` 会报 `invalid type: map, expected unit`。
     `None` 表示「这次不贡献扩展」，语义上是 `{}` 想表达的东西。

## 将来什么情况下值得改成方案 A

只有当中间件能**交出类型化的扩展值**时才值得，例如：

```rust
// 假设将来 server 的签名变成
pub fn next<T: Serialize>(&mut self, ext: T) -> ...;   // 类型仍不进 TExtension
// 或者用 trait 关联常量/类型把「写了什么」变成可检查的：
trait Mw { type Ext; fn ext(&self) -> Self::Ext; }  // 中间件必须返回真实值
```

后者能做到 `t3` 那个场景编译不过（因为必须返回 `Self::Ext`）。真要做，先看上游
是否把 `next({ context })` 收紧成「必须返回扩展值」——目前没有这个迹象。

## 实测（2026-11-02）

用一次性探针验证过（`rustc --test` 直接编译的独立文件，跑完即删）：

1. **HList 最小实现可行**：`Builder<Cons<M2, Cons<M1, Nil>>>` 能推导出合并后的
   `Ext` 类型，`.handler(|ext: User| ..)` 直接可用 —— **3 个测试全过**。
2. **关键反例**：中间件声明 `type Ext = User`，但 `server` 里完全不调 `next(ext)`，
   `.handler(|ext: User| ..)` 照样编译通过，运行时 `ext.user == ""`。
   **编译期检查通过，语义是错的。**
3. **解耦对照**：把 builder 与产物拆成两个结构体后，上面三条代价里第 2、3 条
   确实可以消（产物类型一样、动态加本来也只能在擦除层做）；**第 1 条
   （每种组合一个新 builder 单态化）消不掉**——它来自「类型必须携带组合信息」
   本身。但第 1 条不是否决理由，真正否决方案 A 的是第 2 条。
