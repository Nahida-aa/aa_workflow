//! `ctx.ext` —— 中间件写进 ctx 的扩展袋（对齐上游 `TExtensions`，但**不做类型交集**）。
//!
//! # 为什么不是 TS 的类型交集
//!
//! 上游用 `UnionToIntersection` 把每个中间件的 `TExtension` 叠成 handler 的 ctx 类型，
//! 于是 `ctx.user` / `ctx.ai` 都是**字段**。Rust 做不到：`struct` 的字段必须在编译期
//! 完全确定，而中间件列表由多个 crate 各自提供，没有任何类型运算能把两个 struct 的
//! 字段拼成一个 struct。
//!
//! # 这里的做法：按**命名空间**存取
//!
//! 与 `@tanstack/ai-orchestration` 的 `ctx.ai` 同构——那个 `ai` 也是中间件包自己起的
//! 名字，只不过 TS 里它落在**字段**上，Rust 里落在**访问器**上：
//!
//! ```ignore
//! struct AuthNs;
//! impl ExtNamespace for AuthNs {
//!     const KEY: &'static str = "auth";
//!     type Value = AuthExt;
//! }
//!
//! // 使用者：
//! let auth = ctx.ext.auth();        // → AuthExt（缺失时 panic）
//! let auth = ctx.ext.try_auth();    // → Option<AuthExt>
//! ```
//!
//! 好处是**使用者不需要定义任何合并类型**：A 开发者写 `AuthExt`，B 开发者写
//! `TraceExt`，各自注册各自的命名空间，使用者需要哪个就取哪个；加第三个中间件
//! 不改任何既有代码。
//!
//! # 为什么返回 owned 而不是 `&`
//!
//! 值存在引擎的擦除 `Value` 里（`serde_json`），按需反序列化只能产生临时值。
//! 要返回引用就得在 `ExtBag` 里缓存，而 `BaseCtx` 是 `Clone` 的值类型，缓存会引入
//! 共享可变状态。扩展都很小（几个字段），每次 `clone` 的代价远低于维护缓存的复杂度。
//!
//! # 跨 drive 的确定性
//!
//! 扩展在每次 drive 由各中间件的 `server` 重新写入（与上游「每次 invocation 重新跑
//! 中间件」一致），所以 `ctx.ext` 只会看到本次 drive 写过的值。
use serde_json::Value;

/// 一个扩展命名空间：`KEY` 是 JSON 里的键，`Value` 是读取时反序列化成的类型。
///
/// 中间件作者为每个扩展定义**一个零大小类型**实现它，于是写入与读取共享同一份
/// 键名与类型：
///
/// ```ignore
/// // 写（中间件）
/// sc.next(Some(DefExt::<AuthNs>::new(auth).into_value()))
/// // 读（使用者）
/// ctx.ext.auth()
/// ```
pub trait ExtNamespace {
    /// `ctx.ext` 里这个扩展的键名。
    const KEY: &'static str;
    /// 扩展的值类型。
    type Value: serde::de::DeserializeOwned;
}

/// 把一个扩展值包成 `{ KEY: value }` 的 JSON 片段。
///
/// 存在的意义是让中间件侧不用手写 `json!({ KEY: ... })`——键名与类型只写一次。
pub struct DefExt<Ns: ExtNamespace>(pub Ns::Value);

impl<Ns> DefExt<Ns>
where
    Ns: ExtNamespace,
    Ns::Value: serde::Serialize,
{
    pub fn new(value: Ns::Value) -> Self {
        Self(value)
    }

    /// 转成 `next(ext)` 要的擦除形态：`{ "<Ns::KEY>": <value> }`。
    pub fn into_value(self) -> Value {
        let mut map = serde_json::Map::new();
        map.insert(
            Ns::KEY.to_string(),
            serde_json::to_value(self.0).unwrap_or(Value::Null),
        );
        Value::Object(map)
    }
}

/// handler / 中间件读扩展的入口 —— `ctx.ext`。
///
/// 每个命名空间**独立**反序列化：一个解析失败只影响它自己，不会像旧的多字段
/// `TCtxExt` 那样整份退化成 `Default`。
#[derive(Clone, Default, Debug)]
pub struct ExtBag {
    raw: Value,
}

impl ExtBag {
    /// 从引擎的擦除表示构造。非对象（`null` / 缺失）视为空袋。
    pub fn from_value(raw: Value) -> Self {
        Self {
            raw: if raw.is_object() {
                raw
            } else {
                Value::Object(Default::default())
            },
        }
    }

    /// 擦除形态（调试 / 透传用）。
    pub fn as_value(&self) -> &Value {
        &self.raw
    }

    /// 没有任何扩展（没挂写扩展的中间件）。
    pub fn is_empty(&self) -> bool {
        self.raw.as_object().is_none_or(|m| m.is_empty())
    }

    /// 取某个命名空间的扩展。
    ///
    /// # Panics
    ///
    /// 中间件没挂载、或它没写这个扩展时 panic —— 与 TS 里
    /// 「声明了 `ctx.auth` 却没挂对应中间件」一样是编程错误。
    /// 要不 panic 用 [`try_get`](Self::try_get)。
    pub fn get<Ns: ExtNamespace>(&self) -> Ns::Value {
        self.try_get::<Ns>().unwrap_or_else(|| {
            panic!(
                "ctx.ext.{}: 没有这个扩展 —— 对应的中间件没挂载，或它没在 server 里 next(ext) 写入",
                Ns::KEY
            )
        })
    }

    /// 取某个命名空间的扩展；缺失返回 `None`。
    pub fn try_get<Ns: ExtNamespace>(&self) -> Option<Ns::Value> {
        let v = self.raw.get(Ns::KEY)?.clone();
        serde_json::from_value(v).ok()
    }
}

/// 为扩展命名空间声明 `ctx.ext` 上的访问器 —— 对齐 TS 的「扩展即 ctx 字段」。
///
/// TS 里扩展是 **ctx 上的字段**（`ctx.ai`）。Rust 的 `ctx` 是 struct，字段必须
/// 编译期固定；`ExtBag` 定义在本 crate，外部 crate 不能给它加固有方法（孤儿规则），
/// 所以用**扩展 trait**：中间件作者声明一个 trait，由宏 `impl` 到 `ExtBag` 上。
///
/// ```ignore
/// // 中间件作者（auth 包）
/// struct AuthNs;
/// impl ExtNamespace for AuthNs {
///     const KEY: &'static str = "auth";
///     type Value = AuthExt;
/// }
/// impl_ext_accessors!(AuthNs => AuthExtAccess, auth, try_auth);
/// // ↑ trait AuthExtAccess { fn auth(&self) -> AuthExt; fn try_auth(&self) -> Option<AuthExt>; }
/// //   + impl AuthExtAccess for ExtBag
///
/// // 使用者（只 import 中间件包导出的那个 trait）：
/// use auth_pkg::AuthExtAccess;
/// let a = ctx.ext.auth();          // 对齐 TS 的 ctx.auth；缺失 panic
/// let a = ctx.ext.try_auth();      // Option，不 panic
/// ```
///
/// 与 TS 的差别只在「读之前要 `use` 那个 trait」——两边都要 `import` 中间件包。
#[macro_export]
macro_rules! impl_ext_accessors {
    ($ns:ty => $trait_name:ident, $getter:ident, $try_getter:ident) => {
        /// 由 `impl_ext_accessors!` 生成：`ExtBag` 上该命名空间的访问器 trait。
        pub trait $trait_name {
            #[doc = concat!("取 `ctx.ext.", stringify!($ns), "` 的扩展；缺失时 panic。")]
            fn $getter(&self) -> <$ns as $crate::define::ExtNamespace>::Value;
            #[doc = "取该命名空间的扩展；缺失返回 `None`。"]
            fn $try_getter(&self) -> Option<<$ns as $crate::define::ExtNamespace>::Value>;
        }

        impl $trait_name for $crate::define::ExtBag {
            fn $getter(&self) -> <$ns as $crate::define::ExtNamespace>::Value {
                self.get::<$ns>()
            }
            fn $try_getter(&self) -> Option<<$ns as $crate::define::ExtNamespace>::Value> {
                self.try_get::<$ns>()
            }
        }
    };
}

/// 把多个中间件的扩展合并进同一份擦除表示（注册顺序，后者覆盖同名键）。
pub fn merge_ext_value(base: &mut Value, piece: Value) {
    if let (Some(a), Some(b)) = (base.as_object_mut(), piece.as_object()) {
        for (k, v) in b {
            a.insert(k.clone(), v.clone());
        }
    } else {
        *base = piece;
    }
}
