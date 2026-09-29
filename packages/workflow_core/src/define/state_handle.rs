//! 共享可变的 typed workflow state——对齐 TS 的 `ctx.state === engine.state`。
//!
//! # 为什么是 handle 而不是 owned 工作副本
//!
//! 旧实现（PARITY #7 的「working copy + 边界 flush」）有一个持续付税的分歧：
//! state 是 handler 手里的 owned 字段，**drive 收尾时引擎拿不到它**，导致
//! 最后一段 state 变更（未过耐久边界的）没有 `STATE_DELTA` 上报，且
//! `ctx.clone()` 的快照语义与 TS 的 live 共享不一致。共享 handle 一次对齐，
//! 之后每个功能都不再付这笔税。
//!
//! # 人体工学与语言约束
//!
//! - `DerefMut` 返回 `&mut TState`（直接借用 handle 内的**普通字段**）——
//!   `ctx.state.count += 1` 原样编译，typed 人体工学保留。
//! - **读路径**（`Deref` 返回 `&TState`）同样直接借字段——所以 `TState` 不能装进
//!   `RwLock`（`Deref` 返回借用临时 guard 是 E0515 悬垂）。这就是旧
//!   working-copy 设计的真正根源：**语言约束**，不是实现便利。
//! - **per-mutation 的 mirror 同步同样不可能**（`deref_mut` 返回字段借用后
//!   没有语句级的钩子）。同步发生在 handle 的 [`Drop`](Self::drop)——即
//!   handler 结束、ctx 析构时。对 delta 的粒度足够：上游的 `STATE_DELTA`
//!   也是边界粒度（`flushStateDelta` 在 step / wait / approve / sleep /
//!   handler 返回处），不是语句粒度。
//!
//! # 同步与发射
//!
//! - mirror（引擎侧 `Value` 视图）在 handle **drop 时**被序列化同步
//!   （dirty 才写——克隆的旧快照不得回写）。
//! - `STATE_DELTA` 的发射由引擎在**耐久边界**（`flush_state` →
//!   `emit_state_delta`）与 **drive 收尾**各做一次：前者上报边界内的变更，
//!   后者补上尾段——与上游在 handler 返回 / catch 处的 `flushStateDelta`
//!   一致。
//! - `STATE_DELTA` 是 **emit-only，不落盘**（上游注释原话：state 由日志
//!   重放推导，持久化 delta 会在每次 invocation 重放时重复 append）。
//!
//! # `Clone` 语义
//!
//! [`Clone`](Self::clone) **复制** `TState`（快照）、共享 mirror 与序列化闭包；
//! 克隆的 drop 不回写（dirty 每柄独立）——并行 step 通过 clone 各自持
//! 快照互不可见，与上游 TS 的行为差异保留（Rust 侧这是确定性优势：
//! 并行 step 无法竞写 state）。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;

/// 序列化闭包：在构造处捕获 `TState: Serialize`，guard / drop 的同步无需 bound
/// （E0367 禁止 Drop impl 加约束）。
type ToValue<TState> = Arc<dyn Fn(&TState) -> Value + Send + Sync>;

/// 共享可变的 typed workflow state（对齐 TS 的 `ctx.state === engine.state`）。
///
/// **`Clone` = 快照**（复制 `TState`，共享 mirror），不是 live 共享——并行 step
/// 通过 clone 各自持快照互不可见，避免竞写破坏重放确定性。
pub struct StateHandle<TState> {
    st: TState,
    to_value: ToValue<TState>,
    mirror: Arc<Mutex<Value>>,
    dirty: Arc<AtomicBool>,
}

impl<TState> Clone for StateHandle<TState>
where
    TState: Clone,
{
    fn clone(&self) -> Self {
        Self {
            st: self.st.clone(),
            to_value: Arc::clone(&self.to_value),
            mirror: Arc::clone(&self.mirror),
            dirty: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<TState> StateHandle<TState> {
    /// 引擎侧 mirror 句柄（构造 `EngineRuntime` 时共享给引擎）。
    pub(crate) fn mirror(&self) -> Arc<Mutex<Value>> {
        Arc::clone(&self.mirror)
    }

    /// 构造：`to_value` 捕获 `TState: Serialize` 的序列化行为（在调用点确定），
    /// guard / drop 的同步无需 trait bound。
    pub fn new(st: TState, mirror: Arc<Mutex<Value>>, to_value: ToValue<TState>) -> Self {
        // 注意：这里**不做**初始 sync——调用方的 st 就是从 mirror 来的，
        // 二者已一致；且若调用方在同一条语句里 \`mirror.lock()\`，此处再
        // \`mirror.lock()\` 会同线程重入死锁（临时 guard 活到语句结束）。
        Self {
            st,
            to_value,
            mirror,
            dirty: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 把 typed state 序列化进 mirror。
    pub(crate) fn sync(&self) {
        eprintln!("[trace] sync enter");
        let value = (self.to_value)(&self.st);
        eprintln!("[trace] sync to_value done");
        *self.mirror.lock().expect("state mirror lock poisoned") = value;
        eprintln!("[trace] sync mirror written");
    }

    /// drop 时只在**本 handle** 被变更过才同步（克隆的旧快照不得回写）。
    pub(crate) fn sync_if_dirty(&self) {
        eprintln!(
            "[trace] sync_if_dirty dirty={}",
            self.dirty.load(Ordering::Relaxed)
        );
        if self.dirty.load(Ordering::Relaxed) {
            self.sync();
        }
    }

    /// 当前 state 的序列化快照（构造 `EngineRuntime` / schema 校验用）。
    pub fn snapshot(&self) -> Value {
        (self.to_value)(&self.st)
    }
}

impl<TState> std::ops::Deref for StateHandle<TState> {
    type Target = TState;

    fn deref(&self) -> &TState {
        &self.st
    }
}

impl<TState> std::ops::DerefMut for StateHandle<TState> {
    fn deref_mut(&mut self) -> &mut TState {
        self.dirty.store(true, Ordering::Relaxed);
        &mut self.st
    }
}

impl<TState> Drop for StateHandle<TState> {
    fn drop(&mut self) {
        self.sync_if_dirty();
    }
}
