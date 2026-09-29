//! 共享契约套件的运行入口（第二次兑现「一份规格，N 个实现共用」）。
//!
//! 直接调 `aa_workflow_runtime::store_contract::run_store_contract`——**不另写一套**。
//! 第一个跑它的是 `packages/workflow_store_sqlx_postgres`，这里是第二个。
//!
//! # 隔离
//!
//! 契约套件**每条用例都调一次 `create_store()`**，并要求状态互不影响
//! （`store_contract.rs` 的循环注释）。所以工厂每次返回**全新实例 + 全新临时目录**。
//!
//! # 不需要真并发
//!
//! 套件用**显式时间戳**模拟竞争（其模块文档原话：「不靠并发线程，靠显式时间戳
//! 模拟竞争」），所以单进程的 `FileExecutionStore` 就能跑完全部条款——这也是
//! 本 crate 敢用进程内 `Mutex` 的原因（见 crate 文档的并发边界说明）。

use std::sync::Arc;

use aa_workflow_runtime::run_store_adapter::WorkflowExecutionStore;
use aa_workflow_runtime::store_contract::run_store_contract;
use example_store_file::FileExecutionStore;

#[test]
fn file_store_satisfies_execution_store_contract() {
    // 每个用例一个全新临时子目录：租约、timer、投递记录都落盘，共享目录会互相污染。
    // 工厂是 `Fn`（要能多次调用），所以用计数器生成子目录名而不是往 Vec 里推。
    let base = std::env::temp_dir().join(format!(
        "wf_store_file_contract_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&base).expect("建临时根目录");
    let counter = std::sync::atomic::AtomicUsize::new(0);

    run_store_contract("file", || {
        let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let dir = base.join(format!("case_{n}"));
        std::fs::create_dir_all(&dir).expect("建用例目录");
        Arc::new(FileExecutionStore::new(&dir)) as Arc<dyn WorkflowExecutionStore>
    });

    let _ = std::fs::remove_dir_all(&base);
}
