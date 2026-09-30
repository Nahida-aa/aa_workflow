//! `WorkflowExecutionStore` 契约测试。
//!
//! 对齐上游 `packages/workflow-runtime/tests/contracts/workflow-execution-store.contract.ts`
//! ——那边是一份 657 行的 vitest 套件，被**三个 store** 复用：
//!
//! ```text
//! in-memory-store.test.ts         → runWorkflowExecutionStoreContractTests({ name: 'in-memory', ... })
//! workflow-store-cloudflare-d1    → 同一个函数 + ensureSchema()
//! workflow-store-drizzle-postgres → 同一个函数
//! ```
//!
//! 本仓照搬这个思路：**一份可执行规格，N 个实现共用**。任何 store 想宣告自己
//! 兼容，跑 [`run_store_contract`] 就行。
//!
//! # 为什么需要它
//!
//! 没有契约测试时，「这个 store 对不对」只能靠每个实现自己写测试——而那是在
//! **照着自己的实现写**，不是照着规格写。第二、第三个实现写起来就会各偏一点。
//!
//! 它也是 **D2（lease 放 store 还是 runtime）的判据来源**：接口够不够用，
//! 要看规格写不写得出来。
//!
//! # 「冲突」怎么测
//!
//! 不靠并发线程，靠**显式时间戳**模拟竞争（与上游同一手法）：
//!
//! ```text
//! claimRun({ owner: 'a', leaseMs: 100, now: 10 })    // 拿到
//! claimRun({ owner: 'b', leaseMs: 100, now: 20 })    // 被挡（a 的 lease 未过期）
//! claimRun({ owner: 'b', leaseMs: 100, now: 111 })   // 重新拿到（a 已过期）
//! ```
//!
//! 所以绝大多数用例**不需要真睡觉**，也不会 flaky。
//!
//! # 用法
//!
//! ```ignore
//! #[test]
//! fn in_memory_contract() {
//!     run_store_contract("in-memory", || Arc::new(InMemoryExecutionStore::default()));
//! }
//! ```

use std::sync::Arc;

use aa_workflow_core::{RunState, RunStatus, StoreError, WorkflowEvent};

use crate::run_store_adapter::WorkflowExecutionStore;
use crate::types::*;

/// store 工厂。每次调用给一个**干净**的 store（用例之间不共享状态）。
pub type StoreFactory = dyn Fn() -> Arc<dyn WorkflowExecutionStore>;

/// 跑完整套件。`name` 只用于失败信息。
///
/// # Panics
///
/// 任一条契约不满足即 panic，信息里带 `name` 与用例名。
pub fn run_store_contract(name: &str, create_store: impl Fn() -> Arc<dyn WorkflowExecutionStore>) {
    /// 一条契约：名字 + 接受 store 工厂的检查函数。
    type Case = (&'static str, fn(&StoreFactory));

    // 每条用例都拿一个全新 store：契约测试必须能独立重跑。
    let cases: Vec<Case> = vec![
        (
            "creates runs idempotently for deterministic run IDs",
            create_run_is_idempotent,
        ),
        (
            "enforces CAS append semantics and ordered replay",
            append_is_cas_guarded,
        ),
        (
            "claims, blocks, and reclaims run leases",
            claim_blocks_then_reclaims,
        ),
        ("claims stale running leases", claims_stale_leases),
        (
            "extends active run leases with heartbeats",
            heartbeat_extends_lease,
        ),
        (
            "releases leases only for the owning worker",
            release_lease_is_owner_scoped,
        ),
        (
            "claims due timers once per active lease window",
            timers_claimed_once_per_window,
        ),
        (
            "delivers signals idempotently and marks the run ready",
            signal_delivery_is_idempotent,
        ),
        (
            "rejects signals for runs waiting on a different signal",
            rejects_mismatched_signal,
        ),
        (
            "delivers approvals idempotently and marks the run ready",
            approval_delivery_is_idempotent,
        ),
        (
            "claims due schedule buckets deterministically",
            schedule_buckets_are_deterministic,
        ),
        (
            "does not reclaim a schedule bucket after it starts",
            started_bucket_is_not_reclaimed,
        ),
        ("lists runs by workflow and status", list_runs_filters),
        (
            "exposes run timelines with stored events",
            timeline_exposes_events,
        ),
        (
            "supports the core RunStore adapter",
            core_adapter_round_trips,
        ),
    ];

    for (case, check) in cases {
        // 每条用例独立 store：上一个用例留下的状态不该影响下一个。
        let made: Arc<dyn WorkflowExecutionStore> = create_store();
        let factory = move || made.clone();
        // 失败信息里带上是哪个实现、哪条契约——不然多 store 并行时无从下手。
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(&factory)));
        if result.is_err() {
            panic!("[{name}] 契约不满足: {case}");
        }
    }
}

// ============================================================
// run 生命周期
// ============================================================

fn create_run_is_idempotent(f: &StoreFactory) {
    let store = f();
    let first = store
        .create_run(CreateRunArgs {
            run_id: "run-1".into(),
            workflow_id: "intent-process".into(),
            workflow_version: None,
            input: serde_json::json!({ "a": 1 }),
            now: 100,
        })
        .unwrap();
    let second = store
        .create_run(CreateRunArgs {
            run_id: "run-1".into(),
            workflow_id: "intent-process".into(),
            workflow_version: None,
            input: serde_json::json!({ "a": 2 }),
            now: 200,
        })
        .unwrap();

    assert!(
        matches!(first, CreateRunResult::Created { .. }),
        "首次创建应为 Created"
    );
    match second {
        CreateRunResult::Existing { run } => assert_eq!(
            run.input,
            serde_json::json!({ "a": 1 }),
            "重复创建不该覆盖已有 input"
        ),
        other => panic!("重复创建应为 Existing，实际 {other:?}"),
    }
}

fn append_is_cas_guarded(f: &StoreFactory) {
    let store = f();
    store
        .append_events(AppendEventsArgs {
            run_id: "run-1".into(),
            expected_next_index: 0,
            events: vec![custom("a", 1), custom("b", 2)],
        })
        .unwrap();

    // expectedNextIndex 写 1 但实际是 2 → 冲突。
    let conflict = store.append_events(AppendEventsArgs {
        run_id: "run-1".into(),
        expected_next_index: 1,
        events: vec![custom("c", 3)],
    });
    assert!(
        matches!(
            &conflict,
            Err(e) if e.downcast_ref::<StoreError>().is_some_and(
                |s| matches!(s, StoreError::Conflict { .. })
            )
        ),
        "CAS 不匹配应报 Conflict，实际 {conflict:?}"
    );

    let all = store
        .read_events(ReadEventsArgs {
            run_id: "run-1".into(),
            from_index: None,
        })
        .unwrap();
    assert_eq!(all.len(), 2, "冲突的 append 不该写进去");
    assert_eq!(all[0].event_index, 0);
    assert_eq!(all[1].event_index, 1);

    // fromIndex 是「从该索引（含）开始」。
    let from_second = store
        .read_events(ReadEventsArgs {
            run_id: "run-1".into(),
            from_index: Some(1),
        })
        .unwrap();
    assert_eq!(from_second.len(), 1);
    assert_eq!(from_second[0].event_index, 1);
}

// ============================================================
// lease
// ============================================================

fn claim_blocks_then_reclaims(f: &StoreFactory) {
    let store = f();
    store
        .create_run(CreateRunArgs {
            run_id: "run-1".into(),
            workflow_id: "intent-process".into(),
            workflow_version: None,
            input: serde_json::json!({}),
            now: 0,
        })
        .unwrap();

    let first = store
        .claim_run(ClaimRunArgs {
            run_id: "run-1".into(),
            lease_owner: "worker-a".into(),
            lease_ms: 100,
            now: 10,
        })
        .unwrap();
    // a 的 lease 到 110；20 时 b 抢不走。
    let blocked = store
        .claim_run(ClaimRunArgs {
            run_id: "run-1".into(),
            lease_owner: "worker-b".into(),
            lease_ms: 100,
            now: 20,
        })
        .unwrap();
    // 111 > 110，a 的 lease 已过期，b 可以拿。
    let reclaimed = store
        .claim_run(ClaimRunArgs {
            run_id: "run-1".into(),
            lease_owner: "worker-b".into(),
            lease_ms: 100,
            now: 111,
        })
        .unwrap();

    match first {
        ClaimRunResult::Claimed { run } => {
            let lease = run.lease.expect("认领后应有 lease");
            assert_eq!(lease.owner, "worker-a");
            assert_eq!(lease.expires_at, 110);
        }
        other => panic!("首次认领应成功，实际 {other:?}"),
    }
    assert!(
        matches!(blocked, ClaimRunResult::NotClaimable { .. }),
        "别人持有 lease 期间不该被认领，实际 {blocked:?}"
    );
    match reclaimed {
        ClaimRunResult::Claimed { run } => {
            let lease = run.lease.expect("重认领后应有 lease");
            assert_eq!(lease.owner, "worker-b");
            assert_eq!(lease.expires_at, 211);
        }
        other => panic!("过期后应可重认领，实际 {other:?}"),
    }
}

fn claims_stale_leases(f: &StoreFactory) {
    let store = f();
    store
        .create_run(CreateRunArgs {
            run_id: "run-1".into(),
            workflow_id: "intent-process".into(),
            workflow_version: None,
            input: serde_json::json!({}),
            now: 0,
        })
        .unwrap();
    store
        .claim_run(ClaimRunArgs {
            run_id: "run-1".into(),
            lease_owner: "worker-a".into(),
            lease_ms: 100,
            now: 0,
        })
        .unwrap();

    // lease 到 100：99 时还不算陈旧。
    let early = store
        .claim_stale_runs(ClaimStaleRunsArgs {
            now: 99,
            limit: 10,
            lease_owner: "worker-b".into(),
            lease_ms: 100,
        })
        .unwrap();
    let stale = store
        .claim_stale_runs(ClaimStaleRunsArgs {
            now: 101,
            limit: 10,
            lease_owner: "worker-b".into(),
            lease_ms: 100,
        })
        .unwrap();

    assert!(early.is_empty(), "lease 未过期不该被当作 stale");
    assert_eq!(stale.len(), 1, "过期后应被认领");
    assert_eq!(stale[0].run.run_id, "run-1");
    assert_eq!(stale[0].lease.owner, "worker-b");
    assert_eq!(stale[0].lease.expires_at, 201, "到期时间从 now 起算");
}

fn heartbeat_extends_lease(f: &StoreFactory) {
    let store = f();
    store
        .create_run(CreateRunArgs {
            run_id: "run-1".into(),
            workflow_id: "w".into(),
            workflow_version: None,
            input: serde_json::json!({}),
            now: 0,
        })
        .unwrap();
    store
        .claim_run(ClaimRunArgs {
            run_id: "run-1".into(),
            lease_owner: "worker-a".into(),
            lease_ms: 100,
            now: 0,
        })
        .unwrap();

    // a 在 50 续租 → lease 推到 150。
    store
        .heartbeat_run_lease(HeartbeatRunLeaseArgs {
            run_id: "run-1".into(),
            lease_owner: "worker-a".into(),
            lease_ms: 100,
            now: 50,
        })
        .unwrap();

    // 101 时 b 想抢：对，a 的**原始** lease（到 100）已过期，但心跳续到了 150，
    // 所以抢不到——这正是心跳的意义。
    let early = store
        .claim_stale_runs(ClaimStaleRunsArgs {
            now: 101,
            limit: 10,
            lease_owner: "worker-b".into(),
            lease_ms: 100,
        })
        .unwrap();
    assert!(early.is_empty(), "心跳续租后不该被判为陈旧");

    // 151 > 150 → 真的过期了。
    let stale = store
        .claim_stale_runs(ClaimStaleRunsArgs {
            now: 151,
            limit: 10,
            lease_owner: "worker-b".into(),
            lease_ms: 100,
        })
        .unwrap();
    assert_eq!(stale.len(), 1, "心跳到期后应可被接手");
    assert_eq!(stale[0].lease.owner, "worker-b");
    assert_eq!(stale[0].lease.expires_at, 251);

    // 非持有者续租是**静默 no-op**（对齐上游 `in-memory-store.ts:226` 的
    // `if (run.lease?.owner !== args.leaseOwner) return run`）——不报错，
    // 也不能改动 lease。用一个**别人持有**的 run 来验：
    let other = f();
    other
        .create_run(CreateRunArgs {
            run_id: "run-2".into(),
            workflow_id: "w".into(),
            workflow_version: None,
            input: serde_json::json!({}),
            now: 0,
        })
        .unwrap();
    other
        .claim_run(ClaimRunArgs {
            run_id: "run-2".into(),
            lease_owner: "worker-a".into(),
            lease_ms: 100,
            now: 0,
        })
        .unwrap();
    other
        .heartbeat_run_lease(HeartbeatRunLeaseArgs {
            run_id: "run-2".into(),
            lease_owner: "worker-b".into(),
            lease_ms: 9999,
            now: 60,
        })
        .unwrap();
    let lease = other
        .load_run("run-2")
        .unwrap()
        .unwrap()
        .lease
        .expect("lease 仍在");
    assert_eq!(lease.owner, "worker-a", "非持有者续租不该改到 owner");
    assert_eq!(
        lease.expires_at, 100,
        "非持有者续租不该延长别人的 lease"
    );
}

fn release_lease_is_owner_scoped(f: &StoreFactory) {
    let store = f();
    store
        .create_run(CreateRunArgs {
            run_id: "run-1".into(),
            workflow_id: "w".into(),
            workflow_version: None,
            input: serde_json::json!({}),
            now: 0,
        })
        .unwrap();
    store
        .claim_run(ClaimRunArgs {
            run_id: "run-1".into(),
            lease_owner: "worker-a".into(),
            lease_ms: 100,
            now: 0,
        })
        .unwrap();

    // 非持有者释放 → no-op，lease 还在（否则别人能把你踢下线）。
    store
        .release_run_lease(ReleaseRunLeaseArgs {
            run_id: "run-1".into(),
            lease_owner: "worker-b".into(),
        })
        .unwrap();
    assert!(
        store.load_run("run-1").unwrap().unwrap().lease.is_some(),
        "非持有者释放不该生效"
    );

    // 持有者释放 → 真释放。
    store
        .release_run_lease(ReleaseRunLeaseArgs {
            run_id: "run-1".into(),
            lease_owner: "worker-a".into(),
        })
        .unwrap();
    assert!(
        store.load_run("run-1").unwrap().unwrap().lease.is_none(),
        "持有者应能释放自己的 lease"
    );
}

// ============================================================
// timer
// ============================================================

fn timers_claimed_once_per_window(f: &StoreFactory) {
    let store = f();
    store
        .create_run(CreateRunArgs {
            run_id: "run-1".into(),
            workflow_id: "timer-workflow".into(),
            workflow_version: None,
            input: serde_json::json!({}),
            now: 0,
        })
        .unwrap();
    store
        .schedule_timer(ScheduleTimerArgs {
            run_id: "run-1".into(),
            workflow_id: "timer-workflow".into(),
            workflow_version: None,
            wake_at: 100,
            signal_id: "timer-1".into(),
            now: 1,
        })
        .unwrap();

    // 99 时还没到点。
    let not_yet = store
        .claim_due_timers(ClaimDueTimersArgs {
            now: 99,
            limit: 10,
            lease_owner: "worker-a".into(),
            lease_ms: 50,
        })
        .unwrap();
    assert!(not_yet.is_empty(), "未到期的 timer 不该被认领");

    // 100 到点，a 认领（lease 到 150）。
    let first = store
        .claim_due_timers(ClaimDueTimersArgs {
            now: 100,
            limit: 10,
            lease_owner: "worker-a".into(),
            lease_ms: 50,
        })
        .unwrap();
    // 110 时 b 抢不走：a 的 lease 还没过期。**这是防重复投递的关键**。
    let blocked = store
        .claim_due_timers(ClaimDueTimersArgs {
            now: 110,
            limit: 10,
            lease_owner: "worker-b".into(),
            lease_ms: 50,
        })
        .unwrap();
    // 151 > 150，a 的 lease 过期 → b 可重认领（a 大概崩了）。
    let reclaimed = store
        .claim_due_timers(ClaimDueTimersArgs {
            now: 151,
            limit: 10,
            lease_owner: "worker-b".into(),
            lease_ms: 50,
        })
        .unwrap();

    assert_eq!(first.len(), 1, "到期应被认领");
    assert_eq!(first[0].run_id, "run-1");
    assert_eq!(first[0].wake_at, 100);
    assert_eq!(first[0].signal_id, "timer-1");
    assert!(
        blocked.is_empty(),
        "认领走的是「挂 lease」而非「删除」——未过期时别人不该拿到"
    );
    assert_eq!(reclaimed.len(), 1, "lease 过期后应可重认领");
}

// ============================================================
// 投递
// ============================================================

fn signal_delivery_is_idempotent(f: &StoreFactory) {
    let store = f();
    store
        .save_run_state(SaveRunStateArgs {
            state: paused_state("run-1", "signal-workflow").with_waiting_for("approval-received"),
        })
        .unwrap();

    let delivered = store
        .deliver_signal(DeliverSignalArgs {
            run_id: "run-1".into(),
            delivery: SignalDelivery {
                signal_id: "signal-1".into(),
                step_id: None,
                name: "approval-received".into(),
                payload: serde_json::json!({ "approved": true }),
            },
            now: 100,
        })
        .unwrap();
    // 同 signalId 再投：webhook 重试是常态，必须是幂等 no-op。
    let duplicate = store
        .deliver_signal(DeliverSignalArgs {
            run_id: "run-1".into(),
            delivery: SignalDelivery {
                signal_id: "signal-1".into(),
                step_id: None,
                name: "approval-received".into(),
                payload: serde_json::json!({ "approved": true }),
            },
            now: 101,
        })
        .unwrap();

    match delivered {
        DeliverSignalResult::Delivered { run } => {
            assert_eq!(
                run.status,
                WorkflowExecutionStatus::Queued,
                "投递后应回到 Queued 等认领"
            );
            assert!(
                run.waiting_for.is_none(),
                "投递后挂起投影应清除（否则会被重复投递）"
            );
        }
        other => panic!("首次投递应 Delivered，实际 {other:?}"),
    }
    assert!(
        matches!(duplicate, DeliverSignalResult::Duplicate { .. }),
        "同 signalId 重投应识别为 Duplicate，实际 {duplicate:?}"
    );
}

fn rejects_mismatched_signal(f: &StoreFactory) {
    let store = f();
    store
        .save_run_state(SaveRunStateArgs {
            state: paused_state("run-1", "signal-workflow").with_waiting_for("expected"),
        })
        .unwrap();

    let delivered = store
        .deliver_signal(DeliverSignalArgs {
            run_id: "run-1".into(),
            delivery: SignalDelivery {
                signal_id: "signal-1".into(),
                step_id: None,
                name: "wrong".into(),
                payload: serde_json::json!({}),
            },
            now: 100,
        })
        .unwrap();
    assert!(
        matches!(delivered, DeliverSignalResult::NotWaiting { .. }),
        "名字对不上应 NotWaiting，实际 {delivered:?}"
    );

    // 不存在的 run。
    let missing = store
        .deliver_signal(DeliverSignalArgs {
            run_id: "nope".into(),
            delivery: SignalDelivery {
                signal_id: "s".into(),
                step_id: None,
                name: "wrong".into(),
                payload: serde_json::json!({}),
            },
            now: 100,
        })
        .unwrap();
    assert!(
        matches!(missing, DeliverSignalResult::NotFound),
        "未知 run 应 NotFound，实际 {missing:?}"
    );
}

fn approval_delivery_is_idempotent(f: &StoreFactory) {
    let store = f();
    store
        .save_run_state(SaveRunStateArgs {
            state: paused_state("run-1", "approval-workflow").with_pending_approval("approval-1"),
        })
        .unwrap();

    let approval = |now: i64| DeliverApprovalArgs {
        run_id: "run-1".into(),
        approval: ApprovalResult {
            approval_id: "approval-1".into(),
            approved: true,
            feedback: None,
        },
        now,
    };
    let delivered = store.deliver_approval(approval(100)).unwrap();
    let duplicate = store.deliver_approval(approval(101)).unwrap();

    match delivered {
        DeliverApprovalResult::Delivered { run } => {
            assert_eq!(run.status, WorkflowExecutionStatus::Queued);
            assert!(run.pending_approval.is_none(), "投递后审批投影应清除");
        }
        other => panic!("首次审批投递应 Delivered，实际 {other:?}"),
    }
    assert!(
        matches!(duplicate, DeliverApprovalResult::Duplicate { .. }),
        "同 approvalId 重投应 Duplicate，实际 {duplicate:?}"
    );
}

// ============================================================
// schedule
// ============================================================

fn schedule_buckets_are_deterministic(f: &StoreFactory) {
    let store = f();
    store
        .upsert_schedule(UpsertScheduleArgs {
            schedule_id: "intent-process".into(),
            workflow_id: "intent-process".into(),
            workflow_version: None,
            schedule: WorkflowScheduleSpec::Interval {
                every_ms: 15 * 60 * 1000,
                timezone: None,
            },
            overlap_policy: WorkflowOverlapPolicy::Skip,
            input: Some(serde_json::json!({ "triggeredAt": 900_000 })),
            next_fire_at: Some(900_000),
            enabled: true,
            now: 0,
        })
        .unwrap();

    let buckets = store
        .claim_due_schedule_buckets(ClaimDueScheduleBucketsArgs {
            now: 900_000,
            limit: 10,
            lease_owner: "worker-a".into(),
            lease_ms: 30_000,
        })
        .unwrap();
    // 同一个桶 b 抢不走（a 的 lease 未过期）。
    let blocked = store
        .claim_due_schedule_buckets(ClaimDueScheduleBucketsArgs {
            now: 900_000,
            limit: 10,
            lease_owner: "worker-b".into(),
            lease_ms: 30_000,
        })
        .unwrap();

    assert_eq!(buckets.len(), 1, "应认领一个到期桶");
    let b = &buckets[0];
    assert_eq!(b.schedule_id, "intent-process");
    assert_eq!(b.bucket_id, "900000", "桶 id 是触发时刻");
    assert_eq!(b.workflow_id, "intent-process");
    assert_eq!(
        b.run_id, "intent-process:intent-process:900000",
        "runId 由 workflowId:scheduleId:bucketId 推导（确定性，防重复触发）"
    );
    assert_eq!(b.fire_at, 900_000);
    assert_eq!(b.input, Some(serde_json::json!({ "triggeredAt": 900_000 })));
    assert_eq!(b.overlap_policy, WorkflowOverlapPolicy::Skip);
    assert!(blocked.is_empty(), "lease 未过期时不该被重复认领");
}

fn started_bucket_is_not_reclaimed(f: &StoreFactory) {
    let store = f();
    store
        .upsert_schedule(UpsertScheduleArgs {
            schedule_id: "intent-process".into(),
            workflow_id: "intent-process".into(),
            workflow_version: None,
            schedule: WorkflowScheduleSpec::Interval {
                every_ms: 15 * 60 * 1000,
                timezone: None,
            },
            overlap_policy: WorkflowOverlapPolicy::Skip,
            input: Some(serde_json::json!({})),
            next_fire_at: Some(900_000),
            enabled: true,
            now: 0,
        })
        .unwrap();

    let buckets = store
        .claim_due_schedule_buckets(ClaimDueScheduleBucketsArgs {
            now: 900_000,
            limit: 10,
            lease_owner: "worker-a".into(),
            lease_ms: 30_000,
        })
        .unwrap();
    store
        .mark_schedule_bucket_started(MarkScheduleBucketStartedArgs {
            schedule_id: "intent-process".into(),
            bucket_id: "900000".into(),
            run_id: buckets[0].run_id.clone(),
        })
        .unwrap();

    // 即使 a 的 lease 早就过期，已启动的桶也不该再被认领。
    let later = store
        .claim_due_schedule_buckets(ClaimDueScheduleBucketsArgs {
            now: 930_001,
            limit: 10,
            lease_owner: "worker-b".into(),
            lease_ms: 30_000,
        })
        .unwrap();
    assert!(
        later.is_empty(),
        "已 markScheduleBucketStarted 的桶不该再被认领，实际 {later:?}"
    );
}

// ============================================================
// 查询
// ============================================================

fn list_runs_filters(f: &StoreFactory) {
    let store = f();
    let mut a = paused_state("run-a", "intent-process");
    a.status = RunStatus::Finished;
    a.updated_at = 10;
    store
        .save_run_state(SaveRunStateArgs { state: a })
        .unwrap();
    let mut b = paused_state("run-b", "intent-discover");
    b.updated_at = 20;
    store
        .save_run_state(SaveRunStateArgs { state: b })
        .unwrap();

    let runs = store
        .list_runs(ListRunsArgs {
            workflow_id: Some("intent-process".into()),
            status: Some(WorkflowExecutionStatus::Finished),
            limit: 10,
            cursor: None,
        })
        .unwrap();
    let ids: Vec<&str> = runs.iter().map(|r| r.run_id.as_str()).collect();
    assert_eq!(ids, vec!["run-a"], "应按 workflowId + status 过滤");
}

fn timeline_exposes_events(f: &StoreFactory) {
    let store = f();
    let mut st = paused_state("run-1", "timeline-workflow");
    st.status = RunStatus::Running;
    store
        .save_run_state(SaveRunStateArgs { state: st })
        .unwrap();
    store
        .append_events(AppendEventsArgs {
            run_id: "run-1".into(),
            expected_next_index: 0,
            events: vec![custom("timeline", 1)],
        })
        .unwrap();

    let timeline = store
        .get_run_timeline("run-1")
        .unwrap()
        .expect("timeline 应存在");
    assert_eq!(timeline.run.run_id, "run-1");
    assert_eq!(timeline.events.len(), 1);
}

// ============================================================
// 与 core 的降格适配
// ============================================================

fn core_adapter_round_trips(f: &StoreFactory) {
    use std::sync::Arc as StdArc;

    let store = f();
    // 适配器把 `WorkflowExecutionStore` 降格成 core 的 `RunStore`。
    let run_store: StdArc<dyn aa_workflow_core::RunStore> =
        crate::run_store_adapter::create_run_store_adapter(store.clone());

    let mut st = paused_state("run-1", "adapter-workflow");
    st.status = RunStatus::Running;
    run_store.set_run_state("run-1", &st).unwrap();
    run_store
        .append_event("run-1", 0, &custom("adapter", 1))
        .unwrap();

    // CAS 冲突要**原样**穿过适配层（core 靠它做乐观并发）。
    let conflict = run_store.append_event("run-1", 0, &custom("conflict", 2));
    assert!(
        matches!(conflict, Err(StoreError::Conflict { .. })),
        "适配层应透传 Conflict，实际 {conflict:?}"
    );

    let back = run_store.get_run_state("run-1").unwrap().unwrap();
    assert_eq!(back.run_id, "run-1");
    assert_eq!(run_store.get_events("run-1").unwrap().len(), 1);
}

// ============================================================
// 测试夹具
// ============================================================

fn custom(name: &str, ts: i64) -> WorkflowEvent {
    WorkflowEvent::Custom {
        ts,
        run_id: "run-1".into(),
        name: name.into(),
        value: serde_json::json!({}),
    }
}

fn paused_state(run_id: &str, workflow_id: &str) -> RunState {
    RunState {
        run_id: run_id.into(),
        workflow_id: workflow_id.into(),
        workflow_version: None,
        status: RunStatus::Paused,
        input: serde_json::json!({}),
        output: None,
        error: None,
        waiting_for: None,
        pending_approval: None,
        awaiting: vec![],
        created_at: 0,
        updated_at: 0,
    }
}

trait WithWait {
    fn with_waiting_for(self, name: &str) -> Self;
    fn with_pending_approval(self, id: &str) -> Self;
}

impl WithWait for RunState {
    fn with_waiting_for(mut self, name: &str) -> Self {
        self.waiting_for = Some(aa_workflow_core::WaitForState {
            step_id: Some("wait-step".into()),
            signal_name: name.into(),
            deadline: None,
            meta: None,
        });
        self
    }

    fn with_pending_approval(mut self, id: &str) -> Self {
        self.pending_approval = Some(aa_workflow_core::PendingApproval {
            step_id: Some(id.into()),
            approval_id: id.into(),
            title: "Approve?".into(),
            description: None,
            meta: None,
        });
        self
    }
}
