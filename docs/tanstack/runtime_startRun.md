```text
POST /runs                                        worker.ts:123
└─ runtime.startRun({workflowId, runId, input})   define-runtime.ts:14 展开 driver 的方法
   └─ startRun(config, telemetry, args)           runtime-driver.ts:55（span 'start_run'）
      ├─ loadWorkflow(args.workflowId)            :77 → 定义在 :856   从 config.workflows 注册表取 definition
      ├─ store.createRun({runId, input, ...})     :92                 幂等创建 run
      │  └─ 已存在且 status ≠ 'queued'
      │     → resultFromExistingRun               :102 → :928         ⚠️ 修正①
      └─ driveClaimedRun(...)                     :107 → :645（span 'drive_run'）
         ├─ store.claimRun({runId, leaseOwner, leaseMs})   :693      原子认领 lease
         │    not-found     → kind 'not-found'      :701
         │    not-claimable → kind 'not-claimable'  :711
         ├─ createRunStoreAdapter(config.store)     :723   新契约 → core 旧 RunStore 的降格适配
         ├─ startLeaseHeartbeat(...)                :724   lease 续约心跳
         ├─ runWorkflow({workflow, runStore, runId, input, ...})   :735   ← 进 core
         │  └─ drive() 分发                          core/engine/run-workflow.ts:159
         │     ├─ runId+signalDelivery/approval → resumeRun   :173
         │     ├─ runId+recover                 → resumeRun   :169
         │     ├─ runId+attach                  → attachRun   :165
         │     └─ 只有 input（startRun 的情形）
         │        → core 内部 startRun            :182 → :189    ⚠️ 修正②
         │           ├─ getRunState 已存在 → attachRun  :196-202（core 层幂等）
         │           ├─ validateWorkflowInput + buildInitialState  :211-212
         │           ├─ setRunState(status:'running')             :233
         │           ├─ emit RUN_STARTED（仅观测，不进日志）      :238
         │           └─ driveHandler(...)                :245 → :445（重放+跑 handler 到终态/等待点）
         ├─ collectWorkflowEvents(...)              :734/:1179   收集事件流（includeEvents/maxEvents 分页）
         ├─ syncTimerFromRunState(...)              :756         把 wait/sleep 同步进 store 的 timer 索引
         ├─ finally: heartbeat.stop() + store.releaseRunLease   :765/:776
         ├─ store.loadRun(runId)                    :791         取最终快照
         └─ classifyRun(run, eventCount)            :794 → :968  finished→completed / paused→paused
                                                                 / errored|aborted→errored / 其余→running
```
