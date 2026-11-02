# 临时目标（interim goals，TS 版本实现）

> 基线：`learn_ls/workflow` **f2-lite 分支** HEAD = `4cc7a46`。
> ⚠️ **这个功能不在 `taren/durable-recovery`（`0971b4c`）上**——它与 redo
> （`reseedRun`）同为 **f2-lite 自有设计**，引入提交 `b9eebed`。核验命令：
>
> ```bash
> cd /home/aa/repos/lib_ls/learn_ls/workflow
> git grep -c "interimGoals" 0971b4c -- packages/         # 空 = 基线上没有
> git log --oneline -S "interimGoals" f2-lite | tail -1    # b9eebed（引入）
> ```
>
> 本文件回答：**「跑到某几个 step 就停下，等我决定再继续」是怎么实现的**。

> **约定**（同 `_01`）：代码块从真实实现/测试裁剪，不改语义；每块下方给
> 「对端测试」`文件:行` + 测试名，冲突时以测试为准。跑法见 `_01` 顶部
> （**用 `node` 跑 vitest，不要用 `bun`**）。

## 0. 一句话总览

临时目标 = **`ctx.waitForEvent` 的一个保留信号名** + **一个转移检查**。
零新事件类型、零新 store 契约方法、零新原语。

```text
engineStep 成功 append STEP_FINISHED 之后
  ├ 本 step 在名单里？  → interimGoalsDone.add(stepId)
  ├ 名单全齐了？        → engineWaitForEvent('__interim-goal')   ← 泊车
  └ 否则                → 照常返回，继续跑下一步
```

泊车复用 `__timer` 那套机制：append `SIGNAL_AWAITED` → `setRunState(paused)` →
`throw WorkflowPaused` → `driveHandler` 捕获后静默 return（见 `_01` §4）。

---

## 1. 声明（`run-workflow.ts:89-96`）

```ts
/**
 * F2-lite interim goal (INTERIM_GOAL): when every listed step has been
 * checkpointed during a drive, the run parks (reserved signal
 * `__interim-goal`, goals surfaced on `waitingFor.meta.goals`); the
 * host continues with a `recover: true` drive (same or extended
 * goals). A transition check (goals-done at drive start) keeps
 * continuation drives from re-parking. Goals never reached (branched
 * away) never park — the fence is best-effort.
 */
interimGoals?: ReadonlyArray<string>
```

一个 **per-drive 参数**，**不落盘**：不在 `RunState` 里，不参与 version 判定。
「这次 drive 跑到哪停」是调用者的意图，不是 run 的属性——所以每次 resume 都能给
新的/不同的名单。

---

## 2. 三个字段：谁在什么时候初始化（`run-workflow.ts:532-544`）

`driveHandler` 在跑 handler 之前把这些塞进 `EngineRuntime`：

```ts
interimGoals: options.interimGoals,
interimGoalsDone: options.interimGoals
  ? new Set(
      options.interimGoals.every((id) =>
        history.some((e) => e.type === 'STEP_FINISHED' && e.stepId === id),
      )
        ? options.interimGoals      // 开局就全齐 → 预填成「全达成」
        : [],                        // 否则从空集累积
    )
  : undefined,
goalsDoneAtDriveStart: options.interimGoals
  ? options.interimGoals.every((id) =>
      history.some((e) => e.type === 'STEP_FINISHED' && e.stepId === id),
    )
  : false,
```

| 字段 | 含义 |
| --- | --- |
| `interimGoals` | 本次 drive 的目标名单（来自参数） |
| `interimGoalsDone` | 本 drive 已记录的目标（驱动中累加） |
| `goalsDoneAtDriveStart` | **日志里**是否已覆盖全部目标（驱动开始时算一次） |

后两个都在查**同一件事**（`history` 里的 `STEP_FINISHED`），只是用处不同：
`goalsDoneAtDriveStart` 是**闸门**，`interimGoalsDone` 是**计数器**。

---

## 3. 泊车检查：`engineStep` 成功返回之前（`run-workflow.ts:1119-1139`）

```ts
// F2-lite interim goal: park when every goal is checkpointed and
// the set was not already complete at drive start (transition
// check keeps continuation drives from re-parking). The park
// rides the reserved '__interim-goal' signal — same machinery as
// '__timer'; the host continues with a 'recover: true' drive.
if (engine.interimGoals && !engine.goalsDoneAtDriveStart) {
  if (engine.interimGoals.includes(stepId)) {
    engine.interimGoalsDone!.add(stepId)
  }
  if (engine.interimGoals.every((id) => engine.interimGoalsDone!.has(id))) {
    engine.counters.goalParks++
    await engineWaitForEvent<void>(engine, '__interim-goal', {
      id: `__interim-goal-${engine.counters.goalParks}`,
      meta: { goals: [...engine.interimGoals] },
    })
  }
}
return result as T
```

三个要点：

1. **位置在 `STEP_FINISHED` 之后**——目标 step 完整执行并 checkpoint 了才泊，
   恢复时它是短路命中的缓存，副作用不重跑。
2. **`engineWaitForEvent` 就是普通等待原语**：复用 `__timer` 的全部机制，
   于是 sweep / `deliverSignal` / `appendSeed` 全都自动认得它，无需任何特判。
3. **`id` 带计数器**（`__interim-goal-${goalParks}`）——同一个 run 多次泊车
   （多次 resume、多次改目标）时，每次 `SIGNAL_AWAITED` 的 `stepId` 不同，
   `findCheckpoint` 的「按 stepId 匹配 + 已消费下标」才能分清是哪一次。

---

## 4. 转移检查：为什么续驱不会重挂

这是整个功能最容易踩坑的地方。续驱时：

```text
recover drive 开始
  → history 里已有 a、b 的 STEP_FINISHED
  → goalsDoneAtDriveStart = true
  → 重放 handler：engineStep('a')/'b' 短路返回缓存（闭包不执行）
    ↳ 短路路径【不经过】第 3 节的检查块——检查块在闭包真跑完之后
  → engineStep('c') 真跑；检查块进得去，但 goalsDoneAtDriveStart 让它跳过
  → handler 返回 → RUN_FINISHED
```

两道保险：

- **短路路径不进检查块**：`engineStep` 的 `cached` 分支在 `:863-886` 就 `return`，
  根本走不到 `:1119`。所以「重放到目标 step」本身不会触发第二次泊车。
- **`goalsDoneAtDriveStart` 闸**：即便某次 drive 要让目标 step 真跑（例如
  `reseedRun` 的新 run 把目标列进了 `redoSet`），闸也挡着不会重挂。

> 对端测试：`packages/workflow-core/tests/engine.rerun.test.ts:115`
> "parks when every goal is checkpointed; a recover drive completes the run"
> ——断言日志全程只有**一条** `__interim-goal` 的 `SIGNAL_AWAITED`（`:163-164`），
> 且第二次 drive 的流里一条都没有（`:165-166`）。

---

## 5. 多目标 = 全齐才停（`engine.rerun.test.ts:170`）

```ts
runWorkflow({ ..., interimGoals: ['b', 'c'] })
```

`engine.interimGoals.every(...)` —— **所有**目标都 checkpoint 了才泊，不是「任一到达」。
语义是「推进到这个围栏为止，围栏内的事全做完」。

> 对端测试：`:170` "multi-goal list parks only when ALL goals are checkpointed"。

一个反直觉的边界（同测试注释指出）：**目标恰好是最后一步时**，park 落在
handler 返回之前——run 停成 `paused` 而不是 `finished`，需要再驱动一次才收尾。
这是「检查块在 step 完成之后、handler 返回之前」的必然结果，不是 bug。

---

## 6. 软围栏：目标到不了不报错

`run-workflow.ts:94-96` 的注释原话：

> Goals never reached (branched away) never park — the fence is best-effort.

目标 step 因分支走开而永不执行时，run **照常跑完**，不报错、不挂起。
相比 wf-demo 库层的 `rerunDag`（目标落在重跑闭包外会 throw），这里是有意的
**软**语义：

| | 引擎层 `interimGoals` | 库层 `rerunDag` 的 `interimGoals` |
| --- | --- | --- |
| 目标不可达 | 静默跑完（best-effort） | **throw**（`outside the rerun closure`） |
| 理由 | 引擎无图，判不了运行时可达性 | 库有图，能算闭包，误用即错 |

引擎这一层**没有图**：静态可达 ≠ 运行时可达（条件分支、`fail()` 短路径），
所以只能软。库层愿意承担这个判断，就变成了硬校验。

---

## 7. 续驱的两种写法

| 路径 | 怎么写 | 对端 |
| --- | --- | --- |
| 仅 core | `runWorkflow({ runId, recover: true, interimGoals? })` | `engine.rerun.test.ts:146-155` |
| 用 runtime | `runtime.deliverSignal({ runId, name: '__interim-goal', signalId })` | 泊车就是普通 `SIGNAL_AWAITED`，`resumeRun` 的 `appendSeed` 认得它 |

⚠️ 仅 core 路径的坑（`_01` §6 已记）：`recover: true` 与带 delivery 的 resume
进的是同一个 `resumeRun`，两者都**不读 `options.input`**。续驱时改目标名单可以
（per-drive 参数），改 input 不行。

---

## 8. 代码位置索引（f2-lite `4cc7a46`）

| 主题 | 文件 : 行 |
| --- | --- |
| 参数声明 + 语义注释 | `packages/workflow-core/src/engine/run-workflow.ts:89-96` |
| 三个字段初始化 | `run-workflow.ts:532-544` |
| 泊车检查块 | `run-workflow.ts:1119-1139` |
| `goalParks` 计数器 | `run-workflow.ts:523` / `:741` |
| runtime 透传（`startRun` args） | `packages/workflow-runtime/src/runtime-driver.ts:130` / `:884` |
| runtime 类型声明 | `workflow-runtime/src/types.ts:435` |
| 短路分支（不进检查块） | `run-workflow.ts:863-886` |

## 9. 测试位置索引

| 主题 | 测试 : 行（测试名） |
| --- | --- |
| 泊在全齐、recover 续驱、不重挂 | `engine.rerun.test.ts:115` "parks when every goal is checkpointed; a recover drive completes the run" |
| 多目标全齐才停 | `engine.rerun.test.ts:170` "multi-goal list parks only when ALL goals are checkpointed" |

跑法：

```bash
cd /home/aa/repos/lib_ls/learn_ls/workflow   # f2-lite 分支，HEAD 4cc7a46
node node_modules/.bin/vitest run packages/workflow-core/tests/engine.rerun.test.ts
```
