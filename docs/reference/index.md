---
id: aa_workflow_core
title: aa_workflow_core
---

# aa_workflow_core

## Structs

- [AnyWorkflowDefinition](structs/AnyWorkflowDefinition.md)
- [ApproveOptions](structs/ApproveOptions.md)
- [BaseCtx](structs/BaseCtx.md)
- [CreateWorkflowConfig](structs/CreateWorkflowConfig.md)
- [EngineRuntime](structs/EngineRuntime.md)
- [Gate](structs/Gate.md)
- [GateGuard](structs/GateGuard.md)
- [InMemoryStore](structs/InMemoryStore.md)
- [Middleware](structs/Middleware.md)
- [PayloadSchema](structs/PayloadSchema.md)
- [PendingApproval](structs/PendingApproval.md)
- [RetryPolicy](structs/RetryPolicy.md)
- [RunError](structs/RunError.md)
- [RunOutcome](structs/RunOutcome.md)
- [RunState](structs/RunState.md)
- [RunWorkflowOptions](structs/RunWorkflowOptions.md)
- [StateHandle](structs/StateHandle.md)
- [StepAttempt](structs/StepAttempt.md)
- [StepCtx](structs/StepCtx.md)
- [StepHalt](structs/StepHalt.md)
- [StepOptions](structs/StepOptions.md)
- [StepState](structs/StepState.md)
- [WaitForEventOptions](structs/WaitForEventOptions.md)
- [WaitForState](structs/WaitForState.md)
- [WorkflowBuilder](structs/WorkflowBuilder.md)
- [WorkflowCancelled](structs/WorkflowCancelled.md)
- [WorkflowDefinition](structs/WorkflowDefinition.md)
- [WorkflowParked](structs/WorkflowParked.md)
## Enums

- [Backoff](enums/Backoff.md)
- [DeleteReason](enums/DeleteReason.md)
- [Operation](enums/Operation.md)
- [RunAwaitable](enums/RunAwaitable.md)
- [RunErrorCode](enums/RunErrorCode.md)
- [RunStatus](enums/RunStatus.md)
- [StepStatus](enums/StepStatus.md)
- [StoreError](enums/StoreError.md)
- [WorkflowError](enums/WorkflowError.md)
- [WorkflowEvent](enums/WorkflowEvent.md)
## Traits

- [RunStore](traits/RunStore.md)
## Functions

- [cancel_run](functions/cancel_run.md)
- [create_workflow](functions/create_workflow.md)
- [diff_state](functions/diff_state.md)
- [exec_now](functions/exec_now.md)
- [exec_pause](functions/exec_pause.md)
- [exec_pause_with](functions/exec_pause_with.md)
- [exec_uuid](functions/exec_uuid.md)
- [fold_step_states](functions/fold_step_states.md)
- [run_workflow](functions/run_workflow.md)
- [run_workflow_sync](functions/run_workflow_sync.md)
- [select_workflow_version](functions/select_workflow_version.md)
- [signal_event](functions/signal_event.md)
- [signal_run](functions/signal_run.md)
- [snapshot_state](functions/snapshot_state.md)
## Type Aliases

- [BoxFuture](type-aliases/BoxFuture.md)
- [Ctx](type-aliases/Ctx.md)
- [CtxProducer](type-aliases/CtxProducer.md)
- [CtxWrapper](type-aliases/CtxWrapper.md)
- [PayloadValidator](type-aliases/PayloadValidator.md)
- [PublisherFn](type-aliases/PublisherFn.md)
- [ResourceKey](type-aliases/ResourceKey.md)
- [WorkflowCtx](type-aliases/WorkflowCtx.md)
- [WorkflowHandler](type-aliases/WorkflowHandler.md)
## Constants

- [DEFAULT_MIN_YIELD_REMAINING_MS](constants/DEFAULT_MIN_YIELD_REMAINING_MS.md)
