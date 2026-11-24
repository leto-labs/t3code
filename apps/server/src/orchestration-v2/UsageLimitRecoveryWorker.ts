import {
  CommandId,
  MessageId,
  readAutoPools,
  type OrchestrationV2Command,
  type OrchestrationV2ProviderGoalStatus,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type { OrchestrationV2DomainEvent } from "@t3tools/contracts";
import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import {
  limitRecoveryDeliveryMode,
  limitRecoveryIsSnoozed,
  limitRecoveryTiming,
} from "./limitRecoveryTiming.ts";
import * as DueHint from "../scheduling/DueHint.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import { forkParked } from "../serverActivation.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import { routineSpan } from "@t3tools/shared/observability";

export const MAX_OVERDUE_ARM_MS = 24 * 60 * 60 * 1000;

const limitRecoveryText = (goalStatus: OrchestrationV2ProviderGoalStatus | undefined) =>
  goalStatus === "active" || goalStatus === "usage_limited"
    ? "/goal resume"
    : "Continue where you left off.";

export function limitRecoveryCommand(
  thread: ProjectionStore.ProjectionLimitRecoveryCandidate,
  autoResume: boolean,
  nowMs: number,
  snooze = false,
  goalStatus?: OrchestrationV2ProviderGoalStatus,
  pool?: { readonly hasHeadroom: boolean },
): OrchestrationV2Command | null {
  if (
    thread.status !== "failed" ||
    thread.lastErrorClass !== "usage_limit" ||
    !thread.latestRunId ||
    thread.archivedAt !== null ||
    thread.settledOverride === "settled" ||
    thread.pendingRuntimeRequest !== null
  )
    return null;
  const timing = limitRecoveryTiming(
    thread.usageLimitResetAt,
    thread.autoPool?.failedAt ?? thread.latestRunCompletedAt ?? thread.updatedAt,
    pool !== undefined,
  );
  if (!timing) return null;
  const mode = limitRecoveryDeliveryMode(
    timing,
    nowMs,
    pool === undefined ? undefined : thread.autoPool?.canRetryEarly !== false,
  );
  const hasHeadroom = mode === "headroom" && pool?.hasHeadroom === true;
  if (mode === "expired" || (!timing.reported && !hasHeadroom)) return null;
  const { resetMs, resetAt } = timing;
  const identity = `${thread.id}:${thread.latestRunId}:${resetMs}`;
  const recovery = thread.limitRecovery;
  if (recovery?.runId !== thread.latestRunId || recovery.resetAt !== resetAt) {
    if (pool === undefined && nowMs - resetMs > MAX_OVERDUE_ARM_MS) return null;
    if (!autoResume && (!snooze || resetMs <= nowMs || hasHeadroom)) return null;
    return {
      type: "thread.metadata.update",
      commandId: CommandId.make(`limit-arm:${identity}`),
      threadId: thread.id,
      limitRecovery: {
        runId: thread.latestRunId,
        resetAt,
        autoResume,
        snooze: snooze && resetMs > nowMs && !hasHeadroom,
      },
    };
  }
  if (
    !recovery.autoResume ||
    (mode !== "reset" && !hasHeadroom) ||
    limitRecoveryIsSnoozed(thread, nowMs, hasHeadroom)
  )
    return null;
  const deliveryIdentity = `${identity}:${recovery.requestId ?? "legacy"}`;
  return {
    type: "message.dispatch",
    commandId: CommandId.make(`limit-resume:${deliveryIdentity}`),
    messageId: MessageId.make(`limit-resume:${deliveryIdentity}`),
    threadId: thread.id,
    usageLimitContinuationOfRunId: thread.latestRunId,
    ...(recovery.requestId === undefined
      ? {}
      : { usageLimitRecoveryRequestId: recovery.requestId }),
    text: limitRecoveryText(goalStatus),
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "agent",
    creationSource: "server",
  };
}

// Events that can make a thread a candidate or change its command; streaming items cannot.
const affectsLimitRecovery = (event: OrchestrationV2DomainEvent) =>
  event.type.startsWith("thread.") ||
  event.type === "run.created" ||
  event.type === "run.updated" ||
  event.type === "runtime-request.updated" ||
  event.type.startsWith("provider-session.") ||
  (event.type === "turn-item.updated" && event.payload.type === "error");

const poolAvailabilityKey = ({
  selection,
  projectId,
  observedAfter,
}: NonNullable<ProjectionStore.ProjectionLimitRecoveryCandidate["autoPool"]>) =>
  JSON.stringify([selection, projectId, observedAfter]);

const makeSweep = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const settings = yield* ServerSettings.ServerSettingsService;
  const providers = Option.getOrUndefined(
    yield* Effect.serviceOption(ProviderRegistry.ProviderRegistry),
  );
  const instances = Option.getOrUndefined(
    yield* Effect.serviceOption(ProviderInstanceRegistry.ProviderInstanceRegistry),
  );
  const due = DueHint.make();
  yield* forkParked(
    Stream.runForEach(orchestrator.streamDomainEvents, (event) =>
      affectsLimitRecovery(event) ? due.invalidate : Effect.void,
    ),
  );
  yield* forkParked(Stream.runForEach(settings.streamChanges, () => due.invalidate));
  if (providers !== undefined) {
    yield* forkParked(
      providers.streamChanges.pipe(
        Stream.map((snapshots) =>
          snapshots.filter(
            (snapshot) => snapshot.driver === "codex" || snapshot.driver === "claudeAgent",
          ),
        ),
        Stream.changesWith(
          (left, right) =>
            left.length === right.length &&
            left.every((snapshot, index) => {
              const current = right[index]!;
              return (
                snapshot.instanceId === current.instanceId &&
                snapshot.auth === current.auth &&
                snapshot.enabled === current.enabled &&
                snapshot.status === current.status &&
                snapshot.models === current.models &&
                snapshot.usageLimits === current.usageLimits
              );
            }),
        ),
        Stream.runForEach(() => due.invalidate),
      ),
    );
  }
  return Effect.fn("UsageLimitRecoveryWorker.sweep", { attributes: routineSpan })(function* () {
    const now = yield* DateTime.now;
    const nowMs = DateTime.toEpochMillis(now);
    if (yield* due.isIdle(nowMs)) return;
    const readVersion = yield* due.version;
    const preferences = yield* settings.getSettings;
    const candidates = yield* projections.getLimitRecoveryCandidates({
      now,
      autoResume: preferences.autoResumeLimitedThreads,
      snooze: preferences.snoozeLimitedThreads,
      autoPoolInstanceIds:
        instances === undefined
          ? []
          : readAutoPools(preferences.providerInstances).map((pool) => pool.instanceId),
    });
    const availability = new Map<string, boolean>();
    let acted = false;
    for (const thread of candidates) {
      const pool = thread.autoPool;
      let hasHeadroom = false;
      if (pool !== undefined && pool.canRetryEarly !== false && instances !== undefined) {
        const key = poolAvailabilityKey(pool);
        const cached = availability.get(key);
        if (cached !== undefined) {
          hasHeadroom = cached;
        } else {
          const instance = yield* instances.getInstance(pool.selection.instanceId);
          hasHeadroom = yield* (
            instance?.orchestrationAdapter.hasUsageAllowance?.(
              pool.selection,
              pool.projectId,
              pool.observedAfter,
            ) ?? Effect.succeed(false)
          );
          availability.set(key, hasHeadroom);
        }
      }
      const planned = limitRecoveryCommand(
        thread,
        preferences.autoResumeLimitedThreads,
        nowMs,
        preferences.snoozeLimitedThreads,
        undefined,
        pool === undefined ? undefined : { hasHeadroom },
      );
      if (planned === null) continue;
      acted = true;
      yield* Effect.gen(function* () {
        let command = planned;
        if (command.type === "message.dispatch") {
          const current = yield* threads.getThreadRecords(thread.id, ["providerThreads"]);
          const goal = current.providerThreads.find(
            (providerThread) => providerThread.id === current.thread.activeProviderThreadId,
          )?.goal;
          command = { ...command, text: limitRecoveryText(goal?.status) };
        }
        if (command.type === "thread.metadata.update" && command.limitRecovery != null) {
          yield* orchestrator.autoArmLimitRecovery({
            commandId: command.commandId,
            threadId: command.threadId,
            limitRecovery: command.limitRecovery,
          });
        } else {
          yield* threads.dispatch(command);
        }
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("orchestration-v2.limit-recovery.dispatch-failed", {
            threadId: thread.id,
            cause,
          }),
        ),
      );
    }
    // A sweep that acted, or failed to, re-checks on the next tick.
    if (acted) return;
    const wakeAt = yield* projections.getLimitRecoveryWakeAt(now);
    yield* due.record(readVersion, wakeAt ?? Infinity, nowMs);
  });
});

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const sweep = yield* makeSweep;
    const scheduler = yield* Scheduler.Scheduler;
    yield* scheduler.register("usage-limit-recovery", sweep());
  }),
);
