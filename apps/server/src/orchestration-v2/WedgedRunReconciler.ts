import {
  CommandId,
  ProviderSessionId,
  ProviderThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/sql/SqlClient";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import { routineSpan } from "@t3tools/shared/observability";
import * as Scheduler from "../scheduling/Scheduler.ts";
import type * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessions from "./ProviderSessionManager.ts";
import * as RunStateLog from "./RunStateLog.ts";

/** RunExecutionService ends an attempt and its run in one transaction, so a gap only means a lost write. */
const REPAIR_GRACE_MS = 2 * 60_000;
/** Same rule as Core's external watchdog. Observed only: a Codex `sleep` call is silent for hours. */
const STALE_OBSERVE_MS = 10 * 60_000;
const TICK_INTERVAL_MS = 60_000;
const MAX_RUNS_PER_TICK = 100;
const MAX_ACTIONS_PER_TICK = 20;
const OBSERVATION_REPEAT_MS = 30 * 60_000;
const BACKOFF_BASE_MS = 60_000;
const BACKOFF_MAX_MS = 30 * 60_000;

const TERMINAL_ATTEMPT_STATUSES = ["completed", "failed", "interrupted", "cancelled"] as const;
type TerminalAttemptStatus = (typeof TERMINAL_ATTEMPT_STATUSES)[number];
const isTerminalAttemptStatus = (status: string | null): status is TerminalAttemptStatus =>
  TERMINAL_ATTEMPT_STATUSES.some((terminal) => terminal === status);
// A superseded attempt was replaced by a newer one; it no longer owns the run.
const isSettledAttemptStatus = (status: string) =>
  isTerminalAttemptStatus(status) || status === "superseded";
const OPEN_NODE_STATUSES = new Set(["idle", "pending", "running", "waiting"]);
const RECENT_ITEM_COUNT = 3;

/**
 * Stable codes for `reconciler.repair` and `reconciler.observe` records.
 */
export type WedgedRunReason =
  | "attempt_terminal_run_running"
  | "attempt_terminal_provider_turn_live"
  | "attempt_terminal_session_busy"
  | "attempt_terminal_session_live"
  | "attempt_terminal_session_unknown"
  | "attempt_terminal_attempts_unsettled"
  | "attempt_terminal_waiting_on_user"
  | "attempt_terminal_write_refused"
  | "attempt_terminal_repair_failed"
  | "attempt_running_no_live_work"
  | "inspection_failed";

export class WedgedRunReconcilerError extends Schema.TaggedError<WedgedRunReconcilerError>()(
  "WedgedRunReconcilerError",
  {
    operation: Schema.Literals(["scan", "inspect", "finalize"]),
    threadId: Schema.optional(ThreadId),
    runId: Schema.optional(RunId),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Wedged run reconciliation failed during ${this.operation}.`;
  }
}

export type WedgedRunAction = "finalized" | "finalized-and-redelivered" | "none";

export interface WedgedRunReconcileSummary {
  readonly scanned: number;
  readonly actions: ReadonlyArray<{
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly attemptId: RunAttemptId | null;
    readonly event: "reconciler.repair" | "reconciler.observe";
    readonly reason: WedgedRunReason;
    readonly action: WedgedRunAction;
  }>;
}

export class WedgedRunReconciler extends Context.Service<
  WedgedRunReconciler,
  {
    /** Inspect the next batch of running runs. */
    readonly reconcile: Effect.Effect<WedgedRunReconcileSummary, WedgedRunReconcilerError>;
  }
>()("t3/orchestration-v2/WedgedRunReconciler") {}

const RunningRunRow = Schema.Struct({
  threadId: ThreadId,
  rowId: Schema.Number,
  runId: RunId,
  ordinal: Schema.Number,
  providerThreadId: Schema.NullOr(ProviderThreadId),
  runStartedAt: Schema.NullOr(Schema.String),
  activeAttemptId: Schema.NullOr(RunAttemptId),
  attemptStatus: Schema.NullOr(Schema.String),
  attemptStartedAt: Schema.NullOr(Schema.String),
  attemptCompletedAt: Schema.NullOr(Schema.String),
  deliveryDisposition: Schema.NullOr(Schema.String),
  deliveryGeneration: Schema.NullOr(Schema.Number),
  providerThreadRowId: Schema.NullOr(ProviderThreadId),
  providerSessionId: Schema.NullOr(ProviderSessionId),
  pendingBackgroundTaskCount: Schema.NullOr(Schema.Number),
});
type RunningRunRow = typeof RunningRunRow.Type;
type ObservationState = readonly [
  RunStateLog.RunStateLogEvent,
  ProviderThreadId | null,
  ProviderSessionId | null,
];

// Ids are plain strings, so encoding cannot throw.
const encodeIdList = (values: ReadonlyArray<string>) => JSON.stringify(values);

const parseMs = (value: string | null) => {
  if (value === null) return undefined;
  const ms = Date.parse(value);
  return Number.isNaN(ms) ? undefined : ms;
};

const make = Effect.gen(function* () {
  // Repair still runs outside the thread dispatch lock.
  const repairEnabled = yield* Config.Boolean("T3CODE_RECONCILER_REPAIR").pipe(
    Config.withDefault(false),
    Effect.orElseSucceed(() => false),
  );
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const eventSink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const sessions = yield* ProviderSessions.ProviderSessionManagerV2;
  const continuations = yield* ProviderContinuationRequests.ProviderContinuationRequests;
  const runStateLog = yield* RunStateLog.RunStateLog;
  const backoff = yield* Ref.make(
    new Map<RunId, { readonly failures: number; readonly retryAtMs: number }>(),
  );
  const scanLock = yield* Semaphore.make(1);
  let scanAfter: Pick<RunningRunRow, "threadId" | "rowId"> | undefined;
  const observed = new Map<
    RunId,
    { readonly state: ObservationState; readonly expiresAtMs: number }
  >();

  const mapError =
    (operation: WedgedRunReconcilerError["operation"], threadId?: ThreadId, runId?: RunId) =>
    (cause: unknown) =>
      new WedgedRunReconcilerError({
        operation,
        ...(threadId === undefined ? {} : { threadId }),
        ...(runId === undefined ? {} : { runId }),
        cause,
      });

  // The IN term matches the recovery index predicate. Its implicit rowid breaks ties within a thread.
  const scanRunningRuns = (after: typeof scanAfter) =>
    sql`
        SELECT
          r.thread_id AS "threadId",
          r.rowid AS "rowId",
          r.run_id AS "runId",
          r.ordinal AS "ordinal",
          r.provider_thread_id AS "providerThreadId",
          json_extract(r.payload_json, '$.startedAt') AS "runStartedAt",
          json_extract(r.payload_json, '$.activeAttemptId') AS "activeAttemptId",
          a.status AS "attemptStatus",
          json_extract(a.payload_json, '$.startedAt') AS "attemptStartedAt",
          json_extract(a.payload_json, '$.completedAt') AS "attemptCompletedAt",
          json_extract(r.payload_json, '$.delegatedCompletion.disposition') AS "deliveryDisposition",
          json_extract(r.payload_json, '$.delegatedCompletion.delivery.generation') AS "deliveryGeneration",
          pt.provider_thread_id AS "providerThreadRowId",
          pt.provider_session_id AS "providerSessionId",
          json_array_length(pt.payload_json, '$.pendingBackgroundTasks') AS "pendingBackgroundTaskCount"
        FROM orchestration_v2_projection_runs r
        LEFT JOIN orchestration_v2_projection_run_attempts a
          ON a.attempt_id = json_extract(r.payload_json, '$.activeAttemptId')
        LEFT JOIN orchestration_v2_projection_provider_threads pt
          ON pt.provider_thread_id = r.provider_thread_id
        WHERE r.status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
          AND r.status = 'running'
          AND (r.thread_id, r.rowid) > (${after?.threadId ?? ""}, ${after?.rowId ?? 0})
        ORDER BY r.thread_id, r.rowid
        LIMIT ${MAX_RUNS_PER_TICK}
      `.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(RunningRunRow))),
      Effect.mapError(mapError("scan")),
    );

  // Each read is keyed by run, attempt or thread index; none scans a thread's turn items.
  const inspectRun = Effect.fnUntraced(function* (row: RunningRunRow) {
    const attempts = yield* sql<{ readonly attemptId: string; readonly status: string }>`
      SELECT attempt_id AS "attemptId", status
      FROM orchestration_v2_projection_run_attempts
      WHERE run_id = ${row.runId}
      ORDER BY attempt_ordinal
    `;
    const attemptIds = attempts.map((attempt) => attempt.attemptId);
    const providerTurns = yield* sql<{ readonly status: string }>`
      SELECT status
      FROM orchestration_v2_projection_provider_turns
      WHERE run_attempt_id IN (SELECT value FROM json_each(${encodeIdList(attemptIds)}))
      ORDER BY ordinal DESC
      LIMIT 5
    `;
    const pendingRequests = yield* sql<{ readonly found: number }>`
      SELECT 1 AS found
      FROM orchestration_v2_projection_runtime_requests rr
      JOIN orchestration_v2_projection_nodes n ON n.node_id = rr.node_id
      WHERE rr.thread_id = ${row.threadId}
        AND rr.status = 'pending'
        AND n.run_id = ${row.runId}
      LIMIT 1
    `;
    const waitingCards = yield* sql<{ readonly found: number }>`
      SELECT 1 AS found
      FROM orchestration_v2_projection_turn_items
      WHERE run_id = ${row.runId}
        AND type IN ('approval_request', 'user_input_request')
        AND status = 'waiting'
      LIMIT 1
    `;
    const openWork = yield* sql<{ readonly found: number }>`
      SELECT 1 AS found
      FROM orchestration_v2_projection_turn_items
      WHERE thread_id = ${row.threadId}
        AND type IN ('command_execution', 'dynamic_tool', 'subagent')
        AND status IN ('pending', 'running', 'waiting')
        AND run_id = ${row.runId}
      LIMIT 1
    `;
    // Kinds, statuses and times only; item bodies stay out of the record.
    const recentItems = yield* sql<{
      readonly type: string;
      readonly status: string;
      readonly updatedAt: string;
    }>`
      SELECT type, status, updated_at AS "updatedAt"
      FROM orchestration_v2_projection_turn_items
      WHERE run_id = ${row.runId}
      ORDER BY ordinal DESC
      LIMIT ${RECENT_ITEM_COUNT}
    `;
    const queue = yield* sql<{ readonly queued: number; readonly held: number | null }>`
      SELECT
        COUNT(*) AS queued,
        SUM(CASE WHEN json_extract(payload_json, '$.queueHeld') = 1 THEN 1 ELSE 0 END) AS held
      FROM orchestration_v2_projection_runs
      WHERE thread_id = ${row.threadId} AND status = 'queued'
    `;
    // Without the provider thread row the session cannot be checked, so it stays unknown.
    const session =
      row.providerThreadId === null || row.providerThreadRowId === null
        ? ("unknown" as const)
        : row.providerSessionId === null
          ? ("none" as const)
          : (yield* sessions.hasBusyTurn({
                providerSessionId: row.providerSessionId,
                providerThreadId: row.providerThreadId,
                runOrdinal: row.ordinal,
              }))
            ? ("busy" as const)
            : (yield* sessions.isLive(row.providerSessionId))
              ? ("live" as const)
              : ("none" as const);
    return {
      attemptStatuses: attempts.map((attempt) => attempt.status),
      attemptsSettled: attempts.every((attempt) => isSettledAttemptStatus(attempt.status)),
      providerTurnStatuses: providerTurns.map((turn) => turn.status),
      liveProviderTurn: providerTurns.some(
        (turn) => turn.status === "pending" || turn.status === "running",
      ),
      waitingOnUser: pendingRequests.length > 0 || waitingCards.length > 0,
      openWork: openWork.length > 0 || (row.pendingBackgroundTaskCount ?? 0) > 0,
      session,
      queue: { queued: queue[0]?.queued ?? 0, held: queue[0]?.held ?? 0 },
      recentItems,
    };
  });
  type Inspection = Effect.Success<ReturnType<typeof inspectRun>>;

  /** Ends the run with its attempt's outcome, gated on the run still being this attempt's and running. */
  const finalizeRun = Effect.fn("WedgedRunReconciler.finalizeRun")(function* (
    row: RunningRunRow,
    attemptId: RunAttemptId,
    status: TerminalAttemptStatus,
  ) {
    const { runs, attempts, providerThreads } = yield* projections.getThreadRecords(
      row.threadId,
      ["runs", "attempts", "providerThreads"],
      {
        runIds: [row.runId],
        attemptIds: [attemptId],
        providerThreadIds: row.providerThreadId === null ? [] : [row.providerThreadId],
      },
    );
    const run = runs[0];
    const attempt = attempts.find((candidate) => candidate.id === attemptId);
    if (run === undefined || attempt === undefined || run.rootNodeId === null) {
      return { committed: false, run: undefined } as const;
    }
    const { nodes } = yield* projections.getThreadRecords(row.threadId, ["nodes"], {
      nodeIds: [run.rootNodeId],
    });
    const rootNode = nodes.find((node) => node.id === run.rootNodeId);
    const providerThread = providerThreads.find(
      (candidate) => candidate.id === run.providerThreadId,
    );
    const now = yield* DateTime.now;
    // The attempt's own end time, so the repair never lengthens the recorded run.
    const completedAt = attempt.completedAt ?? now;
    const allocateEventId = () => ids.allocate.event({ threadId: row.threadId });
    const base = {
      threadId: row.threadId,
      runId: run.id,
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
    };
    // Omit delegatedCompletion so the projection keeps the stored cohort and its open delivery.
    const { delegatedCompletion: _delegatedCompletion, ...runWithoutDelegatedCompletion } = run;
    const events: Array<OrchestrationV2DomainEvent> = [];
    if (rootNode !== undefined && OPEN_NODE_STATUSES.has(rootNode.status)) {
      events.push({
        ...base,
        id: yield* allocateEventId(),
        type: "node.updated",
        nodeId: rootNode.id,
        payload: { ...rootNode, status, completedAt },
      });
    }
    if (providerThread?.status === "active" && providerThread.lastRunOrdinal === run.ordinal) {
      events.push({
        ...base,
        id: yield* allocateEventId(),
        type: "provider-thread.updated",
        payload: { ...providerThread, status: "idle", updatedAt: now },
      });
    }
    events.push({
      ...base,
      id: yield* allocateEventId(),
      type: "run.updated",
      ...(rootNode === undefined ? {} : { nodeId: rootNode.id }),
      payload: { ...runWithoutDelegatedCompletion, status, completedAt },
    });
    const scopeId = rootNode?.checkpointScopeId ?? null;
    const effects: Array<EffectOutbox.PendingOrchestrationEffectV2> =
      scopeId !== null && status !== "failed"
        ? [
            {
              id: `effect:checkpoint.capture:${run.id}`,
              commandId: CommandId.make(`command:effect:checkpoint.capture:${run.id}`),
              threadId: row.threadId,
              request: { type: "checkpoint.capture", runId: run.id, scopeId },
            },
          ]
        : [];
    const result = yield* eventSink.writeIfRunCurrent({
      // Not `command:runtime-reconcile:`: the orchestrator ignores those terminals, and this one
      // must reach its run-update reaction so the thread's queue starts again.
      commandId: CommandId.make(`command:wedged-run-reconcile:${run.id}:${attemptId}`),
      threadId: row.threadId,
      runId: run.id,
      activeAttemptId: attemptId,
      expectedStatus: "running",
      // A stored Stop without its result settles the run itself.
      unlessInterruptRequested: {
        requestId: ids.derive.runSignalTurnItem({ runId: run.id, signal: "interrupt-request" }),
        resultId: ids.derive.runSignalTurnItem({ runId: run.id, signal: "interrupt-result" }),
      },
      events,
      effects,
    });
    return { committed: result.committed, run, providerThread } as const;
  });

  // The delivery the run kept open never reached the provider; offer it again as a new turn.
  const redeliver = Effect.fn("WedgedRunReconciler.redeliver")(function* (
    run: OrchestrationV2Run,
    providerThread: OrchestrationV2ProviderThread | undefined,
  ) {
    const { runs } = yield* projections.getThreadRecords(run.threadId, ["runs"], {
      runIds: [run.id],
    });
    const cohort = runs[0]?.delegatedCompletion;
    const delivery = cohort?.delivery;
    if (cohort?.disposition !== "open" || delivery == null || providerThread === undefined) {
      return false;
    }
    yield* continuations.offer({
      threadId: run.threadId,
      providerThreadId: providerThread.id,
      driver: providerThread.driver,
      detail: null,
      delivery: "message_text",
      delegatedCompletion: {
        parentRunId: run.id,
        generation: delivery.generation,
        messageId: delivery.messageId,
      },
    });
    return true;
  });

  const report = Effect.fnUntraced(function* (input: {
    readonly event: "reconciler.repair" | "reconciler.observe";
    readonly reason: WedgedRunReason;
    readonly rule: string;
    readonly action: WedgedRunAction;
    readonly row: RunningRunRow;
    readonly inspection: Inspection | undefined;
    readonly sinceMs: number;
    readonly nowMs: number;
    readonly wouldRepair?: true;
  }) {
    const { row, inspection } = input;
    const state = {
      event: input.event,
      reason: input.reason,
      rule: input.rule,
      action: input.action,
      threadId: row.threadId,
      runId: row.runId,
      attemptId: row.activeAttemptId,
      runStatus: "running",
      attemptStatus: row.attemptStatus,
      attemptStatuses: inspection?.attemptStatuses ?? null,
      providerTurnStatuses: inspection?.providerTurnStatuses ?? null,
      session: inspection?.session ?? null,
      waitingOnUser: inspection?.waitingOnUser ?? null,
      openWork: inspection?.openWork ?? null,
      deliveryDisposition: row.deliveryDisposition,
      deliveryGeneration: row.deliveryGeneration,
      queued: inspection?.queue.queued ?? null,
      queueHeld: inspection?.queue.held ?? null,
      recentItems: inspection?.recentItems ?? null,
      wedgedSince: DateTime.formatIso(DateTime.makeUnsafe(input.sinceMs)),
      ...(input.wouldRepair === true ? { wouldRepair: true } : {}),
    };
    if (input.event === "reconciler.observe") {
      const snapshot = [state, row.providerThreadId, row.providerSessionId] as const;
      const previous = observed.get(row.runId);
      if (
        previous !== undefined &&
        previous.expiresAtMs > input.nowMs &&
        Equal.equals(previous.state, snapshot)
      ) {
        return false;
      }
      observed.delete(row.runId);
      observed.set(row.runId, {
        state: snapshot,
        expiresAtMs: input.nowMs + OBSERVATION_REPEAT_MS,
      });
    }
    const record = { ...state, ageMs: input.nowMs - input.sinceMs };
    yield* input.event === "reconciler.repair"
      ? Effect.logWarning(input.event, record)
      : Effect.logInfo(input.event, record);
    yield* runStateLog.recordEvent(record);
    return true;
  });

  const backOff = (runId: RunId, nowMs: number) =>
    Ref.update(backoff, (current) => {
      const failures = (current.get(runId)?.failures ?? 0) + 1;
      const next = new Map(current);
      next.set(runId, {
        failures,
        retryAtMs: nowMs + Math.min(BACKOFF_BASE_MS * 2 ** (failures - 1), BACKOFF_MAX_MS),
      });
      return next;
    });

  const clearBackOff = (runId: RunId) =>
    Ref.update(backoff, (current) => {
      if (!current.has(runId)) return current;
      const next = new Map(current);
      next.delete(runId);
      return next;
    });

  const reconcile = Effect.fn("WedgedRunReconciler.reconcile", { attributes: routineSpan })(
    function* () {
      const nowMs = yield* Clock.currentTimeMillis;
      const rows = yield* scanRunningRuns(scanAfter);
      const actions: Array<WedgedRunReconcileSummary["actions"][number]> = [];
      for (const [runId, previous] of observed) {
        if (previous.expiresAtMs > nowMs) break;
        observed.delete(runId);
      }
      let scanned = 0;
      for (const row of rows) {
        if (actions.length >= MAX_ACTIONS_PER_TICK) break;
        scanAfter = row;
        scanned++;
        const retry = (yield* Ref.get(backoff)).get(row.runId);
        if (retry !== undefined && retry.retryAtMs > nowMs) continue;
        const attemptId = row.activeAttemptId;
        const attemptStatus = row.attemptStatus;
        if (attemptId === null || attemptStatus === null) {
          observed.delete(row.runId);
          continue;
        }
        const terminal = isTerminalAttemptStatus(attemptStatus);
        const sinceMs = terminal
          ? (parseMs(row.attemptCompletedAt) ?? parseMs(row.runStartedAt))
          : (parseMs(row.attemptStartedAt) ?? parseMs(row.runStartedAt));
        if (
          sinceMs === undefined ||
          nowMs - sinceMs < (terminal ? REPAIR_GRACE_MS : STALE_OBSERVE_MS)
        ) {
          observed.delete(row.runId);
          continue;
        }
        const rule = terminal
          ? `attempt ${attemptStatus} and run running for over ${REPAIR_GRACE_MS} ms`
          : `attempt ${attemptStatus} with no live work for over ${STALE_OBSERVE_MS} ms`;

        const observe = Effect.fnUntraced(function* (
          reason: WedgedRunReason,
          inspection: Inspection | undefined,
          wouldRepair?: true,
        ) {
          const reported = yield* report({
            event: "reconciler.observe",
            reason,
            rule,
            action: "none",
            row,
            inspection,
            sinceMs,
            nowMs,
            ...(wouldRepair === true ? { wouldRepair } : {}),
          });
          if (reported) {
            actions.push({
              threadId: row.threadId,
              runId: row.runId,
              attemptId,
              event: "reconciler.observe",
              reason,
              action: "none",
            });
          }
        });

        const inspection = yield* inspectRun(row).pipe(
          Effect.mapError(mapError("inspect", row.threadId, row.runId)),
          Effect.tapCause((cause) =>
            Effect.logWarning("reconciler.inspection-failed", {
              threadId: row.threadId,
              runId: row.runId,
              cause,
            }),
          ),
          Effect.catchCause(() => Effect.undefined),
        );
        if (inspection === undefined) {
          yield* backOff(row.runId, nowMs);
          yield* observe("inspection_failed", undefined);
          continue;
        }
        if (!terminal) {
          // A live turn or a busy session is a healthy long run; only silence with nothing open is noted.
          if (
            inspection.liveProviderTurn ||
            inspection.session === "busy" ||
            inspection.openWork ||
            inspection.waitingOnUser
          ) {
            observed.delete(row.runId);
          } else {
            yield* observe("attempt_running_no_live_work", inspection);
          }
          continue;
        }
        const uncertain: WedgedRunReason | undefined = inspection.liveProviderTurn
          ? "attempt_terminal_provider_turn_live"
          : inspection.session === "busy"
            ? "attempt_terminal_session_busy"
            : inspection.session === "live"
              ? "attempt_terminal_session_live"
              : inspection.session === "unknown"
                ? "attempt_terminal_session_unknown"
                : !inspection.attemptsSettled
                  ? "attempt_terminal_attempts_unsettled"
                  : inspection.waitingOnUser
                    ? "attempt_terminal_waiting_on_user"
                    : undefined;
        if (uncertain !== undefined) {
          yield* observe(uncertain, inspection);
          continue;
        }
        if (!repairEnabled) {
          yield* observe("attempt_terminal_run_running", inspection, true);
          continue;
        }

        const outcome = yield* Effect.gen(function* () {
          const finalized = yield* finalizeRun(row, attemptId, attemptStatus);
          if (!finalized.committed || finalized.run === undefined) return "refused" as const;
          const redelivered = yield* redeliver(finalized.run, finalized.providerThread);
          return redelivered ? ("finalized-and-redelivered" as const) : ("finalized" as const);
        }).pipe(
          Effect.mapError(mapError("finalize", row.threadId, row.runId)),
          Effect.tapCause((cause) =>
            Effect.logWarning("reconciler.repair-failed", {
              threadId: row.threadId,
              runId: row.runId,
              cause,
            }),
          ),
          Effect.catchCause(() => Effect.succeed("failed" as const)),
        );
        if (outcome === "refused" || outcome === "failed") {
          // A refused write repeats each tick while a Stop is pending, so it backs off too.
          yield* backOff(row.runId, nowMs);
          yield* observe(
            outcome === "refused"
              ? "attempt_terminal_write_refused"
              : "attempt_terminal_repair_failed",
            inspection,
          );
          continue;
        }
        yield* clearBackOff(row.runId);
        observed.delete(row.runId);
        yield* report({
          event: "reconciler.repair",
          reason: "attempt_terminal_run_running",
          rule,
          action: outcome,
          row,
          inspection,
          sinceMs,
          nowMs,
        });
        actions.push({
          threadId: row.threadId,
          runId: row.runId,
          attemptId,
          event: "reconciler.repair",
          reason: "attempt_terminal_run_running",
          action: outcome,
        });
      }
      if (scanned === rows.length && rows.length < MAX_RUNS_PER_TICK) scanAfter = undefined;
      return { scanned, actions } satisfies WedgedRunReconcileSummary;
    },
  );

  return WedgedRunReconciler.of({ reconcile: scanLock.withPermits(1)(reconcile()) });
});

export const layer = Layer.effect(WedgedRunReconciler, make);

/** Runs right after startup recovery, then every minute on the shared scheduler's ticks. */
export const layerWorker = Layer.effectDiscard(
  Effect.gen(function* () {
    const reconciler = yield* WedgedRunReconciler;
    const scheduler = yield* Scheduler.Scheduler;
    const lastRunAt = yield* Ref.make<number | undefined>(undefined);
    yield* scheduler.register(
      "wedged-run-reconciler",
      Effect.gen(function* () {
        const nowMs = yield* Clock.currentTimeMillis;
        const last = yield* Ref.get(lastRunAt);
        if (last !== undefined && nowMs - last < TICK_INTERVAL_MS) return;
        yield* Ref.set(lastRunAt, nowMs);
        yield* reconciler.reconcile;
      }),
    );
  }),
);
