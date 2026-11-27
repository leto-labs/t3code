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
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
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
const REPAIR_GRACE_MS = 30_000;
/** Same rule as Core's external watchdog. Observed only: a Codex `sleep` call is silent for hours. */
const STALE_OBSERVE_MS = 10 * 60_000;
const TICK_INTERVAL_MS = 60_000;
const MAX_ACTIONS_PER_TICK = 20;
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

export type WedgedRunAction =
  | "finalized"
  | "finalized-and-redelivered"
  | "skipped-state-changed"
  | "observed-stale"
  | "failed";

export interface WedgedRunReconcileSummary {
  readonly scanned: number;
  readonly actions: ReadonlyArray<{
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly attemptId: RunAttemptId | null;
    readonly action: WedgedRunAction;
  }>;
}

export class WedgedRunReconciler extends Context.Service<
  WedgedRunReconciler,
  {
    /** One pass over running runs. Idempotent: a repaired run is no longer `running`. */
    readonly reconcile: Effect.Effect<WedgedRunReconcileSummary, WedgedRunReconcilerError>;
  }
>()("t3/orchestration-v2/WedgedRunReconciler") {}

const RunningRunRow = Schema.Struct({
  threadId: ThreadId,
  runId: RunId,
  ordinal: Schema.Number,
  providerThreadId: Schema.NullOr(ProviderThreadId),
  runStartedAt: Schema.NullOr(Schema.String),
  activeAttemptId: Schema.NullOr(RunAttemptId),
  attemptStatus: Schema.NullOr(Schema.String),
  attemptStartedAt: Schema.NullOr(Schema.String),
  attemptCompletedAt: Schema.NullOr(Schema.String),
  providerSessionId: Schema.NullOr(ProviderSessionId),
  pendingBackgroundTaskCount: Schema.NullOr(Schema.Number),
});
type RunningRunRow = typeof RunningRunRow.Type;

// Ids are plain strings, so encoding cannot throw.
const encodeIdList = (values: ReadonlyArray<string>) => JSON.stringify(values);

const parseMs = (value: string | null) => {
  if (value === null) return undefined;
  const ms = Date.parse(value);
  return Number.isNaN(ms) ? undefined : ms;
};

const make = Effect.gen(function* () {
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
  // Observed-only runs log once until they leave the stale state.
  const observed = yield* Ref.make(new Set<RunId>());

  const mapError =
    (operation: WedgedRunReconcilerError["operation"], threadId?: ThreadId, runId?: RunId) =>
    (cause: unknown) =>
      new WedgedRunReconcilerError({
        operation,
        ...(threadId === undefined ? {} : { threadId }),
        ...(runId === undefined ? {} : { runId }),
        cause,
      });

  // The repeated IN term is the partial index's own predicate: SQLite uses
  // `orchestration_v2_projection_runs_recovery_idx` only when the query states it verbatim.
  // Attempts and provider threads join on their primary keys.
  const scanRunningRuns = sql`
        SELECT
          r.thread_id AS "threadId",
          r.run_id AS "runId",
          r.ordinal AS "ordinal",
          r.provider_thread_id AS "providerThreadId",
          json_extract(r.payload_json, '$.startedAt') AS "runStartedAt",
          json_extract(r.payload_json, '$.activeAttemptId') AS "activeAttemptId",
          a.status AS "attemptStatus",
          json_extract(a.payload_json, '$.startedAt') AS "attemptStartedAt",
          json_extract(a.payload_json, '$.completedAt') AS "attemptCompletedAt",
          pt.provider_session_id AS "providerSessionId",
          json_array_length(pt.payload_json, '$.pendingBackgroundTasks') AS "pendingBackgroundTaskCount"
        FROM orchestration_v2_projection_runs r
        LEFT JOIN orchestration_v2_projection_run_attempts a
          ON a.attempt_id = json_extract(r.payload_json, '$.activeAttemptId')
        LEFT JOIN orchestration_v2_projection_provider_threads pt
          ON pt.provider_thread_id = r.provider_thread_id
        WHERE r.status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
          AND r.status = 'running'
        ORDER BY r.thread_id, r.run_id
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
    `;
    const attemptIds = attempts.map((attempt) => attempt.attemptId);
    const liveTurns = yield* sql<{ readonly found: number }>`
      SELECT 1 AS found
      FROM orchestration_v2_projection_provider_turns
      WHERE run_attempt_id IN (SELECT value FROM json_each(${encodeIdList(attemptIds)}))
        AND status IN ('pending', 'running')
      LIMIT 1
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
    const busySession =
      row.providerSessionId === null || row.providerThreadId === null
        ? false
        : yield* sessions.hasBusyTurn({
            providerSessionId: row.providerSessionId,
            providerThreadId: row.providerThreadId,
            runOrdinal: row.ordinal,
          });
    return {
      attemptsSettled: attempts.every((attempt) => isSettledAttemptStatus(attempt.status)),
      liveProviderTurn: liveTurns.length > 0,
      waitingOnUser: pendingRequests.length > 0 || waitingCards.length > 0,
      openWork: openWork.length > 0 || (row.pendingBackgroundTaskCount ?? 0) > 0,
      busySession,
    };
  });

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

  const logAction = (input: {
    readonly row: RunningRunRow;
    readonly observedState: string;
    readonly action: WedgedRunAction;
    readonly ageMs: number;
  }) =>
    Effect.logInfo("orchestration.reconciler", {
      threadId: input.row.threadId,
      runId: input.row.runId,
      attemptId: input.row.activeAttemptId,
      observed: input.observedState,
      action: input.action,
      ageMs: input.ageMs,
    }).pipe(
      Effect.andThen(
        runStateLog.record({
          threadId: input.row.threadId,
          runId: input.row.runId,
          attemptId: input.row.activeAttemptId,
          kind: "reconciler",
          from: input.observedState,
          to: input.action,
          cause: `reconciler:age-ms:${input.ageMs}`,
        }),
      ),
    );

  const reconcile = Effect.fn("WedgedRunReconciler.reconcile", { attributes: routineSpan })(
    function* () {
      const nowMs = yield* Clock.currentTimeMillis;
      const rows = yield* scanRunningRuns;
      const actions: Array<WedgedRunReconcileSummary["actions"][number]> = [];
      const stillObserved = new Set<RunId>();
      const previouslyObserved = yield* Ref.get(observed);
      for (const row of rows) {
        if (actions.length >= MAX_ACTIONS_PER_TICK) break;
        const retry = (yield* Ref.get(backoff)).get(row.runId);
        if (retry !== undefined && retry.retryAtMs > nowMs) continue;
        const attemptId = row.activeAttemptId;
        const attemptStatus = row.attemptStatus;
        if (attemptId === null || attemptStatus === null) continue;
        const terminal = isTerminalAttemptStatus(attemptStatus);
        const sinceMs = terminal
          ? (parseMs(row.attemptCompletedAt) ?? parseMs(row.runStartedAt))
          : (parseMs(row.attemptStartedAt) ?? parseMs(row.runStartedAt));
        if (sinceMs === undefined) continue;
        const ageMs = nowMs - sinceMs;
        if (ageMs < (terminal ? REPAIR_GRACE_MS : STALE_OBSERVE_MS)) continue;

        const backOff = Ref.update(backoff, (current) => {
          const failures = (current.get(row.runId)?.failures ?? 0) + 1;
          const next = new Map(current);
          next.set(row.runId, {
            failures,
            retryAtMs: nowMs + Math.min(BACKOFF_BASE_MS * 2 ** (failures - 1), BACKOFF_MAX_MS),
          });
          return next;
        });
        const logFailure = (cause: unknown) =>
          backOff.pipe(
            Effect.andThen(
              Effect.logWarning("orchestration.reconciler.action-failed", {
                threadId: row.threadId,
                runId: row.runId,
                cause,
              }),
            ),
          );
        const inspection = yield* inspectRun(row).pipe(
          Effect.mapError(mapError("inspect", row.threadId, row.runId)),
          Effect.catchCause((cause) => logFailure(cause).pipe(Effect.as(undefined))),
        );
        if (inspection === undefined) continue;
        if (inspection.liveProviderTurn || inspection.busySession) continue;
        if (!terminal) {
          if (inspection.openWork || inspection.waitingOnUser) continue;
          stillObserved.add(row.runId);
          if (previouslyObserved.has(row.runId)) continue;
          yield* logAction({
            row,
            observedState: `run running, attempt ${attemptStatus}, no live turn`,
            action: "observed-stale",
            ageMs,
          });
          actions.push({ ...row, attemptId, action: "observed-stale" });
          continue;
        }
        if (!inspection.attemptsSettled || inspection.waitingOnUser) continue;

        const observedState = `run running, attempt ${attemptStatus}`;
        const outcome = yield* Effect.gen(function* () {
          const finalized = yield* finalizeRun(row, attemptId, attemptStatus);
          if (!finalized.committed || finalized.run === undefined) {
            return "skipped-state-changed" as const;
          }
          const redelivered = yield* redeliver(finalized.run, finalized.providerThread);
          return redelivered ? ("finalized-and-redelivered" as const) : ("finalized" as const);
        }).pipe(
          Effect.mapError(mapError("finalize", row.threadId, row.runId)),
          Effect.catchCause((cause) => logFailure(cause).pipe(Effect.as("failed" as const))),
        );
        // A refused write repeats each tick while a Stop is pending, so it backs off too.
        if (outcome === "skipped-state-changed") {
          yield* backOff;
        } else if (outcome !== "failed") {
          yield* Ref.update(backoff, (current) => {
            if (!current.has(row.runId)) return current;
            const next = new Map(current);
            next.delete(row.runId);
            return next;
          });
        }
        yield* logAction({ row, observedState, action: outcome, ageMs });
        actions.push({ ...row, attemptId, action: outcome });
      }
      yield* Ref.set(observed, stillObserved);
      return { scanned: rows.length, actions } satisfies WedgedRunReconcileSummary;
    },
  );

  return WedgedRunReconciler.of({ reconcile: reconcile() });
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
