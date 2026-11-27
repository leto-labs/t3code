import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2DomainEvent,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../config.ts";
import * as McpSessionRegistryTestkit from "../mcp/McpSessionRegistry.testkit.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectEnrichmentService from "../project/ProjectEnrichmentService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ModelManifest from "../provider/ModelManifest.ts";
import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderContinuationService from "./ProviderContinuationService.ts";
import * as ProviderSessions from "./ProviderSessionManager.ts";
import * as ProviderTurnStartServiceTestkit from "./ProviderTurnStartService.testkit.ts";
import * as RunStateLog from "./RunStateLog.ts";
import * as RuntimeLayer from "./runtimeLayer.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as WedgedRunReconciler from "./WedgedRunReconciler.ts";

const layerPlatformTest = Layer.merge(
  Layer.mergeAll(NodeServices.layer, NodeCrypto.layer),
  Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
    resolveLink: () => Effect.die("unused title link"),
  }),
);
const layerServerConfig = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-orchestration-v2-wedged-run-",
});
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;
const driver = ProviderDriverKind.make("codex");
const orchestrationAdapter = {
  instanceId: modelSelection.instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("sessions are not used by wedged run tests"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const providerInstance = {
  instanceId: modelSelection.instanceId,
  driverKind: driver,
  continuationIdentity: { driverKind: driver, continuationKey: "codex:test" },
  displayName: "Codex test",
  enabled: true,
  snapshot: { getSnapshot: Effect.succeed({}) } as unknown as ProviderInstance["snapshot"],
  orchestrationAdapter,
  textGeneration: {} as ProviderInstance["textGeneration"],
} satisfies ProviderInstance;

const layerOrchestration = Layer.mergeAll(
  RuntimeLayer.layer,
  RuntimeLayer.layerEventSink,
  ThreadCommandExecutor.layer,
  EffectOutbox.layer,
  ProjectionStore.layer,
).pipe(
  Layer.provideMerge(RuntimeLayer.layerProjectService),
  Layer.provide(
    Layer.mock(WorkspacePaths.WorkspacePaths)({
      normalizeWorkspaceRoot: (workspaceRoot) => Effect.succeed(workspaceRoot),
    }),
  ),
  Layer.provide(ProviderTurnStartServiceTestkit.layer),
  Layer.provide(
    Layer.succeed(ProjectEnrichmentService.ProjectEnrichmentService, {
      peek: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      request: () => Effect.void,
      readRepositoryIdentity: () => Effect.succeed(null),
      getAvailable: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      invalidate: () => Effect.void,
      refreshRepositoryIdentity: () => Effect.void,
      subscribeChanges: Effect.never,
    }),
  ),
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provide(McpProviderSessions.layer),
  Layer.provideMerge(SqlitePersistence.layerMemory),
  Layer.provide(
    CheckpointStore.layer.pipe(
      Layer.provide(VcsDriverRegistry.layer),
      Layer.provide(VcsProcess.layer),
      Layer.provide(layerServerConfig),
      Layer.provide(layerPlatformTest),
    ),
  ),
  Layer.provide(layerServerConfig),
  Layer.provide(ServerSettings.layerTest()),
  Layer.provide(
    Layer.merge(
      ModelManifest.layerTest,
      Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, {
        getInstance: (instanceId) =>
          Effect.succeed(instanceId === providerInstance.instanceId ? providerInstance : undefined),
        listInstances: Effect.succeed([providerInstance]),
        listUnavailable: Effect.succeed([]),
        streamChanges: Stream.empty,
        subscribeChanges: Effect.never,
      }),
    ),
  ),
  Layer.provide(layerPlatformTest),
);

// The continuation worker and the reconciler share the orchestrator's request queue, as in production.
const LIVE_SESSION_ID = ProviderSessionId.make("provider-session:live");
// Tests read their own run's records; every run id is unique to its test.
const recordedEvents: Array<RunStateLog.RunStateLogEvent> = [];
const layerRecordedRunStateLog = Layer.succeed(RunStateLog.RunStateLog, {
  record: () => Effect.void,
  recordEvent: (event) =>
    Effect.sync(() => {
      recordedEvents.push(event);
    }),
});
// The real manager, with one session that stays open after its turn ended.
const layerSessionsWithLiveSession = Layer.effect(
  ProviderSessions.ProviderSessionManagerV2,
  Effect.gen(function* () {
    const sessions = yield* ProviderSessions.ProviderSessionManagerV2;
    return ProviderSessions.ProviderSessionManagerV2.of({
      ...sessions,
      isLive: (providerSessionId) =>
        providerSessionId === LIVE_SESSION_ID
          ? Effect.succeed(true)
          : sessions.isLive(providerSessionId),
    });
  }),
);

const layerTest = Layer.mergeAll(
  ProviderContinuationService.layer,
  WedgedRunReconciler.layer.pipe(
    Layer.provide(Layer.merge(layerSessionsWithLiveSession, layerRecordedRunStateLog)),
  ),
).pipe(
  Layer.provide(Layer.merge(ProviderContinuationRequests.layer, IdAllocator.layer)),
  Layer.provideMerge(layerOrchestration),
);

interface Seed {
  readonly key: string;
  readonly attemptStatus: "running" | "completed";
  /** How long ago the attempt ended (or started, while running). */
  readonly ageMs: number;
  readonly openDelivery?: boolean;
  readonly liveProviderTurn?: boolean;
  readonly pendingQuestion?: boolean;
  /** Codex Auto: placeholder provider thread id, no provider turn id. */
  readonly codexPlaceholder?: boolean;
  /** The run's provider session is still open. */
  readonly liveSession?: boolean;
}

const seedWedgedRun = (seed: Seed) =>
  Effect.gen(function* () {
    const projects = yield* ProjectService.ProjectService;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const threadId = ThreadId.make(`thread:${seed.key}`);
    const projectId = ProjectId.make(`project:${seed.key}`);
    const runId = RunId.make(`run:${seed.key}`);
    const attemptId = RunAttemptId.make(`attempt:${seed.key}:1`);
    const rootNodeId = NodeId.make(`node:${seed.key}:root`);
    const taskId = NodeId.make(`node:${seed.key}:task`);
    const deliveryMessageId = MessageId.make(`message:${seed.key}:delivery`);
    const providerThreadId = ProviderThreadId.make(
      seed.codexPlaceholder === true
        ? `native-thread:pending-${seed.key}`
        : `provider-thread:${seed.key}`,
    );
    yield* projects.create({
      commandId: CommandId.make(`command:${seed.key}:project`),
      projectId,
      title: "Wedged run",
      workspaceRoot: `/workspace/${seed.key}`,
    });
    yield* orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make(`command:${seed.key}:thread`),
      threadId,
      projectId,
      title: "Wedged run",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });
    const now = yield* DateTime.now;
    const attemptTime = now;
    const base = { threadId, providerInstanceId: modelSelection.instanceId };
    const events: Array<OrchestrationV2DomainEvent> = [
      {
        ...base,
        id: EventId.make(`event:${seed.key}:provider-thread`),
        type: "provider-thread.updated",
        occurredAt: attemptTime,
        payload: {
          id: providerThreadId,
          driver,
          providerInstanceId: modelSelection.instanceId,
          providerSessionId: seed.liveSession === true ? LIVE_SESSION_ID : null,
          appThreadId: threadId,
          ownerNodeId: rootNodeId,
          nativeThreadRef:
            seed.codexPlaceholder === true
              ? null
              : { driver, nativeId: `native:${seed.key}`, strength: "strong" },
          nativeConversationHeadRef: null,
          status: "active",
          firstRunOrdinal: 1,
          lastRunOrdinal: 1,
          handoffIds: [],
          forkedFrom: null,
          createdAt: attemptTime,
          updatedAt: attemptTime,
        },
      },
      {
        ...base,
        id: EventId.make(`event:${seed.key}:root`),
        type: "node.updated",
        runId,
        nodeId: rootNodeId,
        occurredAt: attemptTime,
        payload: {
          id: rootNodeId,
          threadId,
          runId,
          parentNodeId: null,
          rootNodeId,
          kind: "root_turn",
          status: "running",
          countsForRun: true,
          providerThreadId,
          providerTurnId: null,
          nativeItemRef: null,
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt: attemptTime,
          completedAt: null,
        },
      },
      {
        ...base,
        id: EventId.make(`event:${seed.key}:run`),
        type: "run.updated",
        runId,
        nodeId: rootNodeId,
        occurredAt: attemptTime,
        payload: {
          id: runId,
          threadId,
          ordinal: 1,
          providerInstanceId: modelSelection.instanceId,
          modelSelection,
          providerThreadId,
          userMessageId: MessageId.make(`message:${seed.key}:user`),
          rootNodeId,
          activeAttemptId: attemptId,
          status: "running",
          requestedAt: attemptTime,
          startedAt: attemptTime,
          completedAt: null,
          checkpointId: null,
          contextHandoffId: null,
          ...(seed.openDelivery === true
            ? {
                delegatedCompletion: {
                  disposition: "open" as const,
                  nextGeneration: 2,
                  delivery: { generation: 1, messageId: deliveryMessageId, taskIds: [taskId] },
                },
              }
            : {}),
        },
      },
      {
        ...base,
        id: EventId.make(`event:${seed.key}:attempt`),
        type: "run-attempt.updated",
        runId,
        nodeId: rootNodeId,
        occurredAt: attemptTime,
        payload: {
          id: attemptId,
          ...(seed.codexPlaceholder === true ? { nativeThreadId: `native:${seed.key}` } : {}),
          runId,
          attemptOrdinal: 1,
          rootNodeId,
          providerInstanceId: modelSelection.instanceId,
          providerThreadId,
          providerTurnId: null,
          reason: "initial",
          status: seed.attemptStatus,
          startedAt: attemptTime,
          completedAt: seed.attemptStatus === "completed" ? attemptTime : null,
        },
      },
    ];
    if (seed.liveProviderTurn === true) {
      events.push({
        ...base,
        id: EventId.make(`event:${seed.key}:turn`),
        type: "provider-turn.updated",
        runId,
        nodeId: rootNodeId,
        occurredAt: attemptTime,
        payload: {
          id: ProviderTurnId.make(`turn:${seed.key}`),
          providerThreadId,
          nodeId: rootNodeId,
          runAttemptId: attemptId,
          nativeTurnRef: null,
          ordinal: 1,
          status: "running",
          startedAt: attemptTime,
          completedAt: null,
        },
      });
    }
    if (seed.pendingQuestion === true) {
      events.push({
        ...base,
        id: EventId.make(`event:${seed.key}:question`),
        type: "runtime-request.updated",
        runId,
        nodeId: rootNodeId,
        occurredAt: attemptTime,
        payload: {
          id: RuntimeRequestId.make(`request:${seed.key}`),
          nodeId: rootNodeId,
          providerTurnId: null,
          nativeRequestRef: null,
          kind: "user_input",
          status: "pending",
          responseCapability: { type: "message" },
          createdAt: attemptTime,
          resolvedAt: null,
        },
      });
    }
    if (seed.openDelivery === true) {
      events.push({
        ...base,
        id: EventId.make(`event:${seed.key}:task`),
        type: "subagent.updated",
        driver,
        runId,
        nodeId: taskId,
        occurredAt: attemptTime,
        payload: {
          id: taskId,
          threadId,
          runId,
          parentNodeId: rootNodeId,
          origin: "app_owned",
          createdBy: "agent",
          driver,
          providerInstanceId: modelSelection.instanceId,
          providerThreadId: null,
          childThreadId: null,
          nativeTaskRef: null,
          prompt: "Finish the merge train.",
          title: null,
          model: null,
          completionWake: "always",
          completionDelivery: { state: "claimed", observedByRunId: null },
          status: "completed",
          result: "finished",
          startedAt: attemptTime,
          completedAt: attemptTime,
          updatedAt: attemptTime,
        },
      });
    }
    yield* sink.write({ commandId: CommandId.make(`command:${seed.key}:seed`), events });
    // A user message queued behind the wedged run.
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make(`command:${seed.key}:queued`),
      threadId,
      messageId: MessageId.make(`message:${seed.key}:queued`),
      text: "Next request",
      attachments: [],
      modelSelection,
      dispatchMode: { type: "queue_after_active" },
    });
    const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
      (run) => run.id !== runId,
    );
    assert.equal(queued?.status, "queued");
    yield* TestClock.adjust(`${seed.ageMs} millis`);
    return { threadId, runId, attemptId, attemptTime, deliveryMessageId, queuedRunId: queued!.id };
  });

const actionsFor = (summary: WedgedRunReconciler.WedgedRunReconcileSummary, runId: RunId) =>
  summary.actions
    .filter((action) => action.runId === runId)
    .map((action) => `${action.event}:${action.reason}:${action.action}`);

it.layer(layerTest)("WedgedRunReconciler", (it) => {
  it.effect("re-drives the run-5110 state: finalizes, redelivers the wake, starts the queue", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const reconciler = yield* WedgedRunReconciler.WedgedRunReconciler;
      const seeded = yield* seedWedgedRun({
        key: "run-5110",
        attemptStatus: "completed",
        ageMs: 11 * 60_000,
        openDelivery: true,
      });
      const afterSequence = yield* sink.latestSequence();

      const summary = yield* reconciler.reconcile;
      assert.deepStrictEqual(actionsFor(summary, seeded.runId), [
        "reconciler.repair:attempt_terminal_run_running:finalized-and-redelivered",
      ]);
      const [record, ...others] = recordedEvents.filter((event) => event.runId === seeded.runId);
      assert.deepStrictEqual(others, []);
      assert.deepInclude(record, {
        event: "reconciler.repair",
        reason: "attempt_terminal_run_running",
        action: "finalized-and-redelivered",
        threadId: seeded.threadId,
        attemptId: seeded.attemptId,
        runStatus: "running",
        attemptStatus: "completed",
        attemptStatuses: ["completed"],
        providerTurnStatuses: [],
        session: "none",
        deliveryDisposition: "open",
        deliveryGeneration: 1,
        queued: 1,
        queueHeld: 0,
        ageMs: 11 * 60_000,
      });
      assert.isString(record?.rule);
      assert.isString(record?.wedgedSince);

      // The wake becomes a turn of its own, and the run that queued behind the wedge starts.
      yield* sink.stream({ afterSequence, eventType: "run.created" }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.created" &&
            stored.event.payload.userMessageId === seeded.deliveryMessageId,
        ),
        Stream.take(1),
        Stream.runDrain,
      );
      yield* sink.stream({ afterSequence, eventType: "run.updated" }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" &&
            stored.event.payload.id === seeded.queuedRunId &&
            stored.event.payload.status !== "queued",
        ),
        Stream.take(1),
        Stream.runDrain,
      );
      const projection = yield* orchestrator.getThreadProjection(seeded.threadId);
      const run = projection.runs.find((candidate) => candidate.id === seeded.runId)!;
      assert.equal(run.status, "completed");
      assert.isTrue(DateTime.Equivalence(run.completedAt!, seeded.attemptTime));
      assert.equal(
        projection.attempts.find((attempt) => attempt.id === seeded.attemptId)?.status,
        "completed",
      );

      // Idempotent: the repaired run is no longer a candidate.
      const again = yield* reconciler.reconcile;
      assert.deepStrictEqual(actionsFor(again, seeded.runId), []);
    }),
  );

  it.effect("finalizes a terminal run with no delivery and starts the queue", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const reconciler = yield* WedgedRunReconciler.WedgedRunReconciler;
      const seeded = yield* seedWedgedRun({
        key: "no-delivery",
        attemptStatus: "completed",
        ageMs: 3 * 60_000,
      });
      const afterSequence = yield* sink.latestSequence();
      const summary = yield* reconciler.reconcile;
      assert.deepStrictEqual(actionsFor(summary, seeded.runId), [
        "reconciler.repair:attempt_terminal_run_running:finalized",
      ]);
      yield* sink.stream({ afterSequence, eventType: "run.updated" }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" &&
            stored.event.payload.id === seeded.queuedRunId &&
            stored.event.payload.status !== "queued",
        ),
        Stream.take(1),
        Stream.runDrain,
      );
      const projection = yield* orchestrator.getThreadProjection(seeded.threadId);
      assert.equal(projection.runs.find((run) => run.id === seeded.runId)?.status, "completed");
    }),
  );

  it.effect.each([
    {
      name: "a live long-running attempt",
      seed: { attemptStatus: "running", ageMs: 3 * 60 * 60_000, liveProviderTurn: true },
      expected: [],
    },
    {
      // A Codex Auto run sleeping in one tool call: no turn row, a placeholder thread id.
      name: "a silent Codex Auto attempt (logged only)",
      seed: { attemptStatus: "running", ageMs: 3 * 60 * 60_000, codexPlaceholder: true },
      expected: ["reconciler.observe:attempt_running_no_live_work:none"],
    },
    {
      name: "a run inside the repair grace",
      seed: { attemptStatus: "completed", ageMs: 90_000, openDelivery: true },
      expected: [],
    },
    {
      // Its process may still deliver the turn's last events, so the state is uncertain.
      name: "a terminal attempt whose session is still live",
      seed: {
        attemptStatus: "completed",
        ageMs: 11 * 60_000,
        openDelivery: true,
        liveSession: true,
      },
      expected: ["reconciler.observe:attempt_terminal_session_live:none"],
    },
    {
      name: "a run waiting on a question",
      seed: {
        attemptStatus: "completed",
        ageMs: 11 * 60_000,
        openDelivery: true,
        pendingQuestion: true,
      },
      expected: ["reconciler.observe:attempt_terminal_waiting_on_user:none"],
    },
  ] as const)("leaves $name running", ({ name, seed, expected }) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const reconciler = yield* WedgedRunReconciler.WedgedRunReconciler;
      const seeded = yield* seedWedgedRun({ key: name.replaceAll(" ", "-"), ...seed });
      const summary = yield* reconciler.reconcile;
      assert.deepStrictEqual(actionsFor(summary, seeded.runId), [...expected]);
      assert.deepStrictEqual(
        recordedEvents
          .filter((event) => event.runId === seeded.runId)
          .map((event) => `${event.event}:${String(event.reason)}:${String(event.action)}`),
        [...expected],
      );
      // An observation logs once while its reason holds.
      const again = yield* reconciler.reconcile;
      assert.deepStrictEqual(actionsFor(again, seeded.runId), []);
      const projection = yield* orchestrator.getThreadProjection(seeded.threadId);
      assert.equal(projection.runs.find((run) => run.id === seeded.runId)?.status, "running");
      assert.equal(projection.runs.find((run) => run.id === seeded.queuedRunId)?.status, "queued");
      assert.equal(
        projection.attempts.find((attempt) => attempt.id === seeded.attemptId)?.status,
        seed.attemptStatus,
      );
    }),
  );

  it.effect("scans running runs through the recovery index", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const plan = yield* sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        SELECT r.run_id
        FROM orchestration_v2_projection_runs r
        LEFT JOIN orchestration_v2_projection_run_attempts a
          ON a.attempt_id = json_extract(r.payload_json, '$.activeAttemptId')
        LEFT JOIN orchestration_v2_projection_provider_threads pt
          ON pt.provider_thread_id = r.provider_thread_id
        WHERE r.status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
          AND r.status = 'running'
      `;
      const details = plan.map((row) => row.detail);
      assert.isTrue(
        details.some((detail) => detail.includes("orchestration_v2_projection_runs_recovery_idx")),
        details.join("\n"),
      );
      assert.isFalse(
        details.some((detail) => /^SCAN (r|a|pt)\b/.test(detail)),
        details.join("\n"),
      );
    }),
  );
});
