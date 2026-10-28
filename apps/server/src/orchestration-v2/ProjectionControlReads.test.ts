import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  NodeId,
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
  TurnItemId,
  type OrchestrationV2DomainEvent,
  PlanId,
} from "@t3tools/contracts";
import { derivePendingThreadRequests } from "../../../../packages/client-runtime/src/state/threadRequests.ts";
import { derivePendingBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import { boundedSnapshotProjection } from "@t3tools/shared/orchestrationV2BoundedSnapshot";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";
import * as RuntimeRequestService from "./RuntimeRequestService.ts";
import { buildBoundedThreadStreamSnapshot } from "./ThreadStream.ts";

const threadId = ThreadId.make("thread:control-reads");
const providerThreadId = ProviderThreadId.make("provider-thread:control-reads");
const providerTurnId = ProviderTurnId.make("provider-turn:control-reads");
const providerSessionId = ProviderSessionId.make("session:control-reads");
const attemptId = RunAttemptId.make("attempt:control-reads");
const runId = RunId.make("run:control-reads");
const messageId = MessageId.make("message:control-reads");
const requestId = RuntimeRequestId.make("request:control-reads");
const nodeId = NodeId.make("node:control-reads");
const providerInstanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId: providerInstanceId, model: "gpt-6" };

function fixtureEvents(now: DateTime.Utc): ReadonlyArray<OrchestrationV2DomainEvent> {
  const common = { threadId, occurredAt: now };
  return [
    {
      ...common,
      id: EventId.make("control:thread"),
      type: "thread.created",
      payload: {
        id: threadId,
        projectId: ProjectId.make("project:control-reads"),
        title: "Control reads",
        providerInstanceId,
        modelSelection,
        createdBy: "user",
        creationSource: "web",
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: "/workspace",
        activeProviderThreadId: providerThreadId,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      },
    },
    {
      ...common,
      id: EventId.make("control:provider-thread"),
      type: "provider-thread.updated",
      payload: {
        id: providerThreadId,
        driver,
        providerInstanceId,
        providerSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver, nativeId: "native-control-thread", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "active",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    },
    {
      ...common,
      id: EventId.make("control:run"),
      type: "run.created",
      payload: {
        id: runId,
        threadId,
        ordinal: 1,
        providerInstanceId,
        modelSelection,
        providerThreadId,
        userMessageId: messageId,
        rootNodeId: nodeId,
        activeAttemptId: attemptId,
        status: "running",
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      },
    },
    {
      ...common,
      id: EventId.make("control:attempt"),
      type: "run-attempt.created",
      payload: {
        id: attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId: nodeId,
        providerInstanceId,
        providerThreadId,
        providerTurnId,
        reason: "initial",
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    },
    {
      ...common,
      id: EventId.make("control:turn"),
      type: "provider-turn.updated",
      payload: {
        id: providerTurnId,
        providerThreadId,
        nodeId,
        runAttemptId: attemptId,
        nativeTurnRef: { driver, nativeId: "native-control-turn", strength: "strong" },
        ordinal: 1,
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    },
    {
      ...common,
      id: EventId.make("control:message"),
      type: "message.updated",
      payload: {
        id: messageId,
        threadId,
        runId,
        nodeId,
        role: "user",
        text: "Use the smaller fix.",
        attachments: [],
        streaming: false,
        createdBy: "user",
        creationSource: "web",
        createdAt: now,
        updatedAt: now,
      },
    },
    {
      ...common,
      id: EventId.make("control:request"),
      type: "runtime-request.updated",
      payload: {
        id: requestId,
        nodeId,
        providerTurnId,
        nativeRequestRef: null,
        kind: "command",
        status: "resolved",
        responseCapability: { type: "live", providerSessionId },
        createdAt: now,
        resolvedAt: now,
      },
    },
  ];
}

const storageCases = [
  {
    storage: "sqlite" as const,
    storeLayer: ProjectionStore.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory)),
  },
];

it.effect.each(storageCases)(
  "$storage: bounds accumulated active plans and preserves the full plan for explicit reads",
  ({ storeLayer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      yield* Effect.forEach(fixtureEvents(now), (event) => store.apply(event), { discard: true });
      const text = "Complete plan contents.\n".repeat(100_000);
      for (let index = 0; index < 40; index++) {
        yield* store.apply({
          id: EventId.make(`control:plan:${index}`),
          type: "plan.updated",
          threadId,
          occurredAt: now,
          payload: {
            id: PlanId.make(`plan:${index}`),
            threadId,
            runId,
            nodeId,
            kind: "proposed_plan",
            status: "active",
            markdown: index === 39 ? text : `Plan ${index}`,
          },
        });
      }
      const snapshot = yield* store.getThreadSnapshotWindow(threadId, { rowLimit: 77 });
      const bounded = buildBoundedThreadStreamSnapshot(snapshot);
      assert.lengthOf(bounded.projection.plans, 1);
      assert.equal(bounded.projection.plans[0]?.id, PlanId.make("plan:39"));
      assert.isTrue(bounded.projection.plans[0]?.detailOmitted);
      assert.isBelow(Buffer.byteLength(JSON.stringify(bounded)), 1_048_576);
      const complete = yield* store.getPlan(threadId, PlanId.make("plan:39"));
      assert.equal(complete?.kind === "proposed_plan" && complete.markdown, text);
      assert.lengthOf((yield* store.getThreadRecords(threadId, ["plans"])).plans, 40);
      assert.isUndefined(yield* store.getPlan(threadId, PlanId.make("missing")));
    }).pipe(Effect.provide(storeLayer)),
);

it.effect.each(storageCases)(
  "$storage: retains a pending secret form and live background work beyond the history window",
  ({ storeLayer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const events = fixtureEvents(now);
      yield* Effect.forEach(events, (event) => store.apply(event), { discard: true });
      const base = {
        threadId,
        runId,
        nodeId,
        providerThreadId,
        providerTurnId,
        nativeItemRef: null,
        parentItemId: null,
        title: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      };
      const secret = {
        ...base,
        id: TurnItemId.make("control:secret"),
        ordinal: 0,
        type: "secret_request" as const,
        status: "waiting" as const,
        label: "Signing key",
        reason: "Authenticate webhook events",
        secretStatus: "pending" as const,
      };
      const work = {
        ...base,
        id: TurnItemId.make("control:background"),
        ordinal: 1,
        type: "command_execution" as const,
        status: "running" as const,
        input: "npm test",
      };
      for (const item of [secret, work]) {
        yield* store.apply({
          id: EventId.make(`event:${item.id}`),
          type: "turn-item.updated",
          threadId,
          occurredAt: now,
          payload: item,
        });
      }
      for (let ordinal = 2; ordinal < 102; ordinal++) {
        yield* store.apply({
          id: EventId.make(`event:recent:${ordinal}`),
          type: "turn-item.updated",
          threadId,
          occurredAt: now,
          payload: {
            ...work,
            id: TurnItemId.make(`control:recent:${ordinal}`),
            ordinal,
            status: "completed",
            completedAt: now,
          },
        });
      }
      for (const compactTurnItems of [false, true]) {
        const snapshot = yield* store.getThreadSnapshotWindow(threadId, { rowLimit: 77 });
        const frame = buildBoundedThreadStreamSnapshot({ ...snapshot, compactTurnItems });
        const projection = boundedSnapshotProjection(frame);
        assert.isTrue(frame.hasMoreHistory);
        assert.isTrue(projection.visibleTurnItems.some((row) => row.sourceItemId === secret.id));
        assert.isTrue(projection.turnItems.some((item) => item.id === work.id));
        assert.deepEqual(
          derivePendingBackgroundWork({
            latestRun: { id: runId, ordinal: 1, status: "completed" },
            providerThreads: [],
            turnItems: projection.turnItems,
          }).map((task) => task.taskId),
          [work.id],
        );
      }
      for (const item of [
        {
          ...secret,
          secretStatus: "declined" as const,
          status: "completed" as const,
          completedAt: now,
        },
        { ...work, status: "completed" as const, completedAt: now },
      ]) {
        yield* store.apply({
          id: EventId.make(`event:resolved:${item.id}`),
          type: "turn-item.updated",
          threadId,
          occurredAt: now,
          payload: item,
        });
      }
      const resolved = buildBoundedThreadStreamSnapshot(
        yield* store.getThreadSnapshotWindow(threadId, { rowLimit: 77 }),
      );
      assert.isFalse(
        resolved.projection.turnItems.some((item) => item.id === secret.id || item.id === work.id),
      );
    }).pipe(Effect.provide(storeLayer)),
);

it.effect.each(
  storageCases.flatMap((storage) =>
    (["message", "live"] as const).map((responseMode) => ({ ...storage, responseMode })),
  ),
)(
  "$storage: keeps older $responseMode questions answerable outside the recent history window",
  ({ storeLayer, responseMode }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      yield* Effect.forEach(fixtureEvents(now), (event) => store.apply(event), { discard: true });
      const request = {
        id: requestId,
        nodeId,
        providerTurnId,
        nativeRequestRef: null,
        kind: "user_input" as const,
        status: "pending" as const,
        responseCapability:
          responseMode === "message"
            ? { type: "message" as const }
            : { type: "live" as const, providerSessionId },
        createdAt: now,
        resolvedAt: null,
      };
      yield* store.apply({
        id: EventId.make("control:pending-question"),
        type: "runtime-request.updated",
        threadId,
        occurredAt: now,
        payload: request,
      });
      const question = {
        id: TurnItemId.make("item:control-question"),
        threadId,
        runId,
        nodeId,
        providerThreadId,
        providerTurnId,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 0,
        status: "waiting" as const,
        title: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "user_input_request" as const,
        requestId,
        ...(responseMode === "message" ? { responseMode } : {}),
        questions: [{ id: "question", header: "Choice", question: "Which approach?", options: [] }],
      };
      yield* store.apply({
        id: EventId.make("control:question-item"),
        type: "turn-item.updated",
        threadId,
        occurredAt: now,
        payload: question,
      });
      for (let ordinal = 1; ordinal <= 100; ordinal++) {
        yield* store.apply({
          id: EventId.make(`control:later-item:${ordinal}`),
          type: "turn-item.updated",
          threadId,
          occurredAt: now,
          payload: {
            ...question,
            id: TurnItemId.make(`item:later:${ordinal}`),
            ordinal,
            type: "command_execution",
            status: "completed",
            completedAt: now,
            input: "echo later activity",
            output: "done",
          },
        });
      }

      for (const compactTurnItems of [false, true]) {
        const snapshot = yield* store.getThreadSnapshotWindow(threadId, { rowLimit: 77 });
        const bounded = buildBoundedThreadStreamSnapshot({ ...snapshot, compactTurnItems });
        assert.isTrue(bounded.hasMoreHistory);
        assert.lengthOf(bounded.projection.visibleTurnItems, 75);
        assert.isFalse(
          bounded.projection.visibleTurnItems.some((row) => row.sourceItemId === question.id),
        );
        assert.deepEqual(derivePendingThreadRequests(bounded.projection).userInputs, [
          {
            requestId,
            createdAt: DateTime.formatIso(now),
            questions: question.questions.map((question) => ({ ...question, multiSelect: false })),
            responseCapability: responseMode,
            ...(responseMode === "message" ? { responseMode } : {}),
            dismissible: responseMode === "message",
          },
        ]);
      }

      yield* store.apply({
        id: EventId.make("control:question-resolved"),
        type: "runtime-request.updated",
        threadId,
        occurredAt: now,
        payload: { ...request, status: "resolved", resolvedAt: now },
      });
      const snapshot = yield* store.getThreadSnapshotWindow(threadId, { rowLimit: 77 });
      const bounded = buildBoundedThreadStreamSnapshot(snapshot);
      assert.isEmpty(derivePendingThreadRequests(bounded.projection).userInputs);
      assert.isFalse(bounded.projection.turnItems.some((item) => item.id === question.id));
    }).pipe(Effect.provide(storeLayer)),
);

it.effect.each(storageCases)(
  "$storage: finds the active root turn without an attempt reverse link",
  ({ storeLayer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const events = fixtureEvents(now).map((event) =>
        event.type === "run-attempt.created"
          ? { ...event, payload: { ...event.payload, providerTurnId: null } }
          : event,
      );
      yield* Effect.forEach(events, (event) => store.apply(event), { discard: true });
      const running = yield* store.getRunningTurnContext(threadId);
      assert.equal(running.providerTurn?.id, providerTurnId);
      const turnEvent = events.find((event) => event.type === "provider-turn.updated")!;
      yield* store.apply({
        ...turnEvent,
        payload: { ...turnEvent.payload, status: "completed", completedAt: now },
      });
      assert.isUndefined((yield* store.getRunningTurnContext(threadId)).providerTurn);
    }).pipe(Effect.provide(storeLayer)),
);
it.effect.each(storageCases)(
  "$storage: controls and replies read only their exact durable targets",
  ({ storage, storeLayer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const events = fixtureEvents(now);
      yield* Effect.forEach(events, (event) => store.apply(event), { discard: true });
      if (storage === "sqlite") {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`INSERT INTO orchestration_v2_projection_messages
        (message_id, thread_id, run_id, node_id, role, streaming, created_at, updated_at, payload_json)
        VALUES ('message:unrelated-obsolete', ${threadId}, ${runId}, ${nodeId}, 'assistant', 0,
          ${DateTime.formatIso(now)}, ${DateTime.formatIso(now)}, '{"obsolete":"transcript"}')`;
        assert.equal((yield* Effect.exit(store.getThreadProjection(threadId)))._tag, "Failure");
        const queryPlan = yield* sql<{ detail: string }>`EXPLAIN QUERY PLAN
          SELECT payload_json FROM orchestration_v2_projection_turn_items
          WHERE thread_id = ${threadId} AND node_id = ${nodeId}
            AND type IN ('approval_request', 'user_input_request')
            AND json_extract(payload_json, '$.requestId') = ${requestId}
          ORDER BY ordinal ASC LIMIT 1`;
        assert.isTrue(queryPlan.some((row) => row.detail.includes("turn_items_node_ordinal_idx")));
      }
      const running = yield* store.getRunningTurnContext(threadId);
      assert.equal(running.run?.id, runId);
      assert.equal(running.providerThread?.id, providerThreadId);
      assert.equal(running.providerTurn?.id, providerTurnId);
      const providerContext = yield* store.getThreadProviderContext(threadId, providerInstanceId);
      assert.equal(providerContext.thread.id, threadId);
      assert.deepEqual(
        providerContext.providerThreads.map((thread) => thread.id),
        [providerThreadId],
      );
      const responseContext = yield* store.getRuntimeResponseContext(threadId, requestId);
      assert.equal(responseContext.request?.id, requestId);
      const target = { providerThreadId, providerTurnId, attemptId, messageId };
      const context = yield* store.getProviderControlContext(threadId, target);
      assert.equal(context.providerThread?.id, providerThreadId);
      assert.equal(context.providerTurn?.id, providerTurnId);
      assert.equal(context.attempt?.id, attemptId);
      assert.equal(context.message?.text, "Use the smaller fix.");
      assert.equal(context.run?.id, runId);
      assert.equal((yield* store.getRuntimeRequest(threadId, requestId))?.status, "resolved");
      assert.isUndefined(
        yield* store.getRuntimeRequest(threadId, RuntimeRequestId.make("request:missing")),
      );
      const absent = yield* store.getProviderControlContext(threadId, {
        providerThreadId: ProviderThreadId.make("provider-thread:missing"),
        providerTurnId: ProviderTurnId.make("provider-turn:missing"),
      });
      assert.isUndefined(absent.providerThread);
      assert.isUndefined(absent.providerTurn);
      const missingThread = yield* store
        .getRuntimeRequest(ThreadId.make("thread:missing"), requestId)
        .pipe(Effect.flip);
      assert.instanceOf(missingThread, ProjectionStore.ProjectionStoreThreadNotFoundError);

      const calls: string[] = [];
      const layerSessions = Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
        get: () =>
          Effect.succeedSome({
            interruptTurn: () =>
              Effect.sync(() => {
                calls.push("interrupt");
              }),
            steerTurn: (input: {
              message: {
                text: string;
              };
              runId: RunId;
            }) =>
              Effect.sync(() => {
                assert.equal(input.runId, runId);
                calls.push(input.message.text);
              }),
            respondToRuntimeRequest: () =>
              Effect.sync(() => {
                calls.push("reply");
              }),
          } as never),
      });
      yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        const reply = yield* RuntimeRequestService.RuntimeRequestServiceV2;
        yield* control.interrupt({ threadId, providerThreadId, providerTurnId, providerSessionId });
        yield* control.steer({
          threadId,
          providerThreadId,
          providerTurnId,
          providerSessionId,
          messageId,
        });
        yield* reply.respond({ threadId, providerSessionId, requestId, decision: "accept" });
        assert.deepEqual(calls, ["interrupt", "Use the smaller fix.", "reply"]);
        const wrongSession = ProviderSessionId.make("session:wrong");
        assert.equal(
          (yield* Effect.exit(
            control.interrupt({
              threadId,
              providerThreadId,
              providerTurnId,
              providerSessionId: wrongSession,
            }),
          ))._tag,
          "Failure",
        );
        assert.equal(
          (yield* Effect.exit(
            reply.respond({
              threadId,
              providerSessionId: wrongSession,
              requestId,
              decision: "accept",
            }),
          ))._tag,
          "Failure",
        );
        assert.lengthOf(calls, 3);
        const turn = context.providerTurn!;
        yield* store.apply({
          id: EventId.make("control:turn-completed"),
          type: "provider-turn.updated",
          threadId,
          occurredAt: now,
          payload: { ...turn, status: "completed", completedAt: now },
        });
        yield* control.interrupt({ threadId, providerThreadId, providerTurnId, providerSessionId });
        assert.deepEqual(calls, ["interrupt", "Use the smaller fix.", "reply", "interrupt"]);
      }).pipe(
        Effect.provide(
          Layer.merge(ProviderTurnControlService.layer, RuntimeRequestService.layer).pipe(
            Layer.provide(layerSessions),
          ),
        ),
      );
    }).pipe(Effect.provide(Layer.merge(storeLayer, SqlitePersistence.layerMemory))),
);
