import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import {
  type LiveVoiceEvent,
  type LiveVoiceHandoffConfig,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2StoredEvent,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as RuntimePolicy from "../orchestration-v2/RuntimePolicy.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as LiveVoiceService from "./LiveVoiceService.ts";

const threadId = ThreadId.make("thread-voice");
const projectId = ProjectId.make("project-voice");
const nativeThreadId = "native-voice";
const runId = RunId.make("run-voice");
const providerThread = {
  id: "provider-thread-voice",
  appThreadId: threadId,
  providerSessionId: "session-voice",
  nativeThreadRef: { nativeId: nativeThreadId },
};

const handoffConfig = (
  instanceId: string,
  runtimeMode: LiveVoiceHandoffConfig["runtimeMode"],
): LiveVoiceHandoffConfig => ({
  modelSelection: { instanceId: ProviderInstanceId.make(instanceId), model: `${instanceId}-model` },
  runtimeMode,
  interactionMode: "default",
});

const makeHarness = Effect.gen(function* () {
  const realtimeEvents = yield* PubSub.unbounded<ProviderAdapter.ProviderAdapterV2RealtimeEvent>();
  // Replays like the event store does for a subscriber that starts after the event.
  const threadEvents = yield* PubSub.unbounded<OrchestrationV2StoredEvent>({ replay: 16 });
  const dispatched = yield* Queue.unbounded<OrchestrationV2ServerCommand>();
  const sent: Array<ThreadManagementService.ThreadManagementSendInput> = [];
  const counts = { opens: 0, resumes: 0, recreates: 0, starts: 0, stops: 0, keptAlive: 0 };
  let failingStops = 0;
  let staleThreads = 0;
  const emit = (event: ProviderAdapter.ProviderAdapterV2RealtimeEvent) =>
    PubSub.publish(realtimeEvents, event);
  const realtime: ProviderAdapter.ProviderAdapterV2Realtime = {
    subscribe: PubSub.subscribe(realtimeEvents).pipe(Effect.map(Stream.fromSubscription)),
    start: (input) =>
      Effect.suspend(() => {
        if (input.providerThreadId === nativeThreadId && staleThreads > 0) {
          staleThreads -= 1;
          return Effect.fail(
            new ProviderAdapter.ProviderAdapterProtocolError({
              driver: ProviderDriverKind.make("codex"),
              detail: `thread not found: ${nativeThreadId}`,
            }),
          );
        }
        counts.starts += 1;
        return emit({
          type: "sdp",
          threadId: input.providerThreadId,
          answerSdp: `answer-${counts.starts}`,
        });
      }),
    stop: () =>
      Effect.suspend(() => {
        counts.stops += 1;
        if (failingStops === 0) return Effect.void;
        failingStops -= 1;
        return Effect.fail(
          new ProviderAdapter.ProviderAdapterProtocolError({
            driver: ProviderDriverKind.make("codex"),
            detail: "realtime/stop rejected",
          }),
        );
      }),
  };
  const runtime = {
    realtime,
    resumeThread: () =>
      Effect.sync(() => {
        counts.resumes += 1;
        return providerThread;
      }),
    ensureThread: () =>
      Effect.sync(() => {
        counts.recreates += 1;
        return { ...providerThread, nativeThreadRef: { nativeId: "native-voice-recreated" } };
      }),
  } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
  let sessionOpen = false;

  const layer = LiveVoiceService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          get: () => Effect.succeed(sessionOpen ? Option.some(runtime) : Option.none()),
          open: () =>
            Effect.sync(() => {
              counts.opens += 1;
              sessionOpen = true;
              return runtime;
            }),
          keepAlive: () =>
            Effect.acquireRelease(
              Effect.sync(() => {
                counts.keptAlive += 1;
              }),
              () =>
                Effect.sync(() => {
                  counts.keptAlive -= 1;
                }),
            ),
        }),
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getThreadRecords: () =>
            Effect.succeed({
              thread: {
                id: threadId,
                projectId,
                modelSelection: {},
                runtimeMode: "approval-required",
                interactionMode: "default",
              },
              providerThreads: [providerThread],
              providerSessions: [],
            } as never),
          dispatch: (command) => Queue.offer(dispatched, command).pipe(Effect.as({} as never)),
          sendToThread: (input) =>
            Effect.sync(() => {
              sent.push(input);
              return { run: { id: runId } } as never;
            }),
          getThreadEventSequence: () => Effect.succeed(0),
          streamStoredEventsFrom: () =>
            Stream.unwrap(PubSub.subscribe(threadEvents).pipe(Effect.map(Stream.fromSubscription))),
          waitForThread: () =>
            Effect.succeed({
              threadId,
              run: { id: runId, status: "completed" },
              timedOut: false,
            } as never),
          getProjectThreadRecords: () =>
            Effect.succeed({
              messages: [{ runId, role: "assistant", text: "The tests pass." }],
            } as never),
        }),
        Layer.mock(RuntimePolicy.RuntimePolicyV2)({
          resolve: () =>
            Effect.succeed(
              ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
                runtimeMode: "full-access",
                interactionMode: "default",
                cwd: "/repo",
              }),
            ),
        }),
        NodeCrypto.layer,
      ),
    ),
  );
  const service = Context.get(yield* Layer.build(layer), LiveVoiceService.LiveVoiceService);
  return {
    service,
    counts,
    dispatched,
    sent,
    emit,
    emitThreadEvent: (event: OrchestrationV2StoredEvent["event"]) =>
      PubSub.publish(threadEvents, { sequence: 1, commandId: null, event }),
    failNextStop: () => {
      failingStops += 1;
    },
    loseNativeThread: () => {
      staleThreads += 1;
    },
  };
});

/** Resolves once subscribed, so later events cannot be missed. */
const collect = (service: LiveVoiceService.LiveVoiceServiceShape) =>
  Effect.gen(function* () {
    const events = yield* Queue.unbounded<LiveVoiceEvent>();
    yield* service.subscribe.pipe(
      Stream.runForEach((event) => Queue.offer(events, event)),
      Effect.forkScoped,
    );
    const snapshot = yield* Queue.take(events);
    return { events, snapshot };
  });

const takeUntil = (events: Queue.Queue<LiveVoiceEvent>, done: (event: LiveVoiceEvent) => boolean) =>
  Stream.fromQueue(events).pipe(Stream.takeUntil(done), Stream.runCollect);

const offerSdp = "v=0\r\n";

it.effect("prepare opens the provider thread once and start reuses it", () =>
  Effect.gen(function* () {
    const { service, counts } = yield* makeHarness;
    yield* service.prepare({ threadId });
    const started = yield* service.start({ threadId, offerSdp });
    assert.equal(started.answerSdp, "answer-1");
    assert.deepEqual({ opens: counts.opens, resumes: counts.resumes }, { opens: 1, resumes: 1 });
    assert.deepEqual(yield* service.stop({ threadId }), { stopped: true });
    assert.equal(counts.stops, 1);
    assert.deepEqual(yield* service.stop({ threadId }), { stopped: false });
  }).pipe(Effect.scoped),
);

it.effect("a reconnecting subscriber receives the live call in its snapshot", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness;
    const first = yield* collect(service);
    assert.deepEqual(first.snapshot, { type: "snapshot", calls: [] });
    const started = yield* service.start({ threadId, offerSdp });
    const statuses = yield* takeUntil(
      first.events,
      (event) => event.type === "status" && event.status === "active",
    );
    assert.deepEqual(
      statuses.map((event) => (event.type === "status" ? event.status : event.type)),
      ["starting", "active"],
    );
    const reconnected = yield* collect(service);
    assert.deepEqual(reconnected.snapshot, {
      type: "snapshot",
      calls: [{ threadId, callId: started.callId, status: "active", handoffs: [] }],
    });
  }).pipe(Effect.scoped),
);

it.effect("stopping and starting again returns the new call's answer", () =>
  Effect.gen(function* () {
    const { service } = yield* makeHarness;
    const first = yield* service.start({ threadId, offerSdp });
    yield* service.stop({ threadId });
    const second = yield* service.start({ threadId, offerSdp });
    assert.equal(second.answerSdp, "answer-2");
    assert.notEqual(second.callId, first.callId);
  }).pipe(Effect.scoped),
);

it.effect("a failed provider stop still closes the call and allows a fresh start", () =>
  Effect.gen(function* () {
    const { service, failNextStop } = yield* makeHarness;
    const { events } = yield* collect(service);
    const first = yield* service.start({ threadId, offerSdp });
    failNextStop();
    const stopped = yield* Effect.flip(service.stop({ threadId }));
    assert.include(stopped.message, "realtime/stop rejected");
    const closed = (yield* takeUntil(
      events,
      (event) => event.type === "status" && event.status === "closed",
    )).at(-1);
    assert.deepInclude(closed, { callId: first.callId, status: "closed" });
    assert.deepEqual((yield* collect(service)).snapshot, { type: "snapshot", calls: [] });
    const second = yield* service.start({ threadId, offerSdp });
    assert.equal(second.answerSdp, "answer-2");
  }).pipe(Effect.scoped),
);

it.effect("persists repeated phrases as separate transcript rows", () =>
  Effect.gen(function* () {
    const { service, emit, dispatched } = yield* makeHarness;
    yield* service.start({ threadId, offerSdp });
    yield* emit({
      type: "transcript",
      threadId: nativeThreadId,
      role: "user",
      text: "yes",
      done: true,
    });
    yield* emit({
      type: "transcript",
      threadId: "another-thread",
      role: "user",
      text: "yes",
      done: true,
    });
    yield* emit({
      type: "transcript",
      threadId: nativeThreadId,
      role: "user",
      text: "yes",
      done: true,
    });
    const first = yield* Queue.take(dispatched);
    const second = yield* Queue.take(dispatched);
    assert.deepInclude(first, { type: "voice.transcript.append", threadId, text: "yes" });
    assert.deepInclude(second, { type: "voice.transcript.append", threadId, text: "yes" });
    assert.notEqual(
      first.type === "voice.transcript.append" ? first.messageId : null,
      second.type === "voice.transcript.append" ? second.messageId : null,
    );
  }).pipe(Effect.scoped),
);

it.effect("runs a repeated handoff request once and publishes its result once", () =>
  Effect.gen(function* () {
    const { service, emit, sent } = yield* makeHarness;
    const { events } = yield* collect(service);
    yield* service.start({ threadId, offerSdp });
    const request = {
      type: "handoff_request",
      handoff_id: "handoff-1",
      input_transcript: "Run the tests",
    };
    yield* emit({ type: "item", threadId: nativeThreadId, item: request });
    yield* emit({ type: "item", threadId: nativeThreadId, item: request });
    const handoffs = (yield* takeUntil(
      events,
      (event) => event.type === "handoff" && event.done,
    )).filter((event) => event.type === "handoff");
    assert.deepEqual(
      handoffs.map((event) => [event.text, event.done]),
      [
        ["T3 is working on that now.", false],
        ["The tests pass.", true],
      ],
    );
    assert.equal(sent.length, 1);
    const reconnected = yield* collect(service);
    assert.deepEqual(
      reconnected.snapshot.type === "snapshot" ? reconnected.snapshot.calls[0]?.handoffs : null,
      [{ handoffId: "handoff-1", kind: "handoff", text: "The tests pass.", done: true }],
    );
  }).pipe(Effect.scoped),
);

it.effect("a provider error ends the call for every subscriber", () =>
  Effect.gen(function* () {
    const { service, emit } = yield* makeHarness;
    const { events } = yield* collect(service);
    const started = yield* service.start({ threadId, offerSdp });
    yield* emit({ type: "error", threadId: nativeThreadId, message: "session expired" });
    const closed = (yield* takeUntil(
      events,
      (event) => event.type === "status" && event.status === "closed",
    )).at(-1);
    assert.deepEqual(closed, {
      type: "status",
      threadId,
      callId: started.callId,
      status: "closed",
      error: "session expired",
    });
  }).pipe(Effect.scoped),
);

it.effect("the next handoff uses the provider and permissions configured during the call", () =>
  Effect.gen(function* () {
    const { service, emit, sent, dispatched } = yield* makeHarness;
    const { events } = yield* collect(service);
    yield* service.start({
      threadId,
      offerSdp,
      handoffConfig: handoffConfig("codex", "approval-required"),
    });
    const handoff = (handoffId: string) =>
      emit({
        type: "item",
        threadId: nativeThreadId,
        item: { type: "handoff_request", handoff_id: handoffId, input_transcript: "Fix it" },
      }).pipe(Effect.andThen(takeUntil(events, (event) => event.type === "handoff" && event.done)));
    yield* handoff("handoff-1");
    assert.deepEqual(
      yield* service.configure({
        threadId,
        handoffConfig: handoffConfig("claude", "full-access"),
      }),
      { configured: true },
    );
    yield* handoff("handoff-2");
    assert.deepEqual(
      sent.map((input) => String(input.modelSelection?.instanceId)),
      ["codex", "claude"],
    );
    const permissionChanges = Array.from(yield* Queue.takeAll(dispatched)).filter(
      (command) => command.type === "thread.runtime-mode.set",
    );
    assert.deepEqual(
      permissionChanges.map((command) =>
        command.type === "thread.runtime-mode.set" ? command.runtimeMode : null,
      ),
      ["full-access"],
    );
  }).pipe(Effect.scoped),
);

it.effect("publishes partial phrases live and saves only the finished phrase", () =>
  Effect.gen(function* () {
    const { service, emit, dispatched } = yield* makeHarness;
    const { events } = yield* collect(service);
    yield* service.start({ threadId, offerSdp });
    yield* emit({
      type: "transcript",
      threadId: nativeThreadId,
      role: "assistant",
      text: "Hel",
      done: false,
    });
    yield* emit({
      type: "transcript",
      threadId: nativeThreadId,
      role: "assistant",
      text: "lo",
      done: false,
    });
    yield* emit({
      type: "transcript",
      threadId: nativeThreadId,
      role: "assistant",
      text: "Hello",
      done: true,
    });
    const transcripts = (yield* takeUntil(
      events,
      (event) => event.type === "transcript" && event.done,
    )).filter((event) => event.type === "transcript");
    assert.deepEqual(
      transcripts.map((event) => [event.text, event.done]),
      [
        ["Hel", false],
        ["lo", false],
        ["Hello", true],
      ],
    );
    const saved = yield* Queue.take(dispatched);
    assert.deepInclude(saved, { type: "voice.transcript.append", text: "Hello" });
    assert.equal(
      transcripts.at(-1)?.messageId,
      saved.type === "voice.transcript.append" ? saved.messageId : null,
    );
  }).pipe(Effect.scoped),
);

it.effect("recreates a native thread the app-server lost and retries the start", () =>
  Effect.gen(function* () {
    const { service, counts, loseNativeThread } = yield* makeHarness;
    loseNativeThread();
    const started = yield* service.start({ threadId, offerSdp });
    assert.equal(started.answerSdp, "answer-1");
    assert.equal(counts.recreates, 1);
  }).pipe(Effect.scoped),
);

it.effect("keeps the provider session alive only while the call is live", () =>
  Effect.gen(function* () {
    const { service, counts } = yield* makeHarness;
    yield* service.start({ threadId, offerSdp });
    assert.equal(counts.keptAlive, 1);
    yield* service.stop({ threadId });
    assert.equal(counts.keptAlive, 0);
  }).pipe(Effect.scoped),
);

it.effect("reports a finished subagent to the call once as a check-in", () =>
  Effect.gen(function* () {
    const { service, emitThreadEvent } = yield* makeHarness;
    const { events } = yield* collect(service);
    yield* service.start({ threadId, offerSdp });
    yield* takeUntil(events, (event) => event.type === "status" && event.status === "active");
    const finished = {
      type: "subagent.updated",
      threadId,
      payload: { id: "subagent-1", title: "Reviewer", status: "completed", result: "Looks good." },
    } as unknown as OrchestrationV2StoredEvent["event"];
    yield* emitThreadEvent(finished);
    yield* emitThreadEvent(finished);
    const checkIn = (yield* takeUntil(events, (event) => event.type === "handoff")).at(-1);
    assert.deepInclude(checkIn, {
      kind: "checkin",
      handoffId: "subagent:subagent-1",
      text: "Reviewer completed: Looks good.",
      done: true,
    });
    const reconnected = yield* collect(service);
    assert.lengthOf(
      reconnected.snapshot.type === "snapshot"
        ? (reconnected.snapshot.calls[0]?.handoffs ?? [])
        : [],
      1,
    );
  }).pipe(Effect.scoped),
);
