import {
  CommandId,
  DEFAULT_LIVE_VOICE,
  liveVoiceHandoffMessageId,
  LiveVoiceRpcError,
  liveVoiceTranscriptIds,
  ProviderSessionId,
  type LiveVoiceCallStatus,
  type LiveVoiceConfigureInput,
  type LiveVoiceHandoffConfig,
  type LiveVoiceEvent,
  type LiveVoiceStartInput,
  type LiveVoiceStartResult,
  type LiveVoiceThreadInput,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Subagent,
  type ProjectId,
  type ThreadId,
} from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as RuntimePolicy from "../orchestration-v2/RuntimePolicy.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";

export interface LiveVoiceServiceShape {
  /** Opens the provider thread while the client is still negotiating media. */
  readonly prepare: (
    input: LiveVoiceThreadInput,
  ) => Effect.Effect<{ readonly prepared: boolean }, LiveVoiceRpcError>;
  /** Changes the settings the next handoff snapshots; no effect without a call. */
  readonly configure: (
    input: LiveVoiceConfigureInput,
  ) => Effect.Effect<{ readonly configured: boolean }, LiveVoiceRpcError>;
  /** Replaces any call already on the thread. */
  readonly start: (
    input: LiveVoiceStartInput,
  ) => Effect.Effect<LiveVoiceStartResult, LiveVoiceRpcError>;
  readonly stop: (
    input: LiveVoiceThreadInput,
  ) => Effect.Effect<{ readonly stopped: boolean }, LiveVoiceRpcError>;
  /** Starts with a snapshot of every call, so a reconnecting client can catch up. */
  readonly subscribe: Stream.Stream<LiveVoiceEvent>;
}

export class LiveVoiceService extends Context.Service<LiveVoiceService, LiveVoiceServiceShape>()(
  "t3/voice/LiveVoiceService",
) {}

type HandoffUpdate = {
  readonly handoffId: string;
  readonly kind: "handoff" | "checkin";
  readonly text: string;
  readonly done: boolean;
};

interface Binding {
  readonly projectId: ProjectId;
  readonly realtime: ProviderAdapter.ProviderAdapterV2Realtime;
  readonly providerSessionId: ProviderSessionId;
  /** Recreates the native thread after the app-server lost it. */
  readonly recreateThread: Effect.Effect<
    OrchestrationV2ProviderThread,
    ProviderAdapter.ProviderAdapterV2Error
  >;
  providerThread: OrchestrationV2ProviderThread;
  nativeThreadId: string;
}

interface Call {
  readonly callId: string;
  readonly scope: Scope.Closeable;
  readonly answer: Deferred.Deferred<string, LiveVoiceRpcError>;
  handoffConfig: LiveVoiceHandoffConfig | undefined;
  readonly handoffs: Map<string, HandoffUpdate>;
  status: Exclude<LiveVoiceCallStatus, "closed">;
  transcriptSequence: number;
}

interface ThreadVoice {
  binding: Binding | null;
  call: Call | null;
}

const ANSWER_TIMEOUT = "10 seconds";
const STOP_TIMEOUT = "10 seconds";
const HANDOFF_WAIT_MS = 30 * 60 * 1_000;

const isLiveVoiceRpcError = Schema.is(LiveVoiceRpcError);

/** Codex keeps native threads in memory, so an app-server restart forgets them. */
const isMissingThreadError = (message: string): boolean =>
  /thread.{0,60}(?:not found|missing|not loaded|no longer loaded)|no such thread/i.test(message);

const FINISHED_SUBAGENT_STATUSES: ReadonlySet<OrchestrationV2Subagent["status"]> = new Set([
  "completed",
  "failed",
  "cancelled",
  "interrupted",
]);

const describeCheckIn = (subagent: OrchestrationV2Subagent): string => {
  const name = subagent.title ?? "A background task";
  const result = subagent.result?.trim();
  return result ? `${name} ${subagent.status}: ${result}` : `${name} ${subagent.status}.`;
};

// Protocol wrappers keep the provider's message in payload or cause.
const causeMessage = (cause: unknown): string | undefined => {
  if (typeof cause === "string") return cause;
  if (typeof cause !== "object" || cause === null) return undefined;
  const record = cause as Record<string, unknown>;
  for (const nested of [record.payload, record.cause, record.error]) {
    const message = causeMessage(nested);
    if (message !== undefined && message.length > 0) return message;
  }
  return typeof record.message === "string" ? record.message : undefined;
};

const voiceError = (threadId: ThreadId, message: string, cause?: unknown) => {
  if (isLiveVoiceRpcError(cause)) return cause;
  const detail = causeMessage(cause);
  return new LiveVoiceRpcError({
    threadId,
    message: detail === undefined || detail === message ? message : `${message} ${detail}`,
  });
};

const readRealtimeHandoff = (
  item: unknown,
): { readonly handoffId: string; readonly request: string } | null => {
  if (typeof item !== "object" || item === null) return null;
  const record = item as Record<string, unknown>;
  if (record.type !== "handoff_request") return null;
  const handoffId = typeof record.handoff_id === "string" ? record.handoff_id.trim() : "";
  const request = typeof record.input_transcript === "string" ? record.input_transcript.trim() : "";
  return handoffId === "" || request === "" ? null : { handoffId, request };
};

const make = Effect.gen(function* () {
  const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const runtimePolicies = yield* RuntimePolicy.RuntimePolicyV2;
  const random = yield* SecureRandom.SecureRandom;
  const serviceScope = yield* Effect.scope;
  const locks = yield* KeyedLock.make<ThreadId>();
  const events = yield* PubSub.unbounded<LiveVoiceEvent>();
  const voices = new Map<ThreadId, ThreadVoice>();

  const publish = (event: LiveVoiceEvent) => PubSub.publish(events, event).pipe(Effect.asVoid);
  const publishStatus = (
    threadId: ThreadId,
    call: Call,
    status: LiveVoiceCallStatus,
    error?: string,
  ) =>
    publish({
      type: "status",
      threadId,
      callId: call.callId,
      status,
      ...(error === undefined ? {} : { error }),
    });
  const isCurrent = (threadId: ThreadId, call: Call) => voices.get(threadId)?.call === call;

  const bind = Effect.fn("LiveVoiceService.bind")(
    function* (threadId: ThreadId) {
      const records = yield* threads.getThreadRecords(threadId, [
        "providerThreads",
        "providerSessions",
      ]);
      const thread = records.thread;
      const providerThread = records.providerThreads.findLast(
        (candidate) => candidate.appThreadId === threadId,
      );
      const providerSessionId =
        providerThread?.providerSessionId ?? ProviderSessionId.make(`live-voice:${threadId}`);
      const runtimePolicy = yield* runtimePolicies.resolve({
        thread,
        modelSelection: thread.modelSelection,
      });
      const live = yield* sessions.get(providerSessionId);
      const persistedSession = records.providerSessions.find(
        (candidate) => candidate.id === providerSessionId,
      );
      const runtime = Option.isSome(live)
        ? live.value
        : yield* sessions.open({
            threadId,
            projectId: thread.projectId,
            providerSessionId,
            modelSelection: thread.modelSelection,
            runtimePolicy,
            ...(persistedSession === undefined ? {} : { resumeFromSession: persistedSession }),
          });
      if (runtime.realtime === undefined) {
        return yield* voiceError(threadId, "Live voice needs a thread on Codex.");
      }
      // Resuming is a no-op when the native thread is already loaded, and
      // reloads it after an app-server restart.
      const loaded =
        providerThread?.nativeThreadRef?.nativeId == null
          ? yield* runtime.ensureThread({
              threadId,
              modelSelection: thread.modelSelection,
              runtimePolicy,
              providerSessionId,
              ...(providerThread === undefined ? {} : { existingProviderThread: providerThread }),
            })
          : yield* runtime.resumeThread({
              providerThread,
              threadId,
              modelSelection: thread.modelSelection,
              runtimePolicy,
            });
      const nativeThreadId = loaded.nativeThreadRef?.nativeId;
      if (nativeThreadId == null) {
        return yield* voiceError(threadId, "The Codex thread has no native id.");
      }
      return {
        projectId: thread.projectId,
        realtime: runtime.realtime,
        providerSessionId,
        recreateThread: runtime.ensureThread({
          threadId,
          modelSelection: thread.modelSelection,
          runtimePolicy,
          providerSessionId,
        }),
        providerThread: loaded,
        nativeThreadId,
      } satisfies Binding;
    },
    (effect, threadId) =>
      effect.pipe(
        Effect.mapError((cause) => voiceError(threadId, "Could not open the Codex thread.", cause)),
      ),
  );

  const bindingFor = Effect.fn("LiveVoiceService.bindingFor")(
    function* (threadId: ThreadId) {
      const voice = voices.get(threadId) ?? { binding: null, call: null };
      voices.set(threadId, voice);
      if (voice.binding === null) voice.binding = yield* bind(threadId);
      return { voice, binding: voice.binding };
    },
    (effect, threadId) =>
      effect.pipe(
        Effect.tapError(() =>
          Effect.sync(() => {
            const voice = voices.get(threadId);
            if (voice?.binding === null && voice.call === null) voices.delete(threadId);
          }),
        ),
      ),
  );

  /** Caller holds the thread lock. */
  const closeCall = Effect.fn("LiveVoiceService.closeCall")(function* (
    threadId: ThreadId,
    call: Call,
    error?: string,
  ) {
    const voice = voices.get(threadId);
    if (voice?.call === call) voices.delete(threadId);
    yield* Scope.close(call.scope, Exit.void);
    yield* publishStatus(threadId, call, "closed", error);
  });

  const stopProvider = (threadId: ThreadId, binding: Binding) =>
    binding.realtime.stop({ providerThreadId: binding.nativeThreadId }).pipe(
      Effect.timeoutOrElse({
        duration: STOP_TIMEOUT,
        orElse: () => Effect.fail(voiceError(threadId, "Codex did not confirm the call ended.")),
      }),
      Effect.mapError((cause) => voiceError(threadId, "Could not stop live voice.", cause)),
    );

  /** The provider ended the call on its own. */
  const endCall = (threadId: ThreadId, call: Call, error?: string) =>
    locks.withLock(
      threadId,
      Effect.suspend(() =>
        isCurrent(threadId, call) ? closeCall(threadId, call, error) : Effect.void,
      ),
    );

  const updateHandoff = (threadId: ThreadId, call: Call, update: HandoffUpdate) =>
    Effect.suspend(() => {
      if (!isCurrent(threadId, call) || call.handoffs.get(update.handoffId)?.done === true)
        return Effect.void;
      call.handoffs.set(update.handoffId, update);
      return publish({ type: "handoff", threadId, callId: call.callId, ...update });
    });

  const runHandoff = Effect.fn("LiveVoiceService.runHandoff")(function* (
    threadId: ThreadId,
    call: Call,
    binding: Binding,
    handoff: { readonly handoffId: string; readonly request: string },
  ) {
    const config = call.handoffConfig;
    // Deterministic, so a replayed request reuses the run it already started.
    const messageId = liveVoiceHandoffMessageId(threadId, handoff.handoffId);
    if (config !== undefined) {
      const { thread } = yield* threads.getThreadRecords(threadId, []);
      // The orchestrator validates these like any client change; the turn clamps them.
      if (thread.runtimeMode !== config.runtimeMode) {
        yield* threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make(`${messageId}:runtime-mode`),
          threadId,
          runtimeMode: config.runtimeMode,
        });
      }
      if (thread.interactionMode !== config.interactionMode) {
        yield* threads.dispatch({
          type: "thread.interaction-mode.set",
          commandId: CommandId.make(`${messageId}:interaction-mode`),
          threadId,
          interactionMode: config.interactionMode,
        });
      }
    }
    const sent = yield* threads.sendToThread({
      projectId: binding.projectId,
      commandId: CommandId.make(messageId),
      threadId,
      messageId,
      text: handoff.request,
      attachments: [],
      ...(config === undefined ? {} : { modelSelection: config.modelSelection }),
      mode: "auto",
      createdBy: "user",
      creationSource: "server",
    });
    yield* updateHandoff(threadId, call, {
      handoffId: handoff.handoffId,
      kind: "handoff",
      text: "T3 is working on that now.",
      done: false,
    });
    const waited = yield* threads.waitForThread({
      projectId: binding.projectId,
      threadId,
      runId: sent.run.id,
      timeoutMs: HANDOFF_WAIT_MS,
    });
    if (waited.timedOut || waited.run === null) {
      return yield* updateHandoff(threadId, call, {
        handoffId: handoff.handoffId,
        kind: "handoff",
        text: "T3 is still working on that. The result will be in the thread.",
        done: true,
      });
    }
    const runId = waited.run.id;
    const answer = (yield* threads.getProjectThreadRecords(
      { projectId: binding.projectId, threadId },
      ["messages"],
      { messageRunIds: [runId], messageRoles: ["assistant"] },
    )).messages
      .findLast((message) => message.runId === runId && message.role === "assistant")
      ?.text.trim();
    yield* updateHandoff(threadId, call, {
      handoffId: handoff.handoffId,
      kind: "handoff",
      text:
        answer ||
        (waited.run.status === "completed"
          ? "T3 finished without a summary."
          : `T3 finished with status ${waited.run.status}.`),
      done: true,
    });
  });

  const handleRealtimeEvent = (
    threadId: ThreadId,
    call: Call,
    binding: Binding,
    event: ProviderAdapter.ProviderAdapterV2RealtimeEvent,
  ): Effect.Effect<void> => {
    if (event.threadId !== binding.nativeThreadId) return Effect.void;
    switch (event.type) {
      case "sdp":
        return Deferred.succeed(call.answer, event.answerSdp).pipe(Effect.asVoid);
      case "transcript": {
        const partial = {
          type: "transcript",
          threadId,
          callId: call.callId,
          role: event.role,
        } as const;
        if (!event.done) return publish({ ...partial, text: event.text, done: false });
        const text = event.text.trim();
        if (text === "") return publish({ ...partial, text: "", done: true });
        call.transcriptSequence += 1;
        const ids = liveVoiceTranscriptIds(call.callId, call.transcriptSequence);
        return threads
          .dispatch({ type: "voice.transcript.append", threadId, role: event.role, text, ...ids })
          .pipe(
            Effect.asVoid,
            Effect.catchCause((cause) =>
              Effect.logWarning("Failed to persist a live voice transcript.", { threadId, cause }),
            ),
            Effect.andThen(publish({ ...partial, text, done: true, messageId: ids.messageId })),
          );
      }
      case "item": {
        const handoff = readRealtimeHandoff(event.item);
        if (handoff === null || call.handoffs.has(handoff.handoffId)) return Effect.void;
        call.handoffs.set(handoff.handoffId, {
          handoffId: handoff.handoffId,
          kind: "handoff",
          text: "",
          done: false,
        });
        return runHandoff(threadId, call, binding, handoff).pipe(
          Effect.catchCause((cause) =>
            updateHandoff(threadId, call, {
              handoffId: handoff.handoffId,
              kind: "handoff",
              text: `T3 could not complete that request. ${causeMessage(cause) ?? ""}`.trim(),
              done: true,
            }),
          ),
          Effect.forkIn(call.scope),
          Effect.asVoid,
        );
      }
      case "error":
      case "closed": {
        const message =
          event.type === "error" ? event.message : (event.reason ?? "Codex ended the call.");
        // Before the answer arrives, start() owns cleanup and reports the error.
        return Deferred.fail(call.answer, voiceError(threadId, message)).pipe(
          Effect.flatMap((failedStart) =>
            failedStart
              ? Effect.void
              : endCall(threadId, call, event.type === "error" ? message : undefined).pipe(
                  Effect.forkIn(serviceScope),
                  Effect.asVoid,
                ),
          ),
        );
      }
    }
  };

  const prepare: LiveVoiceServiceShape["prepare"] = Effect.fn("LiveVoiceService.prepare")(
    function* (input) {
      yield* locks.withLock(input.threadId, bindingFor(input.threadId));
      return { prepared: true };
    },
  );

  const configure: LiveVoiceServiceShape["configure"] = Effect.fn("LiveVoiceService.configure")(
    function* (input) {
      return yield* locks.withLock(
        input.threadId,
        Effect.sync(() => {
          const call = voices.get(input.threadId)?.call;
          if (call == null) return { configured: false };
          call.handoffConfig = input.handoffConfig;
          return { configured: true };
        }),
      );
    },
  );

  /** Native subagent results reach the call as check-ins it can speak or queue. */
  const followCheckIns = Effect.fn("LiveVoiceService.followCheckIns")(function* (
    threadId: ThreadId,
    call: Call,
  ) {
    const afterSequence = yield* threads.getThreadEventSequence(threadId);
    yield* threads
      .streamStoredEventsFrom({ threadId, afterSequence, eventType: "subagent.updated" })
      .pipe(
        Stream.runForEach(({ event }) => {
          if (event.type !== "subagent.updated") return Effect.void;
          const subagent = event.payload;
          const handoffId = `subagent:${subagent.id}`;
          if (!FINISHED_SUBAGENT_STATUSES.has(subagent.status) || call.handoffs.has(handoffId))
            return Effect.void;
          return updateHandoff(threadId, call, {
            handoffId,
            kind: "checkin",
            text: describeCheckIn(subagent),
            done: true,
          });
        }),
      );
  });

  const startRealtime = Effect.fn("LiveVoiceService.startRealtime")(
    function* (threadId: ThreadId, binding: Binding, input: LiveVoiceStartInput) {
      const voice = input.voice ?? DEFAULT_LIVE_VOICE;
      const first = yield* Effect.result(
        binding.realtime.start({
          providerThreadId: binding.nativeThreadId,
          offerSdp: input.offerSdp,
          voice,
        }),
      );
      if (first._tag === "Success") return;
      if (!isMissingThreadError(causeMessage(first.failure) ?? "")) return yield* first.failure;
      const providerThread = yield* binding.recreateThread;
      const nativeThreadId = providerThread.nativeThreadRef?.nativeId;
      if (nativeThreadId == null)
        return yield* voiceError(threadId, "The Codex thread has no native id.");
      binding.providerThread = providerThread;
      binding.nativeThreadId = nativeThreadId;
      yield* binding.realtime.start({
        providerThreadId: nativeThreadId,
        offerSdp: input.offerSdp,
        voice,
      });
    },
    (effect, threadId) =>
      effect.pipe(
        Effect.mapError((cause) =>
          voiceError(threadId, "Codex could not start live voice.", cause),
        ),
      ),
  );

  const start: LiveVoiceServiceShape["start"] = Effect.fn("LiveVoiceService.start")(
    function* (input) {
      const threadId = input.threadId;
      return yield* locks.withLock(
        threadId,
        Effect.gen(function* () {
          const previous = voices.get(threadId);
          if (previous?.call != null && previous.binding !== null) {
            const replaced = previous.call;
            yield* stopProvider(threadId, previous.binding).pipe(Effect.ignore);
            yield* closeCall(threadId, replaced);
          }
          const { voice, binding } = yield* bindingFor(threadId);
          const call: Call = {
            callId: yield* random.uuidV4(),
            scope: yield* Scope.fork(serviceScope),
            answer: yield* Deferred.make<string, LiveVoiceRpcError>(),
            handoffConfig: input.handoffConfig,
            handoffs: new Map(),
            status: "starting",
            transcriptSequence: 0,
          };
          voice.call = call;
          const realtimeEvents = yield* binding.realtime.subscribe.pipe(Scope.provide(call.scope));
          yield* Stream.runForEach(realtimeEvents, (event) =>
            handleRealtimeEvent(threadId, call, binding, event),
          ).pipe(Effect.forkIn(call.scope));
          yield* publishStatus(threadId, call, "starting");
          const answerSdp = yield* startRealtime(threadId, binding, input).pipe(
            Effect.andThen(Deferred.await(call.answer)),
            Effect.timeoutOrElse({
              duration: ANSWER_TIMEOUT,
              orElse: () =>
                Effect.fail(voiceError(threadId, "Codex did not return a WebRTC answer.")),
            }),
            Effect.tapError((error) =>
              stopProvider(threadId, binding).pipe(
                Effect.ignore,
                Effect.andThen(closeCall(threadId, call, error.message)),
              ),
            ),
          );
          // Like a running turn, a call keeps its provider session and thread loaded.
          yield* sessions
            .keepAlive({
              providerSessionId: binding.providerSessionId,
              providerThreadId: binding.providerThread.id,
              key: `live-voice:${call.callId}`,
            })
            .pipe(Scope.provide(call.scope));
          yield* followCheckIns(threadId, call).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("Live voice check-ins stopped.", { threadId, cause }),
            ),
            Effect.forkIn(call.scope),
          );
          call.status = "active";
          yield* publishStatus(threadId, call, "active");
          return { callId: call.callId, answerSdp };
        }),
      );
    },
  );

  const stop: LiveVoiceServiceShape["stop"] = Effect.fn("LiveVoiceService.stop")(function* (input) {
    const threadId = input.threadId;
    return yield* locks.withLock(
      threadId,
      Effect.gen(function* () {
        const voice = voices.get(threadId);
        voices.delete(threadId);
        const call = voice?.call ?? null;
        if (voice?.binding == null || call === null) return { stopped: false };
        call.status = "stopping";
        yield* publishStatus(threadId, call, "stopping");
        const stopped = yield* Effect.result(stopProvider(threadId, voice.binding));
        const error = stopped._tag === "Failure" ? stopped.failure : undefined;
        yield* closeCall(threadId, call, error?.message);
        if (error !== undefined) return yield* error;
        return { stopped: true };
      }),
    );
  });

  const subscribe: LiveVoiceServiceShape["subscribe"] = Stream.unwrap(
    Effect.gen(function* () {
      const subscription = yield* PubSub.subscribe(events);
      const snapshot: LiveVoiceEvent = {
        type: "snapshot",
        calls: Array.from(voices).flatMap(([threadId, { call }]) =>
          call === null
            ? []
            : [
                {
                  threadId,
                  callId: call.callId,
                  status: call.status,
                  handoffs: Array.from(call.handoffs.values()).filter(({ text }) => text !== ""),
                },
              ],
        ),
      };
      return Stream.concat(Stream.make(snapshot), Stream.fromSubscription(subscription));
    }),
  );

  return LiveVoiceService.of({ prepare, configure, start, stop, subscribe });
});

export const layer = Layer.effect(LiveVoiceService, make);
