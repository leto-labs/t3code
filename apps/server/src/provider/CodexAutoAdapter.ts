import {
  ProviderDriverKind,
  type ModelSelection,
  type OrchestrationV2ProviderThread,
  type ProjectId,
  type ProviderInstanceId,
  type ProviderTurnId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import type { CodexAutoMember, CodexAutoReservation, CodexAutoRouter } from "./CodexAutoRouter.ts";
import {
  codexAutoAccountKey,
  codexAutoExhausted,
  isCodexAutoAdmissionQuotaError,
} from "./codexAutoPolicy.ts";

export const CodexAutoCapabilities = {
  ...CodexProviderCapabilitiesV2,
  sessions: {
    ...CodexProviderCapabilitiesV2.sessions,
    supportsMultipleProviderThreadsPerSession: false,
  },
};

type Runtime = ProviderAdapter.ProviderAdapterV2SessionRuntime;
type FailedTerminal = Extract<
  ProviderAdapter.ProviderAdapterV2Event,
  { type: "turn.terminal"; status: "failed" }
>;

/** One member's native Codex runtime. `loaded` means this process holds the thread's writer. */
interface Native {
  readonly member: CodexAutoReservation;
  readonly runtime: Runtime;
  readonly scope: Scope.Closeable;
  loaded: boolean;
}

interface RootTurn {
  input: ProviderAdapter.ProviderAdapterV2TurnInput;
  stop: ProviderAdapter.ProviderAdapterV2InterruptInput | undefined;
  latest: { readonly id: ProviderTurnId; readonly native: Native } | undefined;
  latestOrdinal: number;
  readonly rootTurnIds: Set<ProviderTurnId>;
  readonly attempted: Set<string>;
}

const driver = ProviderDriverKind.make("codex");
const accountOf = (member: CodexAutoMember) =>
  codexAutoAccountKey(member.snapshot) ?? member.instance.instanceId;

export const makeCodexAutoAdapter = (input: {
  readonly instanceId: ProviderInstanceId;
  readonly router: CodexAutoRouter["Service"];
  readonly openNative: (
    member: CodexAutoMember,
    session: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
  ) => Effect.Effect<Runtime, ProviderAdapter.ProviderAdapterV2Error, Scope.Scope>;
}) =>
  ProviderAdapter.ProviderAdapterV2.of({
    instanceId: input.instanceId,
    driver,
    getCapabilities: Effect.fn("ProviderAdapter.ProviderAdapterV2.getCapabilities")(() =>
      Effect.succeed(CodexAutoCapabilities),
    ),
    validateModelSelection: Effect.fn("ProviderAdapter.ProviderAdapterV2.validateModelSelection")(
      (selection, projectId) =>
        input.router.normalizeSelection(selection, projectId).pipe(
          Effect.asVoid,
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterProtocolError({
                driver,
                detail: cause.message,
                cause,
              }),
          ),
        ),
    ),
    planSelectionTransition: Effect.fn("ProviderAdapter.ProviderAdapterV2.planSelectionTransition")(
      () => Effect.succeed({ type: "apply_on_next_turn" as const }),
    ),
    openSession: Effect.fn("CodexAutoAdapter.openSession")(function* (sessionInput) {
      const sessionScope = yield* Scope.Scope;
      const threadId = sessionInput.threadId;
      // Serializes account switches against every call that uses the current native runtime.
      const lock = yield* Semaphore.make(1);
      const output = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error
      >();
      yield* Effect.addFinalizer(() => Queue.shutdown(output));
      yield* Effect.addFinalizer(() => input.router.release(threadId));
      const setupError = (cause: unknown) =>
        new ProviderAdapter.ProviderAdapterOpenSessionError({
          driver,
          providerSessionId: sessionInput.providerSessionId,
          cause,
        });
      let native: Native | undefined;
      let thread: OrchestrationV2ProviderThread | undefined;
      let turn: RootTurn | undefined;
      const pendingRequests = new Set<string>();
      const unfinishedNodes = new Set<string>();

      const stamp = (
        providerThread: OrchestrationV2ProviderThread,
        member: CodexAutoMember,
      ): OrchestrationV2ProviderThread => ({
        ...providerThread,
        nativeMetadata: {
          ...providerThread.nativeMetadata,
          executionInstanceId: member.instance.instanceId,
        },
      });
      const reserve = (
        selection: ModelSelection,
        projectId: ProjectId | undefined,
        exclude?: ReadonlySet<string>,
      ) =>
        input.router
          .reserve(threadId, selection, projectId, exclude)
          .pipe(Effect.mapError(setupError));

      const interruptLatest = (request: ProviderAdapter.ProviderAdapterV2InterruptInput) =>
        Effect.suspend(() => {
          const current = native;
          if (current === undefined) return Effect.void;
          const root = turn?.rootTurnIds.has(request.providerTurnId) === true;
          // The replacement root has not been accepted yet; recovery stops it once it is.
          if (root && turn?.latest?.native !== current) return Effect.void;
          return current.runtime.interruptTurn({
            ...request,
            providerThread: thread ?? request.providerThread,
            providerTurnId: root ? turn!.latest!.id : request.providerTurnId,
          });
        });

      const safeToSwitch = Effect.fn("CodexAutoAdapter.safeToSwitch")(function* (current: Native) {
        if (pendingRequests.size > 0 || unfinishedNodes.size > 0) return false;
        const background =
          thread !== undefined && current.runtime.hasPendingBackgroundWorkForThread
            ? current.runtime.hasPendingBackgroundWorkForThread(thread)
            : (current.runtime.hasPendingBackgroundWork ?? Effect.succeed(false));
        return !(yield* background);
      });

      const forward =
        (current: Native) =>
        (event: ProviderAdapter.ProviderAdapterV2Event): Effect.Effect<void> =>
          Effect.gen(function* () {
            switch (event.type) {
              case "provider_turn.updated": {
                if (turn !== undefined && event.providerTurn.providerThreadId === thread?.id) {
                  turn.latest = { id: event.providerTurn.id, native: current };
                  turn.latestOrdinal = Math.max(turn.latestOrdinal, event.providerTurn.ordinal);
                  turn.rootTurnIds.add(event.providerTurn.id);
                  const stop = turn.stop;
                  if (stop && ["running", "pending"].includes(event.providerTurn.status))
                    yield* interruptLatest(stop).pipe(Effect.ignore, Effect.forkIn(sessionScope));
                }
                yield* Queue.offer(output, {
                  ...event,
                  providerTurn: {
                    ...event.providerTurn,
                    executionInstanceId: current.member.instance.instanceId,
                  },
                });
                return;
              }
              case "runtime_request.updated":
                if (
                  event.runtimeRequest.status === "pending" &&
                  event.runtimeRequest.responseCapability.type === "live"
                )
                  pendingRequests.add(event.runtimeRequest.id);
                else pendingRequests.delete(event.runtimeRequest.id);
                break;
              case "node.updated":
                if (["tool_call", "subagent", "hook"].includes(event.node.kind)) {
                  if (["pending", "running", "waiting"].includes(event.node.status))
                    unfinishedNodes.add(event.node.id);
                  else unfinishedNodes.delete(event.node.id);
                }
                break;
              case "provider_thread.updated":
                if (thread === undefined || event.providerThread.id === thread.id) {
                  thread = stamp(event.providerThread, current.member);
                  yield* Queue.offer(output, { ...event, providerThread: thread });
                  return;
                }
                break;
              case "provider_session.updated":
                yield* Queue.offer(output, {
                  ...event,
                  providerSession: {
                    ...event.providerSession,
                    capabilities: CodexAutoCapabilities,
                  },
                });
                return;
              case "turn.terminal": {
                if (
                  turn === undefined ||
                  event.providerThreadId !== thread?.id ||
                  event.runOrdinal !== turn.input.runOrdinal
                )
                  break;
                if (
                  event.status === "failed" &&
                  turn.stop === undefined &&
                  native === current &&
                  (event.failure.code === "usageLimitExceeded" ||
                    event.failure.code === "rateLimitExceeded")
                ) {
                  turn.rootTurnIds.add(event.providerTurnId);
                  // Recovery closes this reader's process, so it runs outside the reader.
                  yield* recover(event, current, turn).pipe(Effect.forkIn(sessionScope));
                  return;
                }
                yield* input.router.release(threadId);
                break;
              }
            }
            yield* Queue.offer(output, event);
          });

      const open = Effect.fn("CodexAutoAdapter.open")(function* (
        member: CodexAutoReservation,
        runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
      ): Effect.fn.Return<Native, ProviderAdapter.ProviderAdapterV2Error> {
        const scope = yield* Scope.fork(sessionScope);
        const runtime = yield* input
          .openNative(member, {
            ...sessionInput,
            modelSelection: member.modelSelection,
            runtimePolicy,
          })
          .pipe(
            Effect.provideService(Scope.Scope, scope),
            Effect.onError(() => Scope.close(scope, Exit.void)),
          );
        const current: Native = { member, runtime, scope, loaded: false };
        // The reader lives in the process scope, so closing the process stops it too.
        yield* runtime.events.pipe(
          Stream.runForEach(forward(current)),
          Effect.catch((cause) => (native === current ? Queue.fail(output, cause) : Effect.void)),
          Effect.forkIn(scope),
        );
        native = current;
        return current;
      });

      // Codex releases the shared session writer only after thread/closed; the next account
      // may resume the native thread from the shared session files only after that.
      const switchTo = Effect.fn("CodexAutoAdapter.switchTo")(function* (
        next: CodexAutoReservation,
        runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
      ): Effect.fn.Return<Native, ProviderAdapter.ProviderAdapterV2Error> {
        const previous = native;
        if (previous !== undefined) {
          if (previous.loaded && thread?.nativeThreadRef && previous.runtime.unloadThread)
            yield* previous.runtime.unloadThread({ providerThread: thread });
          native = undefined;
          yield* Scope.close(previous.scope, Exit.void);
        }
        return yield* open(next, runtimePolicy);
      });

      const ready = Effect.fn("CodexAutoAdapter.ready")(function* (
        current: Native,
        modelSelection: ModelSelection,
        runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
      ) {
        if (current.loaded || !thread?.nativeThreadRef) return;
        thread = stamp(
          yield* current.runtime.resumeThread({
            providerThread: thread,
            threadId,
            modelSelection,
            runtimePolicy,
          }),
          current.member,
        );
        current.loaded = true;
        yield* Queue.offer(output, {
          type: "provider_thread.updated",
          driver,
          providerThread: thread,
        });
      });

      // Codex rejects an exhausted account before inference; retry on each account not yet tried.
      const admitted = Effect.fn("CodexAutoAdapter.admitted")(function* <A>(
        selection: ModelSelection,
        runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
        projectId: ProjectId | undefined,
        operation: (
          current: Native,
          modelSelection: ModelSelection,
        ) => Effect.Effect<A, ProviderAdapter.ProviderAdapterV2Error>,
      ): Effect.fn.Return<A, ProviderAdapter.ProviderAdapterV2Error> {
        const rejected = new Set<string>();
        while (true) {
          const current =
            native ?? (yield* switchTo(yield* reserve(selection, projectId), runtimePolicy));
          const modelSelection = yield* input.router
            .normalizeSelection(selection, projectId)
            .pipe(Effect.mapError(setupError));
          const result = yield* operation(current, modelSelection).pipe(Effect.result);
          if (result._tag === "Success") return result.success;
          if (!isCodexAutoAdmissionQuotaError(result.failure) || turn?.stop !== undefined)
            return yield* result.failure;
          rejected.add(accountOf(current.member));
          yield* input.router.refreshUsage(current.member);
          yield* switchTo(yield* reserve(modelSelection, projectId, rejected), runtimePolicy);
        }
      });

      const startRoot = (state: RootTurn) =>
        admitted(
          state.input.modelSelection,
          state.input.runtimePolicy,
          state.input.appThread.projectId,
          (current, modelSelection) =>
            Effect.gen(function* () {
              if (state.stop !== undefined)
                return yield* setupError("The turn was stopped before inference.");
              yield* ready(current, modelSelection, state.input.runtimePolicy);
              state.input = {
                ...state.input,
                modelSelection,
                providerThread: thread ?? state.input.providerThread,
              };
              yield* current.runtime.startTurn(state.input);
            }),
        );

      const quotaConfirmed = Effect.fn("CodexAutoAdapter.quotaConfirmed")(function* (
        terminal: FailedTerminal,
        current: Native,
        state: RootTurn,
      ) {
        if (terminal.failure.code === "usageLimitExceeded") return true;
        // A rate limit is transient unless a usage read taken after it shows the allowance is spent.
        const failedAt = yield* Clock.currentTimeMillis;
        const reported = yield* current.member.instance.snapshot.refresh.pipe(
          Effect.timeoutOption("20 seconds"),
          Effect.map(Option.getOrUndefined),
        );
        return (
          reported?.usageLimits !== undefined &&
          Date.parse(reported.usageLimits.checkedAt) >= failedAt &&
          codexAutoExhausted(reported, state.input.modelSelection, failedAt)
        );
      });

      const recover = Effect.fn("CodexAutoAdapter.recover")(function* (
        terminal: FailedTerminal,
        current: Native,
        state: RootTurn,
      ): Effect.fn.Return<void> {
        const confirmed = yield* quotaConfirmed(terminal, current, state);
        const outcome = yield* lock
          .withPermit(
            Effect.gen(function* () {
              if (native !== current || turn !== state) return { done: true } as const;
              if (!confirmed || state.stop !== undefined || !thread?.nativeThreadRef) return {};
              if (!(yield* safeToSwitch(current)))
                return {
                  message:
                    "Resolve pending questions or unfinished provider work before continuing on another account.",
                };
              state.attempted.add(accountOf(current.member));
              if (terminal.failure.code === "usageLimitExceeded")
                yield* input.router.refreshUsage(current.member);
              const next = yield* input.router.reserve(
                threadId,
                state.input.modelSelection,
                state.input.appThread.projectId,
                state.attempted,
              );
              if (state.stop !== undefined) return {};
              const goal = thread.goal;
              const { restartContinuationOfRunId: _restart, ...previous } = state.input;
              state.input = {
                ...previous,
                providerTurnOrdinal:
                  Math.max(previous.providerTurnOrdinal, state.latestOrdinal) + 1,
                message: {
                  ...previous.message,
                  attachments: [],
                  text:
                    goal?.status === "active" || goal?.status === "usage_limited"
                      ? "/goal resume"
                      : "Continue from the latest native conversation state. Previous completed tool actions and results are already in the conversation; do not repeat them.",
                  creationSource: "server",
                },
              };
              yield* switchTo(next, state.input.runtimePolicy);
              yield* startRoot(state);
              if (state.stop !== undefined) yield* interruptLatest(state.stop);
              return { done: true } as const;
            }),
          )
          .pipe(
            Effect.catch((cause) => {
              const failure = makeProviderFailure({ cause });
              return Effect.succeed(
                failure.class === "usage_limit"
                  ? { failure }
                  : {
                      message:
                        cause._tag === "ProviderSetupError"
                          ? cause.message
                          : "Codex Auto could not resume safely. The native conversation was preserved; retry after recovery.",
                    },
              );
            }),
          );
        if ("done" in outcome) return;
        yield* input.router.release(threadId);
        const failure = "failure" in outcome ? outcome.failure : terminal.failure;
        yield* Queue.offer(output, {
          ...terminal,
          providerTurnId: state.latest?.id ?? terminal.providerTurnId,
          failure:
            "message" in outcome
              ? { ...failure, message: `${failure.message} ${outcome.message}` }
              : failure,
        });
      });

      const withNative = <A>(
        use: (runtime: Runtime) => Effect.Effect<A, ProviderAdapter.ProviderAdapterV2Error>,
      ) =>
        lock.withPermit(
          Effect.suspend(() =>
            native === undefined
              ? Effect.fail(setupError("The Codex process is unavailable. Retry after recovery."))
              : use(native.runtime),
          ),
        );

      const first = yield* open(
        yield* reserve(sessionInput.modelSelection, sessionInput.projectId),
        sessionInput.runtimePolicy,
      );
      const optional = first.runtime;
      return {
        instanceId: first.runtime.instanceId,
        driver: first.runtime.driver,
        providerSessionId: first.runtime.providerSessionId,
        providerSession: { ...first.runtime.providerSession, capabilities: CodexAutoCapabilities },
        events: Stream.fromQueue(output),
        // Without a process, the next turn reserves and opens one.
        isReusable: Effect.suspend(() => native?.runtime.isReusable ?? Effect.succeed(true)),
        canReuseContextUsage: (previous, next) =>
          native?.runtime.canReuseContextUsage?.(previous, next) ?? false,
        getModelContextWindow: (modelSelection, cwd) =>
          native?.runtime.getModelContextWindow?.(modelSelection, cwd),
        hasPendingBackgroundWork: Effect.suspend(
          () => native?.runtime.hasPendingBackgroundWork ?? Effect.succeed(false),
        ),
        hasPendingBackgroundWorkForThread: (providerThread) =>
          Effect.suspend(
            () =>
              native?.runtime.hasPendingBackgroundWorkForThread?.(providerThread) ??
              Effect.succeed(false),
          ),
        ensureThread: (request) =>
          lock.withPermit(
            admitted(
              request.modelSelection,
              request.runtimePolicy,
              sessionInput.projectId,
              (current, modelSelection) =>
                current.runtime.ensureThread({ ...request, modelSelection }).pipe(
                  Effect.map((created) => {
                    current.loaded = true;
                    thread = stamp(created, current.member);
                    return thread;
                  }),
                ),
            ),
          ),
        resumeThread: (request) =>
          lock.withPermit(
            admitted(
              request.modelSelection ?? sessionInput.modelSelection,
              request.runtimePolicy ?? sessionInput.runtimePolicy,
              sessionInput.projectId,
              (current, modelSelection) =>
                current.runtime.resumeThread({ ...request, modelSelection }).pipe(
                  Effect.map((resumed) => {
                    current.loaded = true;
                    thread = stamp(resumed, current.member);
                    return thread;
                  }),
                ),
            ),
          ),
        startTurn: (turnInput) =>
          lock
            .withPermit(
              Effect.gen(function* () {
                // The orchestrator binds a newly created native thread to its durable id.
                thread = turnInput.providerThread;
                const state: RootTurn = {
                  input: turnInput,
                  stop: undefined,
                  latest: undefined,
                  latestOrdinal: 0,
                  rootTurnIds: new Set(),
                  attempted: new Set(),
                };
                turn = state;
                const chosen = yield* reserve(
                  turnInput.modelSelection,
                  turnInput.appThread.projectId,
                );
                if (
                  native === undefined ||
                  chosen.instance !== native.member.instance ||
                  accountOf(chosen) !== accountOf(native.member)
                ) {
                  if (native !== undefined && !(yield* safeToSwitch(native)))
                    return yield* setupError(
                      "Resolve pending provider work before switching Codex accounts.",
                    );
                  yield* switchTo(chosen, turnInput.runtimePolicy);
                }
                yield* startRoot(state);
              }),
            )
            .pipe(Effect.onError(() => input.router.release(threadId))),
        steerTurn: (request) => withNative((runtime) => runtime.steerTurn(request)),
        interruptTurn: (request) =>
          Effect.suspend(() => {
            if (turn !== undefined) turn.stop = request;
            return interruptLatest(request);
          }),
        respondToRuntimeRequest: (request) =>
          withNative((runtime) =>
            runtime.respondToRuntimeRequest(request).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  pendingRequests.delete(request.requestId);
                }),
              ),
            ),
          ),
        readThreadSnapshot: (request) =>
          withNative((runtime) => runtime.readThreadSnapshot(request)),
        rollbackThread: (request) => withNative((runtime) => runtime.rollbackThread(request)),
        forkThread: (request) => withNative((runtime) => runtime.forkThread(request)),
        // Members run the same Codex adapter, so the first runtime decides which extras exist.
        ...(optional.compactThread
          ? {
              compactThread: (request: ProviderAdapter.ProviderAdapterV2TurnInput) =>
                withNative((runtime) => runtime.compactThread!(request)),
            }
          : {}),
        ...(optional.unloadThread
          ? {
              unloadThread: (request: { readonly providerThread: OrchestrationV2ProviderThread }) =>
                withNative((runtime) => runtime.unloadThread!(request)),
            }
          : {}),
        ...(optional.stopTask
          ? {
              stopTask: (request: Parameters<NonNullable<Runtime["stopTask"]>>[0]) =>
                withNative((runtime) => runtime.stopTask!(request)),
            }
          : {}),
        ...(optional.stopSubagent
          ? {
              stopSubagent: (request: Parameters<NonNullable<Runtime["stopSubagent"]>>[0]) =>
                withNative((runtime) => runtime.stopSubagent!(request)),
            }
          : {}),
        ...(optional.injectHistory
          ? {
              injectHistory: (request: Parameters<NonNullable<Runtime["injectHistory"]>>[0]) =>
                withNative((runtime) => runtime.injectHistory!(request)),
            }
          : {}),
        ...(optional.uploadFeedback
          ? {
              uploadFeedback: (request: Parameters<NonNullable<Runtime["uploadFeedback"]>>[0]) =>
                withNative((runtime) => runtime.uploadFeedback!(request)),
            }
          : {}),
        ...(optional.mcpApps
          ? {
              mcpApps: {
                listTools: (request: Parameters<NonNullable<Runtime["mcpApps"]>["listTools"]>[0]) =>
                  withNative((runtime) => runtime.mcpApps!.listTools(request)),
                callTool: (request: Parameters<NonNullable<Runtime["mcpApps"]>["callTool"]>[0]) =>
                  withNative((runtime) => runtime.mcpApps!.callTool(request)),
                readResource: (
                  request: Parameters<NonNullable<Runtime["mcpApps"]>["readResource"]>[0],
                ) => withNative((runtime) => runtime.mcpApps!.readResource(request)),
              },
            }
          : {}),
      } satisfies Runtime;
    }),
  });
