import {
  ProviderSetupError,
  type ModelSelection,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderThread,
  type ProjectId,
  type ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderSessionId,
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
import {
  refreshAutoPoolMember,
  type AutoPoolMember,
  type AutoPoolReservation,
  type AutoPoolRouter,
} from "./AutoPoolRouter.ts";
import { autoPoolAccountKey, autoPoolExhausted } from "./autoPoolPolicy.ts";

type Runtime = ProviderAdapter.ProviderAdapterV2SessionRuntime;
type FailedTerminal = Extract<
  ProviderAdapter.ProviderAdapterV2Event,
  { type: "turn.terminal"; status: "failed" }
>;

/** One member's native runtime. `loaded` means this process holds the thread's writer. */
interface Native {
  readonly member: AutoPoolReservation;
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
  /** Auto stopped a turn its account held for allowance, to continue it elsewhere. */
  quotaPaused: boolean;
  /** The highest turn item ordinal seen, so a failure Auto reports for a paused turn sorts last. */
  itemOrdinal: number;
}

const accountOf = (member: AutoPoolMember) =>
  autoPoolAccountKey(member.snapshot) ?? member.instance.instanceId;

/**
 * How a failed turn relates to the account's allowance. A "confirmed" failure moves the thread at
 * once; an "unconfirmed" one moves it only when a usage read taken after it shows the allowance
 * is spent.
 */
export type AutoPoolQuotaFailure = "confirmed" | "unconfirmed" | undefined;

export const makeAutoPoolAdapter = (input: {
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  /** Product name for messages, such as "Codex Auto". */
  readonly label: string;
  readonly capabilities: OrchestrationV2ProviderCapabilities;
  readonly router: AutoPoolRouter["Service"];
  readonly openNative: (
    member: AutoPoolMember,
    session: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
  ) => Effect.Effect<Runtime, ProviderAdapter.ProviderAdapterV2Error, Scope.Scope>;
  readonly quotaFailure: (failure: OrchestrationV2ProviderFailure) => AutoPoolQuotaFailure;
  /** True when the member refused the request for allowance before inference started. */
  readonly isAdmissionQuotaError?: (error: unknown) => boolean;
  /** True for an event saying the member holds the turn open until its allowance resets. */
  readonly isQuotaPause?: (event: ProviderAdapter.ProviderAdapterV2Event) => boolean;
  /**
   * Makes the native conversation readable by `member` before it resumes it, for drivers whose
   * accounts keep conversations in separate homes. The previous writer is released by then.
   */
  readonly prepareResume?: (
    member: AutoPoolMember,
    providerThread: OrchestrationV2ProviderThread,
    providerSessionId: ProviderSessionId,
  ) => Effect.Effect<void, ProviderAdapter.ProviderAdapterV2Error>;
}) => {
  const { driver, label } = input;
  const capabilities = input.capabilities;
  const admissionQuota = input.isAdmissionQuotaError ?? (() => false);
  const prepareResume = input.prepareResume ?? (() => Effect.void);
  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: input.instanceId,
    driver,
    getCapabilities: Effect.fn("ProviderAdapter.ProviderAdapterV2.getCapabilities")(() =>
      Effect.succeed(capabilities),
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
    hasUsageAllowance: input.router.hasUsageAllowance,
    planSelectionTransition: Effect.fn("ProviderAdapter.ProviderAdapterV2.planSelectionTransition")(
      () => Effect.succeed({ type: "apply_on_next_turn" as const }),
    ),
    openSession: Effect.fn("AutoPoolAdapter.openSession")(function* (sessionInput) {
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
        member: AutoPoolMember,
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

      const safeToSwitch = Effect.fn("AutoPoolAdapter.safeToSwitch")(function* (current: Native) {
        if (pendingRequests.size > 0 || unfinishedNodes.size > 0) return false;
        const background =
          thread !== undefined && current.runtime.hasPendingBackgroundWorkForThread
            ? current.runtime.hasPendingBackgroundWorkForThread(thread, { includeSubagents: true })
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
              case "turn_item.updated":
                {
                  if (turn !== undefined && event.turnItem.providerThreadId === thread?.id)
                    turn.itemOrdinal = Math.max(turn.itemOrdinal, event.turnItem.ordinal);
                  const providerThread = thread;
                  const providerTurnId = event.turnItem.providerTurnId;
                  if (
                    turn !== undefined &&
                    turn.stop === undefined &&
                    !turn.quotaPaused &&
                    native === current &&
                    providerThread !== undefined &&
                    providerTurnId !== null &&
                    event.turnItem.providerThreadId === providerThread.id &&
                    input.isQuotaPause?.(event) === true
                  ) {
                    turn.quotaPaused = true;
                    yield* current.runtime
                      .interruptTurn({ providerThread, providerTurnId })
                      .pipe(Effect.ignore, Effect.forkIn(sessionScope));
                  }
                }
                break;
              case "node.updated":
                if (["tool_call", "subagent", "hook"].includes(event.node.kind)) {
                  if (
                    event.node.status === "running" ||
                    event.node.status === "waiting" ||
                    (event.node.status === "pending" && event.node.kind !== "subagent")
                  )
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
                    capabilities,
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
                let quotaTerminal: FailedTerminal | undefined;
                if (event.status === "failed") {
                  if (input.quotaFailure(event.failure) !== undefined) quotaTerminal = event;
                } else if (turn.quotaPaused && event.status !== "completed") {
                  quotaTerminal = {
                    ...event,
                    status: "failed",
                    failureItemOrdinal: turn.itemOrdinal + 1,
                    failure: makeProviderFailure({
                      class: "usage_limit",
                      message: "The account paused the turn until its usage limit resets.",
                      code: "usageLimitPause",
                    }),
                  };
                }
                if (quotaTerminal !== undefined && turn.stop === undefined && native === current) {
                  turn.rootTurnIds.add(event.providerTurnId);
                  // Recovery closes this reader's process, so it runs outside the reader.
                  yield* recover(quotaTerminal, current, turn).pipe(Effect.forkIn(sessionScope));
                  return;
                }
                yield* input.router.release(threadId);
                break;
              }
            }
            yield* Queue.offer(output, event);
          });

      const open = Effect.fn("AutoPoolAdapter.open")(function* (
        member: AutoPoolReservation,
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

      // The previous account must release the native writer before the next one resumes the
      // conversation, so one native conversation never has two writers.
      const switchTo = Effect.fn("AutoPoolAdapter.switchTo")(function* (
        next: AutoPoolReservation,
        runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
      ): Effect.fn.Return<Native, ProviderAdapter.ProviderAdapterV2Error> {
        const previous = native;
        if (previous !== undefined) {
          if (previous.loaded && thread?.nativeThreadRef && previous.runtime.unloadThread)
            yield* previous.runtime.unloadThread({
              providerThread: thread,
              awaitWriterRelease: true,
            });
          native = undefined;
          yield* Scope.close(previous.scope, Exit.void);
        }
        return yield* open(next, runtimePolicy);
      });

      const route = Effect.fn("AutoPoolAdapter.route")(function* (
        providerThread: OrchestrationV2ProviderThread,
        selection: ModelSelection,
        projectId: ProjectId | undefined,
        runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
      ) {
        const chosen = yield* reserve(selection, projectId);
        if (
          native !== undefined &&
          chosen.instance === native.member.instance &&
          accountOf(chosen) === accountOf(native.member)
        )
          return;
        if (native !== undefined && !(yield* safeToSwitch(native))) {
          const retained = yield* input.router
            .reserveCurrent(threadId, selection, projectId, native.member)
            .pipe(Effect.result);
          if (retained._tag === "Success" && retained.success) return;
          return yield* new ProviderAdapter.ProviderAdapterResumeThreadError({
            driver,
            providerSessionId: sessionInput.providerSessionId,
            providerThreadId: providerThread.id,
            preserveNativeThread: true,
            cause:
              retained._tag === "Failure"
                ? retained.failure
                : new ProviderSetupError({
                    instanceId: input.instanceId,
                    operation: "session",
                    detail: `Resolve pending provider work before switching ${label} accounts.`,
                  }),
          });
        }
        yield* switchTo(chosen, runtimePolicy);
      });

      const ready = Effect.fn("AutoPoolAdapter.ready")(function* (
        current: Native,
        modelSelection: ModelSelection,
        runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
      ) {
        if (current.loaded || !thread?.nativeThreadRef) return;
        yield* prepareResume(current.member, thread, sessionInput.providerSessionId);
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

      // A member may reject an exhausted account before inference; retry on each account not yet tried.
      const admitted = Effect.fn("AutoPoolAdapter.admitted")(function* <A>(
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
          if (!admissionQuota(result.failure) || turn?.stop !== undefined)
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

      const quotaConfirmed = Effect.fn("AutoPoolAdapter.quotaConfirmed")(function* (
        terminal: FailedTerminal,
        current: Native,
        state: RootTurn,
      ) {
        if (state.quotaPaused || input.quotaFailure(terminal.failure) === "confirmed") return true;
        // A rate limit is transient unless a usage read taken after it shows the allowance is spent.
        const failedAt = yield* Clock.currentTimeMillis;
        const reported = yield* refreshAutoPoolMember(current.member).pipe(
          Effect.timeoutOption("20 seconds"),
          Effect.map(Option.getOrUndefined),
        );
        return (
          reported?.usageLimits !== undefined &&
          Date.parse(reported.usageLimits.checkedAt) >= failedAt &&
          autoPoolExhausted(reported, state.input.modelSelection.model, failedAt)
        );
      });

      const recover = Effect.fn("AutoPoolAdapter.recover")(function* (
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
              if (state.quotaPaused || input.quotaFailure(terminal.failure) === "confirmed")
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
              state.quotaPaused = false;
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
                          : `${label} could not resume safely. The native conversation was preserved; retry after recovery.`,
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
              ? Effect.fail(
                  setupError(`The ${label} process is unavailable. Retry after recovery.`),
                )
              : use(native.runtime),
          ),
        );

      // Without a process, the next turn applies the change.
      const ifNative = (
        use: (runtime: Runtime) => Effect.Effect<void, ProviderAdapter.ProviderAdapterV2Error>,
      ) =>
        lock.withPermit(
          Effect.suspend(() => (native === undefined ? Effect.void : use(native.runtime))),
        );

      const resume = Effect.fn("AutoPoolAdapter.resume")(
        function* (request: Parameters<Runtime["resumeThread"]>[0], alreadyLoaded = false) {
          const selection = request.modelSelection ?? sessionInput.modelSelection;
          const policy = request.runtimePolicy ?? sessionInput.runtimePolicy;
          const previous = native;
          yield* route(request.providerThread, selection, sessionInput.projectId, policy);
          if (alreadyLoaded && previous?.loaded && native === previous)
            return request.providerThread;
          return yield* admitted(
            selection,
            policy,
            sessionInput.projectId,
            (current, modelSelection) =>
              prepareResume(
                current.member,
                request.providerThread,
                sessionInput.providerSessionId,
              ).pipe(
                Effect.andThen(current.runtime.resumeThread({ ...request, modelSelection })),
                Effect.map((resumed) => {
                  current.loaded = true;
                  thread = stamp(resumed, current.member);
                  return thread;
                }),
              ),
          );
        },
        (effect) => lock.withPermit(effect.pipe(Effect.ensuring(input.router.release(threadId)))),
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
        providerSession: { ...first.runtime.providerSession, capabilities },
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
        hasPendingBackgroundWorkForThread: (providerThread, options) =>
          Effect.suspend(
            () =>
              native?.runtime.hasPendingBackgroundWorkForThread?.(providerThread, options) ??
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
        resumeThread: (request) => resume(request),
        resumeLoadedThread: (request) => resume(request, true),
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
                  quotaPaused: false,
                  itemOrdinal: 0,
                };
                turn = state;
                yield* route(
                  turnInput.providerThread,
                  turnInput.modelSelection,
                  turnInput.appThread.projectId,
                  turnInput.runtimePolicy,
                );
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
        // Members run the same driver's adapter, so the first runtime decides which extras exist.
        ...(optional.backgroundTasksEndWithProcess
          ? { backgroundTasksEndWithProcess: optional.backgroundTasksEndWithProcess }
          : {}),
        ...(optional.applyModelSelection
          ? {
              applyModelSelection: (
                request: Parameters<NonNullable<Runtime["applyModelSelection"]>>[0],
              ) => ifNative((runtime) => runtime.applyModelSelection!(request)),
            }
          : {}),
        ...(optional.applyThreadTitle
          ? {
              applyThreadTitle: (
                request: Parameters<NonNullable<Runtime["applyThreadTitle"]>>[0],
              ) => ifNative((runtime) => runtime.applyThreadTitle!(request)),
            }
          : {}),
        ...(optional.awaitBackgroundTasks
          ? {
              awaitBackgroundTasks: (
                request: Parameters<NonNullable<Runtime["awaitBackgroundTasks"]>>[0],
              ) => withNative((runtime) => runtime.awaitBackgroundTasks!(request)),
            }
          : {}),
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
};
