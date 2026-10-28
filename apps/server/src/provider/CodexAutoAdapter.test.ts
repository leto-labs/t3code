import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
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
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as DateTime from "effect/DateTime";
import * as TestClock from "effect/testing/TestClock";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ServerConfig from "../config.ts";
import { makeCodexAutoAdapter, CodexAutoCapabilities } from "./CodexAutoAdapter.ts";
import { makeCodexAutoRouter } from "./CodexAutoRouter.ts";
import { codexAutoAdmissionQuotaError } from "./codexAutoPolicy.ts";
import { CODEX_AUTO_MODEL, codexAutoFixture } from "./testUtils/codexAuto.ts";
import { materializeCodexShadowHome, resolveCodexHomeLayout } from "./Drivers/CodexHomeLayout.ts";
import { CodexSettings } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

const decodeCodexSettings = Schema.decodeEffect(CodexSettings);

const poolId = ProviderInstanceId.make("codex_auto");
const driver = ProviderDriverKind.make("codex");
const threadId = ThreadId.make("auto-context-thread");
const selection = {
  instanceId: poolId,
  model: CODEX_AUTO_MODEL,
  options: [
    { id: "serviceTier", value: "priority" },
    { id: "reasoningEffort", value: "high" },
  ],
};
const runtimePolicy = {
  cwd: "/test-workspace",
  runtimeMode: "full-access" as const,
  interactionMode: "default" as const,
};

const harness = Effect.fn("CodexAutoAdapter.test.harness")(function* (
  mode:
    | "recover"
    | "pending"
    | "tool"
    | "transport"
    | "identity"
    | "rate-current"
    | "rate-stale"
    | "resume-failure"
    | "goal"
    | "all-exhausted"
    | "start-failure"
    | "project"
    | "stop-race"
    | "admission-ensure"
    | "admission-resume"
    | "admission-start"
    | "admission-all" = "recover",
  requestedSelection: ModelSelection = selection,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-auto-adapter-" });
  const now = yield* DateTime.now;
  const iso = DateTime.formatIso(now);
  const accounts = [codexAutoFixture("a", iso), codexAutoFixture("b", iso)];
  const sharedHome = path.join(stateDir, ".codex");
  const overlays = new Map<string, string>();
  for (const account of accounts) {
    const shadow = path.join(stateDir, `auth-${account.instance.instanceId}`);
    const layout = yield* resolveCodexHomeLayout(
      yield* decodeCodexSettings({ homePath: sharedHome, shadowHomePath: shadow }),
    );
    yield* materializeCodexShadowHome(layout);
    yield* fs.writeFileString(
      path.join(shadow, "auth.json"),
      JSON.stringify({ fixtureAccount: account.instance.instanceId }),
    );
    overlays.set(account.instance.instanceId, shadow);
  }
  const nativeId = "native-shared-history";
  const sharedSessionFile = path.join(sharedHome, "sessions", `${nativeId}.json`);
  yield* fs.writeFileString(sharedSessionFile, JSON.stringify(["initial-context"]));
  const router = yield* makeCodexAutoRouter({
    instanceId: poolId,
    members: (projectId) =>
      Effect.succeed(
        accounts.map((account) => ({
          ...account.member(),
          allowed: !(
            mode === "project" &&
            projectId !== undefined &&
            account.instance.instanceId === "a"
          ),
        })),
      ),
  }).pipe(Effect.provide(ServerConfig.layerTest(stateDir, stateDir)));
  const providerThread: OrchestrationV2ProviderThread = {
    id: ProviderThreadId.make("provider-thread-auto"),
    driver,
    providerInstanceId: poolId,
    providerSessionId: ProviderSessionId.make("session-auto"),
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: { driver, nativeId, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    ...(mode === "goal"
      ? {
          goal: {
            objective: "preserve this goal",
            tokenBudget: 8000,
            tokensUsed: 1200,
            status: "usage_limited" as const,
          },
        }
      : {}),
  };
  const opens: string[] = [];
  const closes: string[] = [];
  const log: string[] = [];
  const starts: Array<{
    account: string;
    input: ProviderAdapter.ProviderAdapterV2TurnInput;
    history: readonly string[];
  }> = [];
  const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const nativeReusable = yield* Ref.make(true);
  let nativeStarts = 0;
  const recoveryAwaiting = yield* Deferred.make<void>();
  const releaseStart = yield* Deferred.make<void>();
  const interruptions: string[] = [];
  const preflightFailure = (id: string) =>
    Effect.sync(() => {
      const account = accounts.find((candidate) => candidate.instance.instanceId === id)!;
      account.set({
        ...account.read(),
        usageLimits: {
          ...account.read().usageLimits!,
          windows: account
            .read()
            .usageLimits!.windows.map((window) => ({ ...window, usedPercent: 100 })),
        },
      });
    }).pipe(Effect.andThen(Effect.fail(codexAutoAdmissionQuotaError(poolId))));
  const unused = () => Effect.die("Unused fixture operation");
  const bTurnObserved = yield* Deferred.make<void>();
  const adapter = makeCodexAutoAdapter({
    instanceId: poolId,
    router,
    openNative: (member, session) =>
      Effect.gen(function* () {
        const id = member.instance.instanceId;
        opens.push(id);
        log.push(`open:${id}`);
        const nativeEvents = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            closes.push(id);
            log.push(`close:${id}`);
          }),
        );
        const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
          isReusable: Ref.get(nativeReusable),
          instanceId: poolId,
          driver,
          providerSessionId: session.providerSessionId,
          providerSession: {
            id: session.providerSessionId,
            driver,
            providerInstanceId: poolId,
            status: "ready",
            cwd: runtimePolicy.cwd,
            model: CODEX_AUTO_MODEL,
            capabilities: CodexAutoCapabilities,
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
          events: Stream.fromQueue(nativeEvents),
          ensureThread: () =>
            mode === "admission-ensure" && id === "a"
              ? preflightFailure(id).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapter.ProviderAdapterEnsureThreadError({
                        driver,
                        threadId,
                        cause,
                      }),
                  ),
                )
              : Effect.succeed(providerThread),
          resumeThread: (input) =>
            mode === "admission-resume" && id === "a"
              ? preflightFailure(id).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapter.ProviderAdapterResumeThreadError({
                        driver,
                        providerSessionId: session.providerSessionId,
                        providerThreadId: providerThread.id,
                        cause,
                      }),
                  ),
                )
              : mode === "resume-failure" && id === "b"
                ? Effect.fail(
                    new ProviderAdapter.ProviderAdapterResumeThreadError({
                      driver,
                      providerSessionId: session.providerSessionId,
                      providerThreadId: providerThread.id,
                      cause: "database is locked",
                    }),
                  )
                : Effect.sync(() => {
                    log.push(`resume:${id}`);
                    return input.providerThread;
                  }),
          unloadThread: () =>
            Effect.sync(() => {
              log.push(`unload:${id}`);
            }),
          startTurn: (input) =>
            Effect.gen(function* () {
              if ((mode === "admission-start" && id === "a") || mode === "admission-all")
                return yield* preflightFailure(id);
              const sessionFile = path.join(
                overlays.get(id)!,
                "sessions",
                `${input.providerThread.nativeThreadRef?.nativeId}.json`,
              );
              const history: readonly string[] = JSON.parse(yield* fs.readFileString(sessionFile));
              starts.push({ account: id, input, history });
              log.push(`start:${id}`);
              nativeStarts++;
              if (id === "b" && mode === "stop-race") {
                yield* Deferred.succeed(recoveryAwaiting, undefined);
                yield* Deferred.await(releaseStart);
                yield* Queue.offer(nativeEvents, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    id: ProviderTurnId.make("accepted-b-turn"),
                    providerThreadId: providerThread.id,
                    ordinal: input.providerTurnOrdinal,
                    status: "running",
                  } as Extract<
                    ProviderAdapter.ProviderAdapterV2Event,
                    { type: "provider_turn.updated" }
                  >["providerTurn"],
                });
                yield* Deferred.await(bTurnObserved);
                return;
              }
              if (id === "b" && mode === "start-failure") {
                yield* Queue.offer(nativeEvents, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    id: ProviderTurnId.make("accepted-b-turn"),
                    providerThreadId: providerThread.id,
                    ordinal: input.providerTurnOrdinal,
                  } as Extract<
                    ProviderAdapter.ProviderAdapterV2Event,
                    { type: "provider_turn.updated" }
                  >["providerTurn"],
                });
                yield* Queue.offer(nativeEvents, {
                  type: "provider_session.updated",
                  driver,
                  providerSession: runtime.providerSession,
                });
                // Wait for the event consumer to observe B before simulating a lost start response.
                yield* Deferred.await(bTurnObserved);
                return yield* new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver,
                  threadId,
                  providerThreadId: providerThread.id,
                  runId: input.runId,
                  cause: "lost start response",
                });
              }
              yield* fs.writeFileString(
                sessionFile,
                JSON.stringify([...history, `tool-result-${id}-${nativeStarts}`]),
              );
              if (mode === "identity") {
                yield* Queue.offer(nativeEvents, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    id: ProviderTurnId.make(`native-turn-${nativeStarts}`),
                    providerThreadId: input.providerThread.id,
                    ordinal: input.providerTurnOrdinal,
                    status: "running",
                  } as Extract<
                    ProviderAdapter.ProviderAdapterV2Event,
                    { type: "provider_turn.updated" }
                  >["providerTurn"],
                });
              }
              if (nativeStarts === 1 && mode !== "identity" && !mode.startsWith("admission")) {
                if (mode === "pending")
                  yield* Queue.offer(nativeEvents, {
                    type: "runtime_request.updated",
                    driver,
                    runtimeRequest: {
                      id: RuntimeRequestId.make("question"),
                      status: "pending",
                      responseCapability: {
                        type: "live",
                        providerSessionId: session.providerSessionId,
                      },
                    } as Extract<
                      ProviderAdapter.ProviderAdapterV2Event,
                      { type: "runtime_request.updated" }
                    >["runtimeRequest"],
                  });
                if (mode === "tool")
                  yield* Queue.offer(nativeEvents, {
                    type: "node.updated",
                    driver,
                    node: {
                      id: NodeId.make("unfinished-tool"),
                      kind: "tool_call",
                      status: "running",
                    } as Extract<
                      ProviderAdapter.ProviderAdapterV2Event,
                      { type: "node.updated" }
                    >["node"],
                  });
                if (mode === "goal")
                  yield* Queue.offer(nativeEvents, {
                    type: "provider_turn.updated",
                    driver,
                    providerTurn: { providerThreadId: providerThread.id, ordinal: 7 } as Extract<
                      ProviderAdapter.ProviderAdapterV2Event,
                      { type: "provider_turn.updated" }
                    >["providerTurn"],
                  });
                if (mode === "all-exhausted")
                  for (const account of accounts)
                    account.set({
                      ...account.read(),
                      usageLimits: {
                        ...account.read().usageLimits!,
                        windows: account
                          .read()
                          .usageLimits!.windows.map((window) => ({ ...window, usedPercent: 100 })),
                      },
                    });
                if (mode === "rate-current" || mode === "rate-stale") {
                  const account = accounts.find((account) => account.instance.instanceId === id)!;
                  const checkedAt =
                    mode === "rate-stale"
                      ? DateTime.formatIso(
                          DateTime.makeUnsafe(DateTime.toEpochMillis(now) - 120_000),
                        )
                      : iso;
                  account.set({
                    ...account.read(),
                    usageLimits: {
                      ...account.read().usageLimits!,
                      checkedAt,
                      windows: account.read().usageLimits!.windows.map((window) => ({
                        ...window,
                        usedPercent: 100,
                        observedAt: checkedAt,
                      })),
                    },
                  });
                }
                yield* Queue.offer(nativeEvents, {
                  type: "turn.terminal",
                  driver,
                  providerThreadId: providerThread.id,
                  providerTurnId: ProviderTurnId.make("native-turn-a"),
                  runOrdinal: 1,
                  status: "failed",
                  failureItemOrdinal: 1,
                  failure: {
                    message: "Quota exhausted",
                    class: mode === "transport" ? "transport_error" : "usage_limit",
                    code:
                      mode === "transport"
                        ? "httpConnectionFailed"
                        : mode.startsWith("rate-")
                          ? "rateLimitExceeded"
                          : "usageLimitExceeded",
                    retryable: false,
                  },
                  threadDisposition: "reusable",
                });
                return;
              }
              yield* Queue.offer(nativeEvents, {
                type: "turn.terminal",
                driver,
                providerThreadId: input.providerThread.id,
                providerTurnId: ProviderTurnId.make(`native-turn-${nativeStarts}`),
                runOrdinal: input.runOrdinal,
                status: "completed",
                failure: null,
                threadDisposition: "reusable",
              });
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterTurnStartError({
                    driver,
                    providerThreadId: providerThread.id,
                    threadId,
                    runId: input.runId,
                    cause,
                  }),
              ),
            ),
          hasPendingBackgroundWork: Effect.succeed(false),
          steerTurn: unused,
          interruptTurn: (request) =>
            Effect.gen(function* () {
              interruptions.push(request.providerTurnId);
              if (mode === "stop-race" && id === "b") {
                if (request.providerTurnId !== "accepted-b-turn")
                  return yield* new ProviderAdapter.ProviderAdapterProtocolError({
                    driver,
                    detail: "Stale Stop target",
                  });
                yield* Queue.offer(nativeEvents, {
                  type: "turn.terminal",
                  driver,
                  providerThreadId: providerThread.id,
                  providerTurnId: request.providerTurnId,
                  runOrdinal: 1,
                  status: "interrupted",
                  failure: null,
                  threadDisposition: "reusable",
                });
              }
            }),
          respondToRuntimeRequest: unused,
          readThreadSnapshot: unused,
          rollbackThread: unused,
          forkThread: unused,
          mcpApps: {
            listTools: () => Effect.succeed([{ name: `app-${id}`, inputSchema: {} }]),
            callTool: unused,
            readResource: unused,
          },
        };
        return runtime;
      }),
  });
  const runtime = yield* adapter.openSession({
    threadId,
    projectId: ProjectId.make("project-auto"),
    providerSessionId: providerThread.providerSessionId!,
    modelSelection: requestedSelection,
    runtimePolicy,
  });
  yield* runtime.ensureThread({ threadId, modelSelection: requestedSelection, runtimePolicy });
  const turn: ProviderAdapter.ProviderAdapterV2TurnInput = {
    appThread: { projectId: ProjectId.make("project-auto") } as OrchestrationV2AppThread,
    threadId,
    runId: RunId.make("run-auto"),
    runOrdinal: 1,
    providerTurnOrdinal: 1,
    attemptId: RunAttemptId.make("attempt-auto"),
    rootNodeId: NodeId.make("root-auto"),
    providerThread,
    message: {
      messageId: MessageId.make("message-auto"),
      text: "Perform the original action",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: requestedSelection,
    runtimePolicy,
  };
  const readTerminal = () =>
    runtime.events.pipe(
      Stream.tap((event) =>
        Queue.offer(events, event).pipe(
          Effect.andThen(
            event.type === "provider_turn.updated" && event.providerTurn.id === "accepted-b-turn"
              ? Deferred.succeed(bTurnObserved, undefined)
              : Effect.void,
          ),
        ),
      ),
      Stream.filter((event) => event.type === "turn.terminal"),
      Stream.take(1),
      Stream.runCollect,
    );
  return {
    adapter,
    accounts,
    runtime,
    turn,
    starts,
    opens,
    closes,
    readTerminal,
    sharedSessionFile,
    recoveryAwaiting,
    releaseStart,
    log,
    interruptions,
    nativeReusable,
  };
});

it.layer(NodeServices.layer)("Codex Auto native recovery", (it) => {
  it.effect("rejects reuse when its native Codex runtime has terminated", () =>
    Effect.gen(function* () {
      const h = yield* harness();
      expect(yield* h.runtime.isReusable!).toBe(true);
      yield* Ref.set(h.nativeReusable, false);
      expect(yield* h.runtime.isReusable!).toBe(false);
    }),
  );

  it.effect("keeps the durable thread identity across successive turns after native creation", () =>
    Effect.gen(function* () {
      const h = yield* harness("identity");
      const boundThread = {
        ...h.turn.providerThread,
        id: ProviderThreadId.make("durable-thread-id"),
      };
      const firstTerminal = yield* h.readTerminal().pipe(Effect.forkScoped);
      yield* h.runtime.startTurn({ ...h.turn, providerThread: boundThread });
      expect((yield* Fiber.join(firstTerminal))[0]).toMatchObject({
        providerThreadId: boundThread.id,
        status: "completed",
      });
      const secondTerminal = yield* h.readTerminal().pipe(Effect.forkScoped);
      yield* h.runtime.startTurn({
        ...h.turn,
        providerThread: boundThread,
        runOrdinal: 2,
        providerTurnOrdinal: 2,
      });
      expect((yield* Fiber.join(secondTerminal))[0]).toMatchObject({
        providerThreadId: boundThread.id,
        status: "completed",
      });
      expect(
        h.starts.map(({ input }) => [input.providerThread.id, input.providerTurnOrdinal]),
      ).toEqual([
        [boundThread.id, 1],
        [boundThread.id, 2],
      ]);
      expect(h.opens).toEqual(["a"]);
    }),
  );

  it.effect(
    "rejects a stale client's unsupported service tier without reserving another account",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const error = yield* h.adapter.validateModelSelection!({
          ...selection,
          options: [{ id: "serviceTier", value: "ultrafast" }],
        }).pipe(Effect.flip);
        expect(error).toBeInstanceOf(ProviderAdapter.ProviderAdapterProtocolError);
        expect(h.opens).toEqual(["a"]);
      }),
  );
  it.effect(
    "continues A -> B -> A through separate auth overlays using the latest shared native history",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();
        yield* h.runtime.startTurn(h.turn);
        expect((yield* h.readTerminal())[0]).toMatchObject({ status: "completed" });
        expect(h.opens).toEqual(["a", "b"]);
        expect(h.closes).toEqual(["a"]);
        expect(h.starts[1]!.history).toContain("tool-result-a-1");
        expect(h.starts[1]!.input.message.text).not.toBe(h.turn.message.text);
        expect(h.starts[1]!.input.modelSelection).toEqual(selection);
        expect(h.starts[1]!.input.runtimePolicy).toEqual(runtimePolicy);
        expect(h.starts[1]!.input.runId).toBe(h.turn.runId);
        expect(
          (yield* h.runtime.mcpApps!.listTools({
            providerThread: h.turn.providerThread,
            server: "test",
          }))[0]!.name,
        ).toBe("app-b");
        const b = h.accounts[1]!;
        b.set({
          ...b.read(),
          usageLimits: {
            ...b.read().usageLimits!,
            windows: b
              .read()
              .usageLimits!.windows.map((window) => ({ ...window, usedPercent: 100 })),
          },
        });
        yield* TestClock.adjust("61 seconds");
        const resetAt = DateTime.formatIso(yield* DateTime.now);
        for (const account of h.accounts)
          account.set({
            ...account.read(),
            usageLimits: {
              ...account.read().usageLimits!,
              checkedAt: resetAt,
              paidCreditsCheckedAt: resetAt,
              windows: account
                .read()
                .usageLimits!.windows.map((window) => ({ ...window, observedAt: resetAt })),
            },
          });
        yield* h.runtime.startTurn({ ...h.turn, runOrdinal: 2 });
        expect((yield* h.readTerminal())[0]).toMatchObject({ status: "completed" });
        expect(h.opens).toEqual(["a", "b", "a"]);
        expect(h.starts[2]!.history).toContain("tool-result-b-2");
        expect(h.starts[2]!.input.providerThread.nativeThreadRef).toEqual(
          h.turn.providerThread.nativeThreadRef,
        );
        expect(h.starts[1]!.input.providerThread.nativeThreadRef).toEqual(
          h.turn.providerThread.nativeThreadRef,
        );
      }),
  );

  it.effect("releases the native writer on A before B resumes the thread mid-turn", () =>
    Effect.gen(function* () {
      const h = yield* harness();
      yield* h.runtime.startTurn(h.turn);
      const terminal = (yield* h.readTerminal())[0];
      expect(terminal).toMatchObject({ status: "completed", runOrdinal: h.turn.runOrdinal });
      expect(h.log).toEqual([
        "open:a",
        "start:a",
        "unload:a",
        "close:a",
        "open:b",
        "resume:b",
        "start:b",
      ]);
      expect(h.starts[1]!.input).toMatchObject({
        runId: h.turn.runId,
        providerTurnOrdinal: h.turn.providerTurnOrdinal + 1,
        message: { creationSource: "server", attachments: [] },
      });
    }),
  );

  it.effect.each(["pending", "tool", "transport", "resume-failure", "all-exhausted"] as const)(
    "holds recoverably for %s without replaying the original turn",
    (mode) =>
      Effect.gen(function* () {
        const h = yield* harness(mode);
        yield* h.runtime.startTurn(h.turn);
        expect((yield* h.readTerminal())[0]).toMatchObject({ status: "failed" });
        expect(h.starts).toHaveLength(1);
        expect(h.opens.length).toBeLessThanOrEqual(mode === "resume-failure" ? 2 : 1);
        expect(h.turn.providerThread.nativeThreadRef?.nativeId).toBe("native-shared-history");
      }),
  );

  it.effect("reports pool exhaustion with a reset for the shared recovery worker", () =>
    Effect.gen(function* () {
      const h = yield* harness("all-exhausted");
      yield* h.runtime.startTurn(h.turn);
      const terminal = (yield* h.readTerminal())[0];
      expect(terminal).toMatchObject({ status: "failed", failure: { class: "usage_limit" } });
      expect(terminal?.status === "failed" && terminal.failure.resetAt).toBeTruthy();
      expect(h.starts).toHaveLength(1);
    }),
  );

  it.effect.each(["rate-current", "rate-stale"] as const)(
    "requires fresh quota confirmation for %s rate-limit failures",
    (mode) =>
      Effect.gen(function* () {
        const h = yield* harness(mode);
        yield* h.runtime.startTurn(h.turn);
        expect((yield* h.readTerminal())[0]).toMatchObject({
          status: mode === "rate-current" ? "completed" : "failed",
        });
        expect(h.opens).toHaveLength(mode === "rate-current" ? 2 : 1);
      }),
  );

  it.effect(
    "finalizes the latest accepted root after an ambiguous continuation start without replay",
    () =>
      Effect.gen(function* () {
        const h = yield* harness("start-failure");
        const terminal = yield* h
          .readTerminal()
          .pipe(Effect.forkScoped({ startImmediately: true }));
        yield* h.runtime.startTurn(h.turn);
        expect((yield* Fiber.join(terminal))[0]).toMatchObject({
          status: "failed",
          providerTurnId: "accepted-b-turn",
        });
        expect(h.starts).toHaveLength(2);
      }),
  );

  it.effect.each(["admission-ensure", "admission-resume", "admission-start"] as const)(
    "retries confirmed %s exhaustion before any A inference",
    (mode) =>
      Effect.gen(function* () {
        const h = yield* harness(mode);
        if (mode === "admission-resume")
          yield* h.runtime.resumeThread({
            providerThread: h.turn.providerThread,
            threadId,
            modelSelection: selection,
            runtimePolicy,
          });
        yield* h.runtime.startTurn(h.turn);
        expect((yield* h.readTerminal())[0]).toMatchObject({ status: "completed" });
        expect(h.opens).toEqual(["a", "b"]);
        expect(h.starts).toHaveLength(1);
        expect(h.starts[0]!.account).toBe("b");
        expect(h.starts[0]!.input.message.text).toBe(h.turn.message.text);
      }),
  );

  it.effect("bounds retries when all actual preflight allowances are exhausted", () =>
    Effect.gen(function* () {
      const h = yield* harness("admission-all");
      expect((yield* h.runtime.startTurn(h.turn).pipe(Effect.result))._tag).toBe("Failure");
      expect(h.opens).toEqual(["a", "b"]);
      expect(h.starts).toHaveLength(0);
    }),
  );

  it.effect("honors Stop for A while B is accepting a replacement native turn", () =>
    Effect.gen(function* () {
      const h = yield* harness("stop-race");
      const terminal = yield* h.readTerminal().pipe(Effect.forkScoped({ startImmediately: true }));
      yield* h.runtime.startTurn(h.turn);
      yield* Deferred.await(h.recoveryAwaiting);
      yield* h.runtime.interruptTurn({
        providerThread: h.turn.providerThread,
        providerTurnId: ProviderTurnId.make("native-turn-a"),
      });
      yield* Deferred.succeed(h.releaseStart, undefined);
      expect((yield* Fiber.join(terminal))[0]).toMatchObject({
        status: "interrupted",
        providerTurnId: "accepted-b-turn",
      });
      expect(h.interruptions.every((id) => id === "accepted-b-turn")).toBe(true);
      expect(h.starts).toHaveLength(2);
    }),
  );

  it.effect("selects an allowed project member before creating native state", () =>
    Effect.gen(function* () {
      const h = yield* harness("project");
      expect(h.opens).toEqual(["b"]);
    }),
  );

  it.effect("makes omitted reasoning and service tier explicit before inference", () =>
    Effect.gen(function* () {
      const h = yield* harness("recover", { instanceId: poolId, model: CODEX_AUTO_MODEL });
      yield* h.runtime.startTurn(h.turn);
      expect((yield* h.readTerminal())[0]).toMatchObject({ status: "completed" });
      for (const start of h.starts)
        expect(start.input.modelSelection.options).toEqual([
          { id: "serviceTier", value: "default" },
          { id: "reasoningEffort", value: "low" },
        ]);
    }),
  );

  it.effect(
    "resumes the existing native goal with its budget and the latest provider turn ordinal",
    () =>
      Effect.gen(function* () {
        const h = yield* harness("goal");
        yield* h.runtime.startTurn(h.turn);
        expect((yield* h.readTerminal())[0]).toMatchObject({ status: "completed" });
        expect(h.starts[1]!.input.message.text).toBe("/goal resume");
        expect(h.starts[1]!.input.message.attachments).toEqual([]);
        expect(h.starts[1]!.input.providerTurnOrdinal).toBe(8);
        expect(h.starts[1]!.input.providerThread.goal).toEqual(h.turn.providerThread.goal);
      }),
  );
});
