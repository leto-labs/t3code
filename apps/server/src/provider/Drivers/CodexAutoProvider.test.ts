import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  CodexSettings,
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
  type ProviderInstanceConfigMap,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Scope from "effect/Scope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ServerConfig from "../../config.ts";
import { CodexAppServerClientFactory } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { ProviderInstanceLookup } from "../ProviderInstanceLookup.ts";
import { codexAutoFixture } from "../testUtils/codexAuto.ts";
import { makeCodexAutoProvider } from "./CodexAutoProvider.ts";

const decodeCodexSettings = Schema.decodeEffect(CodexSettings);

const poolId = ProviderInstanceId.make("codex_auto");

const makeFixture = Effect.fn("CodexAutoProvider.test.fixture")(function* (
  mode: "valid" | "other-home" | "recursive" | "managed" | "empty" = "valid",
  open: CodexAppServerClientFactory["Service"]["open"] = () =>
    Effect.die("This test must never start a provider process."),
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-auto-provider-" });
  const sharedHome = path.join(baseDir, ".codex");
  yield* fs.makeDirectory(sharedHome);
  const accounts = [
    codexAutoFixture("a", DateTime.formatIso(yield* DateTime.now), [
      "default",
      "priority",
      "ultrafast",
    ]),
    codexAutoFixture("b", DateTime.formatIso(yield* DateTime.now)),
  ];
  const registryChanges = yield* PubSub.unbounded<void>();
  const physicalChanges = yield* PubSub.unbounded<void>();
  const configs: ProviderInstanceConfigMap = Object.fromEntries(
    accounts.map((account, index) => [
      account.instance.instanceId,
      {
        driver: ProviderDriverKind.make("codex"),
        config: {
          homePath:
            mode === "other-home" && index === 1
              ? path.join(baseDir, "different-home")
              : sharedHome,
          shadowHomePath: path.join(baseDir, `auth-${index}`),
          ...(mode === "recursive" && index === 1 ? { autoPoolMemberIds: [poolId] } : {}),
          ...(mode === "managed" && index === 1 ? { setupMode: "managed" } : {}),
        },
      },
    ]),
  );
  const settings = { ...DEFAULT_SERVER_SETTINGS, providerInstances: configs };
  const config = yield* decodeCodexSettings({
    autoPoolMemberIds:
      mode === "empty" ? [] : accounts.map((account) => account.instance.instanceId),
  });
  const instances = accounts.map((account) => ({
    ...account.instance,
    continuationIdentity: {
      driverKind: ProviderDriverKind.make("codex"),
      continuationKey: `codex:home:${sharedHome}`,
    },
    snapshot: {
      ...account.instance.snapshot,
      streamChanges: Stream.fromPubSub(physicalChanges).pipe(Stream.map(() => account.read())),
    },
  }));
  const provider = yield* makeCodexAutoProvider({
    instanceId: poolId,
    displayName: undefined,
    enabled: true,
    environment: [],
    config,
  }).pipe(
    Effect.provideService(HostProcess.HomeDirectory, baseDir),
    Effect.provideService(ProviderInstanceLookup, {
      get: (id) => Effect.succeed(instances.find((instance) => instance.instanceId === id)),
      environment: () => Effect.succeed([]),
      changes: Stream.fromPubSub(registryChanges),
      subscribe: PubSub.subscribe(registryChanges).pipe(Effect.map(Stream.fromSubscription)),
    }),
    Effect.provideService(ProviderHost.ProviderHost, {
      paths: {
        baseDir,
        stateDir: baseDir,
        cwd: baseDir,
        attachmentsDir: baseDir,
        providerStatusCacheDir: baseDir,
      },
      settings: {
        get: Effect.succeed(settings),
        changes: Stream.empty,
        subscribe: Effect.succeed(Stream.empty),
        withSnapshot: (use) => use(settings),
      },
      shouldRunBackgroundWork: () => Effect.succeed(false),
      resolveAttachmentPath: () => null,
      credentials: () => Effect.die("Unused fixture credentials"),
    }),
    Effect.provide(
      Layer.mergeAll(
        ServerConfig.layerTest(baseDir, baseDir),
        Layer.mock(CodexAppServerClientFactory)({
          open,
        }),
        IdAllocator.layer,
        McpProviderSessions.layer,
        NodeCrypto.layer,
      ),
    ),
  );
  return { provider, accounts, registryChanges, physicalChanges };
});

it.layer(NodeServices.layer)("Codex Auto instance", (it) => {
  it.effect("releases native resources with the session while the provider stays alive", () =>
    Effect.gen(function* () {
      let opened = false;
      let closed = false;
      const { provider } = yield* makeFixture("valid", () =>
        Effect.acquireRelease(
          Effect.sync(() => {
            opened = true;
          }),
          () =>
            Effect.sync(() => {
              closed = true;
            }),
        ).pipe(Effect.andThen(Effect.die("Session initialization refused"))),
      );
      const sessionScope = yield* Scope.make();
      const result = yield* provider.orchestrationAdapter
        .openSession({
          threadId: ThreadId.make("scope-test-thread"),
          providerSessionId: ProviderSessionId.make("scope-test-session"),
          modelSelection: { instanceId: poolId, model: "gpt-test" },
          runtimePolicy: {
            cwd: "/test-workspace",
            runtimeMode: "full-access",
            interactionMode: "default",
          },
        })
        .pipe(Effect.provideService(Scope.Scope, sessionScope), Effect.exit);
      yield* Scope.close(sessionScope, Exit.void);
      expect(Exit.isFailure(result)).toBe(true);
      expect(opened).toBe(true);
      expect(closed).toBe(true);
      expect((yield* provider.snapshot.refresh).status).toBe("ready");
    }),
  );

  it.effect(
    "accepts separate authentication overlays and publishes a normal authoritative Codex entry",
    () =>
      Effect.gen(function* () {
        const { provider } = yield* makeFixture();
        const snapshot = yield* provider.snapshot.refresh;
        expect(snapshot).toMatchObject({
          instanceId: poolId,
          driver: "codex",
          displayName: "Codex Auto",
          authoritativeModels: true,
          status: "ready",
        });
        expect(snapshot.models).toHaveLength(1);
        expect(
          snapshot.models[0]!.capabilities?.optionDescriptors?.find(
            (option) => option.id === "serviceTier",
          ),
        ).toMatchObject({ options: [{ id: "default" }, { id: "priority" }] });
        expect(
          (yield* provider.orchestrationAdapter.getCapabilities()).sessions
            .supportsMultipleProviderThreadsPerSession,
        ).toBe(false);
      }),
  );

  it.effect("keeps an empty pool disabled without starting a harness or advertising models", () =>
    Effect.gen(function* () {
      const { provider } = yield* makeFixture("empty");
      const snapshot = yield* provider.snapshot.refresh;
      expect(snapshot).toMatchObject({
        enabled: false,
        authoritativeModels: true,
        models: [],
        supportsTextGeneration: false,
      });
      expect(snapshot.message).toContain("Select Codex subscription accounts");
    }),
  );

  it.effect.each(["other-home", "recursive", "managed"] as const)(
    "rejects %s members without advertising capabilities",
    (mode) =>
      Effect.gen(function* () {
        const { provider } = yield* makeFixture(mode);
        expect(yield* provider.snapshot.refresh).toMatchObject({ status: "error", models: [] });
      }),
  );

  it.effect("rechecks configured membership and narrows capabilities after reconciliation", () =>
    Effect.gen(function* () {
      const { provider, accounts, registryChanges } = yield* makeFixture();
      yield* provider.snapshot.refresh;
      const changed = yield* provider.snapshot.streamChanges.pipe(
        Stream.filter((snapshot) => snapshot.models.length === 0),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped({ startImmediately: true }),
      );
      accounts[1]!.set({ ...accounts[1]!.read(), models: [] });
      yield* PubSub.publish(registryChanges, undefined);
      yield* Fiber.join(changed);
    }),
  );
});
