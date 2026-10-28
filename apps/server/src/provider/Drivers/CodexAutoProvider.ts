import {
  CodexSettings,
  ProviderDriverKind,
  ProviderSetupError,
  TextGenerationError,
  readCodexAutoPoolMemberIds,
  resolveProjectProviderInstanceEnabled,
  type ProjectId,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as Scope from "effect/Scope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import type {
  ProviderDriverCreateInput,
  ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import { withInstanceIdentity } from "@t3tools/provider-core/server/instanceIdentity";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderInstanceLookup from "../ProviderInstanceLookup.ts";
import {
  createCodexAdapterV2,
  type CodexAdapterV2DriverEnv,
} from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { makePendingCodexProvider } from "../CodexProvider.ts";
import { ProviderDriverError } from "../Errors.ts";
import { codexContinuationIdentity, resolveCodexHomeLayout } from "./CodexHomeLayout.ts";
import {
  codexAutoModels,
  codexAutoAccountKey,
  codexAutoHeadroom,
  codexAutoAdmissionQuotaError,
} from "../codexAutoPolicy.ts";
import { codexRateLimitsToLimits } from "../codexUsageLimits.ts";
import { makeCodexAutoRouter, type CodexAutoMember } from "../CodexAutoRouter.ts";
import { makeCodexAutoAdapter } from "../CodexAutoAdapter.ts";
import { resolveCodexLaunchArgs } from "../codexLaunchArgs.ts";

const decodeCodexSettings = Schema.decodeUnknownEffect(CodexSettings);
const USAGE_REFRESH_INTERVAL = Duration.minutes(10);
const defaultCodexSettings = Schema.decodeSync(CodexSettings)({});

/** Pool members serve Auto through their own adapter copy; direct selection would race its routing. */
export const reserveCodexAutoPoolMember = Effect.fn("reserveCodexAutoPoolMember")(function* (
  adapter: ProviderAdapter.ProviderAdapterV2["Service"],
  instanceId: ProviderInstanceId,
) {
  const host = yield* ProviderHost.ProviderHost;
  const driver = ProviderDriverKind.make("codex");
  const ensureDirect = host.settings.get.pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapter.ProviderAdapterProtocolError({
          driver,
          detail: "Codex could not read provider settings to check Codex Auto membership.",
          cause,
        }),
    ),
    Effect.flatMap((settings) =>
      Object.values(settings.providerInstances).some(
        (entry) =>
          entry.driver === "codex" && readCodexAutoPoolMemberIds(entry.config).includes(instanceId),
      )
        ? Effect.fail(
            new ProviderAdapter.ProviderAdapterProtocolError({
              driver,
              detail:
                "This Codex account is reserved for Codex Auto and cannot be selected directly.",
            }),
          )
        : Effect.void,
    ),
  );
  return {
    ...adapter,
    validateModelSelection: (selection, projectId) =>
      ensureDirect.pipe(
        Effect.andThen(adapter.validateModelSelection?.(selection, projectId) ?? Effect.void),
      ),
    openSession: (session) =>
      ensureDirect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver,
              providerSessionId: session.providerSessionId,
              cause,
            }),
        ),
        Effect.andThen(adapter.openSession(session)),
      ),
  } satisfies ProviderAdapter.ProviderAdapterV2["Service"];
});

export const makeCodexAutoProvider = Effect.fn("makeCodexAutoProvider")(function* (
  input: ProviderDriverCreateInput<CodexSettings>,
) {
  const registry = yield* ProviderInstanceLookup.ProviderInstanceLookup;
  const host = yield* ProviderHost.ProviderHost;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  // Keep native processes in the calling session's scope, not the provider's lifetime.
  const context = (yield* Effect.context<CodexAdapterV2DriverEnv>()).pipe(
    Context.omit(Scope.Scope),
  );
  const defaultConfig = defaultCodexSettings;
  const defaultLayout = yield* resolveCodexHomeLayout(defaultConfig);
  const ids = input.config.autoPoolMemberIds ?? [];
  const failure = (detail: string) =>
    new ProviderSetupError({ instanceId: input.instanceId, operation: "session", detail });
  if (new Set(ids).size !== ids.length || ids.includes(input.instanceId))
    return yield* new ProviderDriverError({
      driver: ProviderDriverKind.make("codex"),
      instanceId: input.instanceId,
      detail: "Codex Auto requires distinct, non-recursive member instance IDs.",
    });
  const members = Effect.fn("members")(function* (projectId?: ProjectId) {
    return yield* Effect.gen(function* () {
      if (ids.length === 0)
        return yield* failure("Select Codex Auto subscription accounts before starting a session.");
      const settings = yield* host.settings.get.pipe(
        Effect.mapError(() => failure("Codex Auto could not read provider configuration.")),
      );
      const result: CodexAutoMember[] = [];
      for (const id of ids) {
        const entry = settings.providerInstances[id];
        if (entry?.driver !== undefined && entry.driver !== "codex")
          return yield* failure("Codex Auto only supports Codex subscription accounts.");
        const config = yield* decodeCodexSettings(entry?.config ?? {}).pipe(
          Effect.mapError(() => failure("A Codex Auto member has invalid configuration.")),
        );
        if (config.autoPoolMemberIds || config.setupMode === "managed")
          return yield* failure(
            "Codex Auto members must be direct subscription accounts, not another pool or managed API account.",
          );
        const layout = yield* resolveCodexHomeLayout(config);
        const shared = yield* fs
          .realPath(layout.sharedHomePath)
          .pipe(Effect.orElseSucceed(() => path.resolve(layout.sharedHomePath)));
        const expected = yield* fs
          .realPath(defaultLayout.sharedHomePath)
          .pipe(Effect.orElseSucceed(() => path.resolve(defaultLayout.sharedHomePath)));
        if (shared !== expected || layout.continuationKey !== defaultLayout.continuationKey)
          return yield* failure(
            "All Codex Auto accounts must share the default ~/.codex session home and continuation identity. Separate shadow authentication homes are supported.",
          );
        if (
          entry?.environment?.some((variable) =>
            [
              "CODEX_HOME",
              "OPENAI_API_KEY",
              "CODEX_API_KEY",
              "OPENAI_BASE_URL",
              "ACCESS_TOKEN",
            ].includes(variable.name),
          )
        )
          return yield* failure(
            "Codex Auto does not allow member environment overrides for authentication, API billing, or Codex home.",
          );
        const instance = yield* registry.get(id);
        if (
          !instance ||
          instance.driverKind !== "codex" ||
          instance.continuationIdentity.continuationKey !== defaultLayout.continuationKey
        )
          return yield* failure(
            "A configured Codex Auto member is unavailable or has an incompatible session home.",
          );
        result.push({
          instance,
          snapshot: yield* instance.snapshot.getSnapshot,
          allowed:
            projectId === undefined ||
            resolveProjectProviderInstanceEnabled(settings, projectId, id),
        });
      }
      return result;
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      Effect.provideService(HostProcess.HomeDirectory, home),
    );
  });
  const stamp = withInstanceIdentity({
    instanceId: input.instanceId,
    driverKind: ProviderDriverKind.make("codex"),
    displayName: input.displayName ?? "Codex Auto",
    accentColor: input.accentColor,
    continuationGroupKey: defaultLayout.continuationKey,
  });
  const pending = makePendingCodexProvider({
    ...defaultConfig,
    enabled: input.enabled && ids.length > 0,
  }).pipe(
    Effect.map((draft) =>
      stamp({ ...draft, authoritativeModels: true, models: [], supportsTextGeneration: false }),
    ),
  );
  const check = Effect.gen(function* () {
    const base = yield* pending;
    if (!input.enabled) return base;
    if (ids.length === 0)
      return {
        ...base,
        message: "Select Codex subscription accounts in provider settings to enable Auto.",
      };
    const accounts = yield* members();
    return {
      ...base,
      installed: accounts.every((account) => account.snapshot.installed),
      status: "ready" as const,
      message: `Using ${accounts.length} selected Codex subscription account${accounts.length === 1 ? "" : "s"}.`,
      auth: { status: "authenticated" as const, type: "chatgpt" },
      models: codexAutoModels(accounts.map((account) => account.snapshot)),
    };
  }).pipe(
    Effect.catch((cause) =>
      pending.pipe(
        Effect.map((base) => ({ ...base, status: "error" as const, message: cause.message })),
      ),
    ),
  );
  const snapshot = yield* makeManagedServerProvider({
    resolveMaintenance: () =>
      Effect.succeed({
        provider: ProviderDriverKind.make("codex"),
        packageName: null,
        update: null,
      }),
    getSettings: host.settings.get,
    streamSettings: host.settings.changes,
    haveSettingsChanged: () => false,
    initialSnapshot: () => pending,
    checkProvider: check,
  }).pipe(
    Effect.mapError(
      (cause) =>
        new ProviderDriverError({
          driver: ProviderDriverKind.make("codex"),
          instanceId: input.instanceId,
          detail: "Could not prepare Codex Auto.",
          cause,
        }),
    ),
  );
  const registryChanges = yield* registry.subscribe;
  yield* Stream.concat(Stream.succeed(undefined), registryChanges).pipe(
    Stream.switchMap(() =>
      Stream.fromEffect(members().pipe(Effect.orElseSucceed(() => []))).pipe(
        Stream.flatMap((accounts) =>
          Stream.concat(
            Stream.succeed(undefined),
            Stream.mergeAll(
              accounts.map((account) => account.instance.snapshot.streamChanges),
              { concurrency: "unbounded" },
            ),
          ),
        ),
      ),
    ),
    Stream.runForEach(() => snapshot.refresh),
    Effect.forkScoped,
  );
  const router = yield* makeCodexAutoRouter({ instanceId: input.instanceId, members }).pipe(
    Effect.mapError(
      (cause) =>
        new ProviderDriverError({
          driver: ProviderDriverKind.make("codex"),
          instanceId: input.instanceId,
          detail: "Could not load Codex Auto routing state.",
          cause,
        }),
    ),
  );
  // Routing reads cached allowance, so keep idle accounts current off the request path.
  // Active accounts also update from the rate limits Codex streams during turns.
  if (input.enabled && ids.length > 0)
    yield* router
      .refreshStale(Duration.toMillis(USAGE_REFRESH_INTERVAL))
      .pipe(Effect.repeat(Schedule.spaced(USAGE_REFRESH_INTERVAL)), Effect.forkScoped);
  const orchestrationAdapter = makeCodexAutoAdapter({
    instanceId: input.instanceId,
    router,
    openNative: Effect.fn("CodexAutoProvider.openNative")(
      function* (
        member: CodexAutoMember,
        session: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
      ) {
        const current = (yield* members(session.projectId)).find(
          (candidate) => candidate.instance === member.instance,
        );
        if (
          !current ||
          current.allowed === false ||
          codexAutoAccountKey(current.snapshot) !== codexAutoAccountKey(member.snapshot)
        )
          return yield* failure(
            "The selected Codex Auto account changed during admission. Retry with the current configuration.",
          );
        const settings = yield* host.settings.get;
        const entry = settings.providerInstances[member.instance.instanceId];
        const config = yield* decodeCodexSettings(entry?.config ?? {});
        // Codex reserves the built-in OpenAI provider; redefining it aborts startup.
        const launchArgs = `${resolveCodexLaunchArgs(config.launchArgs, yield* HostProcess.Environment)} -c 'forced_login_method="chatgpt"' -c 'model_provider="openai"'`;
        const environment = [
          ...((yield* registry.environment(member.instance.instanceId)) ?? []),
          { name: "T3CODE_CODEX_LAUNCH_ARGS", value: launchArgs, sensitive: false },
          ...["OPENAI_API_KEY", "CODEX_API_KEY", "ACCESS_TOKEN"].map((name) => ({
            name,
            value: "",
            sensitive: false,
          })),
        ];
        const adapter = yield* createCodexAdapterV2(
          { ...input, environment, config: { ...config, homePath: defaultLayout.sharedHomePath } },
          {
            onUsageLimits: (update) => member.instance.snapshot.applyUsageLimits(update),
            validateSubscription: Effect.fn("validateSubscription")(
              ({ account, rateLimits, modelSelection }) =>
                Effect.gen(function* () {
                  const now = yield* DateTime.now;
                  const checkedAt = DateTime.formatIso(now);
                  const usageLimits = codexRateLimitsToLimits({
                    snapshot: rateLimits.rateLimits,
                    rateLimitsByLimitId: rateLimits.rateLimitsByLimitId,
                    checkedAt,
                  });
                  const auth = {
                    status: "authenticated" as const,
                    type: account.account?.type,
                    email:
                      account.account?.type === "chatgpt"
                        ? (account.account.email ?? undefined)
                        : undefined,
                    workspaceId: rateLimits.accountId ?? undefined,
                  };
                  const observed = { ...member.snapshot, auth, usageLimits };
                  if (codexAutoAccountKey(observed) === codexAutoAccountKey(member.snapshot))
                    yield* member.instance.snapshot.applyUsageLimits({
                      checkedAt,
                      windows: usageLimits.windows,
                      paidCreditsAvailable: usageLimits.paidCreditsAvailable,
                    });
                  if (
                    account.account?.type === "chatgpt" &&
                    codexAutoAccountKey(observed) === codexAutoAccountKey(member.snapshot) &&
                    (rateLimits.ordinaryUsageAllowed === false ||
                      usageLimits.windows.some(
                        (window) =>
                          window.usedPercent >= 100 &&
                          (window.model === undefined || window.model === modelSelection.model),
                      ))
                  )
                    return yield* codexAutoAdmissionQuotaError(input.instanceId);
                  if (
                    account.account?.type !== "chatgpt" ||
                    rateLimits.ordinaryUsageAllowed === false ||
                    rateLimits.rateLimits.spendControlReached === true ||
                    codexAutoAccountKey(observed) !== codexAutoAccountKey(member.snapshot) ||
                    !codexAutoHeadroom(observed, modelSelection, DateTime.toEpochMillis(now))
                  )
                    return yield* failure(
                      "Codex Auto could not verify the selected subscription account and unused allowance without paid credits. No inference was started.",
                    );
                }),
            ),
          },
        );
        return yield* adapter.openSession(session);
      },
      (effect, _member, session) =>
        effect.pipe(
          Effect.provideContext(context),
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: ProviderDriverKind.make("codex"),
                providerSessionId: session.providerSessionId,
                cause,
              }),
          ),
        ),
    ),
  });
  // Auto only routes durable agent sessions; background title/commit helpers use an explicitly chosen provider.
  const textGeneration: ProviderInstance["textGeneration"] = {
    generateCommitMessage: () =>
      Effect.fail(
        new TextGenerationError({
          operation: "generateCommitMessage",
          detail: "Choose a physical provider for text generation.",
        }),
      ),
    generatePrContent: () =>
      Effect.fail(
        new TextGenerationError({
          operation: "generatePrContent",
          detail: "Choose a physical provider for text generation.",
        }),
      ),
    generateTranscriptionPostProcessing: () =>
      Effect.fail(
        new TextGenerationError({
          operation: "generateTranscriptionPostProcessing",
          detail: "Choose a physical provider for text generation.",
        }),
      ),
    generateBranchName: () =>
      Effect.fail(
        new TextGenerationError({
          operation: "generateBranchName",
          detail: "Choose a physical provider for text generation.",
        }),
      ),
    generateThreadTitle: () =>
      Effect.fail(
        new TextGenerationError({
          operation: "generateThreadTitle",
          detail: "Choose a physical provider for text generation.",
        }),
      ),
    generateReviewGuide: () =>
      Effect.fail(
        new TextGenerationError({
          operation: "generateReviewGuide",
          detail: "Choose a physical provider for text generation.",
        }),
      ),
  };
  return {
    instanceId: input.instanceId,
    driverKind: ProviderDriverKind.make("codex"),
    continuationIdentity: codexContinuationIdentity(defaultLayout),
    displayName: input.displayName ?? "Codex Auto",
    accentColor: input.accentColor,
    enabled: input.enabled,
    snapshot,
    orchestrationAdapter,
    textGeneration,
  } satisfies ProviderInstance;
});
