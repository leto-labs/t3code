import {
  ClaudeSettings,
  ProviderDriverKind,
  ProviderSetupError,
  resolveProjectProviderInstanceEnabled,
  type ModelSelection,
  type ProjectId,
  type ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ModelCatalog from "@t3tools/provider-core/server/ModelCatalog";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import type {
  ProviderDriverCreateInput,
  ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import { withInstanceIdentity } from "@t3tools/provider-core/server/instanceIdentity";
import {
  ClaudeProviderCapabilitiesV2,
  createClaudeAdapterV2,
  isClaudeUsageLimitPause,
  type ClaudeAdapterV2DriverEnv,
} from "../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as ProviderInstanceLookup from "../ProviderInstanceLookup.ts";
import { makePendingClaudeProvider } from "../ClaudeProvider.ts";
import { resolveClaudeModelCatalog } from "../ClaudeModelCatalog.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeAutoPoolAdapter } from "../AutoPoolAdapter.ts";
import {
  followAutoPoolMembers,
  keepAutoPoolUsageFresh,
  makeAutoPoolRouter,
  type AutoPoolMember,
} from "../AutoPoolRouter.ts";
import {
  autoPoolAccountKey,
  autoPoolEligible,
  autoPoolHeadroom,
  autoPoolAuthStatus,
  autoPoolModels,
  type AutoPoolHeadroom,
} from "../autoPoolPolicy.ts";
import { autoPoolTextGeneration } from "../autoPoolTextGeneration.ts";
import { transferClaudeSession } from "../claudeSessionTransfer.ts";
import { mergeClaudeProcessEnvironment, resolveClaudeHomePath } from "./ClaudeHome.ts";

const DRIVER = ProviderDriverKind.make("claudeAgent");
const LABEL = "Claude Auto";
const decodeClaudeSettings = Schema.decodeUnknownEffect(ClaudeSettings);
const defaultClaudeSettings = Schema.decodeSync(ClaudeSettings)({});
// A member must bill its own subscription login, not an API key or another backend.
const BILLING_OVERRIDES = [
  "ANTHROPIC_API_KEY",
  "ANTHROPIC_AUTH_TOKEN",
  "ANTHROPIC_BASE_URL",
  "CLAUDE_CODE_OAUTH_TOKEN",
  "CLAUDE_CODE_USE_BEDROCK",
  "CLAUDE_CODE_USE_VERTEX",
  "CLAUDE_CODE_USE_FOUNDRY",
];
const NOT_SUBSCRIPTION_AUTH = new Set(["apiKey", "bedrock", "customEndpoint"]);

/** Members share this key while pooled, so moving a member's thread to Auto keeps its session. */
export const claudeAutoContinuationKey = (autoInstanceId: string) =>
  `claude:auto:${autoInstanceId}`;

/** Routing reads cached allowance from subscription accounts only. */
export function claudeAutoHeadroom(
  provider: ServerProvider,
  selection: ModelSelection,
  now: number,
): AutoPoolHeadroom | undefined {
  const type = provider.auth.type;
  if (!autoPoolEligible(provider) || type === undefined || NOT_SUBSCRIPTION_AUTH.has(type))
    return undefined;
  return autoPoolHeadroom(provider, selection.model, now);
}

export const makeClaudeAutoRouter = (
  input: Pick<
    Parameters<typeof makeAutoPoolRouter>[0],
    "instanceId" | "members" | "maxRunningTurns"
  >,
) =>
  makeAutoPoolRouter({
    ...input,
    label: LABEL,
    stateDirectory: "claude-auto",
    models: (members) => autoPoolModels(members),
    headroom: claudeAutoHeadroom,
  });

export const makeClaudeAutoAdapter = Effect.fn("makeClaudeAutoAdapter")(function* (
  input: Pick<Parameters<typeof makeAutoPoolAdapter>[0], "instanceId" | "router" | "openNative"> & {
    /** The member's Claude home, and every pool member's home a newer copy may sit in. */
    readonly sessionHomes: (
      member: AutoPoolMember,
    ) => Effect.Effect<
      { readonly target: string; readonly sources: readonly string[] },
      ProviderSetupError
    >;
  },
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  return makeAutoPoolAdapter({
    ...input,
    driver: DRIVER,
    label: LABEL,
    capabilities: ClaudeProviderCapabilitiesV2,
    // Claude's limit errors do not say which window ran out; a fresh usage read decides.
    quotaFailure: (failure) => (failure.class === "usage_limit" ? "unconfirmed" : undefined),
    // Claude Code can hold a limited turn open until the reset instead of failing it.
    isQuotaPause: isClaudeUsageLimitPause,
    prepareResume: Effect.fn("ClaudeAutoAdapter.prepareResume")(
      function* (member, providerThread) {
        const sessionId = providerThread.nativeThreadRef?.nativeId;
        if (sessionId === undefined || sessionId === null) return;
        const homes = yield* input.sessionHomes(member);
        yield* transferClaudeSession({ sessionId, ...homes });
      },
      (effect, member) =>
        effect.pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, path),
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterProtocolError({
                driver: DRIVER,
                detail: `${LABEL} could not move the conversation to ${member.instance.instanceId}.`,
                cause,
              }),
          ),
        ),
    ),
  });
});

export const makeClaudeAutoProvider = Effect.fn("makeClaudeAutoProvider")(function* (
  input: ProviderDriverCreateInput<ClaudeSettings>,
) {
  const registry = yield* ProviderInstanceLookup.ProviderInstanceLookup;
  const host = yield* ProviderHost.ProviderHost;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  const hostEnvironment = yield* HostProcess.Environment;
  const catalogService = yield* ModelCatalog.ModelCatalog;
  const modelCatalog = catalogService.current(DRIVER).pipe(Effect.map(resolveClaudeModelCatalog));
  // Keep native processes in the calling session's scope, not the provider's lifetime.
  const context = (yield* Effect.context<ClaudeAdapterV2DriverEnv>()).pipe(
    Context.omit(Scope.Scope),
  );
  const ids = input.config.autoPoolMemberIds ?? [];
  const failure = (detail: string) =>
    new ProviderSetupError({ instanceId: input.instanceId, operation: "session", detail });
  if (new Set(ids).size !== ids.length || ids.includes(input.instanceId))
    return yield* new ProviderDriverError({
      driver: DRIVER,
      instanceId: input.instanceId,
      detail: "Claude Auto requires distinct, non-recursive member instance IDs.",
    });

  /** How a member runs: its own config, environment and Claude home. */
  const launch = Effect.fn("ClaudeAutoProvider.launch")(
    function* (id: ProviderInstanceId) {
      const settings = yield* host.settings.get.pipe(
        Effect.mapError(() => failure("Claude Auto could not read provider configuration.")),
      );
      const entry = settings.providerInstances[id];
      if (entry?.driver !== undefined && entry.driver !== DRIVER)
        return yield* failure("Claude Auto only supports Claude subscription accounts.");
      const config = yield* decodeClaudeSettings(entry?.config ?? {}).pipe(
        Effect.mapError(() => failure("A Claude Auto member has invalid configuration.")),
      );
      if (config.autoPoolMemberIds)
        return yield* failure("Claude Auto members must be direct subscription accounts.");
      if (entry?.environment?.some((variable) => BILLING_OVERRIDES.includes(variable.name)))
        return yield* failure(
          "Claude Auto does not allow member environment overrides for API keys or other backends.",
        );
      const environment = (yield* registry.environment(id)) ?? [];
      const processEnvironment = yield* mergeClaudeProcessEnvironment(
        config,
        environment,
        hostEnvironment,
      );
      const claudeHome = yield* resolveClaudeHomePath(config, processEnvironment);
      return { settings, config, environment, claudeHome };
    },
    Effect.provideService(FileSystem.FileSystem, fs),
    Effect.provideService(Path.Path, path),
    Effect.provideService(HostProcess.HomeDirectory, home),
  );

  const members = Effect.fn("ClaudeAutoProvider.members")(function* (projectId?: ProjectId) {
    return yield* Effect.gen(function* () {
      if (ids.length === 0)
        return yield* failure(
          "Select Claude Auto subscription accounts before starting a session.",
        );
      const result: AutoPoolMember[] = [];
      for (const id of ids) {
        const { settings } = yield* launch(id);
        const instance = yield* registry.get(id);
        if (!instance || instance.driverKind !== DRIVER)
          return yield* failure("A configured Claude Auto member is unavailable.");
        result.push({
          instance,
          snapshot: yield* instance.snapshot.getSnapshot,
          allowed:
            projectId === undefined ||
            resolveProjectProviderInstanceEnabled(settings, projectId, id),
        });
      }
      return result;
    });
  });

  const continuationKey = claudeAutoContinuationKey(input.instanceId);
  const stamp = withInstanceIdentity({
    instanceId: input.instanceId,
    driverKind: DRIVER,
    displayName: input.displayName ?? LABEL,
    accentColor: input.accentColor,
    continuationGroupKey: continuationKey,
  });
  const pending = modelCatalog.pipe(
    Effect.flatMap((catalog) =>
      makePendingClaudeProvider(
        { ...defaultClaudeSettings, enabled: input.enabled && ids.length > 0 },
        catalog,
      ),
    ),
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
        message: "Select Claude subscription accounts in provider settings to enable Auto.",
      };
    const accounts = yield* members();
    return {
      ...base,
      installed: accounts.every((account) => account.snapshot.installed),
      status: "ready" as const,
      message: `Using ${accounts.length} selected Claude subscription account${accounts.length === 1 ? "" : "s"}.`,
      auth: { status: autoPoolAuthStatus(accounts.map((account) => account.snapshot)) },
      models: autoPoolModels(accounts.map((account) => account.snapshot)),
    };
  }).pipe(
    Effect.catch((cause) =>
      pending.pipe(
        Effect.map((base) => ({ ...base, status: "error" as const, message: cause.message })),
      ),
    ),
  );
  const snapshot = yield* makeManagedServerProvider({
    resolveMaintenance: () => Effect.succeed({ provider: DRIVER, packageName: null, update: null }),
    getSettings: host.settings.get,
    streamSettings: host.settings.changes,
    haveSettingsChanged: () => false,
    initialSnapshot: () => pending,
    checkProvider: check,
  }).pipe(
    Effect.mapError(
      (cause) =>
        new ProviderDriverError({
          driver: DRIVER,
          instanceId: input.instanceId,
          detail: "Could not prepare Claude Auto.",
          cause,
        }),
    ),
  );
  yield* followAutoPoolMembers({ members, refresh: snapshot.refresh });
  const router = yield* makeClaudeAutoRouter({
    instanceId: input.instanceId,
    members,
    ...(input.config.autoPoolMaxRunningTurns === undefined
      ? {}
      : { maxRunningTurns: input.config.autoPoolMaxRunningTurns }),
  }).pipe(
    Effect.mapError(
      (cause) =>
        new ProviderDriverError({
          driver: DRIVER,
          instanceId: input.instanceId,
          detail: "Could not load Claude Auto routing state.",
          cause,
        }),
    ),
  );
  if (input.enabled && ids.length > 0) yield* keepAutoPoolUsageFresh(router);

  const homeOf = (id: ProviderInstanceId) =>
    launch(id).pipe(Effect.map(({ claudeHome }) => claudeHome));
  const orchestrationAdapter = yield* makeClaudeAutoAdapter({
    instanceId: input.instanceId,
    router,
    sessionHomes: Effect.fn("ClaudeAutoProvider.sessionHomes")(function* (member) {
      const target = yield* homeOf(member.instance.instanceId);
      const sources = yield* Effect.forEach(ids, (id) =>
        homeOf(id).pipe(Effect.orElseSucceed(() => undefined)),
      );
      return {
        target,
        sources: sources.filter((source): source is string => source !== undefined),
      };
    }),
    openNative: Effect.fn("ClaudeAutoProvider.openNative")(
      function* (
        member: AutoPoolMember,
        session: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
      ) {
        const current = (yield* members(session.projectId)).find(
          (candidate) => candidate.instance === member.instance,
        );
        if (
          !current ||
          current.allowed === false ||
          autoPoolAccountKey(current.snapshot) !== autoPoolAccountKey(member.snapshot)
        )
          return yield* failure(
            "The selected Claude Auto account changed during admission. Retry with the current configuration.",
          );
        const { config, environment } = yield* launch(member.instance.instanceId);
        const adapter = yield* createClaudeAdapterV2(
          { ...input, environment, enabled: true, config },
          { onUsageLimits: member.instance.snapshot.applyUsageLimits, modelCatalog },
        );
        return yield* adapter.openSession(session);
      },
      (effect, _member, session) =>
        effect.pipe(
          Effect.provideContext(context),
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: DRIVER,
                providerSessionId: session.providerSessionId,
                cause,
              }),
          ),
        ),
    ),
  });
  return {
    instanceId: input.instanceId,
    driverKind: DRIVER,
    continuationIdentity: { driverKind: DRIVER, continuationKey },
    displayName: input.displayName ?? LABEL,
    accentColor: input.accentColor,
    enabled: input.enabled,
    snapshot,
    orchestrationAdapter,
    textGeneration: autoPoolTextGeneration,
  } satisfies ProviderInstance;
});
