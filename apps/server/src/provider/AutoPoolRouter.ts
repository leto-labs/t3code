import {
  AUTO_POOL_DEFAULT_MAX_RUNNING_TURNS,
  ModelSelection,
  ProviderInstanceId,
  ProviderSetupError,
  ThreadId,
  type ServerProvider,
  type ServerProviderModel,
  type ProjectId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { writeFileStringAtomically } from "@t3tools/shared/atomicWrite";
import * as ServerConfig from "../config.ts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import { AutoPoolAccountLoad } from "./AutoPoolAccountLoad.ts";
import * as ProviderInstanceLookup from "./ProviderInstanceLookup.ts";
import {
  autoPoolAccountKey,
  autoPoolChoose,
  AUTO_POOL_USAGE_REFRESH_INTERVAL_MS,
  autoPoolSubscriptionEligible,
  autoPoolUsageIsStale,
  autoPoolExhausted,
  autoPoolNormalizeSelection,
  autoPoolResetAt,
  autoPoolSelectionAllowed,
  type AutoPoolHeadroom,
} from "./autoPoolPolicy.ts";
import { ProviderUsageLimitError } from "@t3tools/provider-core/server/failure";

const Assignment = Schema.Struct({ memberId: ProviderInstanceId, accountKey: Schema.String });
// Files written before cached routing also carry `cooldowns`; decoding ignores the key.
const RoutingState = Schema.Struct({ assignments: Schema.Record(Schema.String, Assignment) });
type RoutingState = typeof RoutingState.Type;
// Assignments only carry stickiness; dropping the oldest just lets a thread rebalance.
const MAX_ASSIGNMENTS = 1_000;
const REFRESH_TIMEOUT = "20 seconds";

export class AutoPoolRoutingStates extends Context.Reference<
  Map<string, { readonly lock: Semaphore.Semaphore; state: RoutingState | undefined }> | undefined
>("t3/provider/AutoPoolRoutingStates", { defaultValue: () => undefined }) {}
export interface AutoPoolMember {
  readonly instance: ProviderInstance;
  readonly snapshot: ServerProvider;
  readonly allowed?: boolean;
}
export interface AutoPoolReservation extends AutoPoolMember {
  readonly modelSelection: ModelSelection;
}

/** A usage read that bypasses the member's probe cache. */
export const refreshAutoPoolMember = (member: AutoPoolMember) =>
  (member.instance.invalidateCaches ?? Effect.void).pipe(
    Effect.andThen(member.instance.snapshot.refresh),
  );

export class AutoPoolRouter extends Context.Service<
  AutoPoolRouter,
  {
    readonly normalizeSelection: (
      selection: ModelSelection,
      projectId?: ProjectId,
    ) => Effect.Effect<ModelSelection, ProviderSetupError>;
    readonly hasUsageAllowance: (
      selection: ModelSelection,
      projectId: ProjectId,
      observedAfter?: number,
    ) => Effect.Effect<boolean>;
    /** `exclude` holds account keys that already rejected this request. */
    readonly reserve: (
      threadId: ThreadId,
      selection: ModelSelection,
      projectId?: ProjectId,
      exclude?: ReadonlySet<string>,
    ) => Effect.Effect<AutoPoolReservation, ProviderSetupError | ProviderUsageLimitError>;
    readonly release: (threadId: ThreadId) => Effect.Effect<void>;
    /** Re-reads one account's usage after it rejected a request for quota. */
    readonly refreshUsage: (member: AutoPoolMember) => Effect.Effect<void>;
    readonly refreshStale: (maxAgeMs: number, projectId?: ProjectId) => Effect.Effect<void>;
  }
>()("t3/provider/AutoPoolRouter") {}

export const makeAutoPoolRouter = Effect.fn("makeAutoPoolRouter")(function* (input: {
  readonly instanceId: ProviderInstanceId;
  /** Product name for messages, such as "Codex Auto". */
  readonly label: string;
  /** Directory under the state dir that keeps this driver's thread assignments. */
  readonly stateDirectory: string;
  readonly maxRunningTurns?: number | Effect.Effect<number>;
  readonly members: (
    projectId?: ProjectId,
  ) => Effect.Effect<readonly AutoPoolMember[], ProviderSetupError>;
  readonly models: (members: readonly ServerProvider[]) => readonly ServerProviderModel[];
  /** Undefined when the account cannot serve the selection from its cached allowance. */
  readonly headroom: (
    provider: ServerProvider,
    selection: ModelSelection,
    now: number,
  ) => AutoPoolHeadroom | undefined;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig.ServerConfig;
  const load = yield* AutoPoolAccountLoad;
  const filePath = path.join(config.stateDir, input.stateDirectory, `${input.instanceId}.json`);
  const states = yield* AutoPoolRoutingStates;
  const routing = states?.get(filePath) ?? {
    lock: Semaphore.makeUnsafe(1),
    state: undefined,
  };
  states?.set(filePath, routing);
  const { lock } = routing;
  const error = (detail: string) =>
    new ProviderSetupError({ instanceId: input.instanceId, operation: "session", detail });
  const read = fs.readFileString(filePath).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(RoutingState))),
    Effect.catchTags({
      PlatformError: (cause) =>
        cause.reason._tag === "NotFound"
          ? Effect.succeed({ assignments: {} } satisfies RoutingState)
          : Effect.fail(cause),
    }),
    Effect.mapError(() =>
      error(`${input.label} routing state could not be read. Restore it before continuing.`),
    ),
  );
  yield* lock.withPermit(
    Effect.gen(function* () {
      routing.state ??= yield* read;
    }),
  );
  const tokenFor = (threadId: ThreadId) => `${input.instanceId}:${threadId}`;
  const save = (next: RoutingState) => {
    const pruned = {
      assignments: Object.fromEntries(Object.entries(next.assignments).slice(-MAX_ASSIGNMENTS)),
    };
    return writeFileStringAtomically({ filePath, contents: JSON.stringify(pruned) }).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      Effect.mapError(() => error(`${input.label} could not persist the account assignment.`)),
      Effect.tap(() =>
        Effect.sync(() => {
          routing.state = pruned;
        }),
      ),
      Effect.uninterruptible,
    );
  };
  const unsupported = () =>
    error(
      `This model or option is not supported by every configured ${input.label} account. Refresh the provider catalog.`,
    );
  const refreshAll = (members: readonly AutoPoolMember[]) =>
    Effect.forEach(
      members,
      (member) => refreshAutoPoolMember(member).pipe(Effect.timeoutOption(REFRESH_TIMEOUT)),
      { concurrency: 4, discard: true },
    ).pipe(Effect.ignore);
  const headroomFor = (member: AutoPoolMember, selection: ModelSelection, now: number) =>
    member.allowed === false ? undefined : input.headroom(member.snapshot, selection, now);
  const modelsFor = (members: readonly AutoPoolMember[]) =>
    input.models(members.map((member) => member.snapshot));
  // Returns undefined when no account has cached allowance for the request.
  const choose = Effect.fn("AutoPoolRouter.choose")(function* (
    threadId: ThreadId,
    selection: ModelSelection,
    projectId: ProjectId | undefined,
    exclude: ReadonlySet<string>,
  ) {
    const members = yield* input.members(projectId);
    const now = yield* Clock.currentTimeMillis;
    const models = modelsFor(members);
    if (!autoPoolSelectionAllowed(models, selection)) return yield* unsupported();
    const candidates = members.flatMap((member, order) => {
      const key = autoPoolAccountKey(member.snapshot);
      const headroom = headroomFor(member, selection, now);
      if (!key || !headroom || exclude.has(key)) return [];
      return [{ member, key, running: load.count(key, tokenFor(threadId)), headroom, order }];
    });
    const state = routing.state!;
    const sticky = state.assignments[threadId];
    const setting = input.maxRunningTurns ?? AUTO_POOL_DEFAULT_MAX_RUNNING_TURNS;
    const maxRunningTurns = typeof setting === "number" ? setting : yield* setting;
    const chosen = autoPoolChoose(candidates, {
      current: candidates.find(
        (candidate) =>
          candidate.member.instance.instanceId === sticky?.memberId &&
          candidate.key === sticky.accountKey,
      ),
      maxRunningTurns,
    });
    if (!chosen) return undefined;
    const assignment = { memberId: chosen.member.instance.instanceId, accountKey: chosen.key };
    load.reserve(tokenFor(threadId), chosen.key);
    if (sticky?.memberId !== assignment.memberId || sticky.accountKey !== assignment.accountKey) {
      const { [threadId]: _previous, ...assignments } = state.assignments;
      yield* save({ assignments: { ...assignments, [threadId]: assignment } }).pipe(
        Effect.onError(() => Effect.sync(() => load.release(tokenFor(threadId)))),
      );
    }
    return { ...chosen.member, modelSelection: autoPoolNormalizeSelection(models, selection) };
  });
  const noAllowance = Effect.fn("AutoPoolRouter.noAllowance")(function* (
    threadId: ThreadId,
    selection: ModelSelection,
    projectId: ProjectId | undefined,
    exclude: ReadonlySet<string>,
  ) {
    const state = routing.state!;
    if (state.assignments[threadId]) {
      const { [threadId]: _stale, ...assignments } = state.assignments;
      yield* save({ assignments });
    }
    const members = yield* input.members(projectId);
    const now = yield* Clock.currentTimeMillis;
    const eligible = members.filter(
      (member) =>
        member.allowed !== false &&
        autoPoolSubscriptionEligible(member.snapshot) &&
        autoPoolAccountKey(member.snapshot) !== undefined,
    );
    const limited =
      exclude.size > 0 ||
      eligible.some((member) => autoPoolExhausted(member.snapshot, selection.model, now));
    const resets = eligible
      .map((member) => autoPoolResetAt(member.snapshot, selection.model, now))
      .filter((reset): reset is string => reset !== null && Date.parse(reset) > now)
      .sort((a, b) => Date.parse(a) - Date.parse(b));
    const detail = `No ${input.label} subscription allowance is available.${resets[0] ? ` Next reported reset: ${resets[0]}.` : " Refresh usage after resolving unavailable account data."} No credits were redeemed or purchased.`;
    return yield* limited
      ? new ProviderUsageLimitError({
          detail,
          resetAt: resets[0] ?? null,
        })
      : error(detail);
  });
  return AutoPoolRouter.of({
    normalizeSelection: Effect.fn("AutoPoolRouter.normalizeSelection")((selection, projectId) =>
      input.members(projectId).pipe(
        Effect.flatMap((members) => {
          const models = modelsFor(members);
          return autoPoolSelectionAllowed(models, selection)
            ? Effect.succeed(autoPoolNormalizeSelection(models, selection))
            : Effect.fail(unsupported());
        }),
      ),
    ),
    hasUsageAllowance: Effect.fn("AutoPoolRouter.hasUsageAllowance")(
      function* (selection, projectId, observedAfter) {
        const members = yield* input.members(projectId).pipe(Effect.orElseSucceed(() => []));
        const now = yield* Clock.currentTimeMillis;
        return (
          autoPoolSelectionAllowed(modelsFor(members), selection) &&
          members.some(
            (member) =>
              autoPoolAccountKey(member.snapshot) !== undefined &&
              (observedAfter === undefined ||
                Date.parse(member.snapshot.usageLimits?.checkedAt ?? "") > observedAfter) &&
              headroomFor(member, selection, now) !== undefined,
          )
        );
      },
    ),
    reserve: Effect.fn("AutoPoolRouter.reserve")(function* (
      threadId,
      selection,
      projectId,
      exclude = new Set(),
    ) {
      const cached = yield* lock.withPermit(choose(threadId, selection, projectId, exclude));
      if (cached) return cached;
      // Allowance can return before the next background refresh (a reset or banked credit).
      // Probes run outside the lock so one slow account cannot stall every reservation.
      yield* refreshAll(
        (yield* input.members(projectId)).filter(
          (member) =>
            member.allowed !== false && !exclude.has(autoPoolAccountKey(member.snapshot) ?? ""),
        ),
      );
      return yield* lock.withPermit(
        choose(threadId, selection, projectId, exclude).pipe(
          Effect.filterOrElse(
            (chosen) => chosen !== undefined,
            () => noAllowance(threadId, selection, projectId, exclude),
          ),
        ),
      );
    }),
    release: Effect.fn("AutoPoolRouter.release")((threadId) =>
      Effect.sync(() => {
        load.release(tokenFor(threadId));
      }),
    ),
    refreshUsage: Effect.fn("AutoPoolRouter.refreshUsage")((member) => refreshAll([member])),
    refreshStale: Effect.fn("AutoPoolRouter.refreshStale")(function* (maxAgeMs, projectId) {
      const now = yield* Clock.currentTimeMillis;
      const members = yield* input.members(projectId).pipe(Effect.orElseSucceed(() => []));
      yield* refreshAll(
        members.filter(
          ({ snapshot }) =>
            snapshot.usageLimits?.lastAttemptFailed === true ||
            autoPoolUsageIsStale(snapshot, now, maxAgeMs),
        ),
      );
    }),
  });
});

/** Republishes the Auto snapshot when the member list or a member's own snapshot changes. */
export const followAutoPoolMembers = Effect.fn("followAutoPoolMembers")(function* (input: {
  readonly members: () => Effect.Effect<readonly AutoPoolMember[], ProviderSetupError>;
  readonly refresh: Effect.Effect<unknown>;
}) {
  const registryChanges = yield* (yield* ProviderInstanceLookup.ProviderInstanceLookup).subscribe;
  yield* Stream.concat(Stream.succeed(undefined), registryChanges).pipe(
    Stream.switchMap(() =>
      Stream.fromEffect(input.members().pipe(Effect.orElseSucceed(() => []))).pipe(
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
    Stream.runForEach(() => input.refresh),
    Effect.forkScoped,
  );
});

/**
 * Routing reads cached allowance, so keep idle accounts current off the request path. Active
 * accounts also update from the rate limits their provider streams during turns.
 */
export const keepAutoPoolUsageFresh = (router: AutoPoolRouter["Service"]) =>
  router
    .refreshStale(AUTO_POOL_USAGE_REFRESH_INTERVAL_MS)
    .pipe(
      Effect.repeat(Schedule.spaced(Duration.millis(AUTO_POOL_USAGE_REFRESH_INTERVAL_MS))),
      Effect.forkScoped,
    );
