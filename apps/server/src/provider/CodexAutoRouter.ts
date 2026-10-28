import {
  ModelSelection,
  ProviderInstanceId,
  ProviderSetupError,
  ThreadId,
  type ServerProvider,
  type ProjectId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import { writeFileStringAtomically } from "@t3tools/shared/atomicWrite";
import * as ServerConfig from "../config.ts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import {
  codexAutoAccountKey,
  codexAutoExhausted,
  codexAutoHeadroom,
  codexAutoModels,
  codexAutoNormalizeSelection,
  codexAutoResetAt,
  codexAutoSelectionAllowed,
} from "./codexAutoPolicy.ts";
import { CodexAccountLoad } from "./CodexAccountLoad.ts";
import { ProviderUsageLimitError } from "@t3tools/provider-core/server/failure";

const Assignment = Schema.Struct({ memberId: ProviderInstanceId, accountKey: Schema.String });
// Files written before cached routing also carry `cooldowns`; decoding ignores the key.
const RoutingState = Schema.Struct({ assignments: Schema.Record(Schema.String, Assignment) });
type RoutingState = typeof RoutingState.Type;
// Assignments only carry stickiness; dropping the oldest just lets a thread rebalance.
const MAX_ASSIGNMENTS = 1_000;
const REFRESH_TIMEOUT = "20 seconds";
export interface CodexAutoMember {
  readonly instance: ProviderInstance;
  readonly snapshot: ServerProvider;
  readonly allowed?: boolean;
}
export interface CodexAutoReservation extends CodexAutoMember {
  readonly modelSelection: ModelSelection;
}

export class CodexAutoRouter extends Context.Service<
  CodexAutoRouter,
  {
    readonly normalizeSelection: (
      selection: ModelSelection,
      projectId?: ProjectId,
    ) => Effect.Effect<ModelSelection, ProviderSetupError>;
    /** `exclude` holds account keys that already rejected this request. */
    readonly reserve: (
      threadId: ThreadId,
      selection: ModelSelection,
      projectId?: ProjectId,
      exclude?: ReadonlySet<string>,
    ) => Effect.Effect<CodexAutoReservation, ProviderSetupError | ProviderUsageLimitError>;
    readonly release: (threadId: ThreadId) => Effect.Effect<void>;
    /** Re-reads one account's usage after it rejected a request for quota. */
    readonly refreshUsage: (member: CodexAutoMember) => Effect.Effect<void>;
    readonly refreshStale: (maxAgeMs: number, projectId?: ProjectId) => Effect.Effect<void>;
  }
>()("t3/provider/CodexAutoRouter") {}

export const makeCodexAutoRouter = Effect.fn("makeCodexAutoRouter")(function* (input: {
  readonly instanceId: ProviderInstanceId;
  readonly members: (
    projectId?: ProjectId,
  ) => Effect.Effect<readonly CodexAutoMember[], ProviderSetupError>;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig.ServerConfig;
  const load = yield* CodexAccountLoad;
  const filePath = path.join(config.stateDir, "codex-auto", `${input.instanceId}.json`);
  const lock = yield* Semaphore.make(1);
  const error = (detail: string) =>
    new ProviderSetupError({ instanceId: input.instanceId, operation: "session", detail });
  const stateResult = yield* fs.readFileString(filePath).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(RoutingState))),
    Effect.catchTags({
      PlatformError: (cause) =>
        cause.reason._tag === "NotFound"
          ? Effect.succeed({ assignments: {} } satisfies RoutingState)
          : Effect.fail(cause),
    }),
    Effect.mapError(() =>
      error("Codex Auto routing state could not be read. Restore it before continuing."),
    ),
  );
  let state: RoutingState = stateResult;
  const tokenFor = (threadId: ThreadId) => `${input.instanceId}:${threadId}`;
  const save = (next: RoutingState) => {
    const pruned = {
      assignments: Object.fromEntries(Object.entries(next.assignments).slice(-MAX_ASSIGNMENTS)),
    };
    return writeFileStringAtomically({ filePath, contents: JSON.stringify(pruned) }).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      Effect.mapError(() => error("Codex Auto could not persist the account assignment.")),
      Effect.tap(() =>
        Effect.sync(() => {
          state = pruned;
        }),
      ),
    );
  };
  const refreshAll = (members: readonly CodexAutoMember[]) =>
    Effect.forEach(
      members,
      (member) => member.instance.snapshot.refresh.pipe(Effect.timeoutOption(REFRESH_TIMEOUT)),
      { concurrency: 4, discard: true },
    ).pipe(Effect.ignore);
  // Returns undefined when no account has cached allowance for the request.
  const choose = Effect.fn("CodexAutoRouter.choose")(function* (
    threadId: ThreadId,
    selection: ModelSelection,
    projectId: ProjectId | undefined,
    exclude: ReadonlySet<string>,
  ) {
    const members = yield* input.members(projectId);
    const now = yield* Clock.currentTimeMillis;
    const models = codexAutoModels(members.map((member) => member.snapshot));
    if (!codexAutoSelectionAllowed(models, selection))
      return yield* error(
        "This model or option is not supported by every configured Codex Auto account. Refresh the provider catalog.",
      );
    const candidates = members.flatMap((member, order) => {
      const key = codexAutoAccountKey(member.snapshot);
      const headroom = codexAutoHeadroom(member.snapshot, selection, now);
      if (!key || !headroom || member.allowed === false || exclude.has(key)) return [];
      return [{ member, key, load: load.count(key, tokenFor(threadId)), headroom, order }];
    });
    const sticky = state.assignments[threadId];
    // Percentages from different plans are not capacities: use a coarse safety band, then actual load.
    const chosen =
      candidates.find(
        (candidate) =>
          candidate.member.instance.instanceId === sticky?.memberId &&
          candidate.key === sticky.accountKey,
      ) ??
      candidates.toSorted(
        (a, b) =>
          Number(b.headroom.healthy) - Number(a.headroom.healthy) ||
          a.load - b.load ||
          b.headroom.remaining - a.headroom.remaining ||
          a.order - b.order,
      )[0];
    if (!chosen) return undefined;
    const assignment = { memberId: chosen.member.instance.instanceId, accountKey: chosen.key };
    load.reserve(tokenFor(threadId), chosen.key);
    if (sticky?.memberId !== assignment.memberId || sticky.accountKey !== assignment.accountKey) {
      const { [threadId]: _previous, ...assignments } = state.assignments;
      yield* save({ assignments: { ...assignments, [threadId]: assignment } }).pipe(
        Effect.onError(() => Effect.sync(() => load.release(tokenFor(threadId)))),
      );
    }
    return { ...chosen.member, modelSelection: codexAutoNormalizeSelection(models, selection) };
  });
  const noAllowance = Effect.fn("CodexAutoRouter.noAllowance")(function* (
    threadId: ThreadId,
    selection: ModelSelection,
    projectId: ProjectId | undefined,
  ) {
    if (state.assignments[threadId]) {
      const { [threadId]: _stale, ...assignments } = state.assignments;
      yield* save({ assignments });
    }
    const members = yield* input.members(projectId);
    const now = yield* Clock.currentTimeMillis;
    const eligible = members.filter(
      (member) => member.allowed !== false && member.snapshot.enabled,
    );
    const exhausted =
      eligible.length > 0 &&
      eligible.every((member) => codexAutoExhausted(member.snapshot, selection, now));
    const resets = eligible
      .map((member) => codexAutoResetAt(member.snapshot, selection))
      .filter((reset): reset is string => reset !== null && Date.parse(reset) > now)
      .sort();
    const detail = `No Codex Auto subscription allowance is available.${resets[0] ? ` Next reported reset: ${resets[0]}.` : " Refresh usage after resolving unavailable account data."} No credits were redeemed or purchased.`;
    return yield* exhausted
      ? new ProviderUsageLimitError({
          detail,
          resetAt: resets.length === eligible.length ? resets[0]! : null,
        })
      : error(detail);
  });
  return CodexAutoRouter.of({
    normalizeSelection: Effect.fn("CodexAutoRouter.normalizeSelection")((selection, projectId) =>
      input.members(projectId).pipe(
        Effect.flatMap((members) => {
          const models = codexAutoModels(members.map((member) => member.snapshot));
          return codexAutoSelectionAllowed(models, selection)
            ? Effect.succeed(codexAutoNormalizeSelection(models, selection))
            : Effect.fail(
                error(
                  "This model or option is not supported by every configured Codex Auto account. Refresh the provider catalog.",
                ),
              );
        }),
      ),
    ),
    reserve: Effect.fn("CodexAutoRouter.reserve")(function* (
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
          (member) => !exclude.has(codexAutoAccountKey(member.snapshot) ?? ""),
        ),
      );
      return yield* lock.withPermit(
        choose(threadId, selection, projectId, exclude).pipe(
          Effect.filterOrElse(
            (chosen) => chosen !== undefined,
            () => noAllowance(threadId, selection, projectId),
          ),
        ),
      );
    }),
    release: Effect.fn("CodexAutoRouter.release")((threadId) =>
      Effect.sync(() => {
        load.release(tokenFor(threadId));
      }),
    ),
    refreshUsage: Effect.fn("CodexAutoRouter.refreshUsage")((member) => refreshAll([member])),
    refreshStale: Effect.fn("CodexAutoRouter.refreshStale")(function* (maxAgeMs, projectId) {
      const now = yield* Clock.currentTimeMillis;
      const members = yield* input.members(projectId).pipe(Effect.orElseSucceed(() => []));
      yield* refreshAll(
        members.filter(
          ({ snapshot }) =>
            !snapshot.usageLimits ||
            snapshot.usageLimits.lastAttemptFailed === true ||
            now - Date.parse(snapshot.usageLimits.checkedAt) > maxAgeMs,
        ),
      );
    }),
  });
});
