import { DEFAULT_SERVER_SETTINGS } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import * as ServerSettings from "../serverSettings.ts";
import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import * as ServerActivation from "../serverActivation.ts";
import * as GitManager from "../git/GitManager.ts";
import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as Discovery from "./ThreadPullRequestService.ts";
import * as Sync from "./PullRequestSyncReactor.ts";
import * as Watch from "./PullRequestWatchReactor.ts";

const disabledDependencies = (checked: Deferred.Deferred<void>, onQuery: () => void) =>
  Layer.mergeAll(
    Layer.mock(ServerSettings.ServerSettingsService)({
      getSettings: Deferred.succeed(checked, undefined).pipe(
        Effect.as({
          ...DEFAULT_SERVER_SETTINGS,
          backgroundPullRequestRefreshEnabled: false,
        }),
      ),
    }),
    Layer.succeed(ServerActivation.ServerActivation, Effect.void),
    Layer.mock(BackgroundPolicy.BackgroundPolicy)({
      shouldRunScopeWork: () => Effect.succeed(false),
      shouldRunOpportunisticWork: Effect.succeed(false),
    }),
    Layer.mock(Orchestrator.OrchestratorV2)({
      streamDomainEvents: Stream.never,
      getPullRequestSnapshot: () =>
        Effect.sync(() => {
          onQuery();
          throw Error("disabled discovery read the snapshot");
        }),
    }),
    Layer.mock(ProjectionStore.ProjectionStoreV2)({
      getThreadsWithPullRequests: () =>
        Effect.sync(() => {
          onQuery();
          return [];
        }),
    }),
    Layer.mock(ProjectStore.ProjectStoreV2)({}),
    Layer.mock(PullRequestService.PullRequestService)({
      subscribeStateChanges: Effect.succeed(Stream.never),
      withSharedProjectReads: (effect) => effect,
    }),
    Layer.mock(GitManager.GitManager)({
      subscribePullRequestStateChanges: Effect.succeed(Stream.never),
    }),
    Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({}),
    Layer.succeed(
      SecureRandom.SecureRandom,
      SecureRandom.make((size) => new Uint8Array(size).fill(1)),
    ),
    FileSystem.layerNoop({}),
    Path.layer,
  );

describe("background PR refresh policy", () => {
  it.effect.each([
    { name: "discovery", make: Discovery.make },
    { name: "linked PR sync", make: Sync.make },
  ] as const)("parks $name before database or provider reads when disabled", ({ make }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const checked = yield* Deferred.make<void>();
        let queries = 0;
        yield* Effect.gen(function* () {
          const service = yield* make;
          yield* service.start();
          yield* Deferred.await(checked);
          yield* service.drain;
          expect(queries).toBe(0);
        }).pipe(Effect.provide(disabledDependencies(checked, () => queries++)));
      }),
    ),
  );
  it.effect("still reads a requested PR sync when disabled", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const checked = yield* Deferred.make<void>();
        let queries = 0;
        yield* Effect.gen(function* () {
          const service = yield* Sync.make;
          yield* service.requestSync({ host: "github.com", repository: "owner/repo", number: 1 });
          yield* service.drain;
          expect(queries).toBe(1);
        }).pipe(Effect.provide(disabledDependencies(checked, () => queries++)));
      }),
    ),
  );
  it.effect("parks watches without reading threads or checking fingerprints", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let enabled = false;
        let reads = 0;
        const service = yield* Watch.make.pipe(
          Effect.provide(
            Layer.mergeAll(
              Layer.mock(ServerSettings.ServerSettingsService)({
                getSettings: Effect.sync(() => ({
                  ...DEFAULT_SERVER_SETTINGS,
                  backgroundPullRequestRefreshEnabled: enabled,
                })),
              }),
              Layer.mock(BackgroundPolicy.BackgroundPolicy)({
                shouldRunScopeWork: () => Effect.succeed(false),
                shouldRunOpportunisticWork: Effect.succeed(false),
              }),
              Layer.mock(Orchestrator.OrchestratorV2)({}),
              Layer.mock(ProjectionStore.ProjectionStoreV2)({
                getThreadsWithPullRequests: () =>
                  Effect.sync(() => {
                    reads++;
                    return [];
                  }),
              }),
              Layer.mock(PullRequestService.PullRequestService)({
                subscribeStateChanges: Effect.succeed(Stream.never),
              }),
              Layer.succeed(
                SecureRandom.SecureRandom,
                SecureRandom.make((size) => new Uint8Array(size).fill(1)),
              ),
            ),
          ),
        );
        yield* service.sweep;
        expect(reads).toBe(0);
        enabled = true;
        yield* service.sweep;
        expect(reads).toBe(1);
        enabled = false;
        yield* service.sweep;
        expect(reads).toBe(1);
      }),
    ),
  );
});
