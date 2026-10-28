import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as ServerConfig from "../config.ts";
import { makeCodexAutoRouter } from "./CodexAutoRouter.ts";
import { codexAutoAccountKey } from "./codexAutoPolicy.ts";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { CODEX_AUTO_MODEL, codexAutoFixture } from "./testUtils/codexAuto.ts";

const poolId = ProviderInstanceId.make("codex_auto");
const selection = { instanceId: poolId, model: CODEX_AUTO_MODEL };

const setup = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-codex-auto-" });
  const now = DateTime.formatIso(yield* DateTime.now);
  const accounts = ["alice", "bob", "carol"].map((id) => codexAutoFixture(id, now));
  const make = () =>
    makeCodexAutoRouter({
      instanceId: poolId,
      members: () => Effect.succeed(accounts.map((account) => account.member())),
    }).pipe(Effect.provide(ServerConfig.layerTest(stateDir, stateDir)));
  return { accounts, make, stateDir };
});

it.layer(NodeServices.layer)("Codex Auto durable admission", (it) => {
  it.effect("reserves independent accounts atomically for simultaneous child launches", () =>
    Effect.gen(function* () {
      const { make } = yield* setup;
      const router = yield* make();
      const threads = ["child-a", "child-b", "child-c"].map((id) => ThreadId.make(id));
      const chosen = yield* Effect.forEach(threads, (thread) => router.reserve(thread, selection), {
        concurrency: "unbounded",
      });
      expect(new Set(chosen.map((member) => member.instance.instanceId)).size).toBe(3);
      yield* Effect.forEach(threads, router.release);
    }),
  );

  it.effect("shares atomic load reservations across separate Auto pools", () =>
    Effect.gen(function* () {
      const { accounts, stateDir } = yield* setup;
      const pools = yield* Effect.forEach(["pool-one", "pool-two"], (id) =>
        makeCodexAutoRouter({
          instanceId: ProviderInstanceId.make(id),
          members: () => Effect.succeed(accounts.map((account) => account.member())),
        }).pipe(Effect.provide(ServerConfig.layerTest(stateDir, stateDir))),
      );
      const chosen = yield* Effect.forEach(
        pools,
        (router, index) => router.reserve(ThreadId.make(`cross-pool-${index}`), selection),
        { concurrency: "unbounded" },
      );
      expect(new Set(chosen.map((member) => member.instance.instanceId)).size).toBe(2);
      yield* Effect.forEach(pools, (router, index) =>
        router.release(ThreadId.make(`cross-pool-${index}`)),
      );
    }),
  );

  it.effect("keeps healthy assignments after turns and routing-service reconstruction", () =>
    Effect.gen(function* () {
      const { make, accounts } = yield* setup;
      const thread = ThreadId.make("sticky-thread");
      const first = yield* make();
      const chosen = yield* first.reserve(thread, selection);
      yield* first.release(thread);
      accounts[1]!.set({
        ...accounts[1]!.read(),
        usageLimits: {
          ...accounts[1]!.read().usageLimits!,
          windows: accounts[1]!
            .read()
            .usageLimits!.windows.map((window) => ({ ...window, usedPercent: 0 })),
        },
      });
      const restarted = yield* make();
      expect((yield* restarted.reserve(thread, selection)).instance.instanceId).toBe(
        chosen.instance.instanceId,
      );
      yield* restarted.release(thread);
    }),
  );

  it.effect("rebalances a sticky thread away from an account without usage data", () =>
    Effect.gen(function* () {
      const { make, accounts } = yield* setup;
      const router = yield* make();
      const thread = ThreadId.make("unavailable-thread");
      const selected = yield* router.reserve(thread, selection);
      yield* router.release(thread);
      const account = accounts.find(
        (candidate) => candidate.instance.instanceId === selected.instance.instanceId,
      )!;
      account.set({
        ...account.read(),
        usageLimits: {
          ...account.read().usageLimits!,
          windows: [],
          unavailable: { reason: "probeFailed" },
        },
      });
      const replacement = yield* router.reserve(thread, selection);
      expect(replacement.instance.instanceId).not.toBe(selected.instance.instanceId);
      yield* router.release(thread);
      expect((yield* router.reserve(thread, selection)).instance.instanceId).toBe(
        replacement.instance.instanceId,
      );
      yield* router.release(thread);
    }),
  );

  it.effect("uses recent cached windows when every usage refresh fails together", () =>
    Effect.gen(function* () {
      const { make, accounts } = yield* setup;
      for (const account of accounts)
        account.set({
          ...account.read(),
          usageLimits: { ...account.read().usageLimits!, lastAttemptFailed: true },
        });
      const router = yield* make();
      const reservation = yield* router.reserve(ThreadId.make("cached-allowance"), selection);
      expect(accounts.map((account) => account.instance.instanceId)).toContain(
        reservation.instance.instanceId,
      );
      yield* router.release(ThreadId.make("cached-allowance"));
    }),
  );

  it.effect("admits subscription windows when Codex reports no paid-credit state", () =>
    Effect.gen(function* () {
      const { make, accounts } = yield* setup;
      for (const account of accounts) {
        const {
          paidCreditsAvailable: _credits,
          paidCreditsCheckedAt: _creditsCheckedAt,
          ...usageLimits
        } = account.read().usageLimits!;
        account.set({ ...account.read(), usageLimits });
      }
      const router = yield* make();
      const reservation = yield* router.reserve(ThreadId.make("no-credit-state"), selection);
      expect(accounts.map((account) => account.instance.instanceId)).toContain(
        reservation.instance.instanceId,
      );
      yield* router.release(ThreadId.make("no-credit-state"));
    }),
  );

  it.effect("keeps subscription allowance eligible when paid credits are also reported", () =>
    Effect.gen(function* () {
      const { make, accounts } = yield* setup;
      for (const account of accounts) {
        account.set({
          ...account.read(),
          usageLimits: { ...account.read().usageLimits!, paidCreditsAvailable: true },
        });
      }
      const router = yield* make();
      const reservation = yield* router.reserve(
        ThreadId.make("subscription-with-credits"),
        selection,
      );
      expect(accounts.map((account) => account.instance.instanceId)).toContain(
        reservation.instance.instanceId,
      );
      yield* router.release(ThreadId.make("subscription-with-credits"));
    }),
  );

  it.effect("deduplicates account aliases for load and rejected accounts", () =>
    Effect.gen(function* () {
      const { make, accounts } = yield* setup;
      accounts[1]!.set({ ...accounts[1]!.read(), auth: accounts[0]!.read().auth });
      const router = yield* make();
      const firstThread = ThreadId.make("alias-a");
      const secondThread = ThreadId.make("alias-b");
      const first = yield* router.reserve(firstThread, selection);
      const second = yield* router.reserve(secondThread, selection);
      expect(second.instance.instanceId).toBe(accounts[2]!.instance.instanceId);
      yield* router.release(firstThread);
      const rejected = new Set([codexAutoAccountKey(first.snapshot)!]);
      expect(
        (yield* router.reserve(firstThread, selection, undefined, rejected)).instance.instanceId,
      ).toBe(accounts[2]!.instance.instanceId);
      yield* router.release(firstThread);
      yield* router.release(secondThread);
    }),
  );

  it.effect("rejects stale-client options and stops when every account is exhausted", () =>
    Effect.gen(function* () {
      const { make, accounts } = yield* setup;
      const router = yield* make();
      const thread = ThreadId.make("exhausted-thread");
      expect(
        (yield* router
          .reserve(thread, { ...selection, options: [{ id: "serviceTier", value: "ultrafast" }] })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
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
      const outcome = yield* router.reserve(thread, selection).pipe(Effect.result);
      expect(outcome._tag).toBe("Failure");
      if (outcome._tag === "Failure")
        expect(makeProviderFailure({ cause: outcome.failure })).toMatchObject({
          class: "usage_limit",
          resetAt: accounts[0]!.read().usageLimits!.windows[1]!.resetsAt,
        });
    }),
  );

  it.effect(
    "uses the earliest complete account reset and does not schedule unknown allowance",
    () =>
      Effect.gen(function* () {
        const { make, accounts } = yield* setup;
        const now = DateTime.toEpochMillis(yield* DateTime.now);
        for (const [index, account] of accounts.entries()) {
          account.set({
            ...account.read(),
            usageLimits: {
              ...account.read().usageLimits!,
              windows: account.read().usageLimits!.windows.map((window) => ({
                ...window,
                usedPercent: 100,
                resetsAt: DateTime.formatIso(DateTime.makeUnsafe(now + (index + 1) * 3_600_000)),
              })),
            },
          });
        }
        const router = yield* make();
        const error = yield* router
          .reserve(ThreadId.make("first-reset"), selection)
          .pipe(Effect.flip);
        expect(makeProviderFailure({ cause: error })).toMatchObject({
          class: "usage_limit",
          resetAt: DateTime.formatIso(DateTime.makeUnsafe(now + 3_600_000)),
        });
        accounts[0]!.set({
          ...accounts[0]!.read(),
          usageLimits: {
            ...accounts[0]!.read().usageLimits!,
            windows: [],
            unavailable: { reason: "probeFailed" },
          },
        });
        const unavailable = yield* router
          .reserve(ThreadId.make("unknown-reset"), selection)
          .pipe(Effect.flip);
        expect(makeProviderFailure({ cause: unavailable }).class).not.toBe("usage_limit");
      }),
  );

  it.effect("routes on cached allowance without probing usage before the turn", () =>
    Effect.gen(function* () {
      const { make, accounts } = yield* setup;
      const hourAgo = DateTime.formatIso(
        DateTime.makeUnsafe(DateTime.toEpochMillis(yield* DateTime.now) - 3_600_000),
      );
      for (const account of accounts)
        account.set({
          ...account.read(),
          usageLimits: { ...account.read().usageLimits!, checkedAt: hourAgo },
        });
      const router = yield* make();
      yield* router.reserve(ThreadId.make("cached-route"), selection);
      expect(accounts.map((account) => account.refreshes())).toEqual([0, 0, 0]);
      yield* router.release(ThreadId.make("cached-route"));
    }),
  );

  it.effect("rereads usage before failing so a banked reset is used immediately", () =>
    Effect.gen(function* () {
      const { make, accounts } = yield* setup;
      const exhaust = (account: (typeof accounts)[number], usedPercent: number) => ({
        ...account.read(),
        usageLimits: {
          ...account.read().usageLimits!,
          windows: account
            .read()
            .usageLimits!.windows.map((window) => ({ ...window, usedPercent })),
        },
      });
      for (const account of accounts) account.set(exhaust(account, 100));
      const router = yield* make();
      const thread = ThreadId.make("banked-reset");
      expect((yield* router.reserve(thread, selection).pipe(Effect.result))._tag).toBe("Failure");
      accounts[1]!.onRefresh(() => exhaust(accounts[1]!, 0));
      expect((yield* router.reserve(thread, selection)).instance.instanceId).toBe(
        accounts[1]!.instance.instanceId,
      );
      yield* router.release(thread);
    }),
  );

  it.effect("ignores cooldowns persisted by earlier versions", () =>
    Effect.gen(function* () {
      const { make, accounts, stateDir } = yield* setup;
      const fs = yield* FileSystem.FileSystem;
      const config = yield* ServerConfig.ServerConfig.pipe(
        Effect.provide(ServerConfig.layerTest(stateDir, stateDir)),
      );
      yield* fs.makeDirectory(`${config.stateDir}/codex-auto`, { recursive: true });
      const cooldowns = Object.fromEntries(
        accounts.map((account) => [codexAutoAccountKey(account.read())!, "2099-01-01T00:00:00Z"]),
      );
      yield* fs.writeFileString(
        `${config.stateDir}/codex-auto/codex_auto.json`,
        JSON.stringify({ assignments: {}, cooldowns }),
      );
      const router = yield* make();
      yield* router.reserve(ThreadId.make("legacy-cooldown"), selection);
      yield* router.release(ThreadId.make("legacy-cooldown"));
    }),
  );
});
