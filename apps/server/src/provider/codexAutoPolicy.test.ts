import { describe, expect, it } from "vite-plus/test";
import { ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import {
  autoPoolChoose,
  autoPoolHeadroom,
  autoPoolResetAt,
  autoPoolSelectionAllowed,
} from "./autoPoolPolicy.ts";
import { codexAutoHeadroom, codexAutoModels } from "./codexAutoPolicy.ts";
import { mergeProviderSnapshot } from "./ProviderRegistry.ts";
import { providerUsageReport } from "./providerUsageReport.ts";
import { CODEX_AUTO_MODEL, CODEX_AUTO_NOW, codexAutoFixture } from "./testUtils/codexAuto.ts";

const selection = { instanceId: ProviderInstanceId.make("codex_auto"), model: CODEX_AUTO_MODEL };

describe("Codex Auto catalog and usage", () => {
  it("advertises the configured intersection even when the standard-tier account is exhausted", () => {
    const fast = codexAutoFixture("fast", CODEX_AUTO_NOW, ["default", "priority", "ultrafast"]);
    const standard = codexAutoFixture("standard");
    standard.set({
      ...standard.read(),
      usageLimits: {
        ...standard.read().usageLimits!,
        windows: standard
          .read()
          .usageLimits!.windows.map((window) => ({ ...window, usedPercent: 100 })),
      },
    });
    const models = codexAutoModels([fast.read(), standard.read()]);
    expect(
      autoPoolSelectionAllowed(models, {
        ...selection,
        options: [{ id: "serviceTier", value: "ultrafast" }],
      }),
    ).toBe(false);
    expect(
      autoPoolSelectionAllowed(models, {
        ...selection,
        options: [
          { id: "serviceTier", value: "priority" },
          { id: "reasoningEffort", value: "high" },
        ],
      }),
    ).toBe(true);
    const before = { ...fast.read(), authoritativeModels: true };
    expect(mergeProviderSnapshot(before, { ...before, models }).models).toEqual(models);
    expect(
      mergeProviderSnapshot(before, { ...before, status: "error", models: [] }).models,
    ).toEqual([]);
  });

  it("rejects unsupported models, unknown or duplicate options, and unverifiable capabilities", () => {
    const member = codexAutoFixture("one");
    const models = codexAutoModels([member.read()]);
    expect(autoPoolSelectionAllowed(models, { ...selection, model: "unknown" })).toBe(false);
    expect(
      autoPoolSelectionAllowed(models, {
        ...selection,
        options: [{ id: "unknown", value: true }],
      }),
    ).toBe(false);
    expect(
      autoPoolSelectionAllowed(models, {
        ...selection,
        options: [
          { id: "serviceTier", value: "default" },
          { id: "serviceTier", value: "default" },
        ],
      }),
    ).toBe(false);
    expect(codexAutoModels([member.read(), { ...member.read(), models: [] }])).toEqual([]);
  });

  it("limits members without a tier selector to explicit Standard and excludes unknown capabilities", () => {
    const fast = codexAutoFixture("fast", CODEX_AUTO_NOW, ["default", "ultrafast"]).read();
    const standard = {
      ...fast,
      models: fast.models.map((model) => ({
        ...model,
        capabilities: {
          ...model.capabilities!,
          optionDescriptors: model.capabilities!.optionDescriptors!.filter(
            (option) => option.id !== "serviceTier",
          ),
        },
      })),
    };
    const models = codexAutoModels([fast, standard]);
    expect(
      autoPoolSelectionAllowed(models, {
        ...selection,
        options: [{ id: "serviceTier", value: "default" }],
      }),
    ).toBe(true);
    expect(
      autoPoolSelectionAllowed(models, {
        ...selection,
        options: [{ id: "serviceTier", value: "ultrafast" }],
      }),
    ).toBe(false);
    expect(
      codexAutoModels([
        { ...fast, models: fast.models.map((model) => ({ ...model, capabilities: {} })) },
      ]),
    ).toEqual([]);
  });

  it("uses every applicable cached quota window, whatever its age or paid-credit state", () => {
    const provider = codexAutoFixture("one").read();
    const now = Date.parse(CODEX_AUTO_NOW);
    expect(
      codexAutoHeadroom(
        {
          ...provider,
          usageLimits: {
            ...provider.usageLimits!,
            windows: provider.usageLimits!.windows.filter((window) => window.kind === "weekly"),
          },
        },
        selection,
        now,
      ),
    ).toBeDefined();
    expect(codexAutoHeadroom(provider, selection, now)).toBeDefined();
    for (const id of ["primary", "secondary"])
      expect(
        codexAutoHeadroom(
          {
            ...provider,
            usageLimits: {
              ...provider.usageLimits!,
              windows: provider.usageLimits!.windows.map((window) => ({
                ...window,
                usedPercent: window.id === id ? 100 : 0,
              })),
            },
          },
          selection,
          now,
        ),
      ).toBeUndefined();
    expect(
      codexAutoHeadroom(
        { ...provider, usageLimits: { ...provider.usageLimits!, paidCreditsAvailable: true } },
        selection,
        now,
      ),
    ).toBeDefined();
    const {
      paidCreditsAvailable: _credits,
      paidCreditsCheckedAt: _creditsCheckedAt,
      ...withoutCredits
    } = provider.usageLimits!;
    expect(
      codexAutoHeadroom({ ...provider, usageLimits: withoutCredits }, selection, now),
    ).toBeDefined();
    expect(codexAutoHeadroom(provider, selection, now + 61_000)).toBeDefined();
    const spentSession = {
      ...provider,
      usageLimits: {
        ...provider.usageLimits!,
        windows: provider.usageLimits!.windows.map((window) =>
          window.id === "primary" ? { ...window, usedPercent: 100 } : window,
        ),
      },
    };
    expect(codexAutoHeadroom(spentSession, selection, now)).toBeUndefined();
    expect(codexAutoHeadroom(spentSession, selection, now + 3_600_001)).toBeDefined();
    expect(
      codexAutoHeadroom(
        {
          ...provider,
          usageLimits: {
            ...provider.usageLimits!,
            windows: [
              ...provider.usageLimits!.windows,
              {
                id: "model:primary",
                limitId: "model",
                model: selection.model,
                kind: "session",
                label: "Model",
                usedPercent: 100,
              },
            ],
          },
        },
        selection,
        now,
      ),
    ).toBeUndefined();
  });

  it("waits for all exhausted windows and reports unknown reset times honestly", () => {
    const provider = codexAutoFixture("one").read();
    const exhausted = {
      ...provider,
      usageLimits: {
        ...provider.usageLimits!,
        windows: provider.usageLimits!.windows.map((window) => ({ ...window, usedPercent: 100 })),
      },
    };
    expect(autoPoolResetAt(exhausted, selection.model)).toBe("2026-10-18T12:00:00.000Z");
    expect(
      autoPoolResetAt(
        {
          ...exhausted,
          usageLimits: {
            ...exhausted.usageLimits,
            windows: [{ id: "primary", kind: "session", label: "Session", usedPercent: 100 }],
          },
        },
        selection.model,
      ),
    ).toBeNull();
  });

  it("reports freshness and probe failure without returning identity credentials", () => {
    const provider = codexAutoFixture("one").read();
    const report = providerUsageReport(
      [
        {
          ...provider,
          usageLimits: {
            ...provider.usageLimits!,
            credentialFingerprint: "private-fingerprint",
            lastAttemptFailed: true,
            lastAttemptAt: "2026-10-17T12:02:00.000Z",
          },
        },
      ],
      "2026-10-17T12:02:01.000Z",
    );
    expect(report.instances[0]).toMatchObject({
      status: "probeFailed",
      stale: true,
      checkedAt: CODEX_AUTO_NOW,
      windows: [
        expect.objectContaining({ remainingPercent: 90 }),
        expect.objectContaining({ remainingPercent: 80 }),
      ],
    });
    expect(JSON.stringify(report)).not.toContain("private-fingerprint");
    expect(JSON.stringify(report)).not.toContain("example.test");
    expect(
      providerUsageReport([{ ...provider, status: "error" }], CODEX_AUTO_NOW).instances[0]!.status,
    ).toBe("probeFailed");
    expect(
      codexAutoHeadroom({ ...provider, status: "error" }, selection, Date.parse(CODEX_AUTO_NOW)),
    ).toBeUndefined();
  });
});

const HOUR = 3_600_000;
const iso = (offset: number) =>
  DateTime.formatIso(DateTime.makeUnsafe(Date.parse(CODEX_AUTO_NOW) + offset));

/** An account whose weekly windows have `used` percent spent; `inHours` omitted means no reset. */
function account(
  id: string,
  weekly: ReadonlyArray<{ used: number; inHours?: number; model?: string }>,
): ServerProvider {
  const provider = codexAutoFixture(id).read();
  return {
    ...provider,
    usageLimits: {
      ...provider.usageLimits!,
      windows: [
        provider.usageLimits!.windows[0]!,
        ...weekly.map((window, index) => ({
          id: window.model ? `model:${index}` : `weekly:${index}`,
          kind: "weekly" as const,
          label: "Weekly",
          usedPercent: window.used,
          ...(window.model ? { limitId: "model", model: window.model } : {}),
          ...(window.inHours === undefined ? {} : { resetsAt: iso(window.inHours * HOUR) }),
        })),
      ],
    },
  };
}

describe("Auto pool order", () => {
  const candidate = (provider: ServerProvider, running = 0) => ({
    id: provider.instanceId,
    headroom: autoPoolHeadroom(provider, CODEX_AUTO_MODEL, Date.parse(CODEX_AUTO_NOW))!,
    running,
  });
  it.each([
    {
      name: "spends the allowance that resets first, however small",
      accounts: [
        candidate(account("late", [{ used: 2, inHours: 144 }])),
        candidate(account("soon", [{ used: 92, inHours: 32 }])),
      ],
      expected: "soon",
    },
    {
      name: "binds on a model-specific weekly window",
      accounts: [
        candidate(account("general", [{ used: 50, inHours: 96 }])),
        candidate(
          account("model", [
            { used: 10, inHours: 144 },
            { used: 90, inHours: 48, model: CODEX_AUTO_MODEL },
          ]),
        ),
      ],
      expected: "model",
    },
    {
      name: "breaks equal resets by fewer running turns",
      accounts: [
        candidate(account("busy", [{ used: 10, inHours: 32 }]), 2),
        candidate(account("idle", [{ used: 60, inHours: 32 }]), 1),
      ],
      expected: "idle",
    },
    {
      name: "spills past an account at the running-turn cap",
      accounts: [
        candidate(account("soon", [{ used: 50, inHours: 32 }]), 3),
        candidate(account("late", [{ used: 50, inHours: 144 }]), 0),
      ],
      expected: "late",
    },
    {
      name: "uses the earliest reset when every account is at the cap",
      accounts: [
        candidate(account("late", [{ used: 50, inHours: 144 }]), 3),
        candidate(account("soon", [{ used: 50, inHours: 32 }]), 4),
      ],
      expected: "soon",
    },
    {
      name: "falls back to remaining percent without reset data",
      accounts: [
        candidate(account("low", [{ used: 70 }])),
        candidate(account("high", [{ used: 30 }])),
      ],
      expected: "high",
    },
    {
      name: "prefers a known reset over an unknown one",
      accounts: [
        candidate(account("unknown", [{ used: 0 }])),
        candidate(account("known", [{ used: 80, inHours: 100 }])),
      ],
      expected: "known",
    },
    {
      name: "keeps a thread on its account above the floor",
      accounts: [
        candidate(account("current", [{ used: 90, inHours: 144 }])),
        candidate(account("soon", [{ used: 10, inHours: 32 }])),
      ],
      current: "current",
      expected: "current",
    },
    {
      name: "moves a thread off an account below the floor",
      accounts: [
        candidate(account("current", [{ used: 96, inHours: 32 }])),
        candidate(account("other", [{ used: 40, inHours: 144 }])),
      ],
      current: "current",
      expected: "other",
    },
    {
      name: "stays below the floor when no other account is above it",
      accounts: [
        candidate(account("current", [{ used: 96, inHours: 144 }])),
        candidate(account("other", [{ used: 97, inHours: 32 }])),
      ],
      current: "current",
      expected: "current",
    },
  ])("$name", ({ accounts, current, expected }) => {
    const order = accounts.map((entry, index) => ({ ...entry, order: index }));
    expect(
      autoPoolChoose(order, {
        current: order.find((entry) => entry.id === current),
        maxRunningTurns: 3,
      })?.id,
    ).toBe(expected);
  });

  it("pools usage for agents while members stay out of the top-level list", () => {
    const direct = codexAutoFixture("codex").read();
    const { usageLimits: _none, ...auto } = codexAutoFixture("codex_auto").read();
    const spent = account("spent", [{ used: 100, inHours: 2 }]);
    const open = account("open", [{ used: 40, inHours: 120 }]);
    const pools = [
      {
        instanceId: auto.instanceId,
        memberIds: [spent.instanceId, open.instanceId],
        maxRunningTurns: 3,
      },
    ];
    const report = providerUsageReport([direct, auto, spent, open], CODEX_AUTO_NOW, {
      pools,
      runningTurns: (key) => (key === "open:open@example.test" ? 2 : 0),
    });
    expect(report.instances.map((entry) => entry.instanceId)).toEqual(["codex", "codex_auto"]);
    const pooled = report.instances[1]!;
    expect(pooled).toMatchObject({ status: "available", stale: false });
    expect(pooled.windows).toEqual([
      expect.objectContaining({ kind: "session", remainingPercent: 90 }),
      expect.objectContaining({ kind: "weekly", remainingPercent: 60, resetsAt: iso(2 * HOUR) }),
    ]);
    expect(pooled.members).toEqual([
      expect.objectContaining({
        instanceId: "spent",
        runningTurns: 0,
        note: expect.stringMatching(/^Not used/),
      }),
      expect.objectContaining({
        instanceId: "open",
        runningTurns: 2,
        note: expect.stringMatching(/^First choice for new threads; weekly window 60% left/),
      }),
    ]);
    const exhausted = providerUsageReport([auto, spent], CODEX_AUTO_NOW, {
      pools: [{ ...pools[0]!, memberIds: [spent.instanceId] }],
    });
    expect(exhausted.instances[0]).toMatchObject({
      status: "unavailable",
      windows: expect.arrayContaining([
        expect.objectContaining({ kind: "weekly", remainingPercent: 0, resetsAt: iso(2 * HOUR) }),
      ]),
    });
  });
});
