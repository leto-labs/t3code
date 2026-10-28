import { describe, expect, it } from "vite-plus/test";
import { ProviderInstanceId } from "@t3tools/contracts";
import {
  codexAutoHeadroom,
  codexAutoModels,
  codexAutoResetAt,
  codexAutoSelectionAllowed,
} from "./codexAutoPolicy.ts";
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
      codexAutoSelectionAllowed(models, {
        ...selection,
        options: [{ id: "serviceTier", value: "ultrafast" }],
      }),
    ).toBe(false);
    expect(
      codexAutoSelectionAllowed(models, {
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
    expect(codexAutoSelectionAllowed(models, { ...selection, model: "unknown" })).toBe(false);
    expect(
      codexAutoSelectionAllowed(models, {
        ...selection,
        options: [{ id: "unknown", value: true }],
      }),
    ).toBe(false);
    expect(
      codexAutoSelectionAllowed(models, {
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
      codexAutoSelectionAllowed(models, {
        ...selection,
        options: [{ id: "serviceTier", value: "default" }],
      }),
    ).toBe(true);
    expect(
      codexAutoSelectionAllowed(models, {
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
    ).toMatchObject({ healthy: true });
    expect(codexAutoHeadroom(provider, selection, now)).toMatchObject({ healthy: true });
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
    ).toMatchObject({ healthy: true });
    const {
      paidCreditsAvailable: _credits,
      paidCreditsCheckedAt: _creditsCheckedAt,
      ...withoutCredits
    } = provider.usageLimits!;
    expect(
      codexAutoHeadroom({ ...provider, usageLimits: withoutCredits }, selection, now),
    ).toMatchObject({ healthy: true });
    expect(codexAutoHeadroom(provider, selection, now + 61_000)).toMatchObject({ healthy: true });
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
    expect(codexAutoHeadroom(spentSession, selection, now + 3_600_001)).toMatchObject({
      healthy: true,
    });
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
    expect(codexAutoResetAt(exhausted, selection)).toBe("2026-10-18T12:00:00.000Z");
    expect(
      codexAutoResetAt(
        {
          ...exhausted,
          usageLimits: {
            ...exhausted.usageLimits,
            windows: [{ id: "primary", kind: "session", label: "Session", usedPercent: 100 }],
          },
        },
        selection,
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
