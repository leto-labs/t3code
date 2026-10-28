import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as DateTime from "effect/DateTime";
import { applyUsageLimitsUpdate } from "@t3tools/provider-core/server/usageLimits";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";

export const CODEX_AUTO_MODEL = "gpt-test";
export const CODEX_AUTO_NOW = "2026-10-17T12:00:00.000Z";

export function codexAutoFixture(
  id: string,
  now = CODEX_AUTO_NOW,
  tiers = ["default", "priority"],
) {
  const instanceId = ProviderInstanceId.make(id);
  const driver = ProviderDriverKind.make("codex");
  let value: ServerProvider = {
    instanceId,
    driver,
    enabled: true,
    installed: true,
    version: "test",
    status: "ready",
    checkedAt: now,
    auth: {
      status: "authenticated",
      type: "chatgpt",
      email: `${id}@example.test`,
      workspaceId: id,
    },
    models: [
      {
        slug: CODEX_AUTO_MODEL,
        name: "Test model",
        isCustom: false,
        capabilities: {
          optionDescriptors: [
            {
              id: "serviceTier",
              label: "Service Tier",
              type: "select",
              options: tiers.map((tier) => ({
                id: tier,
                label: tier,
                isDefault: tier === "default",
              })),
            },
            {
              id: "reasoningEffort",
              label: "Reasoning",
              type: "select",
              options: ["low", "high"].map((effort) => ({ id: effort, label: effort })),
            },
          ],
        },
      },
    ],
    slashCommands: [],
    skills: [],
    usageLimits: {
      checkedAt: now,
      paidCreditsAvailable: false,
      paidCreditsCheckedAt: now,
      windows: [
        {
          id: "primary",
          kind: "session",
          label: "Session",
          usedPercent: 10,
          observedAt: now,
          resetsAt: DateTime.formatIso(DateTime.makeUnsafe(Date.parse(now) + 3_600_000)),
        },
        {
          id: "secondary",
          kind: "weekly",
          label: "Weekly",
          usedPercent: 20,
          observedAt: now,
          resetsAt: DateTime.formatIso(DateTime.makeUnsafe(Date.parse(now) + 86_400_000)),
        },
      ],
    },
  };
  let refreshes = 0;
  let onRefresh = (current: ServerProvider) => current;
  const unused = () => Effect.die("This fixture operation is not used.");
  const instance: ProviderInstance = {
    instanceId,
    driverKind: driver,
    continuationIdentity: { driverKind: driver, continuationKey: "codex:home:/shared" },
    displayName: id,
    enabled: true,
    snapshot: {
      getSnapshot: Effect.sync(() => value),
      refresh: Effect.sync(() => {
        refreshes++;
        value = onRefresh(value);
        return value;
      }),
      streamChanges: Stream.empty,
      resolveMaintenance: () =>
        Effect.succeed({ provider: driver, packageName: null, update: null }),
      applyUsageLimits: (update) =>
        Effect.sync(() => {
          value = {
            ...value,
            usageLimits: applyUsageLimitsUpdate({
              previous: value.usageLimits,
              update,
              checkedAt: update.checkedAt,
            }),
          };
        }),
    },
    orchestrationAdapter: {
      instanceId,
      driver,
      getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: unused,
    },
    textGeneration: {
      generateCommitMessage: unused,
      generatePrContent: unused,
      generateBranchName: unused,
      generateTranscriptionPostProcessing: unused,
      generateThreadTitle: unused,
      generateReviewGuide: unused,
    },
  };
  return {
    instance,
    read: () => value,
    set: (next: ServerProvider) => {
      value = next;
    },
    member: () => ({ instance, snapshot: value }),
    refreshes: () => refreshes,
    /** What the next usage refresh reports. */
    onRefresh: (next: (current: ServerProvider) => ServerProvider) => {
      onRefresh = next;
    },
  };
}
