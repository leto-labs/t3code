import type { ProviderUsageReport, ServerProvider } from "@t3tools/contracts";

const USAGE_FRESHNESS_MS = 60_000;

export function providerUsageReport(
  providers: readonly ServerProvider[],
  now: string,
): ProviderUsageReport {
  return {
    observedAt: now,
    instances: providers.map((provider) => {
      const limits = provider.usageLimits;
      const stale =
        limits === undefined ||
        Date.parse(now) - Date.parse(limits.checkedAt) > USAGE_FRESHNESS_MS ||
        limits.windows.some(
          (window) =>
            Date.parse(now) - Date.parse(window.observedAt ?? limits.checkedAt) >
            USAGE_FRESHNESS_MS,
        );
      return {
        instanceId: provider.instanceId,
        driver: provider.driver,
        displayName: provider.displayName ?? provider.instanceId,
        checkedAt: limits?.checkedAt ?? null,
        lastAttemptAt: limits?.lastAttemptAt ?? limits?.checkedAt ?? null,
        stale,
        status:
          !provider.enabled ||
          !provider.installed ||
          provider.auth.status === "unauthenticated" ||
          limits === undefined
            ? ("unavailable" as const)
            : provider.status === "error" || limits.lastAttemptFailed
              ? ("probeFailed" as const)
              : (limits.unavailable?.reason ??
                (stale ? ("stale" as const) : ("available" as const))),
        windows: (limits?.windows ?? []).map((window) => ({
          ...window,
          remainingPercent: 100 - window.usedPercent,
        })),
      };
    }),
  };
}
