import type {
  AutoPoolConfig,
  ProviderUsageReport,
  ProviderUsageReportPoolMember,
  ServerProvider,
} from "@t3tools/contracts";
import { DEFAULT_PROVIDER_HEALTH_REFRESH_INTERVAL } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import {
  AUTO_POOL_USAGE_REFRESH_INTERVAL_MS,
  autoPoolAccountKey,
  autoPoolCompare,
  autoPoolCurrentWindow,
  autoPoolSubscriptionEligible,
  autoPoolUsageIsStale,
  autoPoolHeadroom,
  autoPoolNote,
  autoPoolWindowGroup,
} from "./autoPoolPolicy.ts";

type ReportEntry = ProviderUsageReport["instances"][number];
type ReportWindow = ReportEntry["windows"][number];
interface PoolGroup {
  readonly window: ReportWindow;
  best: number;
  resetsAt?: string;
}

function usageEntry(provider: ServerProvider, nowMs: number, maxAgeMs: number): ReportEntry {
  const limits = provider.usageLimits;
  const stale = autoPoolUsageIsStale(provider, nowMs, 2 * maxAgeMs);
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
          : (limits.unavailable?.reason ?? (stale ? ("stale" as const) : ("available" as const))),
    windows: (limits?.windows ?? []).map((window) => ({
      ...window,
      remainingPercent: 100 - window.usedPercent,
    })),
  };
}

/**
 * Agents see one entry per Auto pool: available when any member has allowance for one of
 * its models, with each window kind and model reporting the best member and soonest reset.
 */
function poolEntry(
  auto: ServerProvider,
  members: readonly ServerProvider[],
  pool: AutoPoolConfig,
  nowMs: number,
  maxAgeMs: number,
  runningTurns: (accountKey: string) => number,
): ReportEntry {
  const eligible = members.filter(
    (member) => autoPoolSubscriptionEligible(member) && autoPoolAccountKey(member) !== undefined,
  );
  const available = eligible.some((member) =>
    auto.models.some((model) => autoPoolHeadroom(member, model.slug, nowMs) !== undefined),
  );
  const running = (member: ServerProvider) => {
    const key = autoPoolAccountKey(member);
    return key === undefined ? 0 : runningTurns(key);
  };
  const ranked = eligible
    .flatMap((member, order) => {
      const headroom = autoPoolHeadroom(member, undefined, nowMs);
      return headroom === undefined ? [] : [{ member, headroom, running: running(member), order }];
    })
    .toSorted(autoPoolCompare(pool.maxRunningTurns));
  const groups = new Map<string, PoolGroup>();
  for (const member of eligible) {
    const limits = member.usageLimits;
    if (!limits || limits.unavailable) continue;
    const tightest = new Map<string, number>();
    for (const raw of limits.windows) {
      const window = autoPoolCurrentWindow(raw, nowMs);
      const key = autoPoolWindowGroup(member, window);
      if (key === undefined) continue;
      const remaining = 100 - window.usedPercent;
      tightest.set(key, Math.min(tightest.get(key) ?? 100, remaining));
      const group: PoolGroup = groups.get(key) ?? {
        window: {
          id: `pool:${key}`,
          kind: window.kind,
          label: window.label,
          usedPercent: 100,
          remainingPercent: 0,
          ...(window.model === undefined ? {} : { model: window.model }),
          ...(window.limitId === undefined ? {} : { limitId: window.limitId }),
          ...(window.windowDurationMins === undefined
            ? {}
            : { windowDurationMins: window.windowDurationMins }),
        },
        best: 0,
      };
      groups.set(key, group);
      if (
        window.resetsAt !== undefined &&
        (group.resetsAt === undefined || Date.parse(window.resetsAt) < Date.parse(group.resetsAt))
      )
        group.resetsAt = window.resetsAt;
    }
    for (const [key, remaining] of tightest) {
      const group = groups.get(key)!;
      group.best = Math.max(group.best, remaining);
    }
  }
  const rows = eligible.map((member) => usageEntry(member, nowMs, maxAgeMs));
  const memberRows = members.map((member): ProviderUsageReportPoolMember => {
    const rank = ranked.findIndex((candidate) => candidate.member === member);
    return {
      ...usageEntry(member, nowMs, maxAgeMs),
      runningTurns: running(member),
      note: !eligible.includes(member)
        ? "Not used: disabled, signed out, or unavailable."
        : autoPoolNote(ranked[rank], rank, pool.maxRunningTurns),
    };
  });
  const latest = (values: ReadonlyArray<string | null>) =>
    values
      .filter((value): value is string => value !== null)
      .toSorted()
      .at(-1) ?? null;
  return {
    instanceId: auto.instanceId,
    driver: auto.driver,
    displayName: auto.displayName ?? auto.instanceId,
    checkedAt: latest(rows.map((row) => row.checkedAt)),
    lastAttemptAt: latest(rows.map((row) => row.lastAttemptAt)),
    stale: rows.every((row) => row.stale),
    status: available ? "available" : "unavailable",
    windows: [...groups.values()].map(({ window, best, resetsAt }) => ({
      ...window,
      usedPercent: 100 - best,
      remainingPercent: best,
      ...(resetsAt === undefined ? {} : { resetsAt }),
    })),
    members: memberRows,
  };
}

/** Pool members appear only inside their Auto entry, never as targets of their own. */
export function providerUsageReport(
  providers: readonly ServerProvider[],
  now: string,
  options: {
    readonly healthRefreshIntervalMs?: number;
    readonly pools?: readonly AutoPoolConfig[];
    readonly runningTurns?: (accountKey: string) => number;
  } = {},
): ProviderUsageReport {
  const pools = options.pools ?? [];
  const nowMs = Date.parse(now);
  const healthRefreshIntervalMs =
    options.healthRefreshIntervalMs ?? Duration.toMillis(DEFAULT_PROVIDER_HEALTH_REFRESH_INTERVAL);
  const memberIds = new Set<string>(pools.flatMap((pool) => pool.memberIds));
  const byId = new Map(providers.map((provider) => [provider.instanceId, provider]));
  return {
    observedAt: now,
    instances: providers
      .filter((provider) => !memberIds.has(provider.instanceId))
      .map((provider) => {
        const pool = pools.find((candidate) => candidate.instanceId === provider.instanceId);
        return pool === undefined
          ? usageEntry(provider, nowMs, healthRefreshIntervalMs)
          : poolEntry(
              provider,
              pool.memberIds.flatMap((id) => byId.get(id) ?? []),
              pool,
              nowMs,
              AUTO_POOL_USAGE_REFRESH_INTERVAL_MS,
              options.runningTurns ?? (() => 0),
            );
      }),
  };
}
