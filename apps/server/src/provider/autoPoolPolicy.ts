import type {
  ModelSelection,
  ProviderOptionDescriptor,
  ServerProvider,
  ServerProviderAuthStatus,
  ServerProviderModel,
  ServerProviderUsageWindow,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import { isClaudeSubscriptionWindow, windowLimitsModel } from "./claudeUsageLimits.ts";

/** Below this share of its binding window, a thread leaves its account at the next turn. */
const AUTO_POOL_FLOOR_PERCENT = 5;

export const AUTO_POOL_USAGE_REFRESH_INTERVAL_MS = Duration.toMillis(Duration.minutes(10));

export function autoPoolUsageIsStale(provider: ServerProvider, now: number, maxAgeMs: number) {
  const limits = provider.usageLimits;
  return (
    limits === undefined ||
    now - Date.parse(limits.checkedAt) > maxAgeMs ||
    limits.windows.some(
      (window) => now - Date.parse(window.observedAt ?? limits.checkedAt) > maxAgeMs,
    )
  );
}

export function autoPoolAccountKey(provider: ServerProvider): string | undefined {
  const workspace = provider.auth.workspaceId;
  const email = provider.auth.email?.trim().toLowerCase();
  return workspace && email ? `${workspace}:${email}` : undefined;
}

function autoPoolEligible(provider: ServerProvider): boolean {
  return (
    provider.enabled &&
    provider.installed &&
    provider.status !== "error" &&
    provider.auth.status === "authenticated"
  );
}

const CLAUDE_NON_SUBSCRIPTION_AUTH = new Set(["apiKey", "bedrock", "customEndpoint"]);

export function autoPoolSubscriptionEligible(provider: ServerProvider): boolean {
  if (!autoPoolEligible(provider)) return false;
  const type = provider.auth.type;
  return provider.driver === "claudeAgent"
    ? type !== undefined && !CLAUDE_NON_SUBSCRIPTION_AUTH.has(type)
    : provider.driver === "codex" && type === "chatgpt";
}

/** Auto is signed in when any member is; it is signed out only when every member is. */
export function autoPoolAuthStatus(members: readonly ServerProvider[]): ServerProviderAuthStatus {
  if (members.some((member) => member.auth.status === "authenticated")) return "authenticated";
  return members.length > 0 && members.every((member) => member.auth.status === "unauthenticated")
    ? "unauthenticated"
    : "unknown";
}

function intersectOptions(
  models: readonly ServerProviderModel[],
): readonly ProviderOptionDescriptor[] {
  return (models[0]?.capabilities?.optionDescriptors ?? []).flatMap<ProviderOptionDescriptor>(
    (descriptor) => {
      const peers = models.map((model) =>
        model.capabilities?.optionDescriptors?.find((option) => option.id === descriptor.id),
      );
      if (peers.some((peer) => peer?.type !== descriptor.type)) return [];
      if (descriptor.type === "boolean") return [{ ...descriptor, currentValue: false }];
      const choices = descriptor.options.filter((choice) =>
        peers.every(
          (peer) =>
            peer?.type === "select" && peer.options.some((option) => option.id === choice.id),
        ),
      );
      if (choices.length === 0) return [];
      const preferred = choices.find((choice) => choice.isDefault)?.id ?? choices[0]!.id;
      return [
        {
          ...descriptor,
          options: choices.map((choice) => ({ ...choice, isDefault: choice.id === preferred })),
          currentValue: preferred,
        },
      ];
    },
  );
}

/**
 * The models every member offers, with only the options every member accepts. `prepare` lets a
 * driver fill in an option a member omits because it has a single implicit value.
 */
export function autoPoolModels(
  members: readonly ServerProvider[],
  prepare: (model: ServerProviderModel) => ServerProviderModel = (model) => model,
): readonly ServerProviderModel[] {
  if (members.length === 0) return [];
  return members[0]!.models.flatMap((model) => {
    const discovered = members.flatMap((member) =>
      member.models.filter((peer) => peer.slug === model.slug),
    );
    if (
      discovered.length !== members.length ||
      discovered.some((peer) => peer.capabilities?.optionDescriptors === undefined)
    )
      return [];
    const peers = discovered.map(prepare);
    const options = intersectOptions(peers);
    // Removing a required selection category would let a client's implicit default bypass the pool.
    if (
      peers.some((peer) =>
        (peer.capabilities?.optionDescriptors ?? []).some(
          (option) => !options.some((common) => common.id === option.id),
        ),
      )
    )
      return [];
    return [{ ...model, capabilities: { ...model.capabilities!, optionDescriptors: options } }];
  });
}

export function autoPoolSelectionAllowed(
  models: readonly ServerProviderModel[],
  selection: ModelSelection,
): boolean {
  const model = models.find((candidate) => candidate.slug === selection.model);
  if (!model) return false;
  const seen = new Set<string>();
  return (selection.options ?? []).every((option) => {
    if (seen.has(option.id)) return false;
    seen.add(option.id);
    const descriptor = model.capabilities?.optionDescriptors?.find(
      (candidate) => candidate.id === option.id,
    );
    return descriptor?.type === "boolean"
      ? typeof option.value === "boolean"
      : descriptor?.type === "select" &&
          descriptor.options.some((choice) => choice.id === option.value);
  });
}

/** Makes every option explicit, so no member applies its own default. */
export function autoPoolNormalizeSelection(
  models: readonly ServerProviderModel[],
  selection: ModelSelection,
): ModelSelection {
  const model = models.find((candidate) => candidate.slug === selection.model);
  return {
    ...selection,
    options: (model?.capabilities?.optionDescriptors ?? []).map((descriptor) => {
      const explicit = selection.options?.find((option) => option.id === descriptor.id);
      return (
        explicit ?? {
          id: descriptor.id,
          value:
            descriptor.type === "boolean"
              ? false
              : (descriptor.options.find((choice) => choice.isDefault)?.id ??
                descriptor.options[0]!.id),
        }
      );
    }),
  };
}

// Claude names its model-scoped weekly windows by id and reports paid extra usage as a window.
const appliesTo =
  (provider: ServerProvider) =>
  (window: ServerProviderUsageWindow, model: string | undefined): boolean =>
    provider.driver === "claudeAgent"
      ? isClaudeSubscriptionWindow(window) && windowLimitsModel(window, model ?? null)
      : (window.model === undefined || window.model === model) &&
        (window.modelIds === undefined || (model !== undefined && window.modelIds.includes(model)));

/**
 * Which pooled report row a member's window joins, or undefined when it is not subscription
 * allowance. Claude's window ids name their model scope; other drivers scope by model or limit.
 */
export function autoPoolWindowGroup(
  provider: ServerProvider,
  window: ServerProviderUsageWindow,
): string | undefined {
  if (provider.driver !== "claudeAgent")
    return `${window.kind}:${window.model ?? window.limitId ?? ""}`;
  return isClaudeSubscriptionWindow(window) ? `${window.kind}:${window.id}` : undefined;
}

/**
 * The last published windows that limit `model` (account-wide ones when it is undefined),
 * however old. A failed probe keeps the previous windows; one past its reset no longer limits.
 */
function autoPoolWindows(
  provider: ServerProvider,
  model: string | undefined,
  now: number,
): readonly ServerProviderUsageWindow[] | undefined {
  const limits = provider.usageLimits;
  if (!limits || limits.unavailable) return undefined;
  return limits.windows
    .filter((window) => appliesTo(provider)(window, model))
    .map((window) => autoPoolCurrentWindow(window, now));
}

export const autoPoolCurrentWindow = (window: ServerProviderUsageWindow, now: number) =>
  window.resetsAt !== undefined && Date.parse(window.resetsAt) <= now
    ? { ...window, usedPercent: 0 }
    : window;

export function autoPoolExhausted(
  provider: ServerProvider,
  model: string | undefined,
  now: number,
): boolean {
  return (
    autoPoolWindows(provider, model, now)?.some((window) => window.usedPercent >= 100) === true
  );
}

/** When every spent window that limits `model` has reset, or null when one has no reset time. */
export function autoPoolResetAt(
  provider: ServerProvider,
  model: string | undefined,
  now: number,
): string | null {
  const exhausted = (autoPoolWindows(provider, model, now) ?? []).filter(
    (window) => window.usedPercent >= 100,
  );
  if (
    exhausted.length === 0 ||
    exhausted.some((window) => !Number.isFinite(Date.parse(window.resetsAt ?? "")))
  )
    return null;
  return exhausted
    .map((window) => window.resetsAt!)
    .sort((a, b) => Date.parse(a) - Date.parse(b))
    .at(-1)!;
}

export interface AutoPoolHeadroom {
  /** The tightest applicable window. */
  readonly remaining: number;
  /** The long window that limits this model most; its reset decides the order. */
  readonly binding: {
    readonly kind: ServerProviderUsageWindow["kind"] | undefined;
    readonly remaining: number;
    readonly resetsAt: number | undefined;
  };
}

/** Undefined when usage is unavailable or an applicable window is spent. */
export function autoPoolHeadroom(
  provider: ServerProvider,
  model: string | undefined,
  now: number,
): AutoPoolHeadroom | undefined {
  const windows = autoPoolWindows(provider, model, now);
  if (
    !windows ||
    (provider.driver !== "claudeAgent" && !windows.some((window) => !window.limitId)) ||
    windows.some((window) => window.usedPercent >= 100)
  )
    return undefined;
  const long = windows.filter((window) => window.kind === "weekly" || window.kind === "monthly");
  const reset = (window: ServerProviderUsageWindow) =>
    window.resetsAt === undefined ? undefined : Date.parse(window.resetsAt);
  const binding = (long.length > 0 ? long : windows).toSorted(
    (a, b) =>
      b.usedPercent - a.usedPercent ||
      (reset(a) ?? Number.POSITIVE_INFINITY) - (reset(b) ?? Number.POSITIVE_INFINITY),
  )[0];
  return {
    remaining: Math.min(100, ...windows.map((window) => 100 - window.usedPercent)),
    binding: binding
      ? { kind: binding.kind, remaining: 100 - binding.usedPercent, resetsAt: reset(binding) }
      : { kind: undefined, remaining: 100, resetsAt: undefined },
  };
}

interface AutoPoolCandidate {
  readonly headroom: AutoPoolHeadroom;
  readonly running: number;
  /** Position in the pool settings; the last tie-break. */
  readonly order: number;
}

/**
 * Allowance left at a reset is lost and later allowance carries over, so the account whose
 * binding window resets first goes first. Percentages from different plans are not
 * capacities, so accounts without reset data fall back to remaining percentage.
 */
export function autoPoolCompare(maxRunningTurns: number) {
  return (a: AutoPoolCandidate, b: AutoPoolCandidate): number => {
    const full = Number(a.running >= maxRunningTurns) - Number(b.running >= maxRunningTurns);
    if (full !== 0) return full;
    const resetA = a.headroom.binding.resetsAt;
    const resetB = b.headroom.binding.resetsAt;
    if (resetA === undefined && resetB === undefined)
      return (
        b.headroom.remaining - a.headroom.remaining || a.running - b.running || a.order - b.order
      );
    if (resetA === undefined) return 1;
    if (resetB === undefined) return -1;
    return (
      resetA - resetB ||
      a.running - b.running ||
      b.headroom.remaining - a.headroom.remaining ||
      a.order - b.order
    );
  };
}

const belowFloor = (candidate: AutoPoolCandidate) =>
  candidate.headroom.binding.remaining < AUTO_POOL_FLOOR_PERCENT;

/**
 * Switching accounts loses the prompt cache, so a thread keeps its current account until
 * that account falls below the floor and another account is above it.
 */
export function autoPoolChoose<C extends AutoPoolCandidate>(
  candidates: readonly C[],
  input: { readonly current?: C | undefined; readonly maxRunningTurns: number },
): C | undefined {
  const ranked = candidates.toSorted(autoPoolCompare(input.maxRunningTurns));
  const current = input.current;
  if (current === undefined) return ranked[0];
  if (!belowFloor(current)) return current;
  return ranked.find((candidate) => candidate !== current && !belowFloor(candidate)) ?? current;
}

/** Why an account is or is not next in line, for agents reading the usage report. */
export function autoPoolNote(
  candidate: AutoPoolCandidate | undefined,
  rank: number,
  maxRunningTurns: number,
): string {
  if (candidate === undefined) return "Not used: no allowance for any model, or no usage data.";
  const { binding } = candidate.headroom;
  const parts = [
    rank === 0 ? "First choice for new threads" : `Choice ${rank + 1} for new threads`,
    binding.kind === undefined
      ? "No subscription usage window observed"
      : binding.resetsAt === undefined
        ? `${binding.kind} window ${Math.round(binding.remaining)}% left, reset unknown`
        : `${binding.kind} window ${Math.round(binding.remaining)}% left, resets ${DateTime.formatIso(DateTime.makeUnsafe(binding.resetsAt))}`,
    `${candidate.running} of ${maxRunningTurns} running turns`,
  ];
  if (candidate.running >= maxRunningTurns) parts.push("at the cap, so new threads go elsewhere");
  if (belowFloor(candidate))
    parts.push(`below ${AUTO_POOL_FLOOR_PERCENT}%, so threads move off at their next turn`);
  return `${parts.join("; ")}.`;
}
