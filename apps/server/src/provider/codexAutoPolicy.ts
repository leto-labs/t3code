import {
  ProviderSetupError,
  type ProviderInstanceId,
  type ModelSelection,
  ProviderOptionDescriptor,
  ServerProvider,
  ServerProviderModel,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { ProviderUsageLimitError } from "@t3tools/provider-core/server/failure";

const isProviderSetupError = Schema.is(ProviderSetupError);

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

export function codexAutoModels(
  members: readonly ServerProvider[],
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
    // Codex omits the service-tier selector when only Standard is supported.
    const peers = discovered.map((peer) => ({
      ...peer,
      capabilities: {
        ...peer.capabilities!,
        optionDescriptors: peer.capabilities!.optionDescriptors!.some(
          (option) => option.id === "serviceTier",
        )
          ? peer.capabilities!.optionDescriptors!
          : [
              ...peer.capabilities!.optionDescriptors!,
              {
                id: "serviceTier",
                label: "Service Tier",
                type: "select" as const,
                options: [{ id: "default", label: "Standard", isDefault: true }],
              },
            ],
      },
    }));
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

export function codexAutoSelectionAllowed(
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

export function codexAutoNormalizeSelection(
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

export function codexAutoAccountKey(provider: ServerProvider): string | undefined {
  const workspace = provider.auth.workspaceId;
  const email = provider.auth.email?.trim().toLowerCase();
  return workspace && email ? `${workspace}:${email}` : undefined;
}

/**
 * The last published windows that apply to `selection`, however old. A failed
 * probe keeps the previous windows, and a window past its reset no longer limits.
 */
function codexAutoWindows(provider: ServerProvider, selection: ModelSelection, now: number) {
  const limits = provider.usageLimits;
  if (!limits || limits.unavailable) return undefined;
  return limits.windows
    .filter((window) => window.model === undefined || window.model === selection.model)
    .map((window) =>
      window.resetsAt !== undefined && Date.parse(window.resetsAt) <= now
        ? { ...window, usedPercent: 0 }
        : window,
    );
}

/** Routing reads cached allowance; native admission rechecks the account before inference. */
export function codexAutoHeadroom(
  provider: ServerProvider,
  selection: ModelSelection,
  now: number,
) {
  if (
    !provider.enabled ||
    !provider.installed ||
    provider.status === "error" ||
    provider.auth.status !== "authenticated" ||
    provider.auth.type !== "chatgpt"
  )
    return undefined;
  const windows = codexAutoWindows(provider, selection, now);
  if (
    !windows ||
    !windows.some((window) => !window.limitId) ||
    windows.some((window) => window.usedPercent >= 100)
  )
    return undefined;
  const remaining = Math.min(...windows.map((window) => 100 - window.usedPercent));
  return { healthy: remaining >= 20, remaining };
}

export function codexAutoExhausted(
  provider: ServerProvider,
  selection: ModelSelection,
  now: number,
): boolean {
  return (
    codexAutoWindows(provider, selection, now)?.some((window) => window.usedPercent >= 100) === true
  );
}

export function codexAutoResetAt(
  provider: ServerProvider,
  selection: ModelSelection,
): string | null {
  const exhausted = (provider.usageLimits?.windows ?? []).filter(
    (window) =>
      (window.model === undefined || window.model === selection.model) && window.usedPercent >= 100,
  );
  if (exhausted.length === 0 || exhausted.some((window) => !window.resetsAt)) return null;
  return exhausted
    .map((window) => window.resetsAt!)
    .sort()
    .at(-1)!;
}

export const codexAutoAdmissionQuotaError = (instanceId: ProviderInstanceId) =>
  new ProviderSetupError({
    instanceId,
    operation: "codexAuto.quotaBeforeInference",
    detail: "Codex confirmed exhausted subscription allowance before inference.",
    cause: new ProviderUsageLimitError({
      detail: "Codex confirmed exhausted subscription allowance before inference.",
      resetAt: null,
    }),
  });

export function isCodexAutoAdmissionQuotaError(error: unknown): boolean {
  let cause = error;
  for (let depth = 0; depth < 8 && typeof cause === "object" && cause !== null; depth++) {
    if (isProviderSetupError(cause) && cause.operation === "codexAuto.quotaBeforeInference")
      return true;
    cause = "cause" in cause ? cause.cause : undefined;
  }
  return false;
}
