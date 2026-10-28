import {
  ProviderSetupError,
  type ProviderInstanceId,
  type ModelSelection,
  ServerProvider,
  ServerProviderModel,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { ProviderUsageLimitError } from "@t3tools/provider-core/server/failure";
import {
  autoPoolEligible,
  autoPoolHeadroom,
  autoPoolModels,
  type AutoPoolHeadroom,
} from "./autoPoolPolicy.ts";

const isProviderSetupError = Schema.is(ProviderSetupError);

// Codex omits the service-tier selector when only Standard is supported.
const withServiceTier = (model: ServerProviderModel): ServerProviderModel => {
  const options = model.capabilities!.optionDescriptors!;
  return options.some((option) => option.id === "serviceTier")
    ? model
    : {
        ...model,
        capabilities: {
          ...model.capabilities!,
          optionDescriptors: [
            ...options,
            {
              id: "serviceTier",
              label: "Service Tier",
              type: "select" as const,
              options: [{ id: "default", label: "Standard", isDefault: true }],
            },
          ],
        },
      };
};

export function codexAutoModels(
  members: readonly ServerProvider[],
): readonly ServerProviderModel[] {
  return autoPoolModels(members, withServiceTier);
}

/** Routing reads cached allowance; native admission rechecks the account before inference. */
export function codexAutoHeadroom(
  provider: ServerProvider,
  selection: ModelSelection,
  now: number,
): AutoPoolHeadroom | undefined {
  if (!autoPoolEligible(provider) || provider.auth.type !== "chatgpt") return undefined;
  return autoPoolHeadroom(provider, selection.model, now);
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
