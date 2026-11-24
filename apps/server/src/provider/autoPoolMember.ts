import {
  autoPoolMemberRejection,
  autoPoolOwnerId,
  providerInstanceConfigEnabledFlag,
  readAutoPools,
  type ProviderDriverKind,
  type ProviderInstanceConfigMap,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

/** Pool members serve Auto through their own adapter copy; direct selection would race its routing. */
export const reserveAutoPoolMember = Effect.fn("reserveAutoPoolMember")(function* (
  adapter: ProviderAdapter.ProviderAdapterV2["Service"],
  instanceId: ProviderInstanceId,
) {
  const host = yield* ProviderHost.ProviderHost;
  const driver = adapter.driver;
  const ensureDirect = host.settings.get.pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapter.ProviderAdapterProtocolError({
          driver,
          detail: "Could not read provider settings to check Auto pool membership.",
          cause,
        }),
    ),
    Effect.flatMap((settings) => {
      const owner = autoPoolOwnerId(settings.providerInstances, instanceId);
      return owner === undefined
        ? Effect.void
        : Effect.fail(
            new ProviderAdapter.ProviderAdapterProtocolError({
              driver,
              detail: autoPoolMemberRejection(instanceId, owner),
            }),
          );
    }),
  );
  return {
    ...adapter,
    validateModelSelection: (selection, projectId) =>
      ensureDirect.pipe(
        Effect.andThen(adapter.validateModelSelection?.(selection, projectId) ?? Effect.void),
      ),
    openSession: (session) =>
      ensureDirect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver,
              providerSessionId: session.providerSessionId,
              cause,
            }),
        ),
        Effect.andThen(adapter.openSession(session)),
      ),
  } satisfies ProviderAdapter.ProviderAdapterV2["Service"];
});

export function currentAutoPool(
  instances: ProviderInstanceConfigMap,
  instanceId: ProviderInstanceId,
  driver: ProviderDriverKind,
) {
  const entry = instances[instanceId];
  if (entry?.driver !== driver || providerInstanceConfigEnabledFlag(entry.config) === false)
    return undefined;
  const pool = readAutoPools({ [instanceId]: entry })[0];
  return pool &&
    !pool.memberIds.includes(instanceId) &&
    new Set(pool.memberIds).size === pool.memberIds.length
    ? pool
    : undefined;
}
