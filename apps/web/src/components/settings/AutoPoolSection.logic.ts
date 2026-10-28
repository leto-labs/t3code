import {
  readAutoPoolMemberIds,
  type ProviderDriverKind,
  type ProviderInstanceConfig,
} from "@t3tools/contracts";

/** Undefined for a physical account; an Auto pool has a member list, possibly empty. */
export function readAutoPoolMembers(config: unknown): readonly string[] | undefined {
  if (config === null || typeof config !== "object" || !("autoPoolMemberIds" in config)) return;
  return readAutoPoolMemberIds(config);
}

/** Direct subscription accounts of `driver` that an Auto pool may list. */
export function autoPoolMemberCandidates(
  instances: Readonly<Record<string, ProviderInstanceConfig>>,
  driver: ProviderDriverKind,
) {
  return Object.entries(instances).filter(([, instance]) => {
    if (instance.driver !== driver || readAutoPoolMembers(instance.config) !== undefined)
      return false;
    return !(
      instance.config !== null &&
      typeof instance.config === "object" &&
      "setupMode" in instance.config &&
      instance.config.setupMode === "managed"
    );
  });
}
