import { readCodexAutoPoolMemberIds, type ProviderInstanceConfig } from "@t3tools/contracts";

export function readCodexAutoMembers(config: unknown): readonly string[] | undefined {
  if (config === null || typeof config !== "object" || !("autoPoolMemberIds" in config)) return;
  return readCodexAutoPoolMemberIds(config);
}

export function codexAutoMemberCandidates(
  instances: Readonly<Record<string, ProviderInstanceConfig>>,
) {
  return Object.entries(instances).filter(([, instance]) => {
    if (instance.driver !== "codex" || readCodexAutoMembers(instance.config) !== undefined)
      return false;
    return !(
      instance.config !== null &&
      typeof instance.config === "object" &&
      "setupMode" in instance.config &&
      instance.config.setupMode === "managed"
    );
  });
}
