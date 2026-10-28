import { useAtomValue } from "@effect/atom-react";
import { providersIgnoringCheckedAtAtom } from "../../state/server";
import { useGT } from "gt-react";
import { useRef, useState } from "react";
import type { EnvironmentId, ProviderDriverKind } from "@t3tools/contracts";
import { useEnvironmentSettings } from "../../hooks/useSettings";
import { SettingsRow } from "./settingsLayout";
import { Switch } from "../ui/switch";
import { autoPoolMemberCandidates, readAutoPoolMembers } from "./AutoPoolSection.logic";

export function AutoPoolSection({
  environmentId,
  driver,
  value,
  onChange,
}: {
  readonly environmentId: EnvironmentId;
  readonly driver: ProviderDriverKind;
  readonly value: unknown;
  readonly onChange: (value: Record<string, unknown>) => void | Promise<void>;
}) {
  const gt = useGT();
  const settings = useEnvironmentSettings(environmentId);
  const providers = useAtomValue(providersIgnoringCheckedAtAtom(environmentId));
  const [saving, setSaving] = useState(false);
  const savingRef = useRef(false);
  const members = readAutoPoolMembers(value) ?? [];
  // The default instance is a candidate before its settings entry exists.
  const candidates = autoPoolMemberCandidates(
    { [driver]: { driver }, ...settings.providerInstances },
    driver,
  );
  const listedIds = new Set(candidates.map(([id]) => id));
  const missing = members.filter((id) => !listedIds.has(id));
  const config = value !== null && typeof value === "object" ? value : {};
  const update = async (id: string, selected: boolean) => {
    if (savingRef.current) return;
    savingRef.current = true;
    setSaving(true);
    await Promise.resolve(
      onChange({
        ...config,
        autoPoolMemberIds: selected ? [...members, id] : members.filter((member) => member !== id),
      }),
    ).finally(() => {
      savingRef.current = false;
      setSaving(false);
    });
  };

  return (
    <>
      <SettingsRow
        title={gt("Codex Auto accounts")}
        description={gt(
          "Choose the accounts Auto may use. Selected accounts are hidden from the composer and direct agent access. They can sign in separately but must share the server''s {path} session home.",
          { path: "~/.codex" },
        )}
      />
      {members.length === 0 ? (
        <SettingsRow
          title={gt("No accounts selected")}
          description={gt("Auto is unavailable in the composer until you add accounts.")}
        />
      ) : null}
      {candidates.map(([id, instance]) => (
        <SettingsRow
          key={id}
          title={instance.displayName || id}
          description={
            instance.enabled === false
              ? gt("This account is currently disabled. Enable it before using the pool.")
              : [id, providers.find((provider) => provider.instanceId === id)?.auth.label]
                  .filter(Boolean)
                  .join(" · ")
          }
          control={
            <Switch
              disabled={saving}
              checked={members.includes(id)}
              onCheckedChange={(checked) => update(id, Boolean(checked))}
              aria-label={gt("Include {name} in Codex Auto", { name: instance.displayName || id })}
            />
          }
        />
      ))}
      {missing.map((id) => (
        <SettingsRow
          key={id}
          title={id}
          description={gt(
            "This selected account is missing or is no longer a direct Codex subscription. Remove it to repair the pool.",
          )}
          control={
            <Switch
              disabled={saving}
              checked
              onCheckedChange={() => update(id, false)}
              aria-label={gt("Remove {name} from Codex Auto", { name: id })}
            />
          }
        />
      ))}
      <SettingsRow
        title={gt("Supported models")}
        description={gt(
          "Auto offers only models, reasoning options, and service tiers supported by every selected account. Manage model visibility and favorites in Models below.",
        )}
      />
    </>
  );
}
