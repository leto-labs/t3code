import { useAtomValue } from "@effect/atom-react";
import { providersIgnoringCheckedAtAtom } from "../../state/server";
import { useRef, useState } from "react";
import { ProviderDriverKind, type EnvironmentId } from "@t3tools/contracts";
import { useEnvironmentSettings } from "../../hooks/useSettings";
import { SettingsRow } from "./settingsLayout";
import { Switch } from "../ui/switch";
import { codexAutoMemberCandidates, readCodexAutoMembers } from "./CodexAutoPoolSection.logic";

export function CodexAutoPoolSection({
  environmentId,
  value,
  onChange,
}: {
  readonly environmentId: EnvironmentId;
  readonly value: unknown;
  readonly onChange: (value: Record<string, unknown>) => void | Promise<void>;
}) {
  const settings = useEnvironmentSettings(environmentId);
  const providers = useAtomValue(providersIgnoringCheckedAtAtom(environmentId));
  const [saving, setSaving] = useState(false);
  const savingRef = useRef(false);
  const members = readCodexAutoMembers(value) ?? [];
  const candidates = codexAutoMemberCandidates({
    codex: { driver: ProviderDriverKind.make("codex") },
    ...settings.providerInstances,
  });
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
        title="Codex Auto accounts"
        description="Choose the accounts Auto may use. Selected accounts are hidden from the composer and direct agent access. They can sign in separately but must share the server's ~/.codex session home."
      />
      {members.length === 0 ? (
        <SettingsRow
          title="No accounts selected"
          description="Auto is unavailable in the composer until you add accounts."
        />
      ) : null}
      {candidates.map(([id, instance]) => (
        <SettingsRow
          key={id}
          title={instance.displayName || id}
          description={
            instance.enabled === false
              ? "This account is currently disabled. Enable it before using the pool."
              : [id, providers.find((provider) => provider.instanceId === id)?.auth.label]
                  .filter(Boolean)
                  .join(" · ")
          }
          control={
            <Switch
              disabled={saving}
              checked={members.includes(id)}
              onCheckedChange={(checked) => update(id, Boolean(checked))}
              aria-label={`Include ${instance.displayName || id} in Codex Auto`}
            />
          }
        />
      ))}
      {missing.map((id) => (
        <SettingsRow
          key={id}
          title={id}
          description="This selected account is missing or is no longer a direct Codex subscription. Remove it to repair the pool."
          control={
            <Switch
              disabled={saving}
              checked
              onCheckedChange={() => update(id, false)}
              aria-label={`Remove ${id} from Codex Auto`}
            />
          }
        />
      ))}
      <SettingsRow
        title="Supported models"
        description="Auto offers only models, reasoning options, and service tiers supported by every selected account. Manage model visibility and favorites in Models below."
      />
    </>
  );
}
