import { describe, expect, it } from "vite-plus/test";
import { ProviderDriverKind } from "@t3tools/contracts";
import { codexAutoMemberCandidates, readCodexAutoMembers } from "./CodexAutoPoolSection.logic";

describe("Codex Auto pool editing", () => {
  it("distinguishes an empty pool from a physical account", () => {
    expect(readCodexAutoMembers({ autoPoolMemberIds: [] })).toEqual([]);
    expect(readCodexAutoMembers({ homePath: "~/.codex" })).toBeUndefined();
  });
  it("keeps disabled subscription members editable while excluding managed accounts and nested pools", () => {
    const driver = ProviderDriverKind.make("codex");
    const accounts = codexAutoMemberCandidates({
      codex: { driver },
      disabled: { driver, enabled: false },
      nested: { driver, config: { autoPoolMemberIds: [] } },
      managed: { driver, config: { setupMode: "managed" } },
      other: { driver: ProviderDriverKind.make("claudeAgent") },
    });
    expect(accounts.map(([id]) => id)).toEqual(["codex", "disabled"]);
  });
});
