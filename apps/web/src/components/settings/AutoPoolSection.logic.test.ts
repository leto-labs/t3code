import { describe, expect, it } from "vite-plus/test";
import { ProviderDriverKind } from "@t3tools/contracts";
import { autoPoolMemberCandidates, readAutoPoolMembers } from "./AutoPoolSection.logic";

describe("Auto pool editing", () => {
  it("distinguishes an empty pool from a physical account", () => {
    expect(readAutoPoolMembers({ autoPoolMemberIds: [] })).toEqual([]);
    expect(readAutoPoolMembers({ homePath: "~/.codex" })).toBeUndefined();
  });
  it("keeps disabled subscription members editable while excluding managed accounts and nested pools", () => {
    const driver = ProviderDriverKind.make("codex");
    const accounts = autoPoolMemberCandidates(
      {
        codex: { driver },
        disabled: { driver, enabled: false },
        nested: { driver, config: { autoPoolMemberIds: [] } },
        managed: { driver, config: { setupMode: "managed" } },
        other: { driver: ProviderDriverKind.make("claudeAgent") },
      },
      driver,
    );
    expect(accounts.map(([id]) => id)).toEqual(["codex", "disabled"]);
  });
});
