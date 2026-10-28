import * as Context from "effect/Context";

export class CodexAccountLoad extends Context.Reference<{
  readonly reserve: (token: string, account: string) => void;
  readonly release: (token: string) => void;
  readonly count: (account: string, excluding: string) => number;
}>("t3/provider/CodexAccountLoad", {
  defaultValue: () => {
    const reservations = new Map<string, string>();
    return {
      reserve: (token, account) => {
        reservations.set(token, account);
      },
      release: (token) => {
        reservations.delete(token);
      },
      count: (account, excluding) =>
        [...reservations].filter(([token, key]) => token !== excluding && key === account).length,
    };
  },
}) {}
