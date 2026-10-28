import type { ConnectionDiagnosticsSnapshot } from "@t3tools/client-runtime/connection";
import type { EnvironmentId } from "@t3tools/contracts";
import { msg, useGT } from "gt-react";
import { CopyIcon } from "lucide-react";

import { useConnectionDiagnostics } from "../../state/connectionDiagnostics";
import { useCopyToClipboard } from "../../hooks/useCopyToClipboard";
import { Button } from "../ui/button";
import { SettingsSection } from "./settingsLayout";

function formatRtt(snapshot: ConnectionDiagnosticsSnapshot): string {
  const latest = snapshot.recentRttMs.at(-1);
  return latest === undefined ? "n/a" : `${Math.round(latest)} ms`;
}

function connectionSummary(snapshot: ConnectionDiagnosticsSnapshot): string {
  return [
    `path=${snapshot.path}`,
    `phase=${snapshot.phase}`,
    `rtt_recent_ms=${snapshot.recentRttMs.map(Math.round).join(",") || "n/a"}`,
    `connection_attempts=${snapshot.connectionAttempts}`,
    `reconnects=${snapshot.reconnects}`,
    `socket_errors=${snapshot.socketErrors}`,
    `stream_failures=${snapshot.streamFailures}`,
    `last_error=${snapshot.lastErrorKind ?? "n/a"}`,
  ].join("\n");
}

export function ClientConnectionDiagnostics({
  environmentId,
}: {
  readonly environmentId: EnvironmentId | null;
}) {
  const gt = useGT();
  const snapshot = useConnectionDiagnostics(environmentId);
  const { copyToClipboard, isCopied } = useCopyToClipboard({
    target: {
      target: "T3 connection diagnostics",
      unavailableMessage: msg("Clipboard API is unavailable while copying connection diagnostics."),
      failureMessage: msg("Failed to copy connection diagnostics to the clipboard."),
    },
    timeout: 1_200,
  });

  if (environmentId === null) return null;
  const pathLabel = {
    tailscale: "Tailscale",
    relay: gt("T3 Connect relay"),
    ssh: "SSH",
    direct: gt("Direct", { $context: "network route" }),
    unknown: gt("Unknown"),
  }[snapshot.path];

  return (
    <SettingsSection
      title={gt("Client connection")}
      headerAction={
        <Button
          size="sm"
          variant="ghost"
          onClick={() => void copyToClipboard(connectionSummary(snapshot), undefined)}
          aria-label={gt("Copy client connection diagnostics")}
        >
          <CopyIcon className="size-3.5" />
          {isCopied ? gt("Copied") : gt("Copy", { $context: "verb, copies to the clipboard" })}
        </Button>
      }
    >
      <div className="grid grid-cols-2 border-b border-border/60 sm:grid-cols-4">
        <Metric label={gt("Path", { $context: "network route" })} value={pathLabel} />
        <Metric label={gt("State", { $context: "connection state" })} value={snapshot.phase} />
        <Metric label={gt("Probe RTT")} value={formatRtt(snapshot)} />
        <Metric label={gt("Reconnects")} value={String(snapshot.reconnects)} />
      </div>
      <div className="flex flex-wrap gap-x-5 gap-y-1 px-4 py-3 text-xs text-muted-foreground sm:px-5">
        <span>{gt("Socket errors {count}", { count: snapshot.socketErrors })}</span>
        <span>{gt("Stream failures {count}", { count: snapshot.streamFailures })}</span>
        {snapshot.lastErrorKind ? (
          <span>{gt("Last error {kind}", { kind: snapshot.lastErrorKind })}</span>
        ) : null}
        {snapshot.recentRttMs.length > 1 ? (
          <span>
            {gt("Recent probes {values} ms", {
              values: snapshot.recentRttMs.map(Math.round).join(", "),
            })}
          </span>
        ) : null}
      </div>
    </SettingsSection>
  );
}

function Metric({ label, value }: { readonly label: string; readonly value: string }) {
  return (
    <div className="border-r border-t border-border/60 px-4 py-3 first:border-t-0 sm:px-5">
      <div className="text-2xs font-medium uppercase tracking-widest text-muted-foreground/70">
        {label}
      </div>
      <div className="mt-1 truncate font-mono text-sm font-semibold tabular-nums text-foreground">
        {value}
      </div>
    </div>
  );
}
