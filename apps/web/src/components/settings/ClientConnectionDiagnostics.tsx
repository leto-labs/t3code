import type { ConnectionDiagnosticsSnapshot } from "@t3tools/client-runtime/connection";
import type { EnvironmentId } from "@t3tools/contracts";
import { CopyIcon } from "lucide-react";

import { useConnectionDiagnostics } from "../../state/connectionDiagnostics";
import { useCopyToClipboard } from "../../hooks/useCopyToClipboard";
import { Button } from "../ui/button";
import { SettingsSection } from "./settingsLayout";

function formatRtt(snapshot: ConnectionDiagnosticsSnapshot): string {
  const latest = snapshot.recentRttMs.at(-1);
  return latest === undefined ? "n/a" : `${Math.round(latest)} ms`;
}

function pathLabel(path: ConnectionDiagnosticsSnapshot["path"]): string {
  switch (path) {
    case "tailscale":
      return "Tailscale";
    case "relay":
      return "T3 Connect relay";
    case "ssh":
      return "SSH";
    case "direct":
      return "Direct";
    case "unknown":
      return "Unknown";
  }
}

function connectionSummary(snapshot: ConnectionDiagnosticsSnapshot): string {
  return [
    `path=${pathLabel(snapshot.path)}`,
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
  const snapshot = useConnectionDiagnostics(environmentId);
  const { copyToClipboard, isCopied } = useCopyToClipboard({
    target: "T3 connection diagnostics",
    timeout: 1_200,
  });

  if (environmentId === null) return null;

  return (
    <SettingsSection
      title="Client connection"
      headerAction={
        <Button
          size="sm"
          variant="ghost"
          onClick={() => void copyToClipboard(connectionSummary(snapshot), undefined)}
          aria-label="Copy client connection diagnostics"
        >
          <CopyIcon className="size-3.5" />
          {isCopied ? "Copied" : "Copy"}
        </Button>
      }
    >
      <div className="grid grid-cols-2 border-b border-border/60 sm:grid-cols-4">
        <Metric label="Path" value={pathLabel(snapshot.path)} />
        <Metric label="State" value={snapshot.phase} />
        <Metric label="Probe RTT" value={formatRtt(snapshot)} />
        <Metric label="Reconnects" value={String(snapshot.reconnects)} />
      </div>
      <div className="flex flex-wrap gap-x-5 gap-y-1 px-4 py-3 text-xs text-muted-foreground sm:px-5">
        <span>Socket errors {snapshot.socketErrors}</span>
        <span>Stream failures {snapshot.streamFailures}</span>
        {snapshot.lastErrorKind ? <span>Last error {snapshot.lastErrorKind}</span> : null}
        {snapshot.recentRttMs.length > 1 ? (
          <span>Recent probes {snapshot.recentRttMs.map(Math.round).join(", ")} ms</span>
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
