export type ConnectionDiagnosticsPath = "direct" | "tailscale" | "relay" | "ssh" | "unknown";
export type ConnectionDiagnosticsPhase = "disconnected" | "connecting" | "connected";

export interface ConnectionDiagnosticsSnapshot {
  readonly path: ConnectionDiagnosticsPath;
  readonly phase: ConnectionDiagnosticsPhase;
  readonly connectionAttempts: number;
  readonly reconnects: number;
  readonly socketErrors: number;
  readonly streamFailures: number;
  readonly lastErrorKind: string | null;
  /** Latest connection probe round trips, oldest first. */
  readonly recentRttMs: ReadonlyArray<number>;
}

const MAX_RTT_SAMPLES = 10;

const EMPTY_CONNECTION_DIAGNOSTICS: ConnectionDiagnosticsSnapshot = {
  path: "unknown",
  phase: "disconnected",
  connectionAttempts: 0,
  reconnects: 0,
  socketErrors: 0,
  streamFailures: 0,
  lastErrorKind: null,
  recentRttMs: [],
};

/** Per-environment transport counters. Snapshots are immutable, so reads are free per render. */
export class ConnectionDiagnosticsRegistry {
  private readonly snapshots = new Map<string, ConnectionDiagnosticsSnapshot>();
  private readonly listeners = new Map<string, Set<() => void>>();

  private update(
    environmentId: string,
    next: (current: ConnectionDiagnosticsSnapshot) => ConnectionDiagnosticsSnapshot,
  ): void {
    const current = this.snapshot(environmentId);
    const updated = next(current);
    if (updated === current) return;
    this.snapshots.set(environmentId, updated);
    for (const listener of this.listeners.get(environmentId) ?? []) listener();
  }

  setPath(environmentId: string, path: ConnectionDiagnosticsPath): void {
    this.update(environmentId, (current) =>
      current.path === path ? current : { ...current, path },
    );
  }

  setPhase(environmentId: string, phase: ConnectionDiagnosticsPhase): void {
    this.update(environmentId, (current) => {
      if (current.phase === phase) return current;
      if (phase !== "connecting") return { ...current, phase };
      return {
        ...current,
        phase,
        connectionAttempts: current.connectionAttempts + 1,
        reconnects: current.connectionAttempts > 0 ? current.reconnects + 1 : current.reconnects,
      };
    });
  }

  recordSocketError(environmentId: string, errorKind: string): void {
    this.update(environmentId, (current) => ({
      ...current,
      socketErrors: current.socketErrors + 1,
      lastErrorKind: errorKind,
    }));
  }

  recordStreamFailure(environmentId: string, method: string): void {
    this.update(environmentId, (current) => ({
      ...current,
      streamFailures: current.streamFailures + 1,
      lastErrorKind: `${method}:transport`,
    }));
  }

  recordProbe(environmentId: string, rttMs: number): void {
    this.update(environmentId, (current) => ({
      ...current,
      recentRttMs: [...current.recentRttMs.slice(1 - MAX_RTT_SAMPLES), Math.max(0, rttMs)],
    }));
  }

  snapshot(environmentId: string): ConnectionDiagnosticsSnapshot {
    return this.snapshots.get(environmentId) ?? EMPTY_CONNECTION_DIAGNOSTICS;
  }

  subscribe(environmentId: string, listener: () => void): () => void {
    const listeners = this.listeners.get(environmentId) ?? new Set<() => void>();
    listeners.add(listener);
    this.listeners.set(environmentId, listeners);
    return () => {
      listeners.delete(listener);
      if (listeners.size === 0) this.listeners.delete(environmentId);
    };
  }
}

export const connectionDiagnostics = new ConnectionDiagnosticsRegistry();
