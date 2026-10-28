import { describe, expect, it } from "vite-plus/test";

import { ConnectionDiagnosticsRegistry } from "./diagnostics.ts";

describe("ConnectionDiagnosticsRegistry", () => {
  it("counts reconnects only after the first connection attempt", () => {
    const registry = new ConnectionDiagnosticsRegistry();
    registry.setPath("env", "tailscale");
    registry.setPhase("env", "connecting");
    registry.setPhase("env", "connected");
    expect(registry.snapshot("env")).toMatchObject({
      path: "tailscale",
      phase: "connected",
      connectionAttempts: 1,
      reconnects: 0,
    });

    registry.recordSocketError("env", "ping-timeout");
    registry.setPhase("env", "disconnected");
    registry.setPhase("env", "connecting");
    expect(registry.snapshot("env")).toMatchObject({
      phase: "connecting",
      connectionAttempts: 2,
      reconnects: 1,
      socketErrors: 1,
      lastErrorKind: "ping-timeout",
    });
  });

  it("keeps the latest stream failure as the last error", () => {
    const registry = new ConnectionDiagnosticsRegistry();
    registry.recordSocketError("env", "socket-closed");
    registry.recordStreamFailure("env", "subscribeThread");
    expect(registry.snapshot("env")).toMatchObject({
      socketErrors: 1,
      streamFailures: 1,
      lastErrorKind: "subscribeThread:transport",
    });
  });

  it("retains only the most recent probe round trips", () => {
    const registry = new ConnectionDiagnosticsRegistry();
    for (let rtt = 1; rtt <= 12; rtt += 1) registry.recordProbe("env", rtt);
    expect(registry.snapshot("env").recentRttMs).toEqual([3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
  });

  it("returns a stable snapshot until the environment changes", () => {
    const registry = new ConnectionDiagnosticsRegistry();
    const notified: string[] = [];
    const unsubscribe = registry.subscribe("env", () => notified.push("env"));
    registry.setPhase("other", "connecting");
    const before = registry.snapshot("env");
    expect(registry.snapshot("env")).toBe(before);

    registry.setPhase("env", "disconnected");
    expect(notified).toEqual([]);
    expect(registry.snapshot("env")).toBe(before);

    registry.setPhase("env", "connecting");
    expect(notified).toEqual(["env"]);
    expect(registry.snapshot("env")).not.toBe(before);
    unsubscribe();
  });
});
