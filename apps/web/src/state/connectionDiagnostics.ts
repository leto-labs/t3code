import {
  connectionDiagnostics,
  type ConnectionDiagnosticsSnapshot,
} from "@t3tools/client-runtime/connection";
import type { EnvironmentId } from "@t3tools/contracts";
import { useSyncExternalStore } from "react";

export function useConnectionDiagnostics(
  environmentId: EnvironmentId | null,
): ConnectionDiagnosticsSnapshot {
  const id = environmentId ?? "";
  const subscribe = (listener: () => void) => connectionDiagnostics.subscribe(id, listener);
  const getSnapshot = () => connectionDiagnostics.snapshot(id);
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
