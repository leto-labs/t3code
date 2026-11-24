import type { OrchestrationV2AppThread } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

export function limitRecoveryTiming(
  reportedResetAt: string | null | undefined,
  failedAt: DateTime.Utc,
  isAutoPool: boolean,
) {
  const failureMs = DateTime.toEpochMillis(failedAt);
  const resetMs = Date.parse(reportedResetAt ?? "");
  if (reportedResetAt != null && Number.isFinite(resetMs) && resetMs > failureMs)
    return { resetAt: reportedResetAt, resetMs, reported: true };
  // Pool recovery without a usable reset still needs a stable identity across restarts.
  return isAutoPool
    ? { resetAt: DateTime.formatIso(failedAt), resetMs: failureMs, reported: false }
    : null;
}

export function limitRecoveryIsSnoozed(
  thread: Pick<OrchestrationV2AppThread, "snoozedUntil" | "limitRecovery">,
  nowMs: number,
  hasHeadroom: boolean,
) {
  if (thread.snoozedUntil == null) return false;
  const snoozedUntil = DateTime.toEpochMillis(thread.snoozedUntil);
  return (
    snoozedUntil > nowMs &&
    !(
      hasHeadroom &&
      thread.limitRecovery?.snooze &&
      Date.parse(thread.limitRecovery.resetAt) === snoozedUntil
    )
  );
}
