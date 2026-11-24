import type { OrchestrationV2AppThread } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

const MAX_AUTO_POOL_FAILURE_AGE_MS = 8 * 24 * 60 * 60 * 1000;
export const autoPoolRecoveryCutoff = (nowMs: number) => nowMs - MAX_AUTO_POOL_FAILURE_AGE_MS;

export function limitRecoveryTiming(
  reportedResetAt: string | null | undefined,
  failedAt: DateTime.Utc,
  isAutoPool: boolean,
) {
  const failedAtMs = DateTime.toEpochMillis(failedAt);
  const resetMs = Date.parse(reportedResetAt ?? "");
  if (reportedResetAt != null && Number.isFinite(resetMs) && resetMs > failedAtMs)
    return { resetAt: reportedResetAt, resetMs, failedAtMs, reported: true };
  // Pool recovery without a usable reset still needs a stable identity across restarts.
  return isAutoPool
    ? { resetAt: DateTime.formatIso(failedAt), resetMs: failedAtMs, failedAtMs, reported: false }
    : null;
}

export function limitRecoveryDeliveryMode(
  timing: NonNullable<ReturnType<typeof limitRecoveryTiming>>,
  nowMs: number,
  poolCanRetryEarly?: boolean,
): "reset" | "headroom" | "expired" | null {
  if (poolCanRetryEarly !== undefined && timing.resetMs < autoPoolRecoveryCutoff(nowMs))
    return "expired";
  if (timing.reported && timing.resetMs <= nowMs) return "reset";
  return poolCanRetryEarly === true ? "headroom" : null;
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
