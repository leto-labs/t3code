import { AuthOrchestrationOperateScope, type AuthSessionState } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import { describe, expect, it } from "vite-plus/test";

import { sessionHasScope } from "./session";

describe("sessionHasScope", () => {
  it("retains a cached grant while session refresh fails", () => {
    const granted = {
      authenticated: true,
      scopes: [AuthOrchestrationOperateScope],
      auth: {
        policy: "remote-reachable",
        bootstrapMethods: ["one-time-token"],
        sessionMethods: ["browser-session-cookie"],
        sessionCookieName: "t3_session",
      },
    } satisfies AuthSessionState;

    const failedRefresh = AsyncResult.failureWithPrevious(
      Cause.fail(new Error("SocketReadError: connection lost")),
      { previous: Option.some(AsyncResult.success(granted)) },
    );

    expect(sessionHasScope(failedRefresh, AuthOrchestrationOperateScope)).toBe(true);
  });

  it("drops a grant when the successful session response revokes it", () => {
    const revoked = {
      authenticated: true,
      scopes: [],
      auth: {
        policy: "remote-reachable",
        bootstrapMethods: ["one-time-token"],
        sessionMethods: ["browser-session-cookie"],
        sessionCookieName: "t3_session",
      },
    } satisfies AuthSessionState;

    expect(sessionHasScope(AsyncResult.success(revoked), AuthOrchestrationOperateScope)).toBe(
      false,
    );
  });

  it("does not retain a grant for an authentication failure", () => {
    const granted = {
      authenticated: true,
      scopes: [AuthOrchestrationOperateScope],
      auth: {
        policy: "remote-reachable",
        bootstrapMethods: ["one-time-token"],
        sessionMethods: ["browser-session-cookie"],
        sessionCookieName: "t3_session",
      },
    } satisfies AuthSessionState;
    const failedRefresh = AsyncResult.failureWithPrevious(Cause.fail(new Error("Unauthorized")), {
      previous: Option.some(AsyncResult.success(granted)),
    });

    expect(sessionHasScope(failedRefresh, AuthOrchestrationOperateScope)).toBe(false);
  });
});
