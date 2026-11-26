import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import {
  MAX_REMOTE_DELEGATED_TASK_QUESTION_ID_LENGTH,
  MAX_REMOTE_DELEGATED_TASK_QUESTIONS,
} from "./orchestrationV2.ts";

import {
  OrchestratorMcpDelegateTaskInput,
  OrchestratorMcpDelegateTaskResult,
  OrchestratorMcpThreadWaitResult,
} from "./orchestratorMcp.ts";

const decodeDelegateTaskInput = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskInput);
const decodeDelegateTaskResult = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskResult);
const decodeThreadWaitResult = Schema.decodeUnknownSync(OrchestratorMcpThreadWaitResult);

describe("orchestrator MCP contracts", () => {
  it("decodes cross-provider delegated task requests and durable results", () => {
    const request = decodeDelegateTaskInput({
      task: "Inspect the workspace and report the result.",
      target: {
        providerInstanceId: "claudeAgent",
        model: "claude-sonnet-4-6",
      },
      mode: "wait",
      timeoutMs: 5_000,
      clientRequestId: "delegate-1",
      runtimeMode: "inherit",
      interactionMode: "inherit",
    });
    const result = decodeDelegateTaskResult({
      taskId: "node-task-1",
      childThreadId: "thread-child-1",
      childRunId: "run-child-1",
      childNodeId: "node-task-1",
      status: "completed",
      workState: "result_available",
      hasPendingChildRuns: false,
      providerInstanceId: "claudeAgent",
      model: "claude-sonnet-4-6",
      summary: "Workspace inspected.",
      resultContextTransferId: "context-transfer-result-1",
      latestTerminalRunId: "run-child-1",
      latestTerminalStatus: "completed",
      latestTerminalSummary: "Workspace inspected.",
      latestTerminalResultContextTransferId: "context-transfer-result-1",
      hasPendingQuestion: false,
      pendingApproval: null,
      waitTimedOut: false,
    });

    expect(request.target?.providerInstanceId).toBe("claudeAgent");
    expect(result.status).toBe("completed");
    expect(result.summary).toBe("Workspace inspected.");
    expect(result.pendingApproval).toBeNull();
    const pendingApproval = {
      requestId: "request-approval-1",
      kind: "command",
      runId: "run-child-1",
    };
    expect(
      decodeDelegateTaskResult({
        ...result,
        pendingApproval: { ...pendingApproval, input: { command: "private command" } },
      }).pendingApproval,
    ).toEqual(pendingApproval);
    expect(() =>
      decodeDelegateTaskResult({
        ...result,
        pendingApproval: { ...pendingApproval, kind: "user_input" },
      }),
    ).toThrow();
  });

  it("decodes a running-task next written by another version", () => {
    const next = "Completion wakes this thread, so end your turn. Do not wait on it or poll it.";
    expect(
      decodeThreadWaitResult({
        threadId: "thread-child-1",
        runId: "run-child-1",
        status: "running",
        timedOut: true,
        next,
      }).next,
    ).toBe(next);
  });

  it("rejects oversized remote question updates", () => {
    const result = {
      threadId: "thread-child-1",
      runId: "run-child-1",
      status: "waiting",
      timedOut: false,
    };
    const questions = Array.from({ length: MAX_REMOTE_DELEGATED_TASK_QUESTIONS }, (_, index) => ({
      id: `request-${index}`,
      blocking: true,
    }));
    expect(
      decodeThreadWaitResult({ ...result, pendingQuestions: questions }).pendingQuestions,
    ).toEqual(questions);
    expect(() =>
      decodeThreadWaitResult({
        ...result,
        pendingQuestions: [...questions, { id: "extra", blocking: true }],
      }),
    ).toThrow();
    expect(() =>
      decodeThreadWaitResult({
        ...result,
        pendingQuestions: [
          { id: "x".repeat(MAX_REMOTE_DELEGATED_TASK_QUESTION_ID_LENGTH + 1), blocking: true },
        ],
      }),
    ).toThrow();
  });
});
