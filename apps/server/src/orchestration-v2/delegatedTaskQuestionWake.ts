import {
  CommandId,
  MessageId,
  type OrchestrationV2Command,
  type OrchestrationV2DelegatedTaskQuestion,
  type OrchestrationV2Subagent,
  type RuntimeRequestId,
} from "@t3tools/contracts";
import { reportLabel } from "@t3tools/provider-core/server/notification";

export const delegatedTaskQuestionMessageId = (
  task: Pick<OrchestrationV2Subagent, "id" | "threadId" | "remoteChild">,
  requestId: RuntimeRequestId,
) =>
  MessageId.make(
    task.remoteChild === undefined
      ? `message:delegated-task-question:${task.threadId}:${requestId}`
      : `message:delegated-task-question:${task.id}:${requestId}`,
  );

export function delegatedTaskQuestionWake(
  task: OrchestrationV2Subagent,
  question: OrchestrationV2DelegatedTaskQuestion,
): Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }> {
  const messageId = delegatedTaskQuestionMessageId(task, question.id);
  const label = reportLabel(task.title?.trim() || task.prompt);
  const situation = question.blocking
    ? `Delegated task ${task.id} is blocked on a question and cannot continue until it is answered.`
    : `Delegated task ${task.id} asked a question and is still working.`;
  const target =
    task.remoteChild === undefined
      ? `threadId ${task.childThreadId}`
      : `environmentId ${task.remoteChild.environmentId}, threadId ${task.remoteChild.threadId}`;
  return {
    type: "message.dispatch",
    commandId: CommandId.make(`server:${messageId.slice("message:".length)}`),
    threadId: task.threadId,
    messageId,
    text: `${situation} Read it with t3_pending_request action read, ${target}, and requestId ${question.id}. Answer it with action respond when the answer follows from the task or the user's instructions; otherwise ask the user, then answer it. If it is no longer pending, it was answered or dismissed.`,
    notification: {
      source: {
        kind: "delegated_task",
        taskIds: [task.id],
        ...(task.childThreadId === null ? {} : { childThreadId: task.childThreadId }),
      },
      outcome: "updated",
      summary: `Delegated task${label === undefined ? "" : ` "${label}"`} has a question`,
    },
    attachments: [],
    dispatchMode: { type: "queue_after_active" },
    createdBy: "agent",
    creationSource: "server",
  };
}
