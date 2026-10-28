import type { ChatMessage } from "../../types";

export function selectOptimisticQueuedMessages(
  messages: ReadonlyArray<Pick<ChatMessage, "id" | "inputIntent" | "text" | "attachments">>,
  projectedQueuedMessageIds: ReadonlySet<string>,
) {
  return messages.filter(
    (message) =>
      message.inputIntent === "queued_turn" && !projectedQueuedMessageIds.has(message.id),
  );
}
