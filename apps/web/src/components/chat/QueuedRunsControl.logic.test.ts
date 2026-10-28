import { MessageId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { selectOptimisticQueuedMessages } from "./QueuedRunsControl.logic";

describe("selectOptimisticQueuedMessages", () => {
  it("keeps a locally queued message until its queued run is projected", () => {
    const messageId = MessageId.make("message:queued-race");
    expect(
      selectOptimisticQueuedMessages(
        [
          { id: messageId, inputIntent: "queued_turn", text: "queued" },
          { id: MessageId.make("message:steer"), inputIntent: "steer", text: "steer" },
        ],
        new Set(),
      ),
    ).toEqual([{ id: messageId, inputIntent: "queued_turn", text: "queued" }]);
  });

  it("hands the row to the server queue once its run is visible", () => {
    const messageId = MessageId.make("message:queued");
    expect(
      selectOptimisticQueuedMessages(
        [{ id: messageId, inputIntent: "queued_turn", text: "queued" }],
        new Set([messageId]),
      ),
    ).toEqual([]);
  });
});
