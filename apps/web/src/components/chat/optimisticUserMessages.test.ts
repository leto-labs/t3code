import { MessageId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";

import { appAtomRegistry } from "../../rpc/atomRegistry";
import type { ChatMessage } from "../../types";
import {
  handoffPreloadsAtom,
  pendingSendsAtom,
  reconcilePendingSends,
  serverUserMessages,
  updatePendingSends,
  type PendingSends,
} from "./optimisticUserMessages";

function message(id: string, previewUrl?: string): ChatMessage {
  return {
    id: MessageId.make(id),
    role: "user",
    text: id,
    runId: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    streaming: false,
    inputIntent: "queued_turn",
    ...(previewUrl
      ? {
          attachments: [
            {
              type: "image",
              id: `${id}-image`,
              name: "image.png",
              mimeType: "image/png",
              sizeBytes: 1,
              previewUrl,
            },
          ],
        }
      : {}),
  } as ChatMessage;
}

const ids = (...values: string[]) => new Set(values.map((value) => MessageId.make(value)));
const pending = (...messages: ChatMessage[]): PendingSends => ({
  messages,
  queued: new Set(),
  handoff: {},
});
// The fields getUserQueuedThreadRuns reads.
const projection = (input: { messages?: string[]; queuedRuns?: string[] }) =>
  ({
    messages: (input.messages ?? []).map((id) => ({ id: MessageId.make(id) })),
    runs: (input.queuedRuns ?? []).map((id) => ({
      status: "queued",
      userMessageId: MessageId.make(id),
    })),
  }) as never;

describe("pending sends", () => {
  it("are acknowledged by a user_message turn item or a queued run, not a projected message", () => {
    const sent = message("m1", "blob:m1");
    const state = pending(sent);

    const messageOnly = serverUserMessages(projection({ messages: ["m1"] }), ids());
    expect(reconcilePendingSends(state, messageOnly)).toBe(state);

    const queued = reconcilePendingSends(
      state,
      serverUserMessages(projection({ messages: ["m1"], queuedRuns: ["m1"] }), ids()),
    );
    expect(queued.messages).toEqual([sent]);
    expect(queued.queued).toEqual(ids("m1"));

    const committed = reconcilePendingSends(queued, serverUserMessages(projection({}), ids("m1")));
    expect(committed.messages).toEqual([]);
    expect(committed.handoff).toEqual({ m1: ["blob:m1"] });
  });

  it("keep a send while the projection is not loaded, and retire a queued one that left the queue", () => {
    const state = reconcilePendingSends(
      pending(message("m1")),
      serverUserMessages(projection({ queuedRuns: ["m1"] }), ids()),
    );
    expect(reconcilePendingSends(state, null)).toBe(state);

    const cancelled = reconcilePendingSends(state, serverUserMessages(projection({}), ids()));
    expect(cancelled.messages).toEqual([]);
    expect(cancelled.handoff).toEqual({});
  });

  it("revoke a failed send's previews when it is removed", () => {
    using revoke = vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});
    const threadKey = "environment:failed-send";
    const sent = message("m1", "blob:m1");
    updatePendingSends(threadKey, (existing) => [...existing, sent]);
    expect(revoke).not.toHaveBeenCalled();

    updatePendingSends(threadKey, (existing) => existing.filter((entry) => entry.id !== sent.id));
    expect(appAtomRegistry.get(pendingSendsAtom(threadKey)).messages).toEqual([]);
    expect(revoke.mock.calls.map(([url]) => url)).toEqual(["blob:m1"]);
  });
});

class FakeImage {
  static readonly created: FakeImage[] = [];
  src = "";
  private readonly listeners = new Map<string, () => void>();
  constructor() {
    FakeImage.created.push(this);
  }
  addEventListener(type: string, listener: () => void) {
    this.listeners.set(type, listener);
  }
  fire(type: "load" | "error") {
    this.listeners.get(type)?.();
  }
}

describe("handoff preloads", () => {
  it("end a handoff, and revoke its blobs, once every server preview loads", async () => {
    vi.stubGlobal("Image", FakeImage);
    try {
      using revoke = vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => undefined);
      FakeImage.created.length = 0;
      const threadKey = "environment:handoff";
      appAtomRegistry.set(pendingSendsAtom(threadKey), {
        messages: [],
        queued: new Set<MessageId>(),
        handoff: { "m-loaded": ["blob:a", "blob:b"], "m-failed": ["blob:c"] },
      });
      const release = appAtomRegistry.mount(
        handoffPreloadsAtom(
          JSON.stringify([
            [threadKey, "m-loaded", ["https://server/a.png", "https://server/b.png"]],
            [threadKey, "m-failed", ["https://server/c.png"]],
          ]),
        ),
      );
      const image = (src: string) => FakeImage.created.find((candidate) => candidate.src === src);
      const handoff = () => appAtomRegistry.get(pendingSendsAtom(threadKey)).handoff;

      image("https://server/a.png")?.fire("load");
      image("https://server/c.png")?.fire("error");
      await Promise.resolve();
      expect(Object.keys(handoff())).toEqual(["m-loaded", "m-failed"]);

      image("https://server/b.png")?.fire("load");
      await vi.waitFor(() => expect(handoff()).toEqual({ "m-failed": ["blob:c"] }));
      expect(revoke.mock.calls.map(([url]) => url)).toEqual(["blob:a", "blob:b"]);
      release();
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
