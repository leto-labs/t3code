import { RegistryContext } from "@effect/atom-react";
import { MessageId } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";

import { act, render } from "../../../test/render";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import type { ChatMessage } from "../../types";
import {
  pendingSendsAtom,
  updatePendingSends,
  usePendingSends,
  type ServerUserMessages,
} from "./optimisticUserMessages";

const NOT_LOADED = null;

function sent(id: string, text: string, extra: Partial<ChatMessage> = {}): ChatMessage {
  return {
    id: MessageId.make(id),
    role: "user",
    text,
    runId: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    streaming: false,
    ...extra,
  };
}

/** A thread view as ChatView shows it: the server's rows, then pending sends it does not show. */
function ThreadView(props: {
  readonly threadKey: string;
  readonly server: ServerUserMessages | null;
  readonly serverRows: ReadonlyArray<string>;
}) {
  const pending = usePendingSends(props.threadKey, props.server);
  const rows = [
    ...props.serverRows.map((text) => ({ key: `server:${text}`, text })),
    ...pending.messages
      .filter((message) => !props.server?.queued.has(message.id))
      .map((message) => ({ key: `pending:${message.id}`, text: message.text })),
  ];
  return (
    <ul>
      {rows.map((row) => (
        <li key={row.key}>{row.text}</li>
      ))}
    </ul>
  );
}

// ChatView remounts when the route changes thread, so each thread gets its own instance.
const view = (threadKey: string, server: ServerUserMessages | null, serverRows: string[] = []) => (
  <RegistryContext.Provider value={appAtomRegistry}>
    <ThreadView key={threadKey} threadKey={threadKey} server={server} serverRows={serverRows} />
  </RegistryContext.Provider>
);

it("keeps a send visible across a thread switch until the server's copy replaces it", async () => {
  const threadA = "environment:switch-a";
  const message = sent("message:switch", "Hello from thread A");
  await using screen = await render(view(threadA, NOT_LOADED));
  await act(() => updatePendingSends(threadA, (existing) => [...existing, message]));
  await expect.element(screen.getByText("Hello from thread A")).toBeVisible();

  await screen.rerender(view("environment:switch-b", NOT_LOADED));
  await expect.element(screen.getByText("Hello from thread A")).not.toBeInTheDocument();

  await screen.rerender(view(threadA, NOT_LOADED));
  await expect.element(screen.getByText("Hello from thread A")).toBeVisible();

  await screen.rerender(
    view(threadA, { committed: new Set([message.id]), queued: new Set() }, [message.text]),
  );
  expect(screen.getByText("Hello from thread A").elements()).toHaveLength(1);
  expect(appAtomRegistry.get(pendingSendsAtom(threadA)).messages).toEqual([]);
});

it("keeps a queued send visible across a thread switch and retires it once it leaves the queue", async () => {
  using revoke = vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});
  const threadA = "environment:queue-a";
  const message = sent("message:queued", "Queued in thread A", {
    inputIntent: "queued_turn",
    attachments: [
      {
        type: "image",
        id: "image:queued",
        name: "image.png",
        mimeType: "image/png",
        sizeBytes: 1,
        previewUrl: "blob:queued",
      },
    ],
  });
  const queuedOnServer = { committed: new Set<MessageId>(), queued: new Set([message.id]) };
  await using screen = await render(view(threadA, NOT_LOADED));
  await act(() => updatePendingSends(threadA, (existing) => [...existing, message]));
  await screen.rerender(view(threadA, queuedOnServer, [message.text]));
  expect(screen.getByText("Queued in thread A").elements()).toHaveLength(1);

  await screen.rerender(view("environment:queue-b", NOT_LOADED));
  await screen.rerender(view(threadA, NOT_LOADED));
  await expect.element(screen.getByText("Queued in thread A")).toBeVisible();

  // Cancelled from another client while this one was away.
  await screen.rerender(view(threadA, { committed: new Set(), queued: new Set() }));
  await expect.element(screen.getByText("Queued in thread A")).not.toBeInTheDocument();
  expect(appAtomRegistry.get(pendingSendsAtom(threadA)).messages).toEqual([]);
  expect(revoke.mock.calls.map(([url]) => url)).toEqual(["blob:queued"]);
});
