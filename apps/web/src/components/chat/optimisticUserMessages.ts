import { useAtomValue } from "@effect/atom-react";
import { getUserQueuedThreadRuns } from "@t3tools/client-runtime/state/thread-workflows";
import type { MessageId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Atom } from "effect/reactivity";
import { type SetStateAction, useEffect } from "react";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import type { ChatMessage } from "../../types";
import { collectUserMessageBlobPreviewUrls } from "../ChatView.logic";

/**
 * Sends a thread shows before the server does, kept per scoped thread key so they outlive ChatView
 * remounts and thread switches. The store owns their blob previews: a preview is revoked when no
 * pending message or handoff references it any more.
 */
export interface PendingSends {
  readonly messages: ChatMessage[];
  /** Sends the server's queue has shown; one that leaves the queue without a turn item retires. */
  readonly queued: ReadonlySet<MessageId>;
  /** Blob previews of committed sends, by message id, shown until the server's copies load. */
  readonly handoff: Readonly<Record<string, ReadonlyArray<string>>>;
}

/** What a loaded server projection shows of the thread's user messages. */
export interface ServerUserMessages {
  /** Messages with a user_message turn item. */
  readonly committed: ReadonlySet<MessageId>;
  /** Messages with a user-queued run. */
  readonly queued: ReadonlySet<MessageId>;
}

const EMPTY_PENDING_SENDS: PendingSends = { messages: [], queued: new Set(), handoff: {} };

export const pendingSendsAtom = Atom.family((threadKey: string) =>
  Atom.make(EMPTY_PENDING_SENDS).pipe(Atom.keepAlive, Atom.withLabel(`pending-sends:${threadKey}`)),
);

/**
 * A server row in `projection.messages` alone does not acknowledge a send: queued messages land
 * there before their run, so only a turn item or a queued run replaces the pending row.
 */
export function serverUserMessages(
  projection: Parameters<typeof getUserQueuedThreadRuns>[0] | null,
  committed: ReadonlySet<MessageId>,
): ServerUserMessages | null {
  if (projection === null) return null;
  return {
    committed,
    queued: new Set(getUserQueuedThreadRuns(projection).map((run) => run.userMessageId)),
  };
}

/** Returns `state` when the server view changes nothing. */
export function reconcilePendingSends(
  state: PendingSends,
  server: ServerUserMessages | null,
): PendingSends {
  if (server === null || state.messages.length === 0) return state;
  const retires = (message: ChatMessage) =>
    server.committed.has(message.id) ||
    (state.queued.has(message.id) && !server.queued.has(message.id));
  const changed = state.messages.some(
    (message) =>
      retires(message) || (server.queued.has(message.id) && !state.queued.has(message.id)),
  );
  if (!changed) return state;
  const handoff = { ...state.handoff };
  for (const message of state.messages) {
    if (!server.committed.has(message.id)) continue;
    const previewUrls = collectUserMessageBlobPreviewUrls(message);
    if (previewUrls.length > 0) handoff[message.id] = previewUrls;
  }
  const messages = state.messages.filter((message) => !retires(message));
  const queued = new Set(
    messages.flatMap((message) => (server.queued.has(message.id) ? [message.id] : [])),
  );
  return { messages, queued, handoff };
}

function previewUrlsOf(state: PendingSends): string[] {
  return [
    ...state.messages.flatMap(collectUserMessageBlobPreviewUrls),
    ...Object.values(state.handoff).flat(),
  ];
}

function writePendingSends(threadKey: string, update: (state: PendingSends) => PendingSends) {
  const atom = pendingSendsAtom(threadKey);
  const current = appAtomRegistry.get(atom);
  const next = update(current);
  if (next === current) return;
  appAtomRegistry.set(atom, next);
  const kept = new Set(previewUrlsOf(next));
  for (const previewUrl of previewUrlsOf(current)) {
    if (!kept.has(previewUrl)) URL.revokeObjectURL(previewUrl);
  }
}

/** A send-path update; a message it removes, such as a failed send, releases its previews. */
export function updatePendingSends(threadKey: string, action: SetStateAction<ChatMessage[]>) {
  writePendingSends(threadKey, (state) => {
    const messages = typeof action === "function" ? action(state.messages) : action;
    return messages === state.messages ? state : { ...state, messages };
  });
}

function dropPendingHandoff(threadKey: string, messageId: MessageId) {
  writePendingSends(threadKey, (state) => {
    if (!(messageId in state.handoff)) return state;
    const { [messageId]: _released, ...handoff } = state.handoff;
    return { ...state, handoff };
  });
}

/** The thread's pending sends as of `server`; acknowledged sends leave in the render it shows them. */
export function usePendingSends(threadKey: string, server: ServerUserMessages | null) {
  const stored = useAtomValue(pendingSendsAtom(threadKey));
  const state = reconcilePendingSends(stored, server);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- persists what the server showed into the store that outlives this view
  useEffect(() => {
    if (state !== stored) {
      writePendingSends(threadKey, (current) => reconcilePendingSends(current, server));
    }
  }, [threadKey, server, state, stored]);
  return state;
}

// Server asset URLs carry a signed token, so the error keeps only where the image was loaded from.
class ImagePreloadError extends Schema.TaggedError<ImagePreloadError>()("ImagePreloadError", {
  protocol: Schema.String,
  host: Schema.String,
}) {
  static fromUrl(url: string) {
    const parsed = URL.parse(url, globalThis.location?.href);
    return new ImagePreloadError({ protocol: parsed?.protocol ?? "", host: parsed?.host ?? "" });
  }
}

const preloadImage = Effect.fn("optimisticUserMessages.preloadImage")(function* (url: string) {
  yield* Effect.callback<void, ImagePreloadError>((resume) => {
    const image = new Image();
    image.addEventListener("load", () => resume(Effect.void), { once: true });
    image.addEventListener("error", () => resume(Effect.fail(ImagePreloadError.fromUrl(url))), {
      once: true,
    });
    image.src = url;
    return Effect.sync(() => {
      image.src = "";
    });
  });
});

/**
 * Loads one handoff's server previews, keyed by `[threadKey, messageId, serverUrls]`. Once they
 * have all loaded the handoff ends, which revokes its blobs. A failed load keeps the handoff.
 */
const handoffPreloadAtom = Atom.family((key: string) => {
  const [threadKey, messageId, serverUrls] = JSON.parse(key) as [string, MessageId, string[]];
  return Atom.make(
    typeof Image === "undefined"
      ? Effect.never
      : Effect.forEach(serverUrls, preloadImage, { concurrency: "unbounded", discard: true }).pipe(
          Effect.tap(() => Effect.sync(() => dropPendingHandoff(threadKey, messageId))),
        ),
  );
});

/**
 * Mounts a preload per `[threadKey, messageId, serverUrls]` entry. Each runs in its own atom, so a
 * change to the set does not restart the rest.
 */
export const handoffPreloadsAtom = Atom.family((key: string) => {
  const preloads = JSON.parse(key) as Array<[string, MessageId, string[]]>;
  return Atom.make((get) => {
    for (const preload of preloads) get.mount(handoffPreloadAtom(JSON.stringify(preload)));
  });
});
