import { type EnvironmentId, type MessageId } from "@t3tools/contracts";
import { useAtomMount } from "@effect/atom-react";
import { selectHandoffImageResources } from "../../session-logic";
import type { useQuietThreadVisibleTurnItems } from "../../state/entities";
import { useAssetUrls } from "../../assets/assetUrls";
import { handoffPreloadsAtom, type PendingSends } from "./optimisticUserMessages";

interface AttachmentPreviewHandoffInput {
  readonly environmentId: EnvironmentId;
  readonly threadKey: string;
  readonly serverVisibleTurnItems: ReturnType<typeof useQuietThreadVisibleTurnItems>;
  /** Blob previews of acknowledged messages, by message id; they stand in for the server's copy. */
  readonly handoff: PendingSends["handoff"];
}

/**
 * A sent image keeps showing its local blob preview until the server's copy has loaded, so the
 * timeline does not flash an empty frame. The handoff ends, and its blobs are revoked, once the
 * server previews load.
 */
export function useAttachmentPreviewHandoff(input: AttachmentPreviewHandoffInput) {
  const { environmentId, threadKey, serverVisibleTurnItems } = input;
  const handoffByMessageId = input.handoff;

  const serverAttachmentResources = selectHandoffImageResources(
    Object.keys(handoffByMessageId).length === 0
      ? undefined
      : serverVisibleTurnItems.flatMap(({ item }) =>
          item.type === "user_message"
            ? [{ id: item.messageId, role: "user" as const, attachments: item.attachments }]
            : [],
        ),
    handoffByMessageId,
  );
  const serverAttachmentUrls = useAssetUrls(environmentId, serverAttachmentResources);
  const serverAttachmentUrlById = new Map(
    serverAttachmentResources.flatMap((resource, index) => {
      const url = serverAttachmentUrls[index];
      return url ? [[resource.attachmentId, url] as const] : [];
    }),
  );

  // Each handoff whose server previews are all minted, as [threadKey, messageId, serverUrls].
  const pendingPreloads: Array<readonly [string, MessageId, ReadonlyArray<string>]> = [];
  for (const row of serverVisibleTurnItems) {
    if (row.item.type !== "user_message") continue;
    const handoffUrls = handoffByMessageId[row.item.messageId];
    if (handoffUrls === undefined || row.item.attachments.length === 0) continue;
    const serverPreviewUrls = row.item.attachments.flatMap((attachment) => {
      if (attachment.type !== "image") return [];
      const url = serverAttachmentUrlById.get(attachment.id);
      return url === undefined ? [] : [url];
    });
    if (
      serverPreviewUrls.length === 0 ||
      serverPreviewUrls.length !== handoffUrls.length ||
      serverPreviewUrls.some((previewUrl) => previewUrl.startsWith("blob:"))
    ) {
      continue;
    }
    pendingPreloads.push([threadKey, row.item.messageId, serverPreviewUrls]);
  }
  useAtomMount(handoffPreloadsAtom(JSON.stringify(pendingPreloads)));

  const timelineAttachmentUrlById = (() => {
    const urls = new Map(serverAttachmentUrlById);
    for (const row of serverVisibleTurnItems) {
      if (row.item.type !== "user_message") continue;
      const handoffUrls = handoffByMessageId[row.item.messageId];
      if (handoffUrls === undefined) continue;
      let imageIndex = 0;
      for (const attachment of row.item.attachments) {
        if (attachment.type !== "image") continue;
        const handoffUrl = handoffUrls[imageIndex];
        imageIndex += 1;
        if (handoffUrl !== undefined) urls.set(attachment.id, handoffUrl);
      }
    }
    return urls;
  })();
  return { timelineAttachmentUrlById };
}
