import * as Schema from "effect/Schema";

import {
  CommandId,
  MessageId,
  ThreadId,
  TrimmedNonEmptyString,
  TurnItemId,
} from "./baseSchemas.ts";
import { ModelSelection } from "./modelSelection.ts";
import { ProviderInteractionMode, RuntimeMode } from "./providerPolicy.ts";

export const LIVE_VOICE_NAMES = [
  "alloy",
  "arbor",
  "ash",
  "aube",
  "ballad",
  "breeze",
  "bubbie",
  "cedar",
  "coral",
  "cove",
  "echo",
  "ember",
  "haetsal",
  "hanul",
  "himari",
  "juniper",
  "leher",
  "maple",
  "marin",
  "miko",
  "neer",
  "porto",
  "rindo",
  "rio",
  "rivage",
  "sage",
  "selva",
  "shimmer",
  "sol",
  "sonna",
  "spruce",
  "tinta",
  "vale",
  "verse",
  "viola",
] as const;
export const LiveVoiceName = Schema.Literals(LIVE_VOICE_NAMES);
export type LiveVoiceName = typeof LiveVoiceName.Type;
export const DEFAULT_LIVE_VOICE: LiveVoiceName = "cove";

export class LiveVoiceRpcError extends Schema.TaggedError<LiveVoiceRpcError>()(
  "LiveVoiceRpcError",
  {
    threadId: ThreadId,
    message: Schema.String,
  },
) {}

export const LiveVoiceThreadInput = Schema.Struct({ threadId: ThreadId });
export type LiveVoiceThreadInput = typeof LiveVoiceThreadInput.Type;

/** Run settings for handoffs; the realtime conversation itself stays on Codex. */
export const LiveVoiceHandoffConfig = Schema.Struct({
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
});
export type LiveVoiceHandoffConfig = typeof LiveVoiceHandoffConfig.Type;

export const LiveVoiceStartInput = Schema.Struct({
  threadId: ThreadId,
  // SDP is line-oriented and the provider's parser needs the final CRLF, so a
  // trimming schema would corrupt the offer.
  offerSdp: Schema.String.check(Schema.isNonEmpty()),
  voice: Schema.optional(LiveVoiceName),
  handoffConfig: Schema.optional(LiveVoiceHandoffConfig),
});
export type LiveVoiceStartInput = typeof LiveVoiceStartInput.Type;

/** Applies to the next handoff; a running handoff keeps the settings it started with. */
export const LiveVoiceConfigureInput = Schema.Struct({
  threadId: ThreadId,
  handoffConfig: LiveVoiceHandoffConfig,
});
export type LiveVoiceConfigureInput = typeof LiveVoiceConfigureInput.Type;

export const LiveVoiceStartResult = Schema.Struct({
  callId: TrimmedNonEmptyString,
  answerSdp: Schema.String,
});
export type LiveVoiceStartResult = typeof LiveVoiceStartResult.Type;

export const LiveVoiceCallStatus = Schema.Literals(["starting", "active", "stopping", "closed"]);
export type LiveVoiceCallStatus = typeof LiveVoiceCallStatus.Type;

const LiveVoiceHandoffFields = {
  handoffId: TrimmedNonEmptyString,
  /** A check-in reports finished background work that the call did not request. */
  kind: Schema.Literals(["handoff", "checkin"]),
  /** Progress while the run works, then the result the call should speak. */
  text: Schema.String,
  done: Schema.Boolean,
};

export const LiveVoiceCallSnapshot = Schema.Struct({
  threadId: ThreadId,
  callId: TrimmedNonEmptyString,
  status: LiveVoiceCallStatus,
  /** Latest update per handoff, so a reconnecting client can still deliver results. */
  handoffs: Schema.Array(Schema.Struct(LiveVoiceHandoffFields)),
});
export type LiveVoiceCallSnapshot = typeof LiveVoiceCallSnapshot.Type;

/** The first event of every subscription is a snapshot of the environment's calls. */
export const LiveVoiceEvent = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("snapshot"),
    calls: Schema.Array(LiveVoiceCallSnapshot),
  }),
  Schema.Struct({
    type: Schema.Literal("status"),
    threadId: ThreadId,
    callId: TrimmedNonEmptyString,
    status: LiveVoiceCallStatus,
    error: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("handoff"),
    threadId: ThreadId,
    callId: TrimmedNonEmptyString,
    ...LiveVoiceHandoffFields,
  }),
  /** Partial phrases are live only; a finished phrase names the row it was saved as. */
  Schema.Struct({
    type: Schema.Literal("transcript"),
    threadId: ThreadId,
    callId: TrimmedNonEmptyString,
    role: Schema.Literals(["user", "assistant"]),
    text: Schema.String,
    done: Schema.Boolean,
    messageId: Schema.optional(MessageId),
  }),
]);
export type LiveVoiceEvent = typeof LiveVoiceEvent.Type;

const TRANSCRIPT_ID_PREFIX = "voice:";
const HANDOFF_MESSAGE_ID_PREFIX = "voice-handoff:";

/** Ids for one finalized transcript row; a per-call sequence keeps repeated phrases distinct. */
export function liveVoiceTranscriptIds(callId: string, sequence: number) {
  const id = `${TRANSCRIPT_ID_PREFIX}${callId}:${sequence}`;
  return {
    commandId: CommandId.make(id),
    messageId: MessageId.make(id),
    turnItemId: TurnItemId.make(id),
  };
}

export const isLiveVoiceTranscriptItemId = (id: string): boolean =>
  id.startsWith(TRANSCRIPT_ID_PREFIX);

export const liveVoiceHandoffMessageId = (threadId: ThreadId, handoffId: string): MessageId =>
  MessageId.make(`${HANDOFF_MESSAGE_ID_PREFIX}${threadId}:${handoffId}`);

/** The spoken request is already a transcript row, so the run's copy stays hidden. */
export const isLiveVoiceHandoffMessageId = (id: string): boolean =>
  id.startsWith(HANDOFF_MESSAGE_ID_PREFIX);
