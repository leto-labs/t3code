import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { ThreadId } from "./baseSchemas.ts";
import {
  isLiveVoiceTranscriptItemId,
  liveVoiceTranscriptIds,
  LiveVoiceStartInput,
} from "./liveVoice.ts";

const encodeStartInput = Schema.encodeSync(LiveVoiceStartInput);
const decodeStartInput = Schema.decodeSync(LiveVoiceStartInput);

describe("LiveVoiceStartInput", () => {
  it("keeps the SDP line terminator through a wire round trip", () => {
    const offerSdp = "v=0\r\na=setup:actpass\r\n";
    const wire = encodeStartInput({
      threadId: ThreadId.make("thread-1"),
      offerSdp,
    });
    expect(decodeStartInput(wire).offerSdp).toBe(offerSdp);
  });

  it("rejects an empty offer", () => {
    expect(() => decodeStartInput({ threadId: "thread-1", offerSdp: "" })).toThrow();
  });
});

describe("live voice ids", () => {
  it("gives each transcript row of a call its own ids", () => {
    const first = liveVoiceTranscriptIds("call-1", 1);
    const second = liveVoiceTranscriptIds("call-1", 2);
    expect(first.messageId).not.toBe(second.messageId);
    expect(first.commandId).not.toBe(liveVoiceTranscriptIds("call-2", 1).commandId);
    expect(isLiveVoiceTranscriptItemId(first.turnItemId)).toBe(true);
    expect(isLiveVoiceTranscriptItemId("turn-item-1")).toBe(false);
  });
});
