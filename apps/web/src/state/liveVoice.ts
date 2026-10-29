import {
  type EnvironmentId,
  MessageId,
  type ScopedThreadRef,
  type ThreadId,
} from "@t3tools/contracts";
import {
  createLiveVoiceRegistry,
  liveVoiceKey,
  type LiveVoiceCallView,
  type LiveVoicePeerEvents,
  type LiveVoicePlatform,
  type LiveVoiceServerCall,
  type LiveVoiceTarget,
} from "@t3tools/client-runtime/live-voice";
import { createLiveVoiceEnvironmentAtoms } from "@t3tools/client-runtime/state/live-voice";
import {
  runAtomCommand,
  squashAtomCommandFailure,
  type AtomCommand,
} from "@t3tools/client-runtime/state/runtime";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import { useSyncExternalStore } from "react";

import { toastManager } from "../components/ui/toast";
import { connectionAtomRuntime } from "../connection/runtime";
import { appAtomRegistry } from "../rpc/atomRegistry";
import type { ChatMessage } from "../types";
import { environmentPresentations } from "./presentation";

const liveVoiceEnvironment = createLiveVoiceEnvironmentAtoms(connectionAtomRuntime);

const ICE_GATHERING_LIMIT_MS = 2_500;
const RECONNECT_WINDOW_MS = 10_000;
const HOLD_BUFFER_LIMIT_BYTES = 8 * 1024 * 1024;
const HOLD_RECORDING_SLICE_MS = 500;
const SAMPLE_INTERVAL_MS = 250;
const REMOTE_SPEECH_LEVEL = 0.02;
const METER_STEPS = 5;

async function run<W, A, E>(command: AtomCommand<W, A, E>, value: W): Promise<A> {
  const result = await runAtomCommand(appAtomRegistry, command, value, { reportFailure: false });
  if (result._tag === "Failure") throw squashAtomCommandFailure(result);
  return result.value;
}

export function liveVoiceErrorMessage(error: unknown): string {
  if (error instanceof DOMException) {
    if (error.name === "NotAllowedError")
      return "Microphone access was denied. Allow it for this site and try again.";
    if (error.name === "NotFoundError") return "No microphone was found.";
    if (error.name === "NotReadableError") return "The microphone is in use by another app.";
  }
  if (typeof error === "object" && error !== null && "message" in error) {
    return String(error.message);
  }
  return "Live voice could not start.";
}

/** Samples a stream's loudness (0–1) until the returned function runs. */
function sampleLevel(stream: MediaStream, onSample: (level: number) => void): () => void {
  if (typeof AudioContext === "undefined") return () => undefined;
  const context = new AudioContext();
  const analyser = context.createAnalyser();
  analyser.fftSize = 256;
  context.createMediaStreamSource(stream).connect(analyser);
  const samples = new Uint8Array(analyser.fftSize);
  const interval = window.setInterval(() => {
    analyser.getByteTimeDomainData(samples);
    let sum = 0;
    for (const sample of samples) sum += ((sample - 128) / 128) ** 2;
    onSample(Math.sqrt(sum / samples.length));
  }, SAMPLE_INTERVAL_MS);
  return () => {
    window.clearInterval(interval);
    void context.close();
  };
}

function pickRecordingType(): string | undefined {
  return ["audio/webm;codecs=opus", "audio/ogg;codecs=opus", "audio/webm"].find(
    (type) =>
      typeof MediaRecorder.isTypeSupported === "function" && MediaRecorder.isTypeSupported(type),
  );
}

function createPeer(events: LiveVoicePeerEvents) {
  const connection = new RTCPeerConnection({
    iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
  });
  const audio = new Audio();
  audio.autoplay = true;
  const pending: string[] = [];
  let channel = connection.createDataChannel("oai-events");
  let remote: MediaStream | null = null;
  let held = false;
  let recorder: MediaRecorder | null = null;
  let chunks: Blob[] = [];
  let bufferedBytes = 0;
  let spokeWhileHeld = false;
  let stopWatching: (() => void) | null = null;
  let reconnectTimer: number | undefined;

  const flush = () => {
    while (channel.readyState === "open" && pending.length > 0) channel.send(pending.shift()!);
  };
  channel.addEventListener("open", flush);
  connection.addEventListener("datachannel", (event) => {
    if (event.channel.label !== "oai-events") return;
    channel = event.channel;
    channel.addEventListener("open", flush);
    flush();
  });

  const startRecording = (): boolean => {
    if (typeof MediaRecorder === "undefined") return false;
    if (remote === null || recorder !== null) return true;
    try {
      const mimeType = pickRecordingType();
      recorder = mimeType ? new MediaRecorder(remote, { mimeType }) : new MediaRecorder(remote);
    } catch {
      return false;
    }
    recorder.addEventListener("dataavailable", (event) => {
      // Keep the start of the recording: later chunks cannot be decoded without it.
      if (event.data.size === 0 || bufferedBytes + event.data.size > HOLD_BUFFER_LIMIT_BYTES)
        return;
      chunks.push(event.data);
      bufferedBytes += event.data.size;
    });
    recorder.start(HOLD_RECORDING_SLICE_MS);
    return true;
  };

  const watchForSpeech = () => {
    if (remote === null || stopWatching !== null || spokeWhileHeld) return;
    stopWatching = sampleLevel(remote, (level) => {
      if (level < REMOTE_SPEECH_LEVEL) return;
      spokeWhileHeld = true;
      stopWatching?.();
      stopWatching = null;
      events.onResponseWaiting();
    });
  };

  const playLive = () => {
    audio.removeAttribute("src");
    audio.srcObject = remote;
    audio.muted = held;
    void audio.play().catch(() => undefined);
  };

  const replay = async (blobs: Blob[]) => {
    const url = URL.createObjectURL(new Blob(blobs, { type: blobs[0]!.type || "audio/webm" }));
    audio.srcObject = null;
    audio.src = url;
    audio.muted = false;
    try {
      await new Promise<void>((resolve) => {
        audio.addEventListener("ended", () => resolve(), { once: true });
        audio.addEventListener("error", () => resolve(), { once: true });
        audio.play().catch(() => resolve());
      });
    } finally {
      URL.revokeObjectURL(url);
    }
  };

  connection.addEventListener("track", (event) => {
    // Some answers carry no stream ids, so the track arrives without a stream.
    if (event.streams[0] !== undefined) remote = event.streams[0];
    else if (remote === null) remote = new MediaStream([event.track]);
    else remote.addTrack(event.track);
    if (held) {
      startRecording();
      watchForSpeech();
    }
    playLive();
  });
  connection.addEventListener("connectionstatechange", () => {
    const state = connection.connectionState;
    if (state === "connected") {
      window.clearTimeout(reconnectTimer);
      events.onReconnecting(false);
    } else if (state === "disconnected") {
      events.onReconnecting(true);
      window.clearTimeout(reconnectTimer);
      reconnectTimer = window.setTimeout(events.onFailed, RECONNECT_WINDOW_MS);
    } else if (state === "failed") {
      events.onFailed();
    }
  });

  return {
    addMedia: (stream: MediaStream) => {
      for (const track of stream.getAudioTracks()) connection.addTrack(track, stream);
    },
    createOffer: () => connection.createOffer(),
    setLocalDescription: (offer: { readonly sdp?: string | null | undefined }) =>
      connection.setLocalDescription({ type: "offer", ...(offer.sdp ? { sdp: offer.sdp } : {}) }),
    waitForIceGathering: () =>
      new Promise<void>((resolve) => {
        if (connection.iceGatheringState === "complete") return resolve();
        const done = () => {
          window.clearTimeout(timer);
          connection.removeEventListener("icegatheringstatechange", onChange);
          resolve();
        };
        const onChange = () => {
          if (connection.iceGatheringState === "complete") done();
        };
        // Gathering can stall on some networks; the candidates found so far still work.
        const timer = window.setTimeout(done, ICE_GATHERING_LIMIT_MS);
        connection.addEventListener("icegatheringstatechange", onChange);
      }),
    localDescriptionSdp: () => connection.localDescription?.sdp,
    setRemoteDescription: (answer: { readonly type: "answer"; readonly sdp: string }) =>
      connection.setRemoteDescription(answer),
    sendRealtimeEvent: (message: string) => {
      pending.push(message);
      flush();
    },
    hold: () => {
      held = true;
      audio.muted = true;
      watchForSpeech();
      return startRecording();
    },
    resume: async () => {
      held = false;
      stopWatching?.();
      stopWatching = null;
      const active = recorder;
      recorder = null;
      if (active !== null && active.state !== "inactive") {
        await new Promise<void>((resolve) => {
          active.addEventListener("stop", () => resolve(), { once: true });
          active.stop();
        });
      }
      const blobs = chunks;
      chunks = [];
      bufferedBytes = 0;
      const spoke = spokeWhileHeld;
      spokeWhileHeld = false;
      // The element already played the live stream, so the browser allows this playback.
      if (spoke && blobs.length > 0) await replay(blobs);
      if (!held) playLive();
    },
    close: () => {
      window.clearTimeout(reconnectTimer);
      stopWatching?.();
      if (recorder?.state === "recording") recorder.stop();
      connection.close();
      audio.pause();
      audio.srcObject = null;
    },
  };
}

const platform: LiveVoicePlatform<MediaStream> = {
  acquireMedia: async () => {
    if (!navigator.mediaDevices?.getUserMedia) {
      throw new Error("Microphone capture needs a secure (HTTPS) connection to T3.");
    }
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    return { tracks: stream.getAudioTracks(), stream };
  },
  createPeer,
  startMeter: (stream, onLevel) => {
    let current = -1;
    return sampleLevel(stream, (level) => {
      const step = Math.min(METER_STEPS, Math.round(level * METER_STEPS * 4));
      if (step === current) return;
      current = step;
      onLevel(step);
    });
  },
  prepare: async (target) => {
    await run(liveVoiceEnvironment.prepare, { environmentId: target.environmentId, input: target });
  },
  start: (target, input) =>
    run(liveVoiceEnvironment.start, {
      environmentId: target.environmentId,
      input: { threadId: target.threadId, ...input },
    }),
  configure: async (target, handoffConfig) => {
    await run(liveVoiceEnvironment.configure, {
      environmentId: target.environmentId,
      input: { threadId: target.threadId, handoffConfig },
    });
  },
  stop: async (target) => {
    await run(liveVoiceEnvironment.stop, { environmentId: target.environmentId, input: target });
  },
  followServerState: (environmentId, onState, onLink) => {
    const stopState = appAtomRegistry.subscribe(
      liveVoiceEnvironment.serverState({ environmentId, input: {} }),
      (result) => {
        const state = Option.getOrUndefined(AsyncResult.value(result));
        if (state !== undefined) onState(state);
      },
      { immediate: true },
    );
    const stopLink = appAtomRegistry.subscribe(
      environmentPresentations.presentationAtom(environmentId),
      (presentation) => onLink(presentation?.connection.phase === "connected"),
      { immediate: true },
    );
    return () => {
      stopState();
      stopLink();
    };
  },
  reportError: (_target, message) => {
    toastManager.add({ type: "error", title: "Live voice ended", description: message });
  },
};

/** Calls live here, not in a view, so they continue across navigation. */
export const liveVoiceCalls = createLiveVoiceRegistry(platform);

export function useLiveVoiceCall(
  environmentId: EnvironmentId,
  threadId: ThreadId | null,
): LiveVoiceCallView | null {
  const key = threadId === null ? null : liveVoiceKey(environmentId, threadId);
  return useSyncExternalStore(liveVoiceCalls.subscribe, () =>
    key === null ? null : (liveVoiceCalls.getSnapshot().get(key) ?? null),
  );
}

/** Keys of every call this client owns, for sidebar ordering. */
export function useLiveVoiceCalls(): ReadonlyMap<string, LiveVoiceCallView> {
  return useSyncExternalStore(liveVoiceCalls.subscribe, liveVoiceCalls.getSnapshot);
}

export function useLiveVoiceLevel(target: LiveVoiceTarget): number {
  return useSyncExternalStore(
    (listener) => liveVoiceCalls.subscribeLevel(target, listener),
    () => liveVoiceCalls.getLevel(target),
  );
}

const EMPTY_MESSAGES: ReadonlyArray<ChatMessage> = [];

function timelineMessages(
  threadId: ThreadId,
  call: LiveVoiceServerCall | undefined,
): ReadonlyArray<ChatMessage> {
  if (call === undefined) return EMPTY_MESSAGES;
  const at = new Date().toISOString();
  const message = (
    id: string,
    role: "user" | "assistant",
    text: string,
    streaming: boolean,
  ): ChatMessage => ({
    id: MessageId.make(id),
    role,
    text,
    runId: null,
    streaming,
    createdBy: role === "user" ? "user" : "agent",
    creationSource: role === "user" ? "web" : "provider",
    createdAt: at,
    updatedAt: at,
  });
  const messages = [
    ...call.phrases.map((phrase, index) =>
      message(
        phrase.messageId ?? `voice-live:${threadId}:${call.callId}:${phrase.role}:${index}`,
        phrase.role,
        phrase.text,
        phrase.messageId === undefined,
      ),
    ),
    ...Array.from(call.handoffs).flatMap(([handoffId, handoff]) =>
      handoff.kind === "handoff" && !handoff.done && handoff.text !== ""
        ? [message(`voice-handoff-status:${handoffId}`, "assistant", handoff.text, true)]
        : [],
    ),
  ];
  return messages.length === 0 ? EMPTY_MESSAGES : messages;
}

/** Live phrases and handoff progress of this client's call, shown but never saved. */
export function useLiveVoiceTimelineMessages(
  threadRef: ScopedThreadRef | null,
): ReadonlyArray<ChatMessage> {
  const call = useSyncExternalStore(liveVoiceCalls.subscribe, () =>
    threadRef === null ? undefined : liveVoiceCalls.serverCall(threadRef),
  );
  return threadRef === null ? EMPTY_MESSAGES : timelineMessages(threadRef.threadId, call);
}
