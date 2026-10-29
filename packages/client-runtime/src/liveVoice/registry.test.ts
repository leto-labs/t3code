import {
  EnvironmentId,
  type LiveVoiceEvent,
  type LiveVoiceHandoffConfig,
  MessageId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";

import { LiveVoiceCall } from "./call.ts";
import {
  createLiveVoiceRegistry,
  EMPTY_LIVE_VOICE_SERVER_STATE,
  liveVoiceKey,
  reduceLiveVoiceEvent,
  type LiveVoicePeerEvents,
  type LiveVoicePlatform,
  type LiveVoiceServerState,
  type LiveVoiceTarget,
} from "./registry.ts";

const environmentA = EnvironmentId.make("environment-a");
const environmentB = EnvironmentId.make("environment-b");
const threadA = ThreadId.make("thread-a");
const threadB = ThreadId.make("thread-b");
const threadC = ThreadId.make("thread-c");

function fakePeer(canRecord = true) {
  return {
    addMedia: vi.fn(),
    createOffer: vi.fn(async () => ({ sdp: "offer" })),
    setLocalDescription: vi.fn(async () => undefined),
    waitForIceGathering: vi.fn(async () => undefined),
    localDescriptionSdp: vi.fn((): string | undefined => "gathered-offer"),
    setRemoteDescription: vi.fn(async () => undefined),
    sendRealtimeEvent: vi.fn(),
    hold: vi.fn(() => canRecord),
    resume: vi.fn(async () => undefined),
    close: vi.fn(),
  };
}

function fakeMedia() {
  return { tracks: [{ enabled: true, stop: vi.fn() }], stream: "stream" };
}

interface Leg {
  readonly target: LiveVoiceTarget;
  readonly peer: ReturnType<typeof fakePeer>;
  readonly media: ReturnType<typeof fakeMedia>;
  readonly events: LiveVoicePeerEvents;
}

function setup(options: { readonly canRecord?: boolean } = {}) {
  const legs: Leg[] = [];
  const serverState = new Map<EnvironmentId, (state: LiveVoiceServerState) => void>();
  const unfollow = vi.fn();
  const order: string[] = [];
  let calls = 0;
  let pendingMedia: ReturnType<typeof fakeMedia> | null = null;
  const platform = {
    acquireMedia: vi.fn(async () => {
      pendingMedia = fakeMedia();
      return pendingMedia;
    }),
    createPeer: vi.fn((events: LiveVoicePeerEvents) => {
      const peer = fakePeer(options.canRecord ?? true);
      legs.push({ target: lastTarget!, peer, media: pendingMedia!, events });
      return peer;
    }),
    startMeter: vi.fn(() => () => undefined),
    prepare: vi.fn(async (target: LiveVoiceTarget) => {
      lastTarget = target;
      order.push(`prepare:${target.threadId}`);
    }),
    start: vi.fn(async (_target: LiveVoiceTarget, _input: { handoffConfig?: unknown }) => {
      calls += 1;
      return { callId: `call-${calls}`, answerSdp: `answer-${calls}` };
    }),
    configure: vi.fn(async () => undefined),
    stop: vi.fn(async () => undefined),
    followServerState: vi.fn(
      (id: EnvironmentId, onState: (state: LiveVoiceServerState) => void) => {
        serverState.set(id, onState);
        return unfollow;
      },
    ),
    reportError: vi.fn(),
  } satisfies LiveVoicePlatform<string>;
  let lastTarget: LiveVoiceTarget | null = null;
  const registry = createLiveVoiceRegistry(platform);
  const emit = (environmentId: EnvironmentId, ...events: ReadonlyArray<LiveVoiceEvent>) =>
    serverState.get(environmentId)?.(
      events.reduce(reduceLiveVoiceEvent, EMPTY_LIVE_VOICE_SERVER_STATE),
    );
  const leg = (environmentId: EnvironmentId, threadId: ThreadId) =>
    legs.find(
      (candidate) =>
        candidate.target.environmentId === environmentId && candidate.target.threadId === threadId,
    )!;
  const view = (environmentId: EnvironmentId, threadId: ThreadId) =>
    registry.getSnapshot().get(liveVoiceKey(environmentId, threadId));
  return { registry, platform, legs, leg, view, unfollow, emit, order };
}

const request = (environmentId: EnvironmentId, threadId: ThreadId) => ({
  environmentId,
  threadId,
  voice: "cove" as const,
});

const config = (instanceId: string, runtimeMode: LiveVoiceHandoffConfig["runtimeMode"]) => ({
  modelSelection: { instanceId: ProviderInstanceId.make(instanceId), model: "model" },
  runtimeMode,
  interactionMode: "default" as const,
});

/** Never two open microphones or two audible calls in one environment. */
function expectOneForegroundPerEnvironment(legs: ReadonlyArray<Leg>) {
  for (const environmentId of new Set(legs.map((leg) => leg.target.environmentId))) {
    const inEnvironment = legs.filter((leg) => leg.target.environmentId === environmentId);
    const openMicrophones = inEnvironment.filter((leg) => leg.media.tracks[0]!.enabled);
    const audible = inEnvironment.filter(
      (leg) => leg.peer.hold.mock.calls.length <= leg.peer.resume.mock.calls.length,
    );
    expect(openMicrophones.length).toBeLessThanOrEqual(1);
    expect(audible.length).toBeLessThanOrEqual(1);
  }
}

describe("LiveVoiceCall", () => {
  it("sends the gathered offer with its final line terminator", async () => {
    const peer = fakePeer();
    const startServer = vi.fn(async (_offer: string) => ({
      callId: "call-1",
      answerSdp: "answer",
    }));
    const call = new LiveVoiceCall({
      acquireMedia: async () => fakeMedia(),
      prepare: async () => undefined,
      createPeer: () => peer,
      start: startServer,
      stop: async () => undefined,
    });
    await expect(call.start()).resolves.toBe("call-1");
    expect(startServer).toHaveBeenCalledWith("gathered-offer\r\n");
    expect(peer.setRemoteDescription).toHaveBeenCalledWith({ type: "answer", sdp: "answer" });
  });

  it("ending during negotiation stops the server call and ignores the late answer", async () => {
    const peer = fakePeer();
    const media = fakeMedia();
    let answer!: (value: { callId: string; answerSdp: string }) => void;
    const stop = vi.fn(async () => undefined);
    const call = new LiveVoiceCall({
      acquireMedia: async () => media,
      prepare: async () => undefined,
      createPeer: () => peer,
      start: () => new Promise((resolve) => (answer = resolve)),
      stop,
    });
    const starting = call.start();
    await vi.waitFor(() => expect(answer).toBeDefined());
    await call.stop();
    answer({ callId: "call-1", answerSdp: "late" });
    await expect(starting).rejects.toThrow("Live voice was ended.");
    expect(peer.setRemoteDescription).not.toHaveBeenCalled();
    expect(media.tracks[0]?.stop).toHaveBeenCalled();
    expect(stop).toHaveBeenCalledOnce();
  });

  it("a rejected start releases the media and stops the prepared server thread", async () => {
    const peer = fakePeer();
    const media = fakeMedia();
    const stop = vi.fn(async () => undefined);
    const call = new LiveVoiceCall({
      acquireMedia: async () => media,
      prepare: async () => undefined,
      createPeer: () => peer,
      start: async () => {
        throw new Error("Codex could not start live voice.");
      },
      stop,
    });
    await expect(call.start()).rejects.toThrow("Codex could not start live voice.");
    expect(peer.close).toHaveBeenCalled();
    expect(media.tracks[0]?.stop).toHaveBeenCalled();
    expect(stop).toHaveBeenCalledOnce();
  });
});

describe("reduceLiveVoiceEvent", () => {
  it("joins partial phrases once and keeps the finished phrase under its saved id", () => {
    const base = { threadId: threadA, callId: "call-1", role: "assistant" as const };
    const state = [
      { type: "snapshot", calls: [] },
      { type: "transcript", ...base, text: "Hel", done: false },
      { type: "transcript", ...base, text: "lo", done: false },
      { type: "transcript", ...base, role: "user", text: "Wa", done: false },
    ] satisfies ReadonlyArray<LiveVoiceEvent>;
    const partial = state.reduce(reduceLiveVoiceEvent, EMPTY_LIVE_VOICE_SERVER_STATE);
    expect(partial.calls.get(threadA)?.phrases).toEqual([
      { role: "assistant", text: "Hello" },
      { role: "user", text: "Wa" },
    ]);
    const finished = reduceLiveVoiceEvent(partial, {
      type: "transcript",
      ...base,
      text: "Hello",
      done: true,
      messageId: MessageId.make("voice:call-1:1"),
    });
    expect(finished.calls.get(threadA)?.phrases).toEqual([
      { role: "assistant", text: "Hello", messageId: "voice:call-1:1" },
      { role: "user", text: "Wa" },
    ]);
  });
});

describe("createLiveVoiceRegistry", () => {
  it("follows one server stream per environment while it owns calls there", async () => {
    const { registry, platform, unfollow } = setup();
    await registry.start(request(environmentA, threadA));
    await registry.start(request(environmentA, threadB));
    expect(platform.followServerState).toHaveBeenCalledOnce();
    await registry.stop({ environmentId: environmentA, threadId: threadA });
    expect(unfollow).not.toHaveBeenCalled();
    await registry.stop({ environmentId: environmentA, threadId: threadB });
    expect(unfollow).toHaveBeenCalledOnce();
    expect(registry.getSnapshot().size).toBe(0);
  });

  it("keeps one foreground call per environment and leaves other environments alone", async () => {
    const { registry, leg, legs, view } = setup();
    await registry.start(request(environmentA, threadA));
    await registry.start(request(environmentB, threadC));
    await registry.start(request(environmentA, threadB));
    expectOneForegroundPerEnvironment(legs);
    expect(view(environmentA, threadA)?.state).toBe("held");
    expect(leg(environmentA, threadA).peer.hold).toHaveBeenCalledOnce();
    expect(view(environmentB, threadC)?.state).toBe("active");
    expect(leg(environmentB, threadC).peer.hold).not.toHaveBeenCalled();

    registry.setHeld({ environmentId: environmentA, threadId: threadA }, false);
    await vi.waitFor(() => expect(leg(environmentA, threadA).media.tracks[0]?.enabled).toBe(true));
    expectOneForegroundPerEnvironment(legs);
    expect(view(environmentA, threadB)?.state).toBe("held");
    expect(view(environmentB, threadC)?.state).toBe("active");
  });

  it("holds by buffering, flags a waiting response, and replays it on resume", async () => {
    const { registry, leg, view } = setup();
    const target = { environmentId: environmentA, threadId: threadA };
    await registry.start(request(environmentA, threadA));
    registry.setHeld(target, true);
    expect(leg(environmentA, threadA).peer.hold).toHaveBeenCalledOnce();
    expect(leg(environmentA, threadA).media.tracks[0]?.enabled).toBe(false);
    expect(view(environmentA, threadA)).toMatchObject({ state: "held", responseWaiting: false });
    leg(environmentA, threadA).events.onResponseWaiting();
    expect(view(environmentA, threadA)).toMatchObject({ state: "held", responseWaiting: true });
    registry.setHeld(target, false);
    expect(leg(environmentA, threadA).peer.resume).toHaveBeenCalledOnce();
    expect(view(environmentA, threadA)).toMatchObject({ state: "active", responseWaiting: false });
  });

  it("still mutes on hold when the platform cannot record, and says so", async () => {
    const { registry, view, leg } = setup({ canRecord: false });
    await registry.start(request(environmentA, threadA));
    registry.setHeld({ environmentId: environmentA, threadId: threadA }, true);
    expect(leg(environmentA, threadA).media.tracks[0]?.enabled).toBe(false);
    expect(view(environmentA, threadA)).toMatchObject({ state: "held", replayUnavailable: true });
  });

  it("creates a draft's thread before preparing the call", async () => {
    const { registry, order } = setup();
    await registry.start({
      ...request(environmentA, threadA),
      ensureThread: async () => {
        order.push("launch");
      },
    });
    expect(order).toEqual(["launch", `prepare:${threadA}`]);
  });

  it("sends a changed provider and permission set for the next handoff once", async () => {
    const { registry, platform } = setup();
    const target = { environmentId: environmentA, threadId: threadA };
    await registry.start({
      ...request(environmentA, threadA),
      handoffConfig: config("codex", "approval-required"),
    });
    expect(platform.start.mock.calls[0]?.[1].handoffConfig).toEqual(
      config("codex", "approval-required"),
    );
    registry.configure(target, config("codex", "approval-required"));
    expect(platform.configure).not.toHaveBeenCalled();
    registry.configure(target, config("claude", "full-access"));
    registry.configure(target, config("claude", "full-access"));
    expect(platform.configure).toHaveBeenCalledOnce();
    expect(platform.configure).toHaveBeenCalledWith(target, config("claude", "full-access"));
  });

  it("holds the call while a typed message runs, then resumes it", async () => {
    const { registry, view, leg } = setup();
    const target = { environmentId: environmentA, threadId: threadA };
    await registry.start(request(environmentA, threadA));
    registry.pauseForText(target);
    expect(view(environmentA, threadA)?.state).toBe("held");
    registry.setTurnRunning(target, false);
    expect(view(environmentA, threadA)?.state).toBe("held");
    registry.setTurnRunning(target, true);
    registry.setTurnRunning(target, false);
    expect(view(environmentA, threadA)?.state).toBe("active");
    expect(leg(environmentA, threadA).peer.resume).toHaveBeenCalledOnce();

    registry.pauseForText(target);
    registry.cancelTextPause(target);
    expect(view(environmentA, threadA)?.state).toBe("active");
  });

  it("speaks each handoff update once across repeated states and a reconnect", async () => {
    const { registry, leg, emit } = setup();
    await registry.start(request(environmentA, threadA));
    const progress: LiveVoiceEvent = {
      type: "handoff",
      threadId: threadA,
      callId: "call-1",
      handoffId: "handoff-1",
      kind: "handoff",
      text: "Working on it.",
      done: false,
    };
    const result: LiveVoiceEvent = { ...progress, text: "The tests pass.", done: true };
    const snapshot = (
      handoffs: ReadonlyArray<{
        handoffId: string;
        kind: "handoff" | "checkin";
        text: string;
        done: boolean;
      }>,
    ) =>
      ({
        type: "snapshot",
        calls: [{ threadId: threadA, callId: "call-1", status: "active", handoffs }],
      }) satisfies LiveVoiceEvent;
    emit(environmentA, snapshot([]), progress);
    emit(environmentA, snapshot([]), progress, result);
    emit(
      environmentA,
      snapshot([{ handoffId: "handoff-1", kind: "handoff", text: "The tests pass.", done: true }]),
    );
    expect(
      leg(environmentA, threadA).peer.sendRealtimeEvent.mock.calls.map(([message]) =>
        JSON.parse(message),
      ),
    ).toEqual([
      expect.objectContaining({ channel: "commentary" }),
      expect.objectContaining({
        channel: "speakable",
        content: [{ type: "input_text", text: "The tests pass." }],
      }),
    ]);
  });

  it("ends a call the server closed and reports why", async () => {
    const { registry, platform, leg, emit } = setup();
    await registry.start(request(environmentA, threadA));
    emit(
      environmentA,
      { type: "snapshot", calls: [] },
      { type: "status", threadId: threadA, callId: "call-1", status: "closed", error: "expired" },
    );
    expect(leg(environmentA, threadA).peer.close).toHaveBeenCalled();
    expect(platform.reportError).toHaveBeenCalledWith(
      expect.objectContaining({ threadId: threadA }),
      "expired",
    );
    expect(registry.getSnapshot().size).toBe(0);
  });

  it("keeps a call the server has not reported yet, and ends it once it disappears", async () => {
    const { registry, emit } = setup();
    await registry.start(request(environmentA, threadA));
    emit(environmentA, { type: "snapshot", calls: [] });
    expect(registry.getSnapshot().size).toBe(1);
    emit(
      environmentA,
      { type: "snapshot", calls: [] },
      { type: "status", threadId: threadA, callId: "call-1", status: "active" },
    );
    expect(registry.getSnapshot().size).toBe(1);
    emit(environmentA, { type: "snapshot", calls: [] });
    expect(registry.getSnapshot().size).toBe(0);
  });

  it("ending a call while it connects is not reported as a failure", async () => {
    const { registry, platform } = setup();
    let answer!: (value: { callId: string; answerSdp: string }) => void;
    platform.start.mockImplementationOnce(() => new Promise((resolve) => (answer = resolve)));
    const starting = registry.start(request(environmentA, threadA));
    await vi.waitFor(() => expect(answer).toBeDefined());
    const stopping = registry.stop({ environmentId: environmentA, threadId: threadA });
    answer({ callId: "call-1", answerSdp: "late" });
    await expect(starting).resolves.toBeUndefined();
    await stopping;
    expect(registry.getSnapshot().size).toBe(0);
  });
});
