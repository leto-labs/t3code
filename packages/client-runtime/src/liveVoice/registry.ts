import type {
  EnvironmentId,
  LiveVoiceCallStatus,
  LiveVoiceEvent,
  LiveVoiceHandoffConfig,
  LiveVoiceName,
  MessageId,
  ThreadId,
} from "@t3tools/contracts";

import { LiveVoiceCall, type LiveVoiceMedia, type LiveVoicePeer } from "./call.ts";

interface LiveVoiceHandoff {
  readonly kind: "handoff" | "checkin";
  readonly text: string;
  readonly done: boolean;
}

export interface LiveVoicePhrase {
  readonly role: "user" | "assistant";
  readonly text: string;
  /** The saved row's id, once the phrase is finished. */
  readonly messageId?: MessageId;
}

export interface LiveVoiceServerCall {
  readonly callId: string;
  readonly status: LiveVoiceCallStatus;
  readonly error?: string;
  readonly handoffs: ReadonlyMap<string, LiveVoiceHandoff>;
  /** Phrases in progress, then the last few finished ones until their rows load. */
  readonly phrases: ReadonlyArray<LiveVoicePhrase>;
}

/** The environment's calls folded from its event stream; only the latest value matters. */
export interface LiveVoiceServerState {
  /** False until the subscription's snapshot arrives. */
  readonly synced: boolean;
  readonly calls: ReadonlyMap<ThreadId, LiveVoiceServerCall>;
}

export const EMPTY_LIVE_VOICE_SERVER_STATE: LiveVoiceServerState = {
  synced: false,
  calls: new Map(),
};

const KEPT_FINISHED_PHRASES = 4;

function appendPhrase(
  phrases: ReadonlyArray<LiveVoicePhrase>,
  event: Extract<LiveVoiceEvent, { readonly type: "transcript" }>,
): ReadonlyArray<LiveVoicePhrase> {
  const open = phrases.findIndex(
    (phrase) => phrase.role === event.role && phrase.messageId === undefined,
  );
  const previous = open === -1 ? "" : phrases[open]!.text;
  const remaining = open === -1 ? phrases : phrases.filter((_, index) => index !== open);
  if (!event.done) return [...remaining, { role: event.role, text: previous + event.text }];
  if (event.messageId === undefined) return remaining;
  const finished = remaining.filter((phrase) => phrase.messageId !== undefined);
  const inProgress = remaining.filter((phrase) => phrase.messageId === undefined);
  return [
    ...finished.slice(-(KEPT_FINISHED_PHRASES - 1)),
    { role: event.role, text: event.text, messageId: event.messageId },
    ...inProgress,
  ];
}

export function reduceLiveVoiceEvent(
  state: LiveVoiceServerState,
  event: LiveVoiceEvent,
): LiveVoiceServerState {
  if (event.type === "snapshot") {
    return {
      synced: true,
      calls: new Map(
        event.calls.map((call) => [
          call.threadId,
          {
            callId: call.callId,
            status: call.status,
            handoffs: new Map(call.handoffs.map(({ handoffId, ...rest }) => [handoffId, rest])),
            phrases: [],
          },
        ]),
      ),
    };
  }
  const previous = state.calls.get(event.threadId);
  const same = previous?.callId === event.callId ? previous : undefined;
  const base: LiveVoiceServerCall = same ?? {
    callId: event.callId,
    status: "active",
    handoffs: new Map(),
    phrases: [],
  };
  const next: LiveVoiceServerCall =
    event.type === "status"
      ? {
          ...base,
          status: event.status,
          ...(event.error === undefined ? {} : { error: event.error }),
        }
      : event.type === "handoff"
        ? {
            ...base,
            handoffs: new Map(base.handoffs).set(event.handoffId, {
              kind: event.kind,
              text: event.text,
              done: event.done,
            }),
          }
        : { ...base, phrases: appendPhrase(base.phrases, event) };
  return { ...state, calls: new Map(state.calls).set(event.threadId, next) };
}

/** Codex's client-managed handoff lane; only final results are spoken. */
const encodeHandoffUpdate = (handoffId: string, handoff: LiveVoiceHandoff): string =>
  JSON.stringify({
    type: "delegation.context.append",
    delegation_item_id: handoffId,
    channel: handoff.done ? "speakable" : "commentary",
    content: [{ type: "input_text", text: handoff.text }],
  });

export interface LiveVoiceTarget {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}

export interface LiveVoiceCallView {
  readonly state: "starting" | "active" | "held" | "stopping";
  /** The remote side spoke while held; resuming plays it. */
  readonly responseWaiting: boolean;
  /** Hold mutes but cannot record, so nothing will be replayed. */
  readonly replayUnavailable: boolean;
  /** The media path or the T3 connection dropped and may come back. */
  readonly reconnecting: boolean;
}

export interface LiveVoiceStartRequest extends LiveVoiceTarget {
  readonly voice: LiveVoiceName;
  readonly handoffConfig?: LiveVoiceHandoffConfig;
  /** Creates a draft's thread through the same launch as its first message. */
  readonly ensureThread?: () => Promise<void>;
}

export interface LiveVoicePeerEvents {
  /** The media connection cannot recover. */
  readonly onFailed: () => void;
  readonly onReconnecting: (reconnecting: boolean) => void;
  /** Remote audio arrived while held. */
  readonly onResponseWaiting: () => void;
}

export interface LiveVoicePlatform<Stream> {
  readonly acquireMedia: () => Promise<LiveVoiceMedia<Stream>>;
  readonly createPeer: (events: LiveVoicePeerEvents) => LiveVoicePeer<Stream>;
  /** Reports a quantized microphone level, only when it changes, until stopped. */
  readonly startMeter: (stream: Stream, onLevel: (level: number) => void) => () => void;
  readonly prepare: (target: LiveVoiceTarget) => Promise<void>;
  readonly start: (
    target: LiveVoiceTarget,
    input: {
      readonly offerSdp: string;
      readonly voice: LiveVoiceName;
      readonly handoffConfig?: LiveVoiceHandoffConfig;
    },
  ) => Promise<{ readonly callId: string; readonly answerSdp: string }>;
  readonly configure: (target: LiveVoiceTarget, config: LiveVoiceHandoffConfig) => Promise<void>;
  readonly stop: (target: LiveVoiceTarget) => Promise<void>;
  /** Follows the environment's server state and T3 link until the returned function runs. */
  readonly followServerState: (
    environmentId: EnvironmentId,
    onState: (state: LiveVoiceServerState) => void,
    onLink: (connected: boolean) => void,
  ) => () => void;
  readonly reportError: (target: LiveVoiceTarget, message: string) => void;
}

interface Entry<Stream> extends LiveVoiceTarget {
  readonly key: string;
  readonly call: LiveVoiceCall<Stream>;
  state: LiveVoiceCallView["state"];
  callId: string | null;
  /** The server state has shown this call, so its absence now means it ended. */
  seen: boolean;
  responseWaiting: boolean;
  replayUnavailable: boolean;
  peerReconnecting: boolean;
  /** Held for a text turn: "sent" until it runs, then "running" until it ends. */
  textPause: "sent" | "running" | null;
  handoffConfig: LiveVoiceHandoffConfig | undefined;
  sentConfig: string | null;
  level: number;
  stopMeter: (() => void) | null;
  view: LiveVoiceCallView | null;
  readonly delivered: Set<string>;
}

export const liveVoiceKey = (environmentId: EnvironmentId, threadId: ThreadId): string =>
  `${environmentId}:${threadId}`;

/**
 * Calls this client owns, kept outside any view so they survive navigation.
 * In each environment one call has the microphone and speaker; the others are held.
 */
export function createLiveVoiceRegistry<Stream>(platform: LiveVoicePlatform<Stream>) {
  const entries = new Map<string, Entry<Stream>>();
  const followers = new Map<EnvironmentId, () => void>();
  const serverStates = new Map<EnvironmentId, LiveVoiceServerState>();
  const linkDown = new Set<EnvironmentId>();
  const listeners = new Set<() => void>();
  const levelListeners = new Map<string, Set<() => void>>();
  let snapshot: ReadonlyMap<string, LiveVoiceCallView> = new Map();

  const viewOf = (entry: Entry<Stream>): LiveVoiceCallView => {
    const next: LiveVoiceCallView = {
      state: entry.state,
      responseWaiting: entry.responseWaiting,
      replayUnavailable: entry.replayUnavailable,
      reconnecting: entry.peerReconnecting || linkDown.has(entry.environmentId),
    };
    const previous = entry.view;
    if (
      previous !== null &&
      previous.state === next.state &&
      previous.responseWaiting === next.responseWaiting &&
      previous.replayUnavailable === next.replayUnavailable &&
      previous.reconnecting === next.reconnecting
    )
      return previous;
    entry.view = next;
    return next;
  };

  const publish = () => {
    snapshot = new Map(Array.from(entries, ([key, entry]) => [key, viewOf(entry)]));
    for (const listener of listeners) listener();
  };

  const setLevel = (entry: Entry<Stream>, level: number) => {
    if (entry.level === level) return;
    entry.level = level;
    for (const listener of levelListeners.get(entry.key) ?? []) listener();
  };

  /** The meter runs only for the call that has the microphone. */
  const syncMeter = (entry: Entry<Stream>) => {
    const stream = entry.call.mediaStream;
    const metering = entry.state === "active" && stream !== undefined && entries.has(entry.key);
    if (metering && entry.stopMeter === null) {
      entry.stopMeter = platform.startMeter(stream, (level) => setLevel(entry, level));
    } else if (!metering && entry.stopMeter !== null) {
      entry.stopMeter();
      entry.stopMeter = null;
      setLevel(entry, 0);
    }
  };

  const remove = (entry: Entry<Stream>) => {
    if (entries.get(entry.key) !== entry) return;
    entries.delete(entry.key);
    syncMeter(entry);
    entry.call.close();
    const environmentHasCalls = Array.from(entries.values()).some(
      (other) => other.environmentId === entry.environmentId,
    );
    if (!environmentHasCalls) {
      followers.get(entry.environmentId)?.();
      followers.delete(entry.environmentId);
      serverStates.delete(entry.environmentId);
      linkDown.delete(entry.environmentId);
    }
    publish();
  };

  const hold = (entry: Entry<Stream>) => {
    if (entry.state === "held") return;
    entry.replayUnavailable = !entry.call.hold();
    if (entry.state === "active") entry.state = "held";
    syncMeter(entry);
  };

  const release = (entry: Entry<Stream>) => {
    entry.textPause = null;
    entry.responseWaiting = false;
    entry.replayUnavailable = false;
    if (entry.state === "held") entry.state = "active";
    void entry.call.resume();
    syncMeter(entry);
  };

  /** Gives this call the environment's microphone and speaker; never two at once. */
  const focus = (entry: Entry<Stream>) => {
    for (const other of entries.values()) {
      if (other !== entry && other.environmentId === entry.environmentId) hold(other);
    }
    release(entry);
    publish();
  };

  const deliverHandoffs = (entry: Entry<Stream>, call: LiveVoiceServerCall) => {
    for (const [handoffId, handoff] of call.handoffs) {
      if (handoff.text === "") continue;
      const key = `${handoffId}:${handoff.done ? "done" : "progress"}`;
      if (entry.delivered.has(key)) continue;
      entry.delivered.add(key);
      if (handoff.done) entry.delivered.add(`${handoffId}:progress`);
      // A held call answers into its hold buffer, so a check-in is not lost.
      entry.call.sendRealtimeEvent(encodeHandoffUpdate(handoffId, handoff));
    }
  };

  const sendConfig = (entry: Entry<Stream>) => {
    const config = entry.handoffConfig;
    if (entry.callId === null || config === undefined) return;
    const key = JSON.stringify(config);
    if (key === entry.sentConfig) return;
    entry.sentConfig = key;
    platform
      .configure({ environmentId: entry.environmentId, threadId: entry.threadId }, config)
      .catch(() => {
        entry.sentConfig = null;
      });
  };

  const reconcile = (entry: Entry<Stream>) => {
    const state = serverStates.get(entry.environmentId);
    if (entry.callId === null || state === undefined || !state.synced) return;
    const call = state.calls.get(entry.threadId);
    if (call === undefined || call.callId !== entry.callId) {
      if (!entry.seen) return;
      platform.reportError(entry, "The voice call ended while T3 was disconnected.");
      return remove(entry);
    }
    entry.seen = true;
    if (call.status === "closed") {
      if (call.error !== undefined) platform.reportError(entry, call.error);
      return remove(entry);
    }
    deliverHandoffs(entry, call);
  };

  const follow = (environmentId: EnvironmentId) => {
    if (followers.has(environmentId)) return;
    followers.set(
      environmentId,
      platform.followServerState(
        environmentId,
        (state) => {
          serverStates.set(environmentId, state);
          for (const entry of Array.from(entries.values())) {
            if (entry.environmentId === environmentId) reconcile(entry);
          }
          for (const listener of listeners) listener();
        },
        (connected) => {
          if (connected === !linkDown.has(environmentId)) return;
          if (connected) linkDown.delete(environmentId);
          else linkDown.add(environmentId);
          publish();
        },
      ),
    );
  };

  const entryFor = (target: LiveVoiceTarget) =>
    entries.get(liveVoiceKey(target.environmentId, target.threadId));

  const stop = async (target: LiveVoiceTarget): Promise<void> => {
    const entry = entryFor(target);
    if (entry === undefined) return;
    entry.state = "stopping";
    syncMeter(entry);
    publish();
    await entry.call.stop();
    remove(entry);
  };

  const start = async (request: LiveVoiceStartRequest): Promise<void> => {
    const target = { environmentId: request.environmentId, threadId: request.threadId };
    const key = liveVoiceKey(request.environmentId, request.threadId);
    const existing = entries.get(key);
    if (existing !== undefined) return focus(existing);
    let entry: Entry<Stream>;
    const call: LiveVoiceCall<Stream> = new LiveVoiceCall({
      acquireMedia: platform.acquireMedia,
      prepare: async () => {
        await request.ensureThread?.();
        await platform.prepare(target);
      },
      createPeer: () =>
        platform.createPeer({
          onFailed: () => void stop(target),
          onReconnecting: (reconnecting) => {
            if (entry.peerReconnecting === reconnecting) return;
            entry.peerReconnecting = reconnecting;
            publish();
          },
          onResponseWaiting: () => {
            if (entry.state !== "held" || entry.responseWaiting) return;
            entry.responseWaiting = true;
            publish();
          },
        }),
      start: (offerSdp) =>
        platform.start(target, {
          offerSdp,
          voice: request.voice,
          ...(entry.handoffConfig === undefined ? {} : { handoffConfig: entry.handoffConfig }),
        }),
      stop: () => platform.stop(target),
    });
    entry = {
      ...target,
      key,
      call,
      state: "starting",
      callId: null,
      seen: false,
      responseWaiting: false,
      replayUnavailable: false,
      peerReconnecting: false,
      textPause: null,
      handoffConfig: request.handoffConfig,
      sentConfig:
        request.handoffConfig === undefined ? null : JSON.stringify(request.handoffConfig),
      level: 0,
      stopMeter: null,
      view: null,
      delivered: new Set(),
    };
    entries.set(key, entry);
    // Subscribe before starting so no server event for this call is missed.
    follow(request.environmentId);
    focus(entry);
    try {
      entry.callId = await call.start();
    } catch (error) {
      const endedByUser = entry.state === "stopping";
      remove(entry);
      if (endedByUser) return;
      throw error;
    }
    if (entries.get(key) !== entry) return;
    // Another call may have taken focus while this one connected.
    const foreground = !Array.from(entries.values()).some(
      (other) =>
        other !== entry && other.environmentId === entry.environmentId && other.state === "active",
    );
    entry.state = "active";
    if (foreground) focus(entry);
    else {
      hold(entry);
      publish();
    }
    sendConfig(entry);
    reconcile(entry);
  };

  return {
    start,
    stop,
    /** Holding keeps the call connected and records what the other side says. */
    setHeld: (target: LiveVoiceTarget, held: boolean): void => {
      const entry = entryFor(target);
      if (entry === undefined || entry.state === "starting" || entry.state === "stopping") return;
      if (!held) return focus(entry);
      hold(entry);
      publish();
    },
    /** Settings the next handoff snapshots; a running handoff keeps its own. */
    configure: (target: LiveVoiceTarget, config: LiveVoiceHandoffConfig): void => {
      const entry = entryFor(target);
      if (entry === undefined) return;
      entry.handoffConfig = config;
      sendConfig(entry);
    },
    /** Holds the call while a typed message runs on its thread. */
    pauseForText: (target: LiveVoiceTarget): void => {
      const entry = entryFor(target);
      if (entry?.state !== "active") return;
      hold(entry);
      entry.textPause = "sent";
      publish();
    },
    /** The typed message was not sent after all. */
    cancelTextPause: (target: LiveVoiceTarget): void => {
      const entry = entryFor(target);
      if (entry?.textPause === "sent") focus(entry);
    },
    /** Resumes a call held for text once that turn has run and ended. */
    setTurnRunning: (target: LiveVoiceTarget, running: boolean): void => {
      const entry = entryFor(target);
      if (entry === undefined || entry.textPause === null) return;
      if (running) entry.textPause = "running";
      else if (entry.textPause === "running") focus(entry);
    },
    getSnapshot: (): ReadonlyMap<string, LiveVoiceCallView> => snapshot,
    subscribe: (listener: () => void): (() => void) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    /** The server's view of an owned call, for its live phrases and handoff progress. */
    serverCall: (target: LiveVoiceTarget): LiveVoiceServerCall | undefined =>
      entryFor(target) === undefined
        ? undefined
        : serverStates.get(target.environmentId)?.calls.get(target.threadId),
    getLevel: (target: LiveVoiceTarget): number => entryFor(target)?.level ?? 0,
    subscribeLevel: (target: LiveVoiceTarget, listener: () => void): (() => void) => {
      const key = liveVoiceKey(target.environmentId, target.threadId);
      const set = levelListeners.get(key) ?? new Set();
      levelListeners.set(key, set.add(listener));
      return () => {
        set.delete(listener);
        if (set.size === 0) levelListeners.delete(key);
      };
    },
  };
}
