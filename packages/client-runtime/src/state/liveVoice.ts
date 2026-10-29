import { WS_METHODS } from "@t3tools/contracts";
import * as Stream from "effect/Stream";
import type { Atom } from "effect/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import { EMPTY_LIVE_VOICE_SERVER_STATE, reduceLiveVoiceEvent } from "../liveVoice/registry.ts";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcSubscriptionAtomFamily,
} from "./runtime.ts";

export function createLiveVoiceEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  return {
    prepare: createEnvironmentRpcCommand(runtime, {
      label: "environment-command:live-voice:prepare",
      tag: WS_METHODS.liveVoicePrepare,
    }),
    configure: createEnvironmentRpcCommand(runtime, {
      label: "environment-command:live-voice:configure",
      tag: WS_METHODS.liveVoiceConfigure,
    }),
    start: createEnvironmentRpcCommand(runtime, {
      label: "environment-command:live-voice:start",
      tag: WS_METHODS.liveVoiceStart,
    }),
    stop: createEnvironmentRpcCommand(runtime, {
      label: "environment-command:live-voice:stop",
      tag: WS_METHODS.liveVoiceStop,
    }),
    /** Folded so a reader of the latest value never misses an event. */
    serverState: createEnvironmentRpcSubscriptionAtomFamily(runtime, {
      label: "environment-data:live-voice:server-state",
      tag: WS_METHODS.liveVoiceSubscribe,
      idleTtlMs: 0,
      transform: (events) =>
        events.pipe(Stream.scan(() => EMPTY_LIVE_VOICE_SERVER_STATE, reduceLiveVoiceEvent)),
    }),
  };
}
