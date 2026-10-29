import type {
  EnvironmentId,
  LiveVoiceHandoffConfig,
  LiveVoiceName,
  ThreadId,
} from "@t3tools/contracts";
import { PhoneIcon, PhoneOffIcon } from "lucide-react";
import { memo, useEffect } from "react";

import {
  liveVoiceCalls,
  liveVoiceErrorMessage,
  useLiveVoiceCall,
  useLiveVoiceLevel,
} from "../../state/liveVoice";
import { Button } from "../ui/button";
import { toastManager } from "../ui/toast";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";

export interface LiveVoiceControlsProps {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId | null;
  readonly voice: LiveVoiceName;
  /** The composer's current run settings; the next handoff uses them. */
  readonly handoffConfig: LiveVoiceHandoffConfig;
  /** Shown while a call is active, so the user knows what the next handoff will use. */
  readonly handoffLabel: string;
  readonly turnRunning: boolean;
  /** Null when a call can start. */
  readonly unavailableReason: string | null;
  /** Launches a draft's thread the way its first message would. */
  readonly onEnsureThread: (() => Promise<void>) | undefined;
  /** The collapsed mobile row has no room for status text. */
  readonly compact?: boolean;
}

function ControlButton(props: {
  readonly label: string;
  readonly variant: "success" | "warning-outline" | "destructive";
  readonly disabled?: boolean;
  readonly onClick: () => void;
  readonly children: React.ReactNode;
}) {
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button
            type="button"
            variant={props.variant}
            size="icon-sm"
            disabled={props.disabled}
            onPointerDown={(event) => event.preventDefault()}
            onClick={(event) => {
              event.stopPropagation();
              props.onClick();
            }}
            aria-label={props.label}
          />
        }
      >
        {props.children}
      </TooltipTrigger>
      <TooltipPopup>{props.label}</TooltipPopup>
    </Tooltip>
  );
}

/** Steps from the call's meter, which reports only changes, so silence does not repaint. */
function VolumeBars(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly dimmed: boolean;
}) {
  const level = useLiveVoiceLevel(props);
  return (
    <span className="flex h-4 items-end gap-0.5" aria-hidden="true">
      {[1, 2, 3, 4, 5].map((step) => (
        <span
          key={step}
          className={
            props.dimmed || step > level
              ? "w-0.5 rounded-sm bg-current opacity-35"
              : "w-0.5 rounded-sm bg-current"
          }
          style={{ height: `${4 + step * 2}px` }}
        />
      ))}
    </span>
  );
}

export const LiveVoiceControls = memo(function LiveVoiceControls(props: LiveVoiceControlsProps) {
  const { environmentId, threadId, turnRunning } = props;
  const call = useLiveVoiceCall(environmentId, threadId);
  const configKey = JSON.stringify(props.handoffConfig);
  const live = call !== null && threadId !== null;
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Sends composer run settings to the server for the next handoff.
  useEffect(() => {
    if (!live) return;
    liveVoiceCalls.configure(
      { environmentId, threadId },
      JSON.parse(configKey) as LiveVoiceHandoffConfig,
    );
  }, [live, environmentId, threadId, configKey]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Resumes a call held for a typed message once that turn ends.
  useEffect(() => {
    if (!live) return;
    liveVoiceCalls.setTurnRunning({ environmentId, threadId }, turnRunning);
  }, [live, environmentId, threadId, turnRunning]);

  if (!live) {
    const startCall = () => {
      if (threadId === null) return;
      liveVoiceCalls
        .start({
          environmentId,
          threadId,
          voice: props.voice,
          handoffConfig: props.handoffConfig,
          ...(props.onEnsureThread === undefined ? {} : { ensureThread: props.onEnsureThread }),
        })
        .catch((error: unknown) => {
          toastManager.add({
            type: "error",
            title: "Live voice could not start",
            description: liveVoiceErrorMessage(error),
          });
        });
    };
    return (
      <ControlButton
        label={props.unavailableReason ?? "Start call"}
        variant="success"
        disabled={props.unavailableReason !== null}
        onClick={startCall}
      >
        <PhoneIcon />
      </ControlButton>
    );
  }
  const target = { environmentId, threadId };
  const held = call.state === "held";
  const status =
    call.state === "starting"
      ? "Connecting…"
      : call.state === "stopping"
        ? "Ending…"
        : call.reconnecting
          ? "Reconnecting…"
          : held
            ? call.responseWaiting
              ? "Response waiting"
              : call.replayUnavailable
                ? "On hold (no replay)"
                : "On hold"
            : `Next: ${props.handoffLabel}`;
  return (
    <div className="flex min-w-0 items-center gap-1" role="group" aria-label="Voice call">
      {props.compact ? (
        <span className="sr-only" role="status">
          {status}
        </span>
      ) : (
        <span role="status" className="min-w-0 truncate px-1 text-xs text-muted-foreground">
          {status}
        </span>
      )}
      <ControlButton
        label={held ? "Resume call" : "Hold call"}
        variant="warning-outline"
        disabled={call.state === "starting" || call.state === "stopping"}
        onClick={() => liveVoiceCalls.setHeld(target, !held)}
      >
        <VolumeBars environmentId={environmentId} threadId={threadId} dimmed={held} />
      </ControlButton>
      <ControlButton
        label="End call"
        variant="destructive"
        disabled={call.state === "stopping"}
        onClick={() => void liveVoiceCalls.stop(target)}
      >
        <PhoneOffIcon />
      </ControlButton>
    </div>
  );
});
