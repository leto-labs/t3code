import type { OrchestrationV2StoredEvent, RunAttemptId, RunId, ThreadId } from "@t3tools/contracts";
import { makeAsyncRotatingFileSink } from "@t3tools/shared/logging";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../config.ts";
import * as EventSink from "./EventSink.ts";

const MEBIBYTE = 1024 * 1024;
const MAX_BYTES = 100 * MEBIBYTE;
// The sink counts archives separately from the active file, for 1,000 MiB total.
const MAX_ARCHIVES = 9;
const FLUSH_INTERVAL = "1 second";
const MAX_BUFFERED_LINES = 20_000;
const MAX_TRACKED_RECORDS = 50_000;

/** One status transition. Ids and statuses only: never message text, prompts or paths. */
export interface RunStateLogEntry {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly attemptId: RunAttemptId | null;
  readonly kind: "run" | "attempt" | "delivery";
  readonly from: string | null;
  readonly to: string;
  readonly cause: "command" | "event";
}

/** A reconciler record: ids, statuses, counts and times only. */
export interface RunStateLogEvent {
  readonly event: string;
  readonly cause?: never;
  readonly [field: string]: unknown;
}

/** The default drops entries; the server provides the file-backed layer. */
export class RunStateLog extends Context.Reference<{
  readonly record: (entry: RunStateLogEntry) => Effect.Effect<void>;
  readonly recordEvent: (event: RunStateLogEvent) => Effect.Effect<void>;
}>("t3/orchestration-v2/RunStateLog", {
  defaultValue: () => ({ record: () => Effect.void, recordEvent: () => Effect.void }),
}) {}

// Remembers each record's last state so only changes are logged. Bounded: the oldest entries
// fall out, and a record seen again logs `from: null`.
const makeLastStates = () => {
  const states = new Map<string, string>();
  return (key: string, state: string) => {
    const previous = states.get(key);
    if (previous === state) return { changed: false, previous } as const;
    states.delete(key);
    states.set(key, state);
    if (states.size > MAX_TRACKED_RECORDS) {
      const oldest = states.keys().next().value;
      if (oldest !== undefined) states.delete(oldest);
    }
    return { changed: true, previous: previous ?? null } as const;
  };
};

/** The transitions one stored run or attempt event makes, given the states seen before it. */
export const makeTransitionTracker = () => {
  const track = makeLastStates();
  return (stored: OrchestrationV2StoredEvent): ReadonlyArray<RunStateLogEntry> => {
    const cause = stored.commandId === null ? "event" : "command";
    const event = stored.event;
    if (event.type === "run.created" || event.type === "run.updated") {
      const run = event.payload;
      const entries: Array<RunStateLogEntry> = [];
      const status = track(`run:${run.id}`, run.status);
      if (status.changed) {
        entries.push({
          threadId: run.threadId,
          runId: run.id,
          attemptId: run.activeAttemptId,
          kind: "run",
          from: status.previous,
          to: run.status,
          cause,
        });
      }
      // A run.updated without the cohort keeps the stored one; only a stated cohort changes it.
      if (run.delegatedCompletion !== undefined) {
        const delivery = run.delegatedCompletion.delivery;
        const generation = delivery === null ? "none" : `generation:${delivery.generation}`;
        const change = track(`delivery:${run.id}`, generation);
        if (change.changed && (change.previous !== null || delivery !== null)) {
          entries.push({
            threadId: run.threadId,
            runId: run.id,
            attemptId: run.activeAttemptId,
            kind: "delivery",
            from: change.previous,
            to: delivery === null ? "consumed" : `opened:${generation}`,
            cause,
          });
        }
      }
      return entries;
    }
    if (event.type === "run-attempt.created" || event.type === "run-attempt.updated") {
      const attempt = event.payload;
      const status = track(`attempt:${attempt.id}`, attempt.status);
      return status.changed
        ? [
            {
              threadId: event.threadId,
              runId: attempt.runId,
              attemptId: attempt.id,
              kind: "attempt",
              from: status.previous,
              to: attempt.status,
              cause,
            },
          ]
        : [];
    }
    return [];
  };
};

const encode = (ts: string, entry: RunStateLogEntry) =>
  `${JSON.stringify({
    ts,
    threadId: entry.threadId,
    runId: entry.runId,
    attemptId: entry.attemptId,
    kind: entry.kind,
    from: entry.from,
    to: entry.to,
    cause: entry.cause === "command" ? "command" : "event",
  })}\n`;

const encodeEvent = (ts: string, event: RunStateLogEvent) =>
  `${JSON.stringify({ ts, ...event, cause: undefined })}\n`;

/** Writes `logs/run-state.ndjson`, off the commit path: lines buffer in memory and flush each second. */
export const layer = Layer.effect(
  RunStateLog,
  Effect.gen(function* () {
    const { serverTracePath } = yield* ServerConfig.ServerConfig;
    const path = yield* Path.Path;
    const eventSink = yield* EventSink.EventSinkV2;
    const filePath = path.join(path.dirname(serverTracePath), "run-state.ndjson");
    const sink = yield* makeAsyncRotatingFileSink({
      filePath,
      maxBytes: MAX_BYTES,
      maxFiles: MAX_ARCHIVES,
    }).pipe(
      Effect.catch((cause) =>
        Effect.logWarning("run state log unavailable", { filePath, cause }).pipe(
          Effect.as(undefined),
        ),
      ),
    );
    // Not yet provided here, so this is the default that drops entries.
    if (sink === undefined) return yield* RunStateLog;

    let buffer: Array<string> = [];
    let dropped = 0;
    // Runs for every run and attempt transition; the stream fiber needs no span per line.
    const push = Effect.fnUntraced(function* (line: (ts: string) => string) {
      const now = yield* DateTime.now;
      if (buffer.length >= MAX_BUFFERED_LINES) {
        dropped += 1;
        return;
      }
      buffer.push(line(DateTime.formatIso(now)));
    });
    const append = (entry: RunStateLogEntry) => push((ts) => encode(ts, entry));
    const flush = Effect.gen(function* () {
      if (dropped > 0) {
        yield* Effect.logWarning("run state log dropped lines", { filePath, dropped });
        dropped = 0;
      }
      if (buffer.length === 0) return;
      const lines = buffer.join("");
      buffer = [];
      yield* sink.write(lines);
    }).pipe(
      Effect.catch((cause) => Effect.logWarning("run state log write failed", { filePath, cause })),
    );
    yield* Effect.addFinalizer(() => flush);
    yield* flush.pipe(Effect.delay(FLUSH_INTERVAL), Effect.forever, Effect.forkScoped);

    // Unbounded subscriptions never hold up a commit; each type has its own channel.
    const latest = yield* eventSink.latestSequence().pipe(Effect.orElseSucceed(() => 0));
    const transitions = makeTransitionTracker();
    yield* Stream.mergeAll(
      (["run.created", "run.updated", "run-attempt.created", "run-attempt.updated"] as const).map(
        (eventType) => eventSink.stream({ afterSequence: latest, eventType }),
      ),
      { concurrency: "unbounded" },
    ).pipe(
      Stream.runForEach((stored) => Effect.forEach(transitions(stored), append, { discard: true })),
      Effect.catchCause((cause) => Effect.logWarning("run state log stream ended", { cause })),
      Effect.forkScoped,
    );

    const appendEvent = (event: RunStateLogEvent) => push((ts) => encodeEvent(ts, event));
    return RunStateLog.of({ record: append, recordEvent: appendEvent });
  }),
);
