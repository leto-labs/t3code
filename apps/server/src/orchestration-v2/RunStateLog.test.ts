import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2StoredEvent,
  ProviderInstanceId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../config.ts";
import * as EventSink from "./EventSink.ts";
import * as RunStateLog from "./RunStateLog.ts";

const threadId = ThreadId.make("thread:run-state");
const runId = RunId.make("run:run-state");
const attemptId = RunAttemptId.make("attempt:run-state:1");
const instanceId = ProviderInstanceId.make("codex");
const now = DateTime.makeUnsafe("2026-11-27T08:40:15.844Z");

const run = (
  status: OrchestrationV2Run["status"],
  delivery?: OrchestrationV2Run["delegatedCompletion"],
): OrchestrationV2Run => ({
  id: runId,
  threadId,
  ordinal: 1,
  providerInstanceId: instanceId,
  modelSelection: { instanceId, model: "gpt-5.4" },
  providerThreadId: ProviderThreadId.make("provider-thread:run-state"),
  userMessageId: MessageId.make("message:run-state"),
  rootNodeId: NodeId.make("node:run-state"),
  activeAttemptId: attemptId,
  status,
  requestedAt: now,
  startedAt: now,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
  ...(delivery === undefined ? {} : { delegatedCompletion: delivery }),
});

const attempt = (status: OrchestrationV2RunAttempt["status"]): OrchestrationV2RunAttempt => ({
  id: attemptId,
  runId,
  attemptOrdinal: 1,
  rootNodeId: NodeId.make("node:run-state"),
  providerInstanceId: instanceId,
  providerThreadId: ProviderThreadId.make("provider-thread:run-state"),
  providerTurnId: null,
  reason: "initial",
  status,
  startedAt: now,
  completedAt: null,
});

let sequence = 0;
const stored = (
  event:
    | { readonly type: "run.created" | "run.updated"; readonly payload: OrchestrationV2Run }
    | { readonly type: "run-attempt.updated"; readonly payload: OrchestrationV2RunAttempt },
): OrchestrationV2StoredEvent => {
  sequence += 1;
  return {
    sequence,
    commandId: CommandId.make(`/private/workspace:caller-text:${sequence}`),
    event: {
      id: EventId.make(`event:${sequence}`),
      threadId,
      runId,
      providerInstanceId: instanceId,
      occurredAt: now,
      ...event,
    } as OrchestrationV2StoredEvent["event"],
  };
};

const delivery = (generation: number | null): OrchestrationV2Run["delegatedCompletion"] => ({
  disposition: "open",
  nextGeneration: 2,
  delivery:
    generation === null
      ? null
      : {
          generation,
          messageId: MessageId.make("message:wake"),
          taskIds: [NodeId.make("node:task")],
        },
});

describe("RunStateLog transitions", () => {
  it("logs each status change once, and delivery opens and consumption", () => {
    const track = RunStateLog.makeTransitionTracker();
    const lines = [
      stored({ type: "run.created", payload: run("queued") }),
      stored({ type: "run.updated", payload: run("running") }),
      stored({ type: "run-attempt.updated", payload: attempt("running") }),
      // A non-status update is not a transition.
      stored({ type: "run.updated", payload: { ...run("running"), workStartedAt: now } }),
      stored({ type: "run-attempt.updated", payload: attempt("completed") }),
      stored({ type: "run.updated", payload: run("running", delivery(1)) }),
      stored({ type: "run.updated", payload: run("completed") }),
      {
        ...stored({ type: "run.updated", payload: run("completed", delivery(null)) }),
        commandId: null,
      },
    ].flatMap(track);
    assert.deepStrictEqual(
      lines.map(({ kind, from, to, cause }) => ({ kind, from, to, cause })),
      [
        { kind: "run", from: null, to: "queued", cause: "command" },
        { kind: "run", from: "queued", to: "running", cause: "command" },
        { kind: "attempt", from: null, to: "running", cause: "command" },
        { kind: "attempt", from: "running", to: "completed", cause: "command" },
        { kind: "delivery", from: null, to: "opened:generation:1", cause: "command" },
        { kind: "run", from: "running", to: "completed", cause: "command" },
        { kind: "delivery", from: "generation:1", to: "consumed", cause: "event" },
      ],
    );
  });
});

const layerServerConfig = ServerConfig.layerTest(process.cwd(), { prefix: "t3-run-state-log-" });

describe("RunStateLog file", () => {
  it.effect("flushes buffered NDJSON and prunes archives to the retention limit", () =>
    Effect.gen(function* () {
      const { serverTracePath } = yield* ServerConfig.ServerConfig;
      const path = yield* Path.Path;
      const fs = yield* FileSystem.FileSystem;
      const logDirectory = path.dirname(serverTracePath);
      const filePath = path.join(logDirectory, "run-state.ndjson");
      yield* fs.makeDirectory(logDirectory, { recursive: true });
      yield* fs.writeFileString(`${filePath}.9`, "retained\n");
      yield* fs.writeFileString(`${filePath}.10`, "expired\n");
      yield* fs.writeFileString(`${filePath}.140`, "expired\n");
      yield* Effect.scoped(
        Effect.gen(function* () {
          const log = yield* RunStateLog.RunStateLog;
          yield* log.record({
            threadId,
            runId,
            attemptId,
            kind: "run",
            from: "running",
            to: "completed",
            cause: "command",
          });
          yield* log.recordEvent({
            event: "reconciler.repair",
            reason: "attempt_terminal_run_running",
            threadId,
            runId,
          });
        }).pipe(
          Effect.provide(
            RunStateLog.layer.pipe(
              Layer.provide(
                Layer.mock(EventSink.EventSinkV2)({
                  latestSequence: () => Effect.succeed(0),
                  stream: () => Stream.never,
                }),
              ),
            ),
          ),
        ),
      );
      assert.equal(yield* fs.readFileString(`${filePath}.9`), "retained\n");
      assert.isFalse(yield* fs.exists(`${filePath}.10`));
      assert.isFalse(yield* fs.exists(`${filePath}.140`));
      const text = yield* fs.readFileString(filePath);
      const lines = text.trimEnd().split("\n");
      assert.equal(lines.length, 2);
      const line = JSON.parse(lines[0]!);
      assert.deepStrictEqual(Object.keys(line), [
        "ts",
        "threadId",
        "runId",
        "attemptId",
        "kind",
        "from",
        "to",
        "cause",
      ]);
      assert.equal(line.to, "completed");
      assert.equal(line.cause, "command");
      const repair = JSON.parse(lines[1]!);
      assert.deepStrictEqual(Object.keys(repair), ["ts", "event", "reason", "threadId", "runId"]);
      assert.equal(repair.reason, "attempt_terminal_run_running");
    }).pipe(Effect.provide(Layer.provideMerge(layerServerConfig, NodeServices.layer))),
  );
});
