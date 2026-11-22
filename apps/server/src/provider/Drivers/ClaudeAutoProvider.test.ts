import * as ClaudeSdk from "@anthropic-ai/claude-agent-sdk";
import type { SDKMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ClaudeSettings,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2ProviderThread,
  type ServerProvider,
  type ServerProviderUsageWindow,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";
import { vi } from "vite-plus/test";
import * as ServerConfig from "../../config.ts";
import * as ClaudeAdapterV2 from "../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { autoPoolAuthStatus, autoPoolChoose, autoPoolHeadroom } from "../autoPoolPolicy.ts";
import { providerUsageReport } from "../providerUsageReport.ts";
import { codexAutoFixture } from "../testUtils/codexAuto.ts";
import {
  claudeAutoHeadroom,
  makeClaudeAutoAdapter,
  makeClaudeAutoRouter,
} from "./ClaudeAutoProvider.ts";

vi.mock("@anthropic-ai/claude-agent-sdk", async (importOriginal) => ({
  ...(await importOriginal<typeof ClaudeSdk>()),
  resolveSettings: vi.fn(async () => ({ effective: {}, provenance: {}, sources: [] })),
}));

const poolId = ProviderInstanceId.make("claude_auto");
const DRIVER = ProviderDriverKind.make("claudeAgent");
const FABLE = "claude-fable-5-2";
const OPUS = "claude-opus-4-7";
const SONNET = "claude-sonnet-4-6";
const NOW = Date.parse("2026-11-20T12:00:00.000Z");
const iso = (offsetHours: number) =>
  DateTime.formatIso(DateTime.makeUnsafe(NOW + offsetHours * 3_600_000));
const decodeClaudeSettings = Schema.decodeSync(ClaudeSettings);

const window = (
  id: string,
  kind: ServerProviderUsageWindow["kind"],
  usedPercent: number,
  resetInHours?: number,
): ServerProviderUsageWindow => ({
  id,
  kind,
  label: id,
  usedPercent,
  ...(resetInHours === undefined ? {} : { resetsAt: iso(resetInHours) }),
});

/** A Claude subscription account as its snapshot reports it. */
function claudeMember(id: string, windows: readonly ServerProviderUsageWindow[]) {
  const fixture = codexAutoFixture(id, iso(0));
  const effort = {
    id: "effort",
    label: "Effort",
    type: "select" as const,
    options: ["low", "high"].map((value) => ({
      id: value,
      label: value,
      isDefault: value === "high",
    })),
  };
  fixture.set({
    ...fixture.read(),
    driver: DRIVER,
    auth: { status: "authenticated", type: "max", email: `${id}@example.test`, workspaceId: id },
    models: [FABLE, OPUS, SONNET].map((slug) => ({
      slug,
      name: slug,
      isCustom: false,
      capabilities: { optionDescriptors: [effort] },
    })),
    usageLimits: { checkedAt: iso(0), windows: [...windows] },
  });
  return fixture;
}

it.layer(NodeServices.layer)("Claude Auto routing policy", (it) => {
  it("binds a Fable request to its own weekly window and an Opus request to the account window", () => {
    const provider = claudeMember("alice", [
      window("five_hour", "session", 30, 2),
      window("seven_day", "weekly", 40, 24),
      window("seven_day_fable", "weekly", 90, 96),
      window("monthly_spend", "monthly", 100),
    ]).read();

    const fable = autoPoolHeadroom(provider, FABLE, NOW);
    expect(fable?.binding).toEqual({
      kind: "weekly",
      remaining: 10,
      resetsAt: Date.parse(iso(96)),
    });
    const opus = autoPoolHeadroom(provider, OPUS, NOW);
    // Paid extra usage does not limit the subscription, and the Fable window does not limit Opus.
    expect(opus).toEqual({
      remaining: 60,
      binding: { kind: "weekly", remaining: 60, resetsAt: Date.parse(iso(24)) },
    });
  });

  it("reports Auto as signed in only while a member account is", () => {
    const signedIn = claudeMember("alice", []).read();
    const signedOut = { ...signedIn, auth: { status: "unauthenticated" as const } };
    const unknown = { ...signedIn, auth: { status: "unknown" as const } };
    expect(autoPoolAuthStatus([signedOut, signedIn])).toBe("authenticated");
    expect(autoPoolAuthStatus([signedOut, signedOut])).toBe("unauthenticated");
    expect(autoPoolAuthStatus([signedOut, unknown])).toBe("unknown");
  });

  it("filters on a spent session window without ordering by it", () => {
    const spent = claudeMember("alice", [
      window("five_hour", "session", 100, 1),
      window("seven_day", "weekly", 10, 24),
    ]).read();
    expect(autoPoolHeadroom(spent, OPUS, NOW)).toBeUndefined();

    const busy = claudeMember("busy", [
      window("five_hour", "session", 95, 1),
      window("seven_day", "weekly", 50, 24),
    ]).read();
    expect(autoPoolHeadroom(busy, OPUS, NOW)?.binding.kind).toBe("weekly");
  });

  it("routes Fable away from an account whose Fable allowance is spent, but keeps Opus there", () => {
    const alice = claudeMember("alice", [
      window("seven_day", "weekly", 20, 24),
      window("seven_day_fable", "weekly", 100, 24),
    ]).read();
    const sam = claudeMember("sam", [window("seven_day", "weekly", 20, 72)]).read();
    const choose = (model: string) =>
      autoPoolChoose(
        [alice, sam].flatMap((snapshot, order) => {
          const headroom = claudeAutoHeadroom(snapshot, { instanceId: poolId, model }, NOW);
          return headroom === undefined ? [] : [{ snapshot, headroom, running: 0, order }];
        }),
        { maxRunningTurns: 3 },
      )?.snapshot.instanceId;
    expect(choose(FABLE)).toBe("sam");
    expect(choose(OPUS)).toBe("alice");
    expect(
      claudeAutoHeadroom(
        { ...alice, auth: { ...alice.auth, type: "apiKey" } },
        { instanceId: poolId, model: OPUS },
        NOW,
      ),
    ).toBeUndefined();
  });

  it("reports the pool's account and Fable weekly windows as separate rows", () => {
    const members = [
      claudeMember("alice", [
        window("seven_day", "weekly", 80, 24),
        window("seven_day_fable", "weekly", 100, 24),
        window("monthly_spend", "monthly", 50),
      ]).read(),
      claudeMember("sam", [
        window("seven_day", "weekly", 30, 72),
        window("seven_day_fable", "weekly", 40, 72),
      ]).read(),
    ];
    const auto: ServerProvider = { ...members[0]!, instanceId: poolId };
    const report = providerUsageReport([auto, ...members], iso(0), {
      pools: [
        { instanceId: poolId, memberIds: members.map((m) => m.instanceId), maxRunningTurns: 3 },
      ],
    });
    const [entry] = report.instances;
    expect(report.instances).toHaveLength(1);
    expect(
      entry!.windows.map(({ id, remainingPercent, resetsAt }) => ({
        id,
        remainingPercent,
        resetsAt,
      })),
    ).toEqual([
      { id: "pool:weekly:seven_day", remainingPercent: 70, resetsAt: iso(24) },
      { id: "pool:weekly:seven_day_fable", remainingPercent: 60, resetsAt: iso(24) },
    ]);
    expect(entry!.members?.map((member) => member.instanceId)).toEqual(["alice", "sam"]);
  });
});

const SESSION = "5d3c1f0e-0b7a-4c55-9a1e-3f2d6c8b9a10";
const PROJECT = "-workspace";
const cwd = "/workspace";
const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd,
});
const selection = { instanceId: poolId, model: SONNET, options: [{ id: "effort", value: "high" }] };

type Turn = "limit" | "pause" | "done";
type Record =
  | { readonly type: "open"; readonly member: string; readonly resume: string | undefined }
  | { readonly type: "offer"; readonly member: string; readonly text: string }
  | { readonly type: "interrupt"; readonly member: string }
  | { readonly type: "close"; readonly member: string };

const frame = (value: unknown) => value as SDKMessage;
const promptText = (message: SDKUserMessage) => {
  const content = message.message.content;
  return typeof content === "string"
    ? content
    : content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("");
};

const harness = Effect.fn("ClaudeAutoProvider.test.harness")(function* (turns: {
  readonly alice: readonly Turn[];
  readonly sam: readonly Turn[];
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-claude-auto-" });
  const homes = { alice: path.join(root, "alice"), sam: path.join(root, "sam") };
  const transcript = (member: keyof typeof homes) =>
    path.join(homes[member], "projects", PROJECT, `${SESSION}.jsonl`);
  const records: Record[] = [];
  const accounts = {
    // Alice's weekly allowance resets first, so new threads start there.
    alice: claudeMember("alice", [window("seven_day", "weekly", 20, 24)]),
    sam: claudeMember("sam", [window("seven_day", "weekly", 20, 72)]),
  };
  accounts.alice.onRefresh((current) => ({
    ...current,
    usageLimits: { checkedAt: iso(1), windows: [window("seven_day", "weekly", 100, 24)] },
  }));
  const appendLine = (member: keyof typeof homes, line: unknown) =>
    fs
      .makeDirectory(path.dirname(transcript(member)), { recursive: true })
      .pipe(
        Effect.andThen(
          fs.writeFileString(transcript(member), `${JSON.stringify(line)}\n`, { flag: "a" }),
        ),
      );
  const remaining = { alice: [...turns.alice], sam: [...turns.sam] };
  const services = (yield* Effect.context<
    | FileSystem.FileSystem
    | Path.Path
    | SecureRandom.SecureRandom
    | IdAllocator.IdAllocatorV2
    | McpProviderSessions.McpProviderSessions
    | ChildProcessSpawner.ChildProcessSpawner
  >()).pipe(Context.omit(Scope.Scope));

  const queryRunner = (
    member: keyof typeof homes,
  ): ClaudeAdapterV2.ClaudeAgentSdkQueryRunnerShape => ({
    allocateSessionId: Effect.succeed(SESSION),
    open: (input) =>
      Effect.gen(function* () {
        records.push({ type: "open", member, resume: input.options.resume });
        const messages = yield* Queue.unbounded<SDKMessage, Cause.Done>();
        const sessionId = input.options.resume ?? input.options.sessionId ?? SESSION;
        const uuid = (suffix: string) =>
          `00000000-0000-4000-8000-${member === "alice" ? "a" : "b"}${suffix}`;
        const result = (turn: Turn) =>
          frame({
            type: "result",
            subtype: "success",
            duration_ms: 1,
            duration_api_ms: 1,
            is_error: turn === "limit",
            num_turns: 1,
            result:
              turn === "limit"
                ? "You've hit your weekly limit"
                : turn === "pause"
                  ? ""
                  : `${member} answered`,
            stop_reason: "end_turn",
            total_cost_usd: 0,
            usage: {
              input_tokens: 1,
              output_tokens: 1,
              cache_creation_input_tokens: 0,
              cache_read_input_tokens: 0,
            },
            modelUsage: {},
            permission_denials: [],
            uuid: uuid("00000000004"),
            session_id: sessionId,
            ...(turn === "limit"
              ? { api_error_status: 429, terminal_reason: "api_error" }
              : { terminal_reason: turn === "pause" ? "aborted_streaming" : "completed" }),
          });
        // Claude Code holds a limited turn open until the reset; only Stop ends it.
        let held = false;
        return {
          messages: Stream.fromQueue(messages),
          offer: (message) =>
            Effect.gen(function* () {
              const text = promptText(message);
              records.push({ type: "offer", member, text });
              yield* appendLine(member, {
                type: "user",
                message: { role: "user", content: text },
              }).pipe(Effect.orDie);
              const turn = remaining[member].shift() ?? "done";
              held = turn === "pause";
              yield* Queue.offerAll(messages, [
                frame({
                  type: "system",
                  subtype: "init",
                  uuid: uuid("00000000001"),
                  session_id: sessionId,
                }),
                ...(turn !== "done"
                  ? [
                      frame({
                        type: "rate_limit_event",
                        rate_limit_info: { status: "rejected", rateLimitType: "seven_day" },
                        uuid: uuid("00000000002"),
                        session_id: sessionId,
                      }),
                    ]
                  : [
                      frame({
                        type: "assistant",
                        message: {
                          model: SONNET,
                          id: `msg_${member}`,
                          type: "message",
                          role: "assistant",
                          content: [{ type: "text", text: `${member} answered` }],
                          stop_reason: null,
                          stop_sequence: null,
                          usage: {
                            input_tokens: 1,
                            output_tokens: 1,
                            cache_creation_input_tokens: 0,
                            cache_read_input_tokens: 0,
                          },
                        },
                        parent_tool_use_id: null,
                        uuid: uuid("00000000003"),
                        session_id: sessionId,
                      }),
                    ]),
                ...(held ? [] : [result(turn)]),
              ]);
              if (turn === "done")
                yield* appendLine(member, {
                  type: "assistant",
                  message: { role: "assistant", content: `${member} answered` },
                }).pipe(Effect.orDie);
            }),
          setModel: () => Effect.void,
          applyFlagSettings: () => Effect.void,
          setPermissionMode: () => Effect.void,
          renameSession: () => Effect.void,
          backgroundTasks: () => Effect.succeed(false),
          mcpServerStatus: Effect.succeed([]),
          readMcpResource: () => Effect.die("unexpected MCP resource read"),
          callMcpTool: () => Effect.die("unexpected MCP tool call"),
          listPermissionRules: Effect.succeed([]),
          stopTask: () => Effect.void,
          interrupt: Effect.suspend(() => {
            if (!held) return Effect.void;
            held = false;
            records.push({ type: "interrupt", member });
            return Queue.offer(messages, result("pause")).pipe(Effect.asVoid);
          }),
          close: Effect.sync(() => {
            records.push({ type: "close", member });
          }).pipe(Effect.andThen(Queue.end(messages))),
        } satisfies ClaudeAdapterV2.ClaudeAgentSdkQuerySession;
      }),
    forkSession: () => Effect.die("unused"),
    subagentLaunchToolUseId: () => Effect.succeed(null),
    subagentModel: () => Effect.succeed(null),
    assertComplete: Effect.void,
  });

  const stateDir = path.join(root, "state");
  const router = yield* makeClaudeAutoRouter({
    instanceId: poolId,
    members: () => Effect.succeed([accounts.alice.member(), accounts.sam.member()]),
  }).pipe(Effect.provide(ServerConfig.layerTest(stateDir, stateDir)));
  const adapter = yield* makeClaudeAutoAdapter({
    instanceId: poolId,
    router,
    sessionHomes: (member) =>
      Effect.succeed({
        target: homes[member.instance.instanceId as keyof typeof homes],
        sources: [homes.alice, homes.sam],
      }),
    openNative: (member, session) =>
      Effect.gen(function* () {
        const id = member.instance.instanceId as keyof typeof homes;
        const native = yield* ClaudeAdapterV2.makeClaudeAdapterV2({
          instanceId: poolId,
          settings: decodeClaudeSettings({ homePath: homes[id] }),
          environment: { CLAUDE_CONFIG_DIR: homes[id] },
          attachmentsDir: root,
          fileSystem: fs,
          path,
          random: yield* SecureRandom.SecureRandom,
          idAllocator: yield* IdAllocator.IdAllocatorV2,
          queryRunner: queryRunner(id),
        });
        return yield* native.openSession(session);
      }).pipe(Effect.provideContext(services)),
  });
  const threadId = ThreadId.make("claude-auto-thread");
  const runtime = yield* adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make("claude-auto-session"),
    modelSelection: selection,
    runtimePolicy,
  });
  const terminals = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  yield* runtime.events.pipe(
    Stream.filter((event) => event.type === "turn.terminal"),
    Stream.runForEach((event) => Queue.offer(terminals, event)),
    Effect.forkScoped,
  );
  const turnInput = (providerThread: OrchestrationV2ProviderThread, text: string) => {
    const now = DateTime.makeUnsafe(NOW);
    return {
      appThread: {
        createdBy: "user" as const,
        creationSource: "web" as const,
        id: threadId,
        projectId: ProjectId.make("claude-auto-project"),
        title: "Claude Auto",
        providerInstanceId: poolId,
        modelSelection: selection,
        runtimeMode: "full-access" as const,
        interactionMode: "default" as const,
        branch: null,
        worktreePath: null,
        activeProviderThreadId: providerThread.id,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      },
      threadId,
      runId: RunId.make("run-claude-auto"),
      runOrdinal: 1,
      providerTurnOrdinal: 1,
      attemptId: RunAttemptId.make("attempt-claude-auto"),
      rootNodeId: NodeId.make("node-claude-auto"),
      providerThread,
      message: {
        createdBy: "user" as const,
        creationSource: "web" as const,
        messageId: MessageId.make("message-claude-auto"),
        text,
        attachments: [],
      },
      modelSelection: selection,
      runtimePolicy,
    } satisfies ProviderAdapter.ProviderAdapterV2TurnInput;
  };
  return { fs, homes, transcript, records, runtime, threadId, terminals, turnInput, appendLine };
});

const layer = Layer.mergeAll(
  IdAllocator.layer,
  McpProviderSessions.layer,
  NodeServices.layer,
  NodeCrypto.layer,
);

it.layer(layer)("Claude Auto account switching", (it) => {
  it.effect(
    "moves the conversation to the next account after a confirmed limit, without a second writer",
    () =>
      Effect.gen(function* () {
        const h = yield* harness({ alice: ["limit"], sam: ["done"] });
        const providerThread = yield* h.runtime.ensureThread({
          threadId: h.threadId,
          modelSelection: selection,
          runtimePolicy,
        });
        yield* h.runtime.startTurn(h.turnInput(providerThread, "Fix the failing test."));

        const terminal = yield* Queue.take(h.terminals);
        expect(terminal.type === "turn.terminal" && terminal.status).toBe("completed");
        expect(h.records).toEqual([
          { type: "open", member: "alice", resume: undefined },
          { type: "offer", member: "alice", text: "Fix the failing test." },
          // Alice's process exits before Sam's opens the same session.
          { type: "close", member: "alice" },
          { type: "open", member: "sam", resume: SESSION },
          {
            type: "offer",
            member: "sam",
            text: "Continue from the latest native conversation state. Previous completed tool actions and results are already in the conversation; do not repeat them.",
          },
        ]);
        const samLines = (yield* h.fs.readFileString(h.transcript("sam"))).trim().split("\n");
        expect(samLines.map((line) => JSON.parse(line).message.content)).toEqual([
          "Fix the failing test.",
          "Continue from the latest native conversation state. Previous completed tool actions and results are already in the conversation; do not repeat them.",
          "sam answered",
        ]);
      }),
  );

  it.effect(
    "stops a turn its account holds for a usage limit and continues on another account",
    () =>
      Effect.gen(function* () {
        const h = yield* harness({ alice: ["pause"], sam: ["done"] });
        const providerThread = yield* h.runtime.ensureThread({
          threadId: h.threadId,
          modelSelection: selection,
          runtimePolicy,
        });
        yield* h.runtime.startTurn(h.turnInput(providerThread, "Fix the failing test."));

        const terminal = yield* Queue.take(h.terminals);
        expect(terminal.type === "turn.terminal" && terminal.status).toBe("completed");
        expect(h.records.map((record) => `${record.type}:${record.member}`)).toEqual([
          "open:alice",
          "offer:alice",
          "interrupt:alice",
          "close:alice",
          "open:sam",
          "offer:sam",
        ]);
      }),
  );

  it.effect("keeps the conversation of a thread that moves to Auto from a pool member", () =>
    Effect.gen(function* () {
      const h = yield* harness({ alice: ["done"], sam: [] });
      // The thread ran directly on Sam before Sam joined the pool.
      yield* h.appendLine("sam", { type: "user", message: { role: "user", content: "Earlier." } });
      yield* h.appendLine("sam", {
        type: "assistant",
        message: { role: "assistant", content: "Earlier answer." },
      });
      const created = yield* h.runtime.ensureThread({
        threadId: h.threadId,
        modelSelection: selection,
        runtimePolicy,
      });
      const moved: OrchestrationV2ProviderThread = {
        ...created,
        nativeThreadRef: { ...created.nativeThreadRef!, nativeId: SESSION },
      };
      yield* h.runtime.resumeThread({ providerThread: moved, threadId: h.threadId });
      yield* h.runtime.startTurn(h.turnInput(moved, "Next step."));

      const terminal = yield* Queue.take(h.terminals);
      expect(terminal.type === "turn.terminal" && terminal.status).toBe("completed");
      expect(h.records.filter((record) => record.type !== "close")).toEqual([
        { type: "open", member: "alice", resume: SESSION },
        { type: "offer", member: "alice", text: "Next step." },
      ]);
      const aliceLines = (yield* h.fs.readFileString(h.transcript("alice"))).trim().split("\n");
      expect(aliceLines.map((line) => JSON.parse(line).message.content)).toEqual([
        "Earlier.",
        "Earlier answer.",
        "Next step.",
        "alice answered",
      ]);
    }),
  );
});
