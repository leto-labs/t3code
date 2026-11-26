import { describe, expect, it, vi } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { makeSubagentChildThread } from "@t3tools/provider-core/server/subagentProjection";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import {
  CommandId,
  EnvironmentId,
  EventId,
  NodeId,
  type OrchestrationV2ThreadProjection,
  type Project,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnItemId,
  WorktreeMcpHandoffInput,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";

import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as ServerConfig from "../config.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import {
  OrchestratorV2,
  OrchestratorDispatchError,
  OrchestratorProjectionError,
} from "../orchestration-v2/Orchestrator.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as VcsStatusBroadcaster from "../vcs/VcsStatusBroadcaster.ts";
import type * as McpInvocationContext from "./McpInvocationContext.ts";
import * as TaskFolders from "../workspace/TaskFolders.ts";
import * as WorktreeMcpService from "./WorktreeMcpService.ts";

const environmentId = EnvironmentId.make("environment-worktree-test");
const threadId = ThreadId.make("thread-worktree-test");
const projectId = ProjectId.make("project-worktree-test");
const workspaceRoot = "/repo/project";

const makeScope = (
  capabilities: ReadonlySet<McpInvocationContext.McpCapability>,
  callerId = threadId,
): McpInvocationContext.McpInvocationScope => ({
  environmentId,
  requestNamespace: "provider-session-worktree-test",
  thread: {
    threadId: callerId,
    providerSessionId: "provider-session-worktree-test",
    providerInstanceId: ProviderInstanceId.make("claudeAgent"),
  },
  client: undefined,
  capabilities,
  issuedAt: 1,
});

interface ThreadFixture {
  readonly branch?: string | null;
  readonly worktreePath?: string | null;
  readonly archivedAt?: string | null;
  readonly deletedAt?: string | null;
}

const makeProjection = (overrides: ThreadFixture = {}): OrchestrationV2ThreadProjection =>
  ({
    thread: {
      id: threadId,
      projectId,
      title: "Worktree test thread",
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      archivedAt: null,
      deletedAt: null,
      ...overrides,
    },
  }) as OrchestrationV2ThreadProjection;

const project: Project = {
  id: projectId,
  title: "Worktree test project",
  workspaceRoot,
  defaultModelSelection: null,
  scripts: [],
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
  deletedAt: null,
} as Project;

interface HarnessOptions {
  readonly git?: Partial<GitWorkflowService.GitWorkflowService["Service"]>;
  readonly thread?: ThreadFixture | null;
  readonly threadReadError?: "projection" | "dispatch";
  readonly capabilities?: ReadonlySet<McpInvocationContext.McpCapability>;
  readonly currentBranch?: string | null;
  readonly notARepo?: boolean;
  readonly newWorktreesStartFromOrigin?: boolean;
  readonly projectNewWorktreesStartFromOrigin?: boolean;
  readonly setupScript?: "started" | "no-script" | "fails" | "dies";
  readonly dispatchFails?: boolean;
  readonly dispatchDies?: boolean;
  readonly dispatchInterrupts?: boolean;
  readonly dispatchGate?: Effect.Effect<void>;
  readonly threadAttachedOnRecheck?: boolean;
  readonly threadArchivedOnRecheck?: boolean;
  readonly threadReadFailsOnRecheck?: boolean;
  readonly projectMissing?: boolean;
  readonly projectReadFails?: boolean;
  readonly existingBranchWorktreePath?: string | null;
  readonly createWorktreeFails?: boolean;
  readonly fetchWorktreeBaseFails?: boolean;
  readonly worktreeBaseMissing?: boolean;
  readonly removeWorktreeFails?: boolean;
  readonly deleteLocalBranchFails?: boolean;
  readonly createWorktreeGate?: Effect.Effect<void>;
  readonly existsFails?: boolean;
}

const makeHarness = (options: HarnessOptions = {}) => {
  const thread = options.thread === undefined ? {} : options.thread;
  const scope = makeScope(options.capabilities ?? new Set(["preview", "worktree"]));
  const dispatch = vi.fn((_: unknown) =>
    (options.dispatchGate ?? Effect.void).pipe(
      Effect.andThen(
        options.dispatchInterrupts
          ? (Effect.failCause(Cause.interrupt()) as never)
          : options.dispatchDies
            ? Effect.die(new Error("dispatch defect"))
            : options.dispatchFails
              ? (Effect.fail("simulated dispatch failure") as never)
              : Effect.succeed({ sequence: 1, storedEvents: [] }),
      ),
    ),
  );
  const listLocalBranchNames = vi.fn((_: string) =>
    Effect.succeed(
      options.existingBranchWorktreePath === undefined
        ? ["dev"]
        : ["dev", "feature/taken", "feature/taken-idle"],
    ),
  );
  const getThreadRecords = vi.fn((id: ThreadId) => {
    if (options.threadReadError === "dispatch") {
      return Effect.fail(
        new OrchestratorDispatchError({
          commandId: CommandId.make("command:test:read"),
          commandType: "thread.metadata.update",
        }),
      ) as never;
    }
    if (options.threadReadFailsOnRecheck === true && getThreadRecords.mock.calls.length > 1) {
      return Effect.fail(
        new OrchestratorDispatchError({
          commandId: CommandId.make("command:test:recheck"),
          commandType: "thread.metadata.update",
        }),
      ) as never;
    }
    if (
      options.threadAttachedOnRecheck === true &&
      getThreadRecords.mock.calls.length > 1 &&
      thread !== null
    ) {
      return Effect.succeed(
        makeProjection({ ...thread, worktreePath: "/worktrees/project/raced" }),
      );
    }
    if (
      options.threadArchivedOnRecheck === true &&
      getThreadRecords.mock.calls.length > 1 &&
      thread !== null
    ) {
      return Effect.succeed(makeProjection({ ...thread, archivedAt: "2026-01-02T00:00:00.000Z" }));
    }
    return id === threadId && thread !== null
      ? Effect.succeed(makeProjection(thread))
      : Effect.fail(new OrchestratorProjectionError({ threadId: id }));
  });
  const getById = vi.fn((id: ProjectId) =>
    options.projectReadFails
      ? (Effect.fail("simulated project read failure") as never)
      : Effect.succeed(
          id === projectId && options.projectMissing !== true
            ? Option.some(project)
            : Option.none(),
        ),
  );
  const removeWorktree = vi.fn((_: unknown) =>
    options.removeWorktreeFails
      ? (Effect.fail("simulated worktree removal failure") as never)
      : Effect.void,
  );
  const deleteLocalBranch = vi.fn((_: unknown) =>
    options.deleteLocalBranchFails
      ? (Effect.fail("simulated local branch deletion failure") as never)
      : Effect.void,
  );
  const fetchWorktreeBaseCommit = vi.fn((_: unknown) =>
    options.fetchWorktreeBaseFails
      ? (Effect.fail("simulated fetch failure") as never)
      : options.worktreeBaseMissing
        ? Effect.succeed(null)
        : Effect.succeed({ commitSha: "abc123", remoteRefName: "origin/dev" }),
  );
  const createWorktree = vi.fn(
    (input: { readonly newRefName?: string | undefined; readonly path: string | null }) =>
      options.createWorktreeFails
        ? (Effect.fail("simulated worktree creation failure") as never)
        : (options.createWorktreeGate ?? Effect.void).pipe(
            Effect.andThen(
              Effect.succeed({
                worktree: {
                  path: input.path ?? `/worktrees/project/${input.newRefName}`,
                  refName: input.newRefName ?? "detached",
                },
              }),
            ),
          ),
  );
  const listRefs = vi.fn((input: { readonly cwd: string; readonly query?: string | undefined }) =>
    Effect.succeed({
      refs:
        options.existingBranchWorktreePath === undefined
          ? []
          : [
              {
                name: input.query ?? "",
                current: false,
                isDefault: false,
                worktreePath: options.existingBranchWorktreePath,
              },
            ],
      isRepo: true,
      hasPrimaryRemote: true,
      nextCursor: null,
      totalCount: options.existingBranchWorktreePath === undefined ? 0 : 1,
    }),
  );
  const localStatus = vi.fn((_: unknown) =>
    Effect.succeed({
      isRepo: options.notARepo !== true,
      hasPrimaryRemote: true,
      isDefaultRef: false,
      refName: options.currentBranch === undefined ? "dev" : options.currentBranch,
      hasWorkingTreeChanges: false,
      workingTree: { files: [], insertions: 0, deletions: 0 },
    }),
  );
  const refreshStatus = vi.fn((_: string) => Effect.die("refreshStatus stub"));
  const runForThread = vi.fn((input: { readonly worktreePath: string }) => {
    switch (options.setupScript ?? "started") {
      case "no-script":
        return Effect.succeed({ status: "no-script" } as const);
      case "dies":
        return Effect.die(new Error("setup runner defect"));
      case "fails":
        return Effect.fail(
          new ProjectSetupScriptRunner.ProjectSetupScriptProjectNotFoundError({
            threadId,
            worktreePath: input.worktreePath,
          }),
        );
      default:
        return Effect.succeed({
          status: "started",
          async: false,
          scriptId: "setup",
          scriptName: "Setup",
          scriptCommand: "vp install",
          terminalId: "setup-terminal",
          cwd: input.worktreePath,
        } as const);
    }
  });

  const layer = WorktreeMcpService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          dispatch,
          getThreadRecords,
        } satisfies Partial<ThreadManagementService.ThreadManagementService["Service"]>),
        Layer.mock(ProjectService.ProjectService)({
          getById,
        } satisfies Partial<ProjectService.ProjectService["Service"]>),
        ServerSettings.layerTest({
          newWorktreesStartFromOrigin: options.newWorktreesStartFromOrigin ?? false,
          ...(options.projectNewWorktreesStartFromOrigin === undefined
            ? {}
            : {
                projectSettingsOverrides: {
                  [projectId]: {
                    newWorktreesStartFromOrigin: options.projectNewWorktreesStartFromOrigin,
                  },
                },
              }),
        }),
        Layer.mock(GitVcsDriver.GitVcsDriver)({}),
        Layer.mock(GitWorkflowService.GitWorkflowService)({
          listRefs,
          listLocalBranchNames,
          localStatus,
          fetchWorktreeBaseCommit,
          createWorktree,
          removeWorktree,
          deleteLocalBranch,
          ...options.git,
        } satisfies Partial<GitWorkflowService.GitWorkflowService["Service"]>),
        Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({
          runForThread,
        } satisfies Partial<ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]>),
        Layer.mock(TaskFolders.TaskFolders)({
          plan: () => Effect.succeed(null),
          find: () => Effect.succeed(null),
        }),
        Layer.mock(VcsStatusBroadcaster.VcsStatusBroadcaster)({
          refreshStatus,
        } satisfies Partial<VcsStatusBroadcaster.VcsStatusBroadcaster["Service"]>),
        NodeServices.layer,
        NodeCrypto.layer,
        options.existsFails
          ? FileSystem.layerNoop({
              exists: (path) =>
                Effect.fail(
                  PlatformError.systemError({
                    _tag: "PermissionDenied",
                    module: "FileSystem",
                    method: "exists",
                    pathOrDescriptor: path,
                  }),
                ),
            })
          : Layer.empty,
      ),
    ),
  );

  return {
    layer,
    scope,
    listRefs,
    dispatch,
    fetchWorktreeBaseCommit,
    createWorktree,
    removeWorktree,
    deleteLocalBranch,
    localStatus,
    runForThread,
  };
};

const expectTypedFailure = (exit: Exit.Exit<unknown, unknown>, expected: object): void => {
  if (!Exit.isFailure(exit)) {
    expect.fail(`Expected a failure exit, got: ${JSON.stringify(exit)}`);
  }
  const reason = exit.cause.reasons[0];
  if (reason?._tag !== "Fail") {
    expect.fail(`Expected a typed Fail cause, got: ${reason?._tag ?? "no reason"}`);
  }
  expect(reason.error).toMatchObject(expected);
};

// Resolve once so calls share the in-flight guard.
const resolveService = (harness: ReturnType<typeof makeHarness>) =>
  WorktreeMcpService.WorktreeMcpService.pipe(Effect.provide(harness.layer));

const runHandoff = (
  harness: ReturnType<typeof makeHarness>,
  input: Parameters<WorktreeMcpService.WorktreeMcpService["Service"]["handoff"]>[1],
) =>
  Effect.gen(function* () {
    const service = yield* WorktreeMcpService.WorktreeMcpService;
    return yield* service.handoff(harness.scope, input);
  }).pipe(Effect.provide(harness.layer));

const runStatus = (harness: ReturnType<typeof makeHarness>) =>
  Effect.gen(function* () {
    const service = yield* WorktreeMcpService.WorktreeMcpService;
    return yield* service.status(harness.scope);
  }).pipe(Effect.provide(harness.layer));

const decodeHandoffInput = Schema.decodeUnknownEffect(WorktreeMcpHandoffInput);

const rebindGitLayer = GitVcsDriver.layer.pipe(
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "handoff-rebind-" })),
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
const rebindInstanceId = ProviderInstanceId.make("codex");
const rebindRuntime = ProviderReplayHarness.layerWithRegistry(
  { name: "existing-worktree-handoff" },
  ProviderAdapterRegistry.layerSingle({
    instanceId: rebindInstanceId,
    driver: ProviderDriverKind.make("codex"),
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: () => Effect.die("provider execution is disabled in handoff tests"),
  }),
  { runEffectWorker: false },
);

const makeRebindFixture = Effect.fn("makeRebindFixture")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const orchestrator = yield* OrchestratorV2;
  const repo = yield* checkpointWorkspace("handoff-project");
  const worktrees = yield* fs.realPath(
    yield* fs.makeTempDirectoryScoped({ prefix: "handoff-worktrees-" }),
  );
  const source = path.join(worktrees, "source worktree");
  const destination = path.join(worktrees, "destination worktree");
  const gitAt = (cwd: string, ...args: ReadonlyArray<string>) =>
    git.execute({ operation: "handoff-rebind-test", cwd, args });
  yield* gitAt(repo, "worktree", "add", "-b", "source", source);
  yield* gitAt(repo, "worktree", "add", "-b", "destination", destination);

  const createThread = (id: ThreadId, branch: string, worktreePath: string) =>
    orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${id}`),
      threadId: id,
      projectId,
      title: "Worktree handoff",
      createdBy: "user",
      creationSource: "web",
      modelSelection: { instanceId: rebindInstanceId, model: "test" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch,
      worktreePath,
    });
  yield* createThread(threadId, "source", source);
  const createChild = Effect.fn("makeRebindFixture.createChild")(function* (parentId: ThreadId) {
    const sink = yield* EventSink.EventSinkV2;
    const { thread } = yield* orchestrator.getThreadRecords(parentId, []);
    const now = yield* DateTime.now;
    const child = makeSubagentChildThread({
      parentThread: { ...thread, worktreePath: destination, branch: "destination" },
      childThreadId: ThreadId.make(`child:${parentId}`),
      parentNodeId: NodeId.make(`task:${parentId}`),
      activeProviderThreadId: null,
      providerInstanceId: rebindInstanceId,
      modelSelection: thread.modelSelection,
      title: "Finished task",
      now,
      createdBy: "agent",
      creationSource: "mcp",
    });
    yield* sink.write({
      events: [
        {
          id: EventId.make(`create:${child.id}`),
          type: "thread.created",
          threadId: child.id,
          occurredAt: now,
          payload: { ...child, settledAt: now },
        },
      ],
    });
  });
  const service = yield* WorktreeMcpService.WorktreeMcpService.pipe(
    Effect.provide(
      WorktreeMcpService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(ThreadManagementService.ThreadManagementService)({
              getThreadRecords: orchestrator.getThreadRecords,
              dispatch: orchestrator.dispatch,
            }),
            Layer.mock(ProjectService.ProjectService)({
              getById: () => Effect.succeedSome({ ...project, workspaceRoot: repo }),
            }),
            Layer.mock(GitWorkflowService.GitWorkflowService)({}),
            Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({}),
            ServerSettings.layerTest({}),
            Layer.mock(TaskFolders.TaskFolders)({}),
            Layer.mock(VcsStatusBroadcaster.VcsStatusBroadcaster)({
              refreshStatus: () =>
                Effect.succeed({
                  isRepo: true,
                  hasPrimaryRemote: false,
                  isDefaultRef: false,
                  refName: "destination",
                  hasWorkingTreeChanges: false,
                  workingTree: { files: [], insertions: 0, deletions: 0 },
                  hasUpstream: false,
                  aheadCount: 0,
                  behindCount: 0,
                  pr: null,
                }),
            }),
          ),
        ),
      ),
    ),
  );
  return {
    source,
    destination,
    repo,
    fs,
    path,
    gitAt,
    createThread,
    createChild,
    handoff: (
      existingWorktreePath = destination,
      callerId = threadId,
      continuationPrompt?: string,
    ) =>
      service.handoff(makeScope(new Set(["worktree"]), callerId), {
        existingWorktreePath,
        continuationPrompt,
      }),
    readThread: () =>
      orchestrator.getThreadRecords(threadId, []).pipe(Effect.map(({ thread }) => thread)),
  };
});

const withRebindRuntime = Effect.provide(Layer.merge(rebindGitLayer, rebindRuntime));

describe("rebinding to an existing worktree", () => {
  it.live.each(["archived", "calling"] as const)(
    "rebinds without changing Git when the %s thread has a settled child at the destination",
    (owner) =>
      Effect.gen(function* () {
        const fixture = yield* makeRebindFixture();
        if (owner === "archived") {
          const archivedId = ThreadId.make("thread-archived-worktree");
          yield* fixture.createThread(archivedId, "destination", fixture.destination);
          yield* fixture.createChild(archivedId);
          const orchestrator = yield* OrchestratorV2;
          yield* orchestrator.dispatch({
            type: "thread.archive",
            commandId: CommandId.make("archive-occupant"),
            threadId: archivedId,
          });
        } else {
          yield* fixture.createChild(threadId);
        }
        const before = yield* fixture.gitAt(fixture.repo, "worktree", "list", "--porcelain");
        const result = yield* fixture.handoff(`${fixture.destination}${fixture.path.sep}.`);
        expect(result).toMatchObject({
          worktreePath: fixture.destination,
          branch: "destination",
          baseRef: null,
          startedFromOrigin: false,
          setupScript: { status: "skipped" },
        });
        expect(yield* fixture.readThread()).toMatchObject({
          worktreePath: fixture.destination,
          branch: "destination",
        });
        expect((yield* fixture.gitAt(fixture.repo, "worktree", "list", "--porcelain")).stdout).toBe(
          before.stdout,
        );
      }).pipe(withRebindRuntime),
  );

  it.live("refuses the worktree already bound to the caller", () =>
    Effect.gen(function* () {
      const fixture = yield* makeRebindFixture();
      expectTypedFailure(yield* Effect.exit(fixture.handoff(fixture.source)), {
        code: "invalid_request",
        message: expect.stringContaining("This thread is already bound"),
      });
    }).pipe(withRebindRuntime),
  );

  it.live("resolves a relative path from the calling worktree and commits the continuation", () =>
    Effect.gen(function* () {
      const fixture = yield* makeRebindFixture();
      const relative = fixture.path.relative(fixture.source, fixture.destination);
      const prompt = "Run the release tests.";
      const result = yield* fixture.handoff(relative, threadId, prompt);
      expect(result.continuation).toMatchObject({ status: "scheduled" });
      const orchestrator = yield* OrchestratorV2;
      const { thread, messages, runs } = yield* orchestrator.getThreadRecords(threadId, [
        "messages",
        "runs",
      ]);
      expect(thread).toMatchObject({ worktreePath: fixture.destination, branch: "destination" });
      expect(messages).toContainEqual(
        expect.objectContaining({ text: prompt, runId: runs[0]?.id }),
      );
      expect(runs).toHaveLength(1);
    }).pipe(withRebindRuntime),
  );

  it.live("refuses the project checkout", () =>
    Effect.gen(function* () {
      const fixture = yield* makeRebindFixture();
      expectTypedFailure(yield* Effect.exit(fixture.handoff(fixture.repo)), {
        code: "invalid_request",
        message: expect.stringContaining("outside the project checkout"),
      });
    }).pipe(withRebindRuntime),
  );

  it.live("refuses a worktree of another repository", () =>
    Effect.gen(function* () {
      const fixture = yield* makeRebindFixture();
      const other = yield* checkpointWorkspace("another-project");
      const foreignWorktree = fixture.path.join(
        fixture.path.dirname(fixture.destination),
        "foreign",
      );
      yield* fixture.gitAt(other, "worktree", "add", "-b", "foreign", foreignWorktree);
      expectTypedFailure(yield* Effect.exit(fixture.handoff(foreignWorktree)), {
        code: "invalid_request",
        message: expect.stringContaining("thread's project repository"),
      });
    }).pipe(withRebindRuntime),
  );

  it.live("refuses a dirty worktree even if Git hides untracked files by default", () =>
    Effect.gen(function* () {
      const fixture = yield* makeRebindFixture();
      yield* fixture.gitAt(fixture.destination, "config", "status.showUntrackedFiles", "no");
      yield* fixture.fs.writeFileString(
        fixture.path.join(fixture.destination, "untracked"),
        "local work",
      );
      expectTypedFailure(yield* Effect.exit(fixture.handoff()), {
        code: "invalid_request",
        message: expect.stringContaining("uncommitted changes"),
      });
    }).pipe(withRebindRuntime),
  );

  it.live("refuses a worktree with a detached HEAD", () =>
    Effect.gen(function* () {
      const fixture = yield* makeRebindFixture();
      yield* fixture.gitAt(fixture.destination, "switch", "--detach");
      expectTypedFailure(yield* Effect.exit(fixture.handoff()), {
        code: "invalid_request",
        message: expect.stringContaining("must have a local branch checked out"),
      });
    }).pipe(withRebindRuntime),
  );

  it.live("refuses a worktree bound to another non-archived thread", () =>
    Effect.gen(function* () {
      const fixture = yield* makeRebindFixture();
      yield* fixture.createThread(
        ThreadId.make("thread-destination-owner"),
        "destination",
        fixture.destination,
      );
      expectTypedFailure(yield* Effect.exit(fixture.handoff()), {
        code: "invalid_request",
        message: expect.stringContaining("already bound to thread 'thread-destination-owner'"),
      });
      expect(yield* fixture.readThread()).toMatchObject({
        worktreePath: fixture.source,
        branch: "source",
      });
    }).pipe(withRebindRuntime),
  );

  it.live("lets only one thread claim a worktree when handoffs race", () =>
    Effect.gen(function* () {
      const fixture = yield* makeRebindFixture();
      const otherId = ThreadId.make("competing-thread");
      yield* fixture.createThread(otherId, "source", fixture.source);
      const results = yield* Effect.all(
        [
          Effect.exit(fixture.handoff()),
          Effect.exit(fixture.handoff(fixture.destination, otherId)),
        ],
        { concurrency: "unbounded" },
      );
      expect(results.filter(Exit.isSuccess)).toHaveLength(1);
      const rejected = results.find(Exit.isFailure);
      if (rejected === undefined) return expect.fail("Both handoffs succeeded");
      expectTypedFailure(rejected, {
        code: "invalid_request",
        message: expect.stringContaining("already bound to thread"),
      });
    }).pipe(withRebindRuntime),
  );

  it.live.each([
    { origin: "app_owned", status: "waiting", hasLiveItem: false, blocks: true },
    { origin: "provider_native", status: "running", hasLiveItem: true, blocks: true },
    { origin: "provider_native", status: "running", hasLiveItem: false, blocks: false },
  ] as const)(
    "handles a $status $origin child with live item $hasLiveItem",
    ({ origin, status, hasLiveItem, blocks }) =>
      Effect.gen(function* () {
        const fixture = yield* makeRebindFixture();
        const sink = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        const task = NodeId.make("active-child");
        yield* sink.write({
          events: [
            {
              id: EventId.make("event-active-child"),
              type: "subagent.updated",
              threadId,
              nodeId: task,
              occurredAt: now,
              payload: {
                id: task,
                threadId,
                runId: null,
                parentNodeId: NodeId.make("parent"),
                origin,
                createdBy: "agent",
                driver: ProviderDriverKind.make("codex"),
                providerInstanceId: ProviderInstanceId.make("codex"),
                providerThreadId: null,
                childThreadId: origin === "app_owned" ? ThreadId.make("delegated-child") : null,
                nativeTaskRef: null,
                prompt: "Do work",
                title: null,
                model: null,
                status,
                result: null,
                startedAt: now,
                completedAt: null,
                updatedAt: now,
              },
            },
          ],
        });
        if (hasLiveItem) {
          yield* sink.write({
            events: [
              {
                id: EventId.make("event-active-child-item"),
                type: "turn-item.updated",
                threadId,
                occurredAt: now,
                payload: {
                  id: TurnItemId.make("active-child-item"),
                  type: "subagent",
                  threadId,
                  runId: null,
                  nodeId: task,
                  providerThreadId: null,
                  providerTurnId: null,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: 1,
                  status,
                  title: null,
                  startedAt: now,
                  completedAt: null,
                  updatedAt: now,
                  subagentId: task,
                  origin,
                  driver: ProviderDriverKind.make("codex"),
                  providerInstanceId: ProviderInstanceId.make("codex"),
                  childThreadId: null,
                  prompt: "Do work",
                  result: null,
                },
              },
            ],
          });
        }
        const exit = yield* Effect.exit(fixture.handoff());
        if (blocks) {
          expectTypedFailure(exit, {
            code: "invalid_request",
            message: expect.stringContaining("active delegated tasks or subagents"),
          });
        } else {
          expect(Exit.isSuccess(exit)).toBe(true);
        }
        expect(yield* fixture.readThread()).toMatchObject({
          worktreePath: blocks ? fixture.source : fixture.destination,
          branch: blocks ? "source" : "destination",
        });
      }).pipe(withRebindRuntime),
  );
});

describe("t3_worktree_handoff", () => {
  // regression: #22971 a separate send let the detach settle the run before its follow-up existed
  it.effect("commits the continuation with the thread binding", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const result = yield* runHandoff(harness, {
        branch: "feature/continue",
        continuationPrompt: "Keep going.",
      });
      expect(result.continuation).toEqual({ status: "scheduled", delivery: "queued" });
      expect(harness.dispatch).toHaveBeenCalledTimes(1);
      expect(harness.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "thread.metadata.update",
          worktreePath: "/worktrees/project/feature/continue",
          continuation: expect.objectContaining({ text: "Keep going." }),
        }),
      );
    });
  });

  it.effect("uses the fetched commit and remote PR base for an explicit local alias", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const git = yield* GitVcsDriver.GitVcsDriver;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "handoff-remote-base-" });
      const cwd = `${directory}/repo`;
      const remote = `${directory}/remote.git`;
      const worktreePath = `${directory}/worktree`;
      yield* fs.makeDirectory(cwd);
      const command = (at: string, args: string[]) =>
        git.execute({ operation: "handoff-test", cwd: at, args });
      yield* command(cwd, ["init", "-b", "stable"]);
      yield* command(cwd, ["config", "user.name", "Test"]);
      yield* command(cwd, ["config", "user.email", "test@example.test"]);
      yield* command(cwd, ["commit", "--allow-empty", "-m", "remote base"]);
      const remoteHead = (yield* command(cwd, ["rev-parse", "HEAD"])).stdout.trim();
      yield* command(cwd, ["init", "--bare", remote]);
      yield* command(cwd, ["remote", "add", "upstream", remote]);
      yield* command(cwd, ["push", "-u", "upstream", "stable:release"]);
      yield* command(cwd, ["commit", "--allow-empty", "-m", "local only"]);
      const harness = makeHarness({
        git: {
          fetchWorktreeBaseCommit: (input) => git.fetchWorktreeBaseCommit({ ...input, cwd }),
          createWorktree: (input) => git.createWorktree({ ...input, cwd, path: worktreePath }),
        },
      });
      const result = yield* runHandoff(harness, {
        branch: "feature/from-remote",
        baseRef: "stable",
        startFromOrigin: true,
        runSetupScript: false,
      });

      expect(result.worktreePath).toBe(worktreePath);
      expect(result.startedFromOrigin).toBe(true);
      expect(result.setupScript).toEqual({ status: "skipped" });
      expect((yield* command(worktreePath, ["rev-parse", "HEAD"])).stdout.trim()).toBe(remoteHead);
      const preview = yield* git.getReviewDiffPreview({ cwd: worktreePath });
      const comparisonBase = preview.sources.find(
        (source) => source.kind === "branch-range",
      )!.baseRef;
      expect(
        (yield* command(worktreePath, ["rev-parse", `${comparisonBase}^{commit}`])).stdout.trim(),
      ).toBe(remoteHead);
    }).pipe(
      Effect.provide(
        GitVcsDriver.layer.pipe(
          Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "handoff-remote-git-" })),
          Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
        ),
      ),
    ),
  );

  it.effect("fails when the thread is already attached to a worktree", () => {
    const harness = makeHarness({
      thread: { branch: "feature/existing", worktreePath: "/worktrees/project/existing" },
    });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(runHandoff(harness, { branch: "feature/second" }));
      expectTypedFailure(exit, {
        _tag: "WorktreeMcpFailure",
        code: "already_in_worktree",
      });
      expect(harness.createWorktree).not.toHaveBeenCalled();
    });
  });

  it.effect("rejects an existing branch with an actionable error naming its checkout", () => {
    const harness = makeHarness({ existingBranchWorktreePath: "/elsewhere/checkout" });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(runHandoff(harness, { branch: "feature/taken" }));
      if (!Exit.isFailure(exit)) {
        return expect.fail("expected a failure exit");
      }
      const reason = exit.cause.reasons[0];
      expect(reason?._tag).toBe("Fail");
      const error = (reason as { readonly error: { code: string; message: string } }).error;
      expect(error.code).toBe("invalid_request");
      expect(error.message).toContain("feature/taken");
      expect(error.message).toContain("already exists");
      expect(error.message).toContain("/elsewhere/checkout");
      expect(harness.createWorktree).not.toHaveBeenCalled();
      expect(harness.fetchWorktreeBaseCommit).not.toHaveBeenCalled();
    });
  });

  it.effect("does not trip the pre-flight on similarly named branches", () => {
    const harness = makeHarness({ existingBranchWorktreePath: null });
    return Effect.gen(function* () {
      const result = yield* runHandoff(harness, { branch: "feature/take" });
      expect(result.branch).toBe("feature/take");
      expect(harness.createWorktree).toHaveBeenCalledTimes(1);
    });
  });

  it.effect("propagates an interrupted dispatch without rolling back the worktree", () => {
    const harness = makeHarness({ dispatchInterrupts: true });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(runHandoff(harness, { branch: "feature/dispatch-int" }));
      if (!Exit.isFailure(exit)) {
        return expect.fail("expected a failure exit");
      }
      expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      expect(harness.removeWorktree).not.toHaveBeenCalled();
    });
  });

  it.effect("falls back to the local base when no remote holds it", () => {
    const harness = makeHarness({ worktreeBaseMissing: true });
    return Effect.gen(function* () {
      const result = yield* runHandoff(harness, {
        branch: "feature/local-fallback",
        baseRef: "dev",
        startFromOrigin: true,
      });
      expect(harness.createWorktree).toHaveBeenCalledWith(
        expect.objectContaining({ refName: "dev", baseRefName: "dev" }),
      );
      expect(result.startedFromOrigin).toBe(false);
    });
  });

  it.effect("defaults startFromOrigin to the project's override", () => {
    const harness = makeHarness({ projectNewWorktreesStartFromOrigin: true });
    return Effect.gen(function* () {
      const result = yield* runHandoff(harness, { branch: "feature/project-origin" });
      expect(harness.fetchWorktreeBaseCommit).toHaveBeenCalled();
      expect(result.startedFromOrigin).toBe(true);
    });
  });

  it.effect("removes the created worktree when the thread update dies with a defect", () => {
    const harness = makeHarness({ dispatchDies: true });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(runHandoff(harness, { branch: "feature/dispatch-defect" }));
      expectTypedFailure(exit, { _tag: "WorktreeMcpFailure", code: "operation_failed" });
      expect(harness.removeWorktree).toHaveBeenCalledWith({
        cwd: workspaceRoot,
        path: "/worktrees/project/feature/dispatch-defect",
        force: true,
      });
    });
  });

  it.effect("re-checks attachment after creating the worktree and backs out on a race", () => {
    const harness = makeHarness({ threadAttachedOnRecheck: true });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(runHandoff(harness, { branch: "feature/raced" }));
      expectTypedFailure(exit, { _tag: "WorktreeMcpFailure", code: "already_in_worktree" });
      expect(harness.createWorktree).toHaveBeenCalledTimes(1);
      expect(harness.removeWorktree).toHaveBeenCalledWith({
        cwd: workspaceRoot,
        path: "/worktrees/project/feature/raced",
        force: true,
      });
      expect(harness.deleteLocalBranch).toHaveBeenCalledWith({
        cwd: workspaceRoot,
        refName: "feature/raced",
        force: true,
      });
      expect(harness.dispatch).not.toHaveBeenCalled();
    });
  });

  it.effect("removes the created worktree when the recheck read fails", () => {
    const harness = makeHarness({ threadReadFailsOnRecheck: true });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(runHandoff(harness, { branch: "feature/recheck-fails" }));
      expectTypedFailure(exit, { _tag: "WorktreeMcpFailure", code: "operation_failed" });
      expect(harness.removeWorktree).toHaveBeenCalledWith({
        cwd: workspaceRoot,
        path: "/worktrees/project/feature/recheck-fails",
        force: true,
      });
      expect(harness.dispatch).not.toHaveBeenCalled();
    });
  });

  it.effect("backs out when the thread is archived during worktree creation", () => {
    const harness = makeHarness({ threadArchivedOnRecheck: true });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(runHandoff(harness, { branch: "feature/archived-race" }));
      expectTypedFailure(exit, { _tag: "WorktreeMcpFailure", code: "invalid_request" });
      expect(harness.dispatch).not.toHaveBeenCalled();
      expect(harness.removeWorktree).toHaveBeenCalledWith({
        cwd: workspaceRoot,
        path: "/worktrees/project/feature/archived-race",
        force: true,
      });
    });
  });

  it.effect("runs the setup script even when interrupted during the binding dispatch", () =>
    Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      const harness = makeHarness({ dispatchGate: Deferred.await(gate) });

      const fiber = yield* Effect.forkChild(
        runHandoff(harness, {
          branch: "feature/interrupted",
          continuationPrompt: "Keep going in the worktree.",
        }),
      );
      yield* Effect.yieldNow;
      const interruption = yield* Effect.forkChild(Fiber.interrupt(fiber));
      yield* Effect.yieldNow;
      yield* Deferred.succeed(gate, undefined);
      yield* Fiber.await(fiber);
      yield* Fiber.join(interruption);

      expect(harness.dispatch).toHaveBeenCalledTimes(1);
      expect(harness.runForThread).toHaveBeenCalledTimes(1);
    }),
  );

  it.effect("creates the worktree in the managed directory even when a call names a path", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const input = yield* decodeHandoffInput({
        branch: "feature/elsewhere",
        path: "/home/user/.ssh/checkout",
      });
      const result = yield* runHandoff(harness, input);
      expect(harness.createWorktree).toHaveBeenCalledWith(expect.objectContaining({ path: null }));
      expect(result.worktreePath).toBe("/worktrees/project/feature/elsewhere");
    });
  });

  it.effect("releases the per-thread guard after a failed handoff", () => {
    const harness = makeHarness({
      thread: { worktreePath: "/worktrees/project/existing" },
    });
    return Effect.gen(function* () {
      const service = yield* resolveService(harness);

      const first = yield* Effect.exit(
        service.handoff(harness.scope, { branch: "feature/guard-1" }),
      );
      expectTypedFailure(first, { _tag: "WorktreeMcpFailure", code: "already_in_worktree" });
      const second = yield* Effect.exit(
        service.handoff(harness.scope, { branch: "feature/guard-2" }),
      );
      expectTypedFailure(second, { _tag: "WorktreeMcpFailure", code: "already_in_worktree" });
    });
  });

  it.effect("serializes concurrent handoffs for the same thread", () =>
    Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      const harness = makeHarness({ createWorktreeGate: Deferred.await(gate) });

      const service = yield* resolveService(harness);

      const first = yield* Effect.forkChild(
        Effect.exit(service.handoff(harness.scope, { branch: "feature/race-1" })),
      );
      yield* Effect.yieldNow;

      const second = yield* Effect.exit(
        service.handoff(harness.scope, { branch: "feature/race-2" }),
      );
      expectTypedFailure(second, { _tag: "WorktreeMcpFailure", code: "handoff_in_progress" });

      yield* Deferred.succeed(gate, undefined);
      const firstExit = yield* Fiber.join(first);
      expect(Exit.isSuccess(firstExit)).toBe(true);
      expect(harness.createWorktree).toHaveBeenCalledTimes(1);
    }),
  );

  it.effect("reports a setup script defect without failing the handoff", () => {
    const harness = makeHarness({ setupScript: "dies" });
    return Effect.gen(function* () {
      const result = yield* runHandoff(harness, { branch: "feature/setup-dies" });
      expect(result.setupScript).toEqual({ status: "failed", detail: "setup runner defect" });
      expect(harness.dispatch).toHaveBeenCalled();
    });
  });
});

describe("t3_worktree_status", () => {
  it.effect("reports an attached thread's worktree and branch", () => {
    const harness = makeHarness({
      thread: {
        worktreePath: "/worktrees/project/existing",
        branch: "feature/existing",
      },
      projectNewWorktreesStartFromOrigin: true,
    });
    return Effect.gen(function* () {
      const result = yield* runStatus(harness);
      expect(result).toMatchObject({
        attached: true,
        worktreePath: "/worktrees/project/existing",
        branch: "feature/existing",
        defaultStartFromOrigin: true,
      });
    });
  });
});

describe("t3_worktree_list", () => {
  const listRefs = (harness: ReturnType<typeof makeHarness>, worktreePath: string | null) =>
    Effect.gen(function* () {
      const service = yield* WorktreeMcpService.WorktreeMcpService;
      return yield* service.listRefs({ worktreePath, workspaceRoot });
    }).pipe(Effect.provide(harness.layer));

  it.effect("lists refs from the thread's worktree, or the project root once it was removed", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const worktreePath = yield* fs.makeTempDirectoryScoped({ prefix: "worktree-list-" });
      const harness = makeHarness();
      yield* listRefs(harness, worktreePath);
      yield* listRefs(harness, `${worktreePath}/removed`);
      expect(harness.listRefs.mock.calls.map(([input]) => input.cwd)).toEqual([
        worktreePath,
        workspaceRoot,
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("fails instead of falling back when the worktree cannot be checked", () => {
    const harness = makeHarness({ existsFails: true });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(listRefs(harness, "/worktrees/project/unreadable"));
      expectTypedFailure(exit, { _tag: "PlatformError" });
      expect(harness.listRefs).not.toHaveBeenCalled();
    });
  });
});
