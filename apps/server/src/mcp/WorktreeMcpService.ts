import {
  CommandId,
  type GitCommandError,
  type TaskFolderError,
  MessageId,
  type ProjectId,
  WorktreeMcpFailure,
  type WorktreeMcpContinuationStatus,
  type WorktreeMcpHandoffInput,
  type WorktreeMcpHandoffResult,
  type WorktreeMcpSetupScriptStatus,
  type WorktreeMcpStatusResult,
  type OrchestrationV2AppThread,
  type VcsListRefsInput,
  type VcsListRefsResult,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";

import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as VcsStatusBroadcaster from "../vcs/VcsStatusBroadcaster.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as TaskFolders from "../workspace/TaskFolders.ts";
import type { McpInvocationScope, McpThreadInvocationScope } from "./McpInvocationContext.ts";

export class WorktreeMcpService extends Context.Service<
  WorktreeMcpService,
  {
    readonly handoff: (
      scope: McpInvocationScope,
      input: WorktreeMcpHandoffInput,
    ) => Effect.Effect<WorktreeMcpHandoffResult, WorktreeMcpFailure>;
    readonly status: (
      scope: McpInvocationScope,
    ) => Effect.Effect<WorktreeMcpStatusResult, WorktreeMcpFailure>;
    readonly listRefs: (
      input: Omit<VcsListRefsInput, "cwd"> & {
        readonly worktreePath: string | null;
        readonly workspaceRoot: string;
      },
    ) => Effect.Effect<VcsListRefsResult, GitCommandError | PlatformError.PlatformError>;
  }
>()("t3/mcp/WorktreeMcpService") {}

function failure(code: WorktreeMcpFailure["code"], message: string): WorktreeMcpFailure {
  return new WorktreeMcpFailure({ code, message });
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "object" && error !== null && "message" in error) {
    return String((error as { message: unknown }).message);
  }
  return String(error);
}

const asOperationFailed = (prefix: string) =>
  Effect.mapError((error: unknown) =>
    failure("operation_failed", `${prefix}: ${errorMessage(error)}`),
  );

const make = Effect.gen(function* () {
  const random = yield* SecureRandom.SecureRandom;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const projects = yield* ProjectService.ProjectService;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const gitWorkflow = yield* GitWorkflowService.GitWorkflowService;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const setupScriptRunner = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
  const vcsStatusBroadcaster = yield* VcsStatusBroadcaster.VcsStatusBroadcaster;
  const taskFolders = yield* TaskFolders.TaskFolders;
  // The administrator's limit, fixed for the server's life as it is where turns start.
  const maxRuntimeMode = serverSettings.managedPolicy?.limits?.maxRuntimeMode;

  // Serialize handoffs per thread to avoid creating two worktrees and orphaning one.
  const handoffThreadsInFlight = new Set<string>();

  const requireThreadScope = (scope: McpInvocationScope) =>
    scope.thread === undefined
      ? Effect.fail(
          failure(
            "thread_credential_required",
            "Worktree handoff and status act as the calling T3 thread, so they need an agent running inside T3 Code.",
          ),
        )
      : Effect.succeed(scope as McpThreadInvocationScope);

  const requireCapability = (scope: McpInvocationScope) =>
    scope.capabilities.has("worktree")
      ? Effect.void
      : Effect.fail(
          failure("capability_denied", "This MCP credential does not grant worktree capabilities."),
        );

  const loadThread = (scope: McpThreadInvocationScope) =>
    threadManagement.getThreadRecords(scope.thread.threadId, []).pipe(
      Effect.mapError((error) =>
        error._tag === "OrchestratorProjectionError"
          ? failure("thread_not_found", `Thread '${scope.thread.threadId}' was not found.`)
          : failure(
              "operation_failed",
              `Unable to read thread ${scope.thread.threadId}: ${errorMessage(error)}`,
            ),
      ),
      Effect.filterOrFail(
        (projection) => projection.thread.deletedAt === null,
        () => failure("thread_not_found", `Thread '${scope.thread.threadId}' was not found.`),
      ),
    );

  const loadProject = (scope: McpThreadInvocationScope, projectId: ProjectId) =>
    projects.getById(projectId).pipe(
      asOperationFailed(`Unable to read project ${projectId}`),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              failure(
                "project_not_found",
                `Project '${projectId}' was not found for thread '${scope.thread.threadId}'.`,
              ),
            ),
          onSome: Effect.succeed,
        }),
      ),
    );

  const readDefaultStartFromOrigin = (projectId: ProjectId) =>
    serverSettings.getSettings.pipe(
      Effect.map(
        (settings) =>
          resolveProjectSettings(settings, projectId).settings.newWorktreesStartFromOrigin,
      ),
      asOperationFailed("Unable to read server settings"),
    );

  const handoffIds = (scope: McpThreadInvocationScope) =>
    random.uuidV4().pipe(
      Effect.map((uuid) => {
        const part = (kind: string, operation: string) =>
          [kind, "mcp", encodeURIComponent(scope.thread.providerSessionId), operation, uuid].join(
            ":",
          );
        return {
          commandId: CommandId.make(part("command", "worktree-handoff")),
          continuationMessageId: MessageId.make(part("message", "worktree-continuation")),
        };
      }),
      Effect.orDie,
    );

  const bindWorktree = Effect.fn("WorktreeMcpService.bindWorktree")(function* (
    scope: McpThreadInvocationScope,
    ids: { readonly commandId: CommandId; readonly continuationMessageId: MessageId },
    worktree: { readonly path: string; readonly refName: string },
    expectedWorktreePath: string | null,
    options: {
      readonly continuationPrompt: string | undefined;
      readonly exclusiveWorktree?: { readonly projectId: ProjectId };
      readonly localOnly?: boolean;
    },
  ) {
    const { continuationPrompt, exclusiveWorktree, localOnly } = options;
    const update = {
      type: "thread.metadata.update" as const,
      commandId: ids.commandId,
      threadId: scope.thread.threadId,
      branch: worktree.refName,
      worktreePath: worktree.path,
      expectedWorktreePath,
      // Binding detaches the session and ends the caller, so commit the continuation with it.
      ...(continuationPrompt === undefined
        ? {}
        : { continuation: { messageId: ids.continuationMessageId, text: continuationPrompt } }),
    };
    const bound = yield* threadManagement
      .dispatch(exclusiveWorktree === undefined ? update : { ...update, exclusiveWorktree })
      .pipe(
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause as Cause.Cause<never>);
          const error = Option.getOrUndefined(Cause.findErrorOption(cause));
          if (error?._tag === "OrchestratorCommandPreviouslyRejectedError") {
            return Effect.fail(failure("invalid_request", error.detail));
          }
          const detail =
            error?._tag === "OrchestratorDispatchError" && typeof error.cause === "string"
              ? error.cause
              : errorMessage(Cause.squash(cause));
          return Effect.fail(
            failure("operation_failed", `Unable to re-point the thread at the worktree: ${detail}`),
          );
        }),
      );
    yield* vcsStatusBroadcaster
      .refreshStatus(worktree.path, { localOnly: localOnly ?? false })
      .pipe(Effect.ignoreCause({ log: true }), Effect.forkDetach);

    const continuation: WorktreeMcpContinuationStatus =
      continuationPrompt === undefined
        ? { status: "skipped" }
        : {
            status: "scheduled",
            delivery: bound.storedEvents.some(
              ({ event }) => event.type === "run.created" && event.payload.status !== "queued",
            )
              ? "started"
              : "queued",
          };
    return continuation;
  });

  const handoffNote = (continuation: WorktreeMcpContinuationStatus) =>
    continuation.status === "scheduled"
      ? "Handoff recorded. Changing the workspace ends this provider session. The continuation starts the next turn in the worktree with the conversation preserved. The worktree stays after the thread is deleted."
      : "Handoff recorded. Changing the workspace ends this provider session. The next message continues the conversation in the worktree. Set continuationPrompt to resume automatically. The worktree stays after the thread is deleted.";

  const rebindWorktree = Effect.fn("WorktreeMcpService.rebindWorktree")(function* (
    scope: McpThreadInvocationScope,
    thread: OrchestrationV2AppThread,
    projectCwd: string,
    requestedPath: string,
    continuationPrompt: string | undefined,
  ) {
    const wrongRepository = failure(
      "invalid_request",
      `'${requestedPath}' must be an existing Git worktree of the thread's project repository.`,
    );
    const worktreePath = yield* GitVcsDriver.findRegisteredWorktreePath(
      projectCwd,
      path.isAbsolute(requestedPath)
        ? requestedPath
        : path.resolve(thread.worktreePath ?? projectCwd, requestedPath),
    ).pipe(
      Effect.provideService(GitVcsDriver.GitVcsDriver, git),
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.orElseSucceed(() => null),
    );
    if (worktreePath === null) return yield* wrongRepository;

    const realPath = (path: string) =>
      fileSystem.realPath(path).pipe(Effect.mapError(() => wrongRepository));
    const gitPath = (cwd: string, argument: string) =>
      git
        .execute({
          operation: "WorktreeMcpService.rebindWorktree",
          cwd,
          args: ["rev-parse", "--path-format=absolute", argument],
        })
        .pipe(
          Effect.mapError(() => wrongRepository),
          Effect.flatMap(({ stdout }) => realPath(stdout.replace(/\r?\n$/, ""))),
        );
    const [target, root, projectCheckout, projectGitDir, targetGitDir, targetCheckout] =
      yield* Effect.all(
        [
          realPath(worktreePath),
          realPath(projectCwd),
          gitPath(projectCwd, "--show-toplevel"),
          gitPath(projectCwd, "--git-common-dir"),
          gitPath(worktreePath, "--git-common-dir"),
          gitPath(worktreePath, "--show-toplevel"),
        ],
        { concurrency: "unbounded" },
      );
    if (target === root || target === projectCheckout) {
      return yield* failure("invalid_request", "Choose a worktree outside the project checkout.");
    }
    if (targetGitDir !== projectGitDir || targetCheckout !== target) return yield* wrongRepository;
    if (
      thread.worktreePath !== null &&
      (yield* fileSystem
        .realPath(thread.worktreePath)
        .pipe(Effect.orElseSucceed(() => thread.worktreePath))) === target
    ) {
      return yield* failure(
        "invalid_request",
        `This thread is already bound to '${worktreePath}'.`,
      );
    }

    const status = yield* git
      .execute({
        operation: "WorktreeMcpService.rebindWorktree.status",
        cwd: worktreePath,
        args: [
          "-c",
          "core.fsmonitor=false",
          "status",
          "--porcelain",
          "-z",
          "--untracked-files=normal",
          "--ignore-submodules=none",
        ],
        env: { GIT_OPTIONAL_LOCKS: "0" },
        maxOutputBytes: 4096,
      })
      .pipe(asOperationFailed(`Unable to read Git status for '${worktreePath}'`));
    if (status.stdoutTruncated || status.stdout.length > 0) {
      return yield* failure(
        "invalid_request",
        `Worktree '${worktreePath}' has uncommitted changes. Choose a clean worktree.`,
      );
    }
    const head = yield* git
      .execute({
        operation: "WorktreeMcpService.rebindWorktree.head",
        cwd: worktreePath,
        args: ["rev-parse", "--symbolic-full-name", "HEAD"],
        allowNonZeroExit: true,
      })
      .pipe(asOperationFailed(`Unable to read the current branch of '${worktreePath}'`));
    const ref = head.stdout.trimEnd();
    if (head.exitCode !== 0 || !ref.startsWith("refs/heads/")) {
      return yield* failure(
        "invalid_request",
        `Worktree '${worktreePath}' must have a local branch checked out.`,
      );
    }
    const branch = ref.slice("refs/heads/".length);

    const ids = yield* handoffIds(scope);
    const continuation = yield* bindWorktree(
      scope,
      ids,
      { path: worktreePath, refName: branch },
      thread.worktreePath,
      { continuationPrompt, exclusiveWorktree: { projectId: thread.projectId }, localOnly: true },
    ).pipe(Effect.uninterruptible);
    return {
      worktreePath,
      branch,
      baseRef: null,
      startedFromOrigin: false,
      setupScript: { status: "skipped" },
      continuation,
      note: handoffNote(continuation),
    } satisfies WorktreeMcpHandoffResult;
  });

  const performHandoff = Effect.fn("WorktreeMcpService.performHandoff")(function* (
    scope: McpThreadInvocationScope,
    input: WorktreeMcpHandoffInput,
  ) {
    const { branch, existingWorktreePath } = input;
    const alreadyInWorktree = (worktreePath: string) =>
      failure(
        "already_in_worktree",
        `Thread '${scope.thread.threadId}' is already attached to worktree '${worktreePath}'.`,
      );

    const projection = yield* loadThread(scope);
    if (existingWorktreePath === undefined && projection.thread.worktreePath !== null) {
      return yield* alreadyInWorktree(projection.thread.worktreePath);
    }
    if (projection.thread.archivedAt !== null) {
      return yield* failure(
        "invalid_request",
        `Thread '${scope.thread.threadId}' is archived and cannot be handed off to a worktree.`,
      );
    }

    const project = yield* loadProject(scope, projection.thread.projectId);
    const projectCwd = project.workspaceRoot;
    if (existingWorktreePath !== undefined) {
      if (
        branch !== undefined ||
        input.baseRef !== undefined ||
        input.startFromOrigin !== undefined ||
        input.runSetupScript !== undefined
      ) {
        return yield* failure(
          "invalid_request",
          "Use existingWorktreePath without branch, baseRef, startFromOrigin, or runSetupScript.",
        );
      }
      return yield* rebindWorktree(
        scope,
        projection.thread,
        projectCwd,
        existingWorktreePath,
        input.continuationPrompt,
      );
    }
    if (branch === undefined)
      return yield* failure(
        "invalid_request",
        "Set branch to create a worktree, or set existingWorktreePath to use an existing worktree.",
      );
    // The agent asks, so its thread's modes decide what of the project's own code runs for it:
    // as they are when each piece is about to run, since the fetch in between can take a while.
    const projectCommandsNow = loadThread(scope).pipe(
      Effect.map(({ thread }) =>
        TaskFolders.projectCommandsForAgent(thread.linkOrigin, thread, maxRuntimeMode),
      ),
      // A thread that cannot be read might be below Full access.
      Effect.orElseSucceed((): TaskFolders.ProjectCommands => "agent-below-full-access"),
    );

    // A project that tracks repositories moves into a task folder: its own folder, which need
    // not be a repository, and a worktree of each tracked one.
    const taskPlan = yield* taskFolders
      .plan({ projectId: project.id })
      .pipe(asOperationFailed("Unable to read the project's repositories"));
    const projectIsRepository = taskPlan === null || taskPlan.projectIsRepository;
    const localStatus = yield* gitWorkflow
      .localStatus({ cwd: projectCwd })
      .pipe(asOperationFailed("Unable to read git status"));
    if (projectIsRepository && !localStatus.isRepo) {
      return yield* failure(
        "invalid_request",
        `Project workspace '${projectCwd}' is not a git repository.`,
      );
    }

    const localBranchNames = projectIsRepository
      ? yield* gitWorkflow
          .listLocalBranchNames(projectCwd)
          .pipe(asOperationFailed("Unable to list branches"))
      : [];
    if (localBranchNames.includes(branch)) {
      const existingRef = yield* gitWorkflow
        .listRefs({ cwd: projectCwd, query: branch, refKind: "local" })
        .pipe(
          Effect.map((result) =>
            result.refs.find((ref) => ref.name === branch && ref.isRemote !== true),
          ),
          Effect.orElseSucceed(() => undefined),
        );
      const checkoutPath = existingRef?.worktreePath ?? null;
      return yield* failure(
        "invalid_request",
        `Branch '${branch}' already exists${
          checkoutPath === null ? "" : ` and is checked out at '${checkoutPath}'`
        }. Choose a different branch name, or delete the existing branch${
          checkoutPath === null ? "" : " and its worktree"
        } first.`,
      );
    }

    let baseRef = input.baseRef;
    if (baseRef === undefined && !projectIsRepository) {
      // Each repository starts from its own default branch.
      baseRef = "HEAD";
    } else if (baseRef === undefined) {
      if (localStatus.refName === null) {
        return yield* failure(
          "invalid_request",
          "Could not determine the current branch of the project workspace (detached HEAD?). Pass baseRef explicitly.",
        );
      }
      baseRef = localStatus.refName;
    }

    const startFromOrigin =
      input.startFromOrigin ?? (yield* readDefaultStartFromOrigin(project.id));

    let worktreeBaseRef = baseRef;
    let baseRefName = baseRef;
    let startedFromOrigin = false;
    if (startFromOrigin && projectIsRepository) {
      const resolvedRemoteBase = yield* gitWorkflow
        .fetchWorktreeBaseCommit({ cwd: projectCwd, refName: baseRef })
        .pipe(asOperationFailed(`Unable to fetch the remote base of '${baseRef}'`));
      if (resolvedRemoteBase !== null) {
        worktreeBaseRef = resolvedRemoteBase.commitSha;
        baseRefName = `refs/remotes/${resolvedRemoteBase.remoteRefName}`;
        startedFromOrigin = true;
      }
    }

    const ids = yield* handoffIds(scope);

    // Mask interruption after worktree creation through setup; binding can detach and interrupt the caller.
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const worktree = yield* restore(
          Effect.suspend(
            (): Effect.Effect<
              {
                readonly path: string;
                readonly refName: string;
                readonly taskFolder: string | null;
              },
              GitCommandError | TaskFolderError
            > =>
              taskPlan === null
                ? gitWorkflow
                    .createWorktree({
                      cwd: projectCwd,
                      refName: worktreeBaseRef,
                      newRefName: branch,
                      baseRefName,
                      path: null,
                    })
                    .pipe(Effect.map((created) => ({ ...created.worktree, taskFolder: null })))
                : taskFolders
                    .create({
                      plan: taskPlan,
                      branch,
                      projectStart: { refName: worktreeBaseRef, baseRefName },
                      baseBranch: input.baseRef,
                      startFromOrigin,
                      projectCommands: projectCommandsNow,
                    })
                    .pipe(
                      Effect.map((created) => {
                        // A layout command is handed the branch name, not the fetched commit.
                        startedFromOrigin =
                          !created.laidOutByProject &&
                          (startedFromOrigin || created.repositoriesStartedFromOrigin);
                        return {
                          path: created.taskFolder.projectPath,
                          refName: created.branch,
                          taskFolder: created.taskFolder.root,
                        };
                      }),
                    ),
          ).pipe(asOperationFailed("Unable to create the worktree")),
        );
        const worktreePath = worktree.path;

        const reportFailed = (logMessage: string) =>
          Effect.catchCause((cause: Cause.Cause<unknown>) => {
            const detail = errorMessage(Cause.squash(cause));
            return Effect.logWarning(logMessage, {
              threadId: scope.thread.threadId,
              worktreePath,
              detail,
            }).pipe(Effect.as({ status: "failed", detail } as const));
          });

        const createdTaskFolder = worktree.taskFolder;
        const removeCreatedWorktree = Effect.suspend(
          (): Effect.Effect<unknown, GitCommandError | TaskFolderError> =>
            createdTaskFolder === null
              ? gitWorkflow
                  .removeWorktree({ cwd: projectCwd, path: worktreePath, force: true })
                  .pipe(
                    Effect.andThen(
                      Effect.suspend(() =>
                        gitWorkflow.deleteLocalBranch({
                          cwd: projectCwd,
                          refName: worktree.refName,
                          force: true,
                        }),
                      ),
                    ),
                  )
              : taskFolders.remove({
                  root: createdTaskFolder,
                  force: true,
                  forget: true,
                  projectCommands: projectCommandsNow,
                  deleteBranch: worktree.refName,
                }),
        ).pipe(Effect.ignoreCause({ log: true }));

        const recheckAndBind = Effect.gen(function* () {
          // A concurrent UI binding may have landed during git work; do not orphan a second worktree.
          const recheck = yield* loadThread(scope);
          if (recheck.thread.worktreePath !== null) {
            return yield* alreadyInWorktree(recheck.thread.worktreePath);
          }
          if (recheck.thread.archivedAt !== null) {
            return yield* failure(
              "invalid_request",
              `Thread '${scope.thread.threadId}' was archived while the worktree was being created; the handoff was rolled back.`,
            );
          }
          return yield* bindWorktree(scope, ids, worktree, null, {
            continuationPrompt: input.continuationPrompt,
          });
        }).pipe(
          // Do not remove on interruption alone; the thread binding may already have committed.
          Effect.onError((cause) =>
            Cause.hasInterruptsOnly(cause) ? Effect.void : removeCreatedWorktree,
          ),
        );

        const continuation = yield* recheckAndBind;

        let setupScript: WorktreeMcpSetupScriptStatus = { status: "skipped" };
        // The saved setup script is the project's code too: below Full access it waits for a person.
        const setupWaitsForFullAccess =
          (input.runSetupScript ?? true) &&
          (yield* projectCommandsNow) === "agent-below-full-access";
        if ((input.runSetupScript ?? true) && !setupWaitsForFullAccess) {
          setupScript = yield* setupScriptRunner
            .runForThread({
              threadId: scope.thread.threadId,
              projectId: projection.thread.projectId,
              projectCwd,
              worktreePath,
              project: {
                id: project.id,
                workspaceRoot: project.workspaceRoot,
                scripts: project.scripts,
              },
            })
            .pipe(
              Effect.map((result): WorktreeMcpSetupScriptStatus =>
                result.status === "started"
                  ? {
                      status: "started",
                      scriptName: result.scriptName,
                      terminalId: result.terminalId,
                    }
                  : result.status === "skipped-for-link"
                    ? { status: "skipped" }
                    : { status: "no-script" },
              ),
              reportFailed("worktree handoff setup script failed"),
            );
        }

        const result: WorktreeMcpHandoffResult = {
          worktreePath,
          branch: worktree.refName,
          baseRef,
          startedFromOrigin,
          setupScript,
          continuation,
          note: `${handoffNote(continuation)}${
            setupWaitsForFullAccess
              ? " Project setup scripts do not run for a handoff from a thread below Full access."
              : ""
          }`,
        };
        return result;
      }),
    );
  });

  const handoff: WorktreeMcpService["Service"]["handoff"] = Effect.fn("WorktreeMcpService.handoff")(
    function* (callerScope, input) {
      yield* requireCapability(callerScope);
      const scope = yield* requireThreadScope(callerScope);
      // Acquire and register cleanup atomically so cancellation cannot strand the guard until restart.
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.suspend(() => {
          if (handoffThreadsInFlight.has(scope.thread.threadId)) {
            return Effect.fail(
              failure(
                "handoff_in_progress",
                `A worktree handoff is already in progress for thread '${scope.thread.threadId}'.`,
              ),
            );
          }
          handoffThreadsInFlight.add(scope.thread.threadId);
          return restore(performHandoff(scope, input)).pipe(
            Effect.ensuring(
              Effect.sync(() => handoffThreadsInFlight.delete(scope.thread.threadId)),
            ),
          );
        }),
      );
    },
  );

  const status: WorktreeMcpService["Service"]["status"] = Effect.fn("WorktreeMcpService.status")(
    function* (callerScope) {
      yield* requireCapability(callerScope);
      const scope = yield* requireThreadScope(callerScope);
      const projection = yield* loadThread(scope);
      const project = yield* loadProject(scope, projection.thread.projectId);

      const defaultStartFromOrigin = yield* readDefaultStartFromOrigin(project.id);

      const result: WorktreeMcpStatusResult = {
        attached: projection.thread.worktreePath !== null,
        worktreePath: projection.thread.worktreePath,
        branch: projection.thread.branch,
        projectWorkspaceRoot: project.workspaceRoot,
        defaultStartFromOrigin,
      };
      return result;
    },
  );

  const listRefs: WorktreeMcpService["Service"]["listRefs"] = Effect.fn(
    "WorktreeMcpService.listRefs",
  )(function* ({ worktreePath, workspaceRoot, ...refs }) {
    // Storage cleanup removes a worktree but keeps its path on the thread.
    const cwd =
      worktreePath !== null && (yield* fileSystem.exists(worktreePath))
        ? worktreePath
        : workspaceRoot;
    return yield* gitWorkflow.listRefs({ ...refs, cwd });
  });

  return WorktreeMcpService.of({ handoff, status, listRefs });
});

export const layer: Layer.Layer<
  WorktreeMcpService,
  never,
  | SecureRandom.SecureRandom
  | FileSystem.FileSystem
  | Path.Path
  | ThreadManagementService.ThreadManagementService
  | ProjectService.ProjectService
  | ServerSettings.ServerSettingsService
  | GitWorkflowService.GitWorkflowService
  | GitVcsDriver.GitVcsDriver
  | ProjectSetupScriptRunner.ProjectSetupScriptRunner
  | VcsStatusBroadcaster.VcsStatusBroadcaster
  | TaskFolders.TaskFolders
> = Layer.effect(WorktreeMcpService, make);
