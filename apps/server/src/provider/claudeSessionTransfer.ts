import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

// Claude Code names a session's files after its id; anything else could escape the home.
const SESSION_ID = /^[A-Za-z0-9_-]+$/;

interface TranscriptCopy {
  readonly home: string;
  readonly project: string;
  readonly modifiedAt: number;
}

const findCopies = Effect.fnUntraced(function* (home: string, sessionId: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const projects = path.join(home, "projects");
  const copies: TranscriptCopy[] = [];
  for (const project of yield* fileSystem
    .readDirectory(projects)
    .pipe(Effect.orElseSucceed((): string[] => []))) {
    const info = yield* fileSystem
      .stat(path.join(projects, project, `${sessionId}.jsonl`))
      .pipe(Effect.option);
    if (Option.isSome(info) && info.value.type === "File")
      copies.push({
        home,
        project,
        modifiedAt: Option.match(info.value.mtime, {
          onNone: () => 0,
          onSome: (at) => at.getTime(),
        }),
      });
  }
  return copies;
});

const replaceDirectory = Effect.fnUntraced(function* (from: string, to: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  if (!(yield* fileSystem.exists(from))) return;
  yield* fileSystem.remove(to, { recursive: true, force: true });
  yield* fileSystem.copy(from, to);
});

/**
 * Each Claude account keeps its conversations in its own config dir. Before `target` resumes
 * `sessionId`, copy in the most recently written copy from `sources`: the transcript, its
 * subagent and tool-result directory, and the session's task list. The caller has stopped the
 * previous writer. The transcript lands last, by rename, so a partial copy is never resumed.
 */
export const transferClaudeSession = Effect.fn("transferClaudeSession")(function* (input: {
  readonly sessionId: string;
  readonly target: string;
  readonly sources: readonly string[];
}) {
  if (!SESSION_ID.test(input.sessionId)) return;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const { sessionId, target } = input;
  const homes = [...new Set([target, ...input.sources])];
  const copies = (yield* Effect.forEach(homes, (home) => findCopies(home, sessionId))).flat();
  // On a tie the target's own copy wins, so an unchanged conversation is never copied.
  const newest = copies.toSorted(
    (a, b) => b.modifiedAt - a.modifiedAt || Number(b.home === target) - Number(a.home === target),
  )[0];
  if (newest === undefined || newest.home === target) return;
  const from = path.join(newest.home, "projects", newest.project);
  const to = path.join(target, "projects", newest.project);
  yield* fileSystem.makeDirectory(to, { recursive: true });
  yield* replaceDirectory(path.join(from, sessionId), path.join(to, sessionId));
  yield* replaceDirectory(
    path.join(newest.home, "tasks", sessionId),
    path.join(target, "tasks", sessionId),
  );
  const staged = path.join(to, `.${sessionId}.jsonl.t3-transfer`);
  yield* fileSystem.copyFile(path.join(from, `${sessionId}.jsonl`), staged);
  yield* fileSystem.rename(staged, path.join(to, `${sessionId}.jsonl`));
});
