import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { transferClaudeSession } from "./claudeSessionTransfer.ts";

const SESSION = "0b6f8f53-2f7e-4d5f-9d55-1f0b2c3d4e5f";
const PROJECT = "-workspace-repo";

const setup = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-claude-transfer-" });
  const homes = { alice: path.join(root, "alice"), sam: path.join(root, "sam") };
  const transcript = (home: string) => path.join(home, "projects", PROJECT, `${SESSION}.jsonl`);
  const write = Effect.fn("write")(function* (file: string, contents: string, modifiedAt: number) {
    yield* fs.makeDirectory(path.dirname(file), { recursive: true });
    yield* fs.writeFileString(file, contents);
    yield* fs.utimes(file, modifiedAt, modifiedAt);
  });
  return { fs, path, homes, transcript, write };
});

it.layer(NodeServices.layer)("transferClaudeSession", (it) => {
  it.effect("copies the newest transcript, its subagents and its tasks into the target home", () =>
    Effect.gen(function* () {
      const { fs, path, homes, transcript, write } = yield* setup;
      yield* write(transcript(homes.sam), "sam-latest\n", 2_000);
      yield* write(
        path.join(homes.sam, "projects", PROJECT, SESSION, "subagents", "agent-1.jsonl"),
        "subagent\n",
        2_000,
      );
      yield* write(path.join(homes.sam, "tasks", SESSION, "1.json"), "{}", 2_000);
      yield* write(transcript(homes.alice), "alice-stale\n", 1_000);
      yield* write(
        path.join(homes.alice, "projects", PROJECT, SESSION, "subagents", "stale.jsonl"),
        "stale\n",
        1_000,
      );

      yield* transferClaudeSession({
        sessionId: SESSION,
        target: homes.alice,
        sources: [homes.alice, homes.sam],
      });

      expect(yield* fs.readFileString(transcript(homes.alice))).toBe("sam-latest\n");
      expect(
        yield* fs.readDirectory(path.join(homes.alice, "projects", PROJECT, SESSION, "subagents")),
      ).toEqual(["agent-1.jsonl"]);
      expect(yield* fs.exists(path.join(homes.alice, "tasks", SESSION, "1.json"))).toBe(true);
      // The source keeps its copy: a failed resume elsewhere loses nothing.
      expect(yield* fs.readFileString(transcript(homes.sam))).toBe("sam-latest\n");
    }),
  );

  it.effect("keeps the target's copy when it is the newest or the only one", () =>
    Effect.gen(function* () {
      const { fs, homes, transcript, write } = yield* setup;
      yield* write(transcript(homes.alice), "alice-latest\n", 3_000);
      yield* write(transcript(homes.sam), "sam-stale\n", 1_000);

      yield* transferClaudeSession({
        sessionId: SESSION,
        target: homes.alice,
        sources: [homes.sam],
      });
      expect(yield* fs.readFileString(transcript(homes.alice))).toBe("alice-latest\n");

      yield* transferClaudeSession({
        sessionId: "never-written",
        target: homes.sam,
        sources: [homes.alice],
      });
      expect(yield* fs.readFileString(transcript(homes.sam))).toBe("sam-stale\n");
    }),
  );

  it.effect("fails when a required transcript is missing from every known home", () =>
    Effect.gen(function* () {
      const { homes } = yield* setup;
      const failed = yield* transferClaudeSession({
        sessionId: SESSION,
        target: homes.alice,
        sources: [homes.sam],
        requireTranscript: true,
      }).pipe(Effect.result);
      expect(failed).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "ClaudeAutoTranscriptMissingError" },
      });
    }),
  );

  it.effect("ignores a session id that could name a path outside the home", () =>
    Effect.gen(function* () {
      const { fs, path, homes, write } = yield* setup;
      yield* write(path.join(homes.sam, "projects", PROJECT, "..", "x.jsonl"), "x\n", 1_000);

      yield* transferClaudeSession({
        sessionId: "../x",
        target: homes.alice,
        sources: [homes.sam],
      });
      expect(yield* fs.exists(homes.alice)).toBe(false);
    }),
  );
});
