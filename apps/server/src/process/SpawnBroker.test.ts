// @effect-diagnostics nodeBuiltinImport:off -- the tests inspect /proc for real child processes.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as SpawnBroker from "./SpawnBroker.ts";

const layer = SpawnBroker.layer.pipe(Layer.provideMerge(NodeServices.layer));

const parentPid = (pid: number) =>
  Number(NodeFS.readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1]!.split(" ")[1]);

// Stands in for `t3 __spawn-broker` to script deaths and malformed replies exactly.
const withFakeBroker = <A, E>(
  script: string,
  test: Effect.Effect<A, E, ChildProcessSpawner.ChildProcessSpawner | Scope.Scope>,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const executable = `${yield* fs.makeTempDirectoryScoped()}/broker`;
    yield* fs.writeFileString(executable, `#!/bin/sh\n${script}\n`, { mode: 0o755 });
    return yield* test.pipe(
      Effect.scoped,
      Effect.provide(
        SpawnBroker.layer.pipe(
          Layer.provide(Layer.succeed(HostProcess.ExecutablePath, executable)),
          Layer.provide(Layer.succeed(HostProcess.IsExecutable, true)),
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

const text = <E>(stream: Stream.Stream<Uint8Array, E>) =>
  stream.pipe(Stream.decodeText, Stream.mkString);

// The assertions read child parentage from /proc.
describe.runIf(NodeFS.existsSync("/proc/self/stat"))("SpawnBroker", () => {
  it.live("runs git outside the server process and relays stdin and stdout", () =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const child = yield* spawner.spawn(ChildProcess.make("git", ["hash-object", "--stdin"]));
      // The fork happened in the broker, so this event loop never paid for it.
      assert.notEqual(parentPid(child.pid), process.pid);
      yield* Stream.run(Stream.encodeText(Stream.make("hello")), child.stdin);
      assert.equal(yield* text(child.stdout), "b6fc4c620b67d95f953a5c1c1230aaab5db5a1b0\n");
      assert.equal(yield* child.exitCode, 0);
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.live("reports nonzero exits and stderr", () =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const child = yield* spawner.spawn(
        ChildProcess.make("git", ["rev-parse", "--verify", "missing-ref"], {
          cwd: NodeOS.tmpdir(),
        }),
      );
      const [stderr, code] = yield* Effect.all([text(child.stderr), child.exitCode], {
        concurrency: "unbounded",
      });
      assert.notEqual(code, 0);
      assert.include(stderr, "fatal");
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.live("kills a running child and stops it when its scope closes", () =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const killed = yield* spawner.spawn(ChildProcess.make("git", ["hash-object", "--stdin"]));
      yield* killed.kill();
      assert.isFalse(yield* killed.isRunning);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(killed.exitCode)));

      const scope = yield* Scope.make();
      const abandoned = yield* spawner
        .spawn(ChildProcess.make("git", ["hash-object", "--stdin"]))
        .pipe(Scope.provide(scope));
      assert.isTrue(NodeFS.existsSync(`/proc/${abandoned.pid}`));
      yield* Scope.close(scope, Exit.void);
      assert.isFalse(yield* abandoned.isRunning);
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.live("leaves other commands and failed spawns to the default spawner", () =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const shell = yield* spawner.spawn(ChildProcess.make("sh", ["-c", "echo $PPID"]));
      assert.equal((yield* text(shell.stdout)).trim(), String(process.pid));

      const missingCwd = yield* Effect.exit(
        spawner.spawn(ChildProcess.make("git", ["status"], { cwd: "/definitely/missing" })),
      );
      assert.isTrue(Exit.isFailure(missingCwd));
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.live("fails a running command when the broker dies and spawns directly afterwards", () =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const child = yield* spawner.spawn(ChildProcess.make("git", ["hash-object", "--stdin"]));
      process.kill(parentPid(child.pid), "SIGKILL");
      assert.isTrue(Exit.isFailure(yield* Effect.exit(child.exitCode)));
      assert.isFalse(yield* child.isRunning);

      const next = yield* spawner.spawn(ChildProcess.make("git", ["--version"]));
      assert.equal(parentPid(next.pid), process.pid);
      assert.equal(yield* next.exitCode, 0);
    }).pipe(Effect.scoped, Effect.provide(layer)),
  );

  it.live("does not re-run a command the broker read before dying", () =>
    withFakeBroker(
      `echo '{"t":"ready"}'\nread -r request\nexit 1`,
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const spawned = yield* Effect.exit(spawner.spawn(ChildProcess.make("git", ["--version"])));
        assert.isTrue(Exit.isFailure(spawned));
      }),
    ),
  );

  it.live("spawns directly when the broker dies before reading anything", () =>
    withFakeBroker(
      "exit 1",
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const child = yield* spawner.spawn(ChildProcess.make("git", ["--version"]));
        assert.equal(parentPid(child.pid), process.pid);
        assert.include(yield* text(child.stdout), "git version");
      }),
    ),
  );

  it.live("ignores malformed broker lines", () =>
    withFakeBroker(
      [
        `echo '{"t":"ready"}'`,
        "read -r request",
        "echo 'not json'",
        "echo null",
        `echo '{"t":"spawned","id":0,"pid":'$$'}'`,
        `echo '{"t":"out","id":0,"d":"aGk="}'`,
        `echo '{"t":"exit","id":0,"code":0,"signal":null}'`,
        `echo '{"t":"close","id":0}'`,
        "cat >/dev/null",
      ].join("\n"),
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const child = yield* spawner.spawn(ChildProcess.make("git", ["--version"]));
        assert.equal(yield* text(child.all), "hi");
        assert.equal(yield* child.exitCode, 0);
      }),
    ),
  );
});
