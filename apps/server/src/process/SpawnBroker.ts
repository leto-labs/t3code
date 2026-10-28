// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - the broker is a Node child speaking newline-delimited JSON.
// Linux forks copy page tables on the calling thread (~130 ms per spawn at 1.6 GB RSS),
// so Git polling bursts froze the event loop for seconds. Short-lived commands fork from
// the small `t3 __spawn-broker` instead; anything it cannot represent exactly uses Node.
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeURL from "node:url";

import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Redacted from "effect/Redacted";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import type * as ChildProcess from "effect/process/ChildProcess";
import {
  ChildProcessSpawner,
  ExitCode,
  make as makeSpawner,
  makeHandle,
  ProcessId,
} from "effect/process/ChildProcessSpawner";

import type { BrokerReply, BrokerRequest } from "./spawnBrokerProcess.ts";

const BROKERED_COMMANDS = new Set(["git", "gh", "ps", "systemctl"]);
// A broker that keeps dying would cost a fork of its own per command.
const RETRY_BROKER_AFTER_MS = 30_000;

type ExitStatus = readonly [code: number | null, signal: NodeJS.Signals | null];
/** A pid once spawned, "fallback" if the broker never saw the request, "lost" if it may have run it. */
type Start = number | "fallback" | "lost";

interface Broker {
  readonly child: NodeChildProcess.ChildProcess;
  readonly sessions: Map<number, Session>;
  // Written before any request is handled, so a broker that dies unready spawned nothing.
  ready: boolean;
}

interface Session {
  readonly id: number;
  readonly broker: Broker;
  readonly stdout: Queue.Queue<Uint8Array, Cause.Done>;
  readonly stderr: Queue.Queue<Uint8Array, Cause.Done>;
  readonly exit: Deferred.Deferred<ExitStatus>;
  pid: number | undefined;
  started: ((start: Start) => void) | undefined;
}

interface Opened {
  readonly pid: number;
  readonly session: Session;
}

function isBrokerable(command: ChildProcess.Command): command is ChildProcess.StandardCommand {
  if (command._tag !== "StandardCommand") return false;
  const options = command.options;
  const piped = (value: unknown) => value === undefined || value === "pipe";
  return (
    BROKERED_COMMANDS.has(NodePath.basename(command.command)) &&
    options.additionalFds === undefined &&
    (options.shell === undefined || options.shell === false) &&
    options.detached !== false &&
    (piped(options.stdin) || options.stdin === "ignore") &&
    piped(options.stdout) &&
    piped(options.stderr)
  );
}

const commandText = (command: ChildProcess.StandardCommand) =>
  [command.command, ...command.args].join(" ");

const exitCodeError = (command: ChildProcess.StandardCommand, signal: string | null) =>
  PlatformError.systemError({
    _tag: "Unknown",
    module: "ChildProcess",
    method: "exitCode",
    description: `Process interrupted due to receipt of signal: '${signal}'`,
    pathOrDescriptor: commandText(command),
  });

const lostSpawnError = (command: ChildProcess.StandardCommand) =>
  PlatformError.systemError({
    _tag: "Unknown",
    module: "ChildProcess",
    method: "spawn",
    description: "The spawn broker exited before reporting whether the process started",
    pathOrDescriptor: commandText(command),
  });

const parseReply = (line: string): BrokerReply | undefined => {
  try {
    const reply: unknown = JSON.parse(line);
    return typeof reply === "object" && reply !== null ? (reply as BrokerReply) : undefined;
  } catch {
    return undefined;
  }
};

const killGroup = (pid: number) => {
  try {
    process.kill(-pid, "SIGKILL");
  } catch {
    // Already gone.
  }
};

export const layer = Layer.effect(
  ChildProcessSpawner,
  Effect.gen(function* () {
    const fallback = yield* ChildProcessSpawner;
    const platform = yield* HostProcess.Platform;
    const environment = yield* HostProcess.Environment;
    // Only Linux pays for forking a large process; macOS libuv uses posix_spawn.
    // T3CODE_SPAWN_BROKER=0 restores direct spawning without a rebuild.
    if (platform !== "linux" || environment.T3CODE_SPAWN_BROKER === "0") return fallback;
    const executable = yield* HostProcess.ExecutablePath;
    const isExecutable = yield* HostProcess.IsExecutable;
    const clock = yield* Clock.Clock;
    const runSync = Effect.runSyncWith(yield* Effect.context<never>());
    // In a bundle import.meta.url is bin.mjs; development and tests use this source module.
    const entry = NodeURL.fileURLToPath(
      new URL(import.meta.url.endsWith(".ts") ? "../bin.ts" : "./bin.mjs", import.meta.url),
    );

    let nextId = 0;
    let current: Broker | undefined;
    let retryAt = 0;
    let stopping = false;
    // Like the SQLite reader worker: keep the process alive only while a command is pending.
    const setReferenced = (broker: Broker, referenced: boolean) => {
      const stdout = broker.child.stdout as { ref?: () => void; unref?: () => void } | null;
      if (referenced) {
        broker.child.ref();
        stdout?.ref?.();
      } else {
        broker.child.unref();
        stdout?.unref?.();
      }
    };
    const forget = (session: Session) => {
      const { broker } = session;
      if (broker.sessions.delete(session.id) && broker.sessions.size === 0) {
        setReferenced(broker, false);
      }
    };

    const send = (broker: Broker, request: BrokerRequest) => {
      broker.child.stdin?.write(`${JSON.stringify(request)}\n`);
    };
    const settle = (session: Session, start: Start) => {
      const started = session.started;
      session.started = undefined;
      started?.(start);
    };
    const finish = (session: Session, status: ExitStatus) => {
      Deferred.doneUnsafe(session.exit, Effect.succeed(status));
      Queue.endUnsafe(session.stdout);
      Queue.endUnsafe(session.stderr);
    };
    const onReply = (broker: Broker, reply: BrokerReply) => {
      if (reply.t === "ready") {
        broker.ready = true;
        return;
      }
      const session = broker.sessions.get(reply.id);
      if (session === undefined) return;
      switch (reply.t) {
        case "spawned":
          session.pid = reply.pid;
          settle(session, reply.pid);
          return;
        case "spawn-error":
          forget(session);
          settle(session, "fallback");
          return;
        case "out":
        case "err":
          Queue.offerUnsafe(
            reply.t === "out" ? session.stdout : session.stderr,
            Buffer.from(reply.d, "base64"),
          );
          return;
        case "exit":
          Deferred.doneUnsafe(session.exit, Effect.succeed([reply.code, reply.signal] as const));
          return;
        case "close":
          Queue.endUnsafe(session.stdout);
          Queue.endUnsafe(session.stderr);
          return;
      }
    };
    // Re-running a command the broker may have started could commit or push twice,
    // so only requests it provably never read fall back to Node.
    const onBrokerClosed = (broker: Broker) => {
      for (const session of broker.sessions.values()) {
        if (session.started !== undefined) {
          settle(session, broker.ready ? "lost" : "fallback");
          continue;
        }
        // Its children lead their own groups, so they outlive it unless stopped here.
        if (session.pid !== undefined && !Deferred.isDoneUnsafe(session.exit)) {
          killGroup(session.pid);
        }
        finish(session, [null, "SIGKILL"]);
      }
      broker.sessions.clear();
    };
    const start = (): Broker | undefined => {
      let child: NodeChildProcess.ChildProcess;
      try {
        child = NodeChildProcess.spawn(
          executable,
          [...(isExecutable ? [] : [entry]), "__spawn-broker"],
          {
            stdio: ["pipe", "pipe", "inherit"],
            env: {
              ...environment,
              ELECTRON_RUN_AS_NODE: "1",
              // Not the server's options: preloads or a large heap would make its forks slow too.
              NODE_OPTIONS: "--max-old-space-size=128",
            },
            windowsHide: true,
          },
        );
      } catch {
        retryAt = clock.currentTimeMillisUnsafe() + RETRY_BROKER_AFTER_MS;
        return undefined;
      }
      const broker: Broker = { child, sessions: new Map(), ready: false };
      const retire = () => {
        if (current !== broker) return;
        current = undefined;
        if (stopping) return;
        retryAt = clock.currentTimeMillisUnsafe() + RETRY_BROKER_AFTER_MS;
        runSync(
          Effect.logWarning("Spawn broker exited; spawning directly before retrying", {
            code: child.exitCode,
            signal: child.signalCode,
            retryAfterMs: RETRY_BROKER_AFTER_MS,
          }),
        );
      };
      child.stdin?.on("error", () => {});
      // A failed spawn also emits close, which retires the broker.
      child.on("error", () => {});
      child.once("exit", retire);
      NodeReadline.createInterface({ input: child.stdout! }).on("line", (line) => {
        if (line.length === 0) return;
        const reply = parseReply(line);
        if (reply !== undefined) onReply(broker, reply);
        else
          runSync(
            Effect.logWarning("Ignored malformed spawn broker line", { line: line.slice(0, 200) }),
          );
      });
      // Unlike exit, close waits until every reply on stdout has been read.
      child.once("close", () => {
        retire();
        onBrokerClosed(broker);
      });
      current = broker;
      setReferenced(broker, false);
      return broker;
    };
    yield* Effect.acquireRelease(Effect.sync(start), () =>
      Effect.sync(() => {
        stopping = true;
        current?.child.stdin?.end();
        current?.child.kill("SIGTERM");
      }),
    );

    const open = Effect.fnUntraced(function* (command: ChildProcess.StandardCommand) {
      const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
      const stderr = yield* Queue.unbounded<Uint8Array, Cause.Done>();
      return yield* Effect.callback<Opened | "fallback", PlatformError.PlatformError>((resume) => {
        const broker = current ?? (clock.currentTimeMillisUnsafe() < retryAt ? undefined : start());
        if (broker === undefined) {
          resume(Effect.succeed("fallback"));
          return;
        }
        const session: Session = {
          id: nextId++,
          broker,
          stdout,
          stderr,
          exit: Deferred.makeUnsafe<ExitStatus>(),
          pid: undefined,
          started: (start) =>
            resume(
              typeof start === "number"
                ? Effect.succeed({ pid: start, session })
                : start === "fallback"
                  ? Effect.succeed("fallback")
                  : Effect.fail(lostSpawnError(command)),
            ),
        };
        broker.sessions.set(session.id, session);
        if (broker.sessions.size === 1) setReferenced(broker, true);
        const options = command.options;
        send(broker, {
          t: "spawn",
          id: session.id,
          command: command.command,
          args: command.args.map((arg) => (Redacted.isRedacted(arg) ? Redacted.value(arg) : arg)),
          cwd: options.cwd,
          env: Object.fromEntries(
            Object.entries(
              options.extendEnv === false
                ? (options.env ?? {})
                : { ...process.env, ...options.env },
            ).filter((entry): entry is [string, string] => entry[1] !== undefined),
          ),
          stdin: options.stdin !== "ignore",
          killSignal: (options.killSignal ?? "SIGTERM") as NodeJS.Signals,
        });
        return Effect.sync(() => {
          // Interrupted before the child started: stop it if it does.
          session.started = (start) => {
            if (typeof start === "number")
              send(broker, { t: "release", id: session.id, signal: "SIGKILL" });
            forget(session);
          };
        });
      });
    });

    const spawnBrokered = Effect.fnUntraced(function* (command: ChildProcess.StandardCommand) {
      const opened = yield* Effect.acquireRelease(open(command), (opened) =>
        opened === "fallback"
          ? Effect.void
          : Effect.gen(function* () {
              const { session } = opened;
              if (Deferred.isDoneUnsafe(session.exit)) {
                forget(session);
                send(session.broker, {
                  t: "release",
                  id: session.id,
                  signal: command.options.killSignal ?? "SIGTERM",
                });
                return;
              }
              yield* terminate(session, command.options);
              forget(session);
              send(session.broker, { t: "release", id: session.id, signal: undefined });
            }),
      );
      if (opened === "fallback") return yield* fallback.spawn(command);
      const { pid, session } = opened;
      const stdout = Stream.fromQueue(session.stdout);
      const stderr = Stream.fromQueue(session.stderr);
      return makeHandle({
        pid: ProcessId(pid),
        exitCode: Effect.flatMap(Deferred.await(session.exit), ([code, signal]) =>
          code === null
            ? Effect.fail(exitCodeError(command, signal))
            : Effect.succeed(ExitCode(code)),
        ),
        isRunning: Effect.map(Deferred.isDone(session.exit), (done) => !done),
        kill: (options) => terminate(session, options),
        stdin: Sink.forEach((chunk: Uint8Array) =>
          Effect.sync(() =>
            send(session.broker, {
              t: "in",
              id: session.id,
              d: Buffer.from(chunk).toString("base64"),
            }),
          ),
        ).pipe(
          Sink.ensuring(Effect.sync(() => send(session.broker, { t: "in-end", id: session.id }))),
        ),
        stdout,
        stderr,
        all: Stream.merge(stdout, stderr),
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
        unref: Effect.succeed(Effect.void),
      });
    });

    const terminate = (session: Session, options: ChildProcess.KillOptions | undefined) =>
      Effect.suspend(() => {
        if (Deferred.isDoneUnsafe(session.exit)) return Effect.void;
        send(session.broker, {
          t: "kill",
          id: session.id,
          signal: (options?.killSignal ?? "SIGTERM") as NodeJS.Signals,
          forceKillAfterMs:
            options?.forceKillAfter === undefined
              ? undefined
              : Duration.toMillis(Duration.fromInputUnsafe(options.forceKillAfter)),
        });
        return Effect.asVoid(Deferred.await(session.exit));
      });

    return makeSpawner((command) =>
      isBrokerable(command) ? spawnBrokered(command) : fallback.spawn(command),
    );
  }),
);
