// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off globalErrorInEffectFailure:off globalTimers:off - a plain Node child that speaks newline-delimited JSON on stdio.
/**
 * `t3 __spawn-broker`: spawns short-lived commands for the server.
 *
 * Linux forks copy the parent's page tables on the calling thread, so every spawn
 * from a 1 GB server blocks its event loop for ~100 ms. This process stays small,
 * so the fork cost lands here instead. Protocol: one JSON object per line.
 */
import * as NodeChildProcess from "node:child_process";
import * as NodeReadline from "node:readline";

export type BrokerRequest =
  | {
      readonly t: "spawn";
      readonly id: number;
      readonly command: string;
      readonly args: ReadonlyArray<string>;
      readonly cwd: string | undefined;
      readonly env: Record<string, string>;
      readonly stdin: boolean;
      readonly killSignal: NodeJS.Signals;
    }
  | { readonly t: "in"; readonly id: number; readonly d: string }
  | { readonly t: "in-end"; readonly id: number }
  | {
      readonly t: "kill";
      readonly id: number;
      readonly signal: NodeJS.Signals;
      readonly forceKillAfterMs: number | undefined;
    }
  | { readonly t: "release"; readonly id: number; readonly signal: NodeJS.Signals | undefined };

export type BrokerReply =
  | { readonly t: "ready" }
  | { readonly t: "spawned"; readonly id: number; readonly pid: number }
  | { readonly t: "spawn-error"; readonly id: number; readonly message: string }
  | { readonly t: "out" | "err"; readonly id: number; readonly d: string }
  | {
      readonly t: "exit";
      readonly id: number;
      readonly code: number | null;
      readonly signal: NodeJS.Signals | null;
    }
  | { readonly t: "close"; readonly id: number };

export function runSpawnBroker(): void {
  const children = new Map<number, NodeChildProcess.ChildProcess>();
  const send = (reply: BrokerReply) => {
    process.stdout.write(`${JSON.stringify(reply)}\n`);
  };
  // Matches the server's spawner: each child leads its own group, signalled as a whole.
  const signalGroup = (child: NodeChildProcess.ChildProcess, signal: NodeJS.Signals) => {
    if (child.pid === undefined) return;
    try {
      process.kill(-child.pid, signal);
    } catch {
      try {
        child.kill(signal);
      } catch {
        // Already gone.
      }
    }
  };

  const handle = (request: BrokerRequest) => {
    switch (request.t) {
      case "spawn": {
        let child: NodeChildProcess.ChildProcess;
        try {
          child = NodeChildProcess.spawn(request.command, request.args, {
            cwd: request.cwd,
            env: request.env,
            stdio: [request.stdin ? "pipe" : "ignore", "pipe", "pipe"],
            detached: true,
            windowsHide: true,
          });
        } catch (error) {
          send({ t: "spawn-error", id: request.id, message: String(error) });
          return;
        }
        let spawned = false;
        child.once("spawn", () => {
          spawned = true;
          children.set(request.id, child);
          send({ t: "spawned", id: request.id, pid: child.pid! });
        });
        child.once("error", (error) => {
          if (!spawned) send({ t: "spawn-error", id: request.id, message: error.message });
        });
        child.stdin?.on("error", () => {});
        child.stdout?.on("data", (chunk: Buffer) =>
          send({ t: "out", id: request.id, d: chunk.toString("base64") }),
        );
        child.stderr?.on("data", (chunk: Buffer) =>
          send({ t: "err", id: request.id, d: chunk.toString("base64") }),
        );
        child.once("exit", (code, signal) => {
          if (code !== null && code !== 0) signalGroup(child, request.killSignal);
          send({ t: "exit", id: request.id, code, signal });
        });
        child.once("close", () => {
          if (spawned) send({ t: "close", id: request.id });
        });
        return;
      }
      case "in":
        children.get(request.id)?.stdin?.write(Buffer.from(request.d, "base64"));
        return;
      case "in-end":
        children.get(request.id)?.stdin?.end();
        return;
      case "kill": {
        const child = children.get(request.id);
        if (child === undefined) return;
        signalGroup(child, request.signal);
        if (request.forceKillAfterMs !== undefined) {
          const timer = setTimeout(() => {
            if (child.exitCode === null && child.signalCode === null) signalGroup(child, "SIGKILL");
          }, request.forceKillAfterMs);
          child.once("exit", () => clearTimeout(timer));
        }
        return;
      }
      case "release": {
        const child = children.get(request.id);
        children.delete(request.id);
        if (child !== undefined && request.signal !== undefined) signalGroup(child, request.signal);
        return;
      }
    }
  };

  // The server closed the pipe; the close handler below stops the children.
  process.stdout.on("error", () => {});
  // Before any request is read: a broker that dies without saying this spawned nothing.
  send({ t: "ready" });
  const lines = NodeReadline.createInterface({ input: process.stdin });
  lines.on("line", (line) => {
    if (line.length === 0) return;
    handle(JSON.parse(line) as BrokerRequest);
  });
  // The server is gone; leave nothing it started behind.
  lines.on("close", () => {
    for (const child of children.values()) signalGroup(child, "SIGTERM");
    process.exit(0);
  });
}
