// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off globalFetch:off globalConsole:off globalConsoleInEffect:off preferSchemaOverJson:off - host-side end-to-end harness around a real server process.
// Isolated server under bursts of VCS refreshes (as sidebar polling does), timing
// `server.probe` round trips: how long a client waits for anything the server serves.
//   node apps/server/scripts/responsiveness-e2e.ts --variants direct,broker --repos 40
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";

import {
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION,
  ProjectId,
  WsRpcGroup,
} from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { RpcClient, RpcSerialization } from "effect/rpc";
import { Socket } from "effect/socket";

const { values } = NodeUtil.parseArgs({
  options: {
    variants: { type: "string", default: "direct,broker" },
    repos: { type: "string", default: "40" },
    seconds: { type: "string", default: "30" },
    "burst-every-ms": { type: "string", default: "3000" },
    // Grow the server to the live install's RSS; fork cost scales with it.
    "ballast-mb": { type: "string", default: "700" },
    // A built `t3` executable instead of this checkout's source.
    "server-bin": { type: "string" },
  },
});
const repoCount = Number(values.repos);
const seconds = Number(values.seconds);
const burstEveryMs = Number(values["burst-every-ms"]);
const ballastMb = Number(values["ballast-mb"]);
const root = NodePath.resolve(import.meta.dirname, "../../..");

const decodeAccessToken = Schema.decodeUnknownSync(Schema.Struct({ access_token: Schema.String }));
const decodeTicket = Schema.decodeUnknownSync(Schema.Struct({ ticket: Schema.String }));

const percentile = (samples: ReadonlyArray<number>, p: number) => {
  const sorted = [...samples].sort((a, b) => a - b);
  return Math.round(sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * p) - 1)] ?? 0);
};

const workspace = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-responsiveness-"));
const ballastPath = NodePath.join(workspace, "ballast.mjs");
NodeFS.writeFileSync(
  ballastPath,
  `globalThis.__t3Ballast = Array.from({ length: ${ballastMb} }, (_, i) => Buffer.alloc(1 << 20, i));\n`,
);
const repos = Array.from({ length: repoCount }, (_, index) => {
  const repo = NodePath.join(workspace, "repos", `repo-${index}`);
  NodeFS.mkdirSync(repo, { recursive: true });
  NodeChildProcess.execFileSync("git", ["init", "-q", "-b", "main", repo]);
  for (let file = 0; file < 20; file += 1) {
    NodeFS.writeFileSync(NodePath.join(repo, `file-${file}.txt`), `${index}:${file}\n`);
  }
  NodeChildProcess.execFileSync("git", [
    "-C",
    repo,
    "-c",
    "user.name=Bench",
    "-c",
    "user.email=bench@localhost",
    "commit",
    "-qam",
    "initial",
    "--allow-empty",
  ]);
  NodeChildProcess.execFileSync("git", ["-C", repo, "add", "."]);
  NodeFS.writeFileSync(NodePath.join(repo, "dirty.txt"), "uncommitted\n");
  return repo;
});

const freePort = async () => {
  const reservation = NodeHttp.createServer();
  reservation.listen(0, "127.0.0.1");
  await new Promise<void>((done) => reservation.once("listening", done));
  const address = reservation.address();
  NodeAssert.ok(address && typeof address !== "string");
  await new Promise<void>((done) => reservation.close(() => done()));
  return address.port;
};

const rssMb = (pid: number) => {
  const status = NodeFS.readFileSync(`/proc/${pid}/status`, "utf8");
  return Math.round(Number(/VmRSS:\s+(\d+)/.exec(status)?.[1] ?? 0) / 1024);
};

async function runVariant(variant: "direct" | "broker") {
  const home = NodePath.join(workspace, `home-${variant}`);
  const port = await freePort();
  const origin = `http://127.0.0.1:${port}`;
  const bootstrap = NodeCrypto.randomUUID();
  // Never inherit the live install's home or tokens; no user bus keeps agent scopes off.
  const inherited = Object.fromEntries(
    Object.entries(process.env).filter(
      ([key]) => !key.startsWith("T3CODE_") && !key.startsWith("T3_") && key !== "XDG_RUNTIME_DIR",
    ),
  );
  const serverArgs = ["start", "--bootstrap-fd", "3", "--base-dir", home];
  const server = NodeChildProcess.spawn(
    values["server-bin"] ?? process.execPath,
    values["server-bin"] === undefined
      ? ["--import", ballastPath, "apps/server/src/bin.ts", ...serverArgs]
      : serverArgs,
    {
      cwd: root,
      detached: true,
      stdio: ["ignore", "pipe", "pipe", "pipe"],
      env: {
        ...inherited,
        T3CODE_SPAWN_BROKER: variant === "broker" ? "1" : "0",
        ...(values["server-bin"] === undefined ? {} : { NODE_OPTIONS: `--import ${ballastPath}` }),
      },
    },
  );
  const serverPid = server.pid!;
  const stop = () => {
    try {
      process.kill(-serverPid, "SIGTERM");
    } catch {
      // Already stopped.
    }
  };
  process.once("exit", stop);
  const bootstrapPipe = server.stdio[3];
  NodeAssert.ok(bootstrapPipe && "write" in bootstrapPipe);
  bootstrapPipe.end(
    JSON.stringify({
      mode: "desktop",
      noBrowser: true,
      port,
      t3Home: home,
      host: "127.0.0.1",
      desktopBootstrapToken: bootstrap,
      tailscaleServeEnabled: false,
      tailscaleServePort: 443,
    }),
  );
  const ready = Promise.withResolvers<void>();
  let output = "";
  for (const pipe of [server.stdout, server.stderr]) {
    pipe?.on("data", (data: Buffer) => {
      const text = data.toString().replaceAll(bootstrap, "[REDACTED]");
      if (NodeFS.existsSync(workspace)) {
        NodeFS.appendFileSync(NodePath.join(workspace, `server-${variant}.log`), text);
      }
      output = (output + text).slice(-8192);
      if (output.includes("Listening on http")) ready.resolve();
    });
  }
  server.once("exit", (code) => ready.reject(new Error(`Server exited ${code}: ${output}`)));
  const deadline = setTimeout(() => ready.reject(new Error("Server startup timed out")), 120_000);
  try {
    await ready.promise;
    clearTimeout(deadline);
    const tokenResponse = await fetch(`${origin}/oauth/token`, {
      method: "POST",
      body: new URLSearchParams({
        grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
        subject_token: bootstrap,
        subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
        requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
      }),
    });
    NodeAssert.ok(tokenResponse.ok, `Authentication failed: ${tokenResponse.status}`);
    const token = decodeAccessToken(await tokenResponse.json());
    const headers = {
      Authorization: `Bearer ${token.access_token}`,
      "Content-Type": "application/json",
      [ORCHESTRATION_PROTOCOL_HEADER]: String(ORCHESTRATION_PROTOCOL_VERSION),
    };
    for (const [index, repo] of repos.entries()) {
      const created = await fetch(`${origin}/api/projects/mutate`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          type: "project.create",
          commandId: NodeCrypto.randomUUID(),
          projectId: ProjectId.make(NodeCrypto.randomUUID()),
          title: `Bench ${index}`,
          workspaceRoot: repo,
        }),
      });
      NodeAssert.ok(created.ok, `Project creation failed: ${created.status}`);
    }
    const ticketResponse = await fetch(`${origin}/api/auth/websocket-ticket`, {
      method: "POST",
      headers,
    });
    const ticket = decodeTicket(await ticketResponse.json());
    const layerProtocol = RpcClient.layerProtocolSocket().pipe(
      Layer.provide(
        Socket.layerWebSocket(
          `${origin.replace("http:", "ws:")}/ws?orchestrationProtocol=${ORCHESTRATION_PROTOCOL_VERSION}&wsTicket=${encodeURIComponent(ticket.ticket)}`,
        ).pipe(Layer.provide(Socket.layerWebSocketConstructorGlobal)),
      ),
      Layer.provide(RpcSerialization.layerJson),
    );

    return await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* RpcClient.make(WsRpcGroup);
        yield* client["server.probe"]({});
        const refreshAll = Effect.forEach(
          repos,
          (cwd) => client["vcs.refreshStatus"]({ cwd, localOnly: true }).pipe(Effect.ignore),
          { concurrency: "unbounded", discard: true },
        );
        yield* refreshAll; // warm-up: first status per repo also resolves the Git binary
        const probes: Array<number> = [];
        const bursts: Array<number> = [];
        const prober = yield* Effect.gen(function* () {
          const at = performance.now();
          yield* client["server.probe"]({});
          probes.push(performance.now() - at);
          yield* Effect.sleep(Duration.millis(50));
        }).pipe(Effect.forever, Effect.forkChild);
        const endAt = performance.now() + seconds * 1000;
        while (performance.now() < endAt) {
          const at = performance.now();
          yield* refreshAll;
          bursts.push(performance.now() - at);
          const wait = burstEveryMs - (performance.now() - at);
          if (wait > 0) yield* Effect.sleep(Duration.millis(wait));
        }
        yield* Fiber.interrupt(prober);
        return {
          variant,
          repos: repoCount,
          serverRssMb: rssMb(serverPid),
          probes: probes.length,
          probeRttMs: {
            p50: percentile(probes, 0.5),
            p95: percentile(probes, 0.95),
            p99: percentile(probes, 0.99),
            max: percentile(probes, 1),
          },
          probesOver250ms: probes.filter((ms) => ms > 250).length,
          probesOver1s: probes.filter((ms) => ms > 1000).length,
          refreshBurstMs: { p50: percentile(bursts, 0.5), max: percentile(bursts, 1) },
        };
      }).pipe(Effect.scoped, Effect.provide(layerProtocol)),
    );
  } finally {
    clearTimeout(deadline);
    stop();
    process.off("exit", stop);
  }
}

for (const variant of values.variants.split(",")) {
  NodeAssert.ok(variant === "direct" || variant === "broker", `unknown variant ${variant}`);
  console.log(JSON.stringify(await runVariant(variant)));
}
NodeFS.rmSync(workspace, { recursive: true, force: true });
