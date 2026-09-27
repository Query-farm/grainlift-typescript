// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import { type ChildProcess, spawn } from "node:child_process";
import type { X509Certificate } from "node:crypto";
import { once } from "node:events";
import { chmod, mkdtemp, rm } from "node:fs/promises";
import { createServer as createTcpServer, type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { createServer as createTlsServer, type TLSSocket } from "node:tls";
import { readIrohProxyProtocolV2 } from "@query-farm/vgi-rpc";
import { AuthContext } from "./auth.js";
import type { GrainliftService } from "./service.js";

export interface StreamServerOptions {
  host?: string;
  port?: number;
  maxConnections?: number;
  idleTimeoutMs?: number;
  shutdownTimeoutMs?: number;
  /** Lifetime input cap on each reused socket, not a per-request cap. */
  maxInputBytesPerConnection?: number;
}
export interface StreamServer {
  endpoint: string;
  close(): Promise<void>;
}
export interface MutualTlsOptions extends StreamServerOptions {
  ca: string | Buffer;
  cert: string | Buffer;
  key: string | Buffer;
  handshakeTimeoutMs?: number;
  /** Called only after Node verifies the peer certificate against ca. */
  authenticatePeer: (certificate: X509Certificate) => AuthContext;
}
interface Limits {
  connections: number;
  idle: number;
  shutdown: number;
  bytes: number;
}
function limits(options: StreamServerOptions): Limits {
  const values = {
    connections: options.maxConnections ?? 64,
    idle: options.idleTimeoutMs ?? 30_000,
    shutdown: options.shutdownTimeoutMs ?? 5000,
    bytes: options.maxInputBytesPerConnection ?? 128 * 1024 * 1024,
  };
  if (Object.values(values).some((v) => !Number.isSafeInteger(v) || v < 1))
    throw new TypeError("Invalid transport limits");
  return values;
}
function principal(identity: AuthContext): AuthContext {
  if (!identity.authenticated || identity.principal === null || !identity.domain || !identity.principal) {
    throw new TypeError("An authenticated transport principal is required");
  }
  return identity;
}
async function listen(
  service: GrainliftService,
  options: StreamServerOptions,
  create: (accepted: (socket: Socket) => void) => Server,
  authenticate: (socket: Socket) => AuthContext | Promise<AuthContext>,
  uri: string,
  unixPath?: string,
): Promise<StreamServer> {
  const bound = limits(options);
  const sockets = new Set<Socket>();
  const acceptedSockets = new Set<Socket>();
  const tasks = new Set<Promise<void>>();
  let stopping = false;
  const server = create((socket) => {
    if (stopping || sockets.size >= bound.connections) {
      socket.destroy();
      return;
    }
    sockets.add(socket);
    socket.setNoDelay(true);
    socket.setTimeout(bound.idle, () => socket.destroy());
    socket.on("error", () => {});
    const task = (async () => {
      const identity = principal(await authenticate(socket));
      if (!socket.destroyed) await service.serveSocket(socket, identity, bound.bytes);
    })()
      .catch(() => {
        /* Never log peer input or raw backend failures. */
      })
      .finally(() => {
        sockets.delete(socket);
        socket.destroy();
        tasks.delete(task);
      });
    tasks.add(task);
  });
  server.maxConnections = bound.connections;
  server.on("connection", (socket: Socket) => {
    acceptedSockets.add(socket);
    socket.on("error", () => {});
    socket.once("close", () => acceptedSockets.delete(socket));
  });
  server.on("error", () => {});
  if (unixPath) server.listen(unixPath);
  else server.listen(options.port ?? 0, options.host ?? "127.0.0.1");
  let address: ReturnType<Server["address"]>;
  try {
    await once(server, "listening");
    if (unixPath) await chmod(unixPath, 0o600);
    address = server.address();
    if (!address) throw new Error("Listener address unavailable");
  } catch (error) {
    for (const socket of acceptedSockets) socket.destroy();
    server.close();
    throw error;
  }
  const endpoint =
    typeof address === "string"
      ? `unix://${address}`
      : `${uri}://${address.address.includes(":") ? `[${address.address}]` : address.address}:${address.port}`;
  let closing: Promise<void> | undefined;
  return {
    endpoint,
    close: () => {
      closing ??= (async () => {
        stopping = true;
        const drain = new Promise<void>((resolve) => server.close(() => resolve()));
        for (const socket of sockets) socket.destroy();
        for (const socket of acceptedSockets) socket.destroy();
        let timer: NodeJS.Timeout | undefined;
        const deadline = new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            void service.close().catch(() => {});
            reject(new Error("Stream shutdown exceeded deadline"));
          }, bound.shutdown);
        });
        try {
          await Promise.race([
            (async () => {
              await service.cancelPending();
              await Promise.allSettled([...tasks]);
              await drain;
              await service.close();
            })(),
            deadline,
          ]);
        } finally {
          if (timer) clearTimeout(timer);
        }
      })();
      return closing;
    },
  };
}

/** Plain TCP is explicitly local trust: every connection has the configured identity. */
export async function serveTcp(
  service: GrainliftService,
  localIdentity: AuthContext,
  options: StreamServerOptions = {},
): Promise<StreamServer> {
  const host = options.host ?? "127.0.0.1";
  if (host !== "127.0.0.1" && host !== "::1") throw new TypeError("Plain TCP must bind loopback");
  principal(localIdentity);
  return listen(
    service,
    options,
    (accept) => createTcpServer(accept),
    () => localIdentity,
    "tcp",
  );
}
/** Serve raw VGI over TLS1.2+ with a verified and application-authorized peer certificate. */
export async function serveMutualTls(
  service: GrainliftService,
  options: MutualTlsOptions,
): Promise<StreamServer> {
  const timeout = options.handshakeTimeoutMs ?? 5000;
  if (!Number.isSafeInteger(timeout) || timeout < 1) throw new TypeError("Invalid TLS handshake timeout");
  return listen(
    service,
    options,
    (accept) => {
      const pending = new Map<string, { socket: Socket; timer: NodeJS.Timeout }>();
      const id = (socket: Socket) => `${socket.remoteAddress}:${socket.remotePort}`;
      const server = createTlsServer({
        ca: options.ca,
        cert: options.cert,
        key: options.key,
        requestCert: true,
        rejectUnauthorized: true,
        minVersion: "TLSv1.2",
        handshakeTimeout: timeout,
      });
      server.on("connection", (socket: Socket) => {
        const key = id(socket);
        const timer = setTimeout(() => socket.destroy(), timeout);
        pending.set(key, { socket, timer });
        socket.once("close", () => {
          clearTimeout(timer);
          if (pending.get(key)?.socket === socket) pending.delete(key);
        });
      });
      server.on("secureConnection", (socket) => {
        const key = id(socket);
        const entry = pending.get(key);
        if (entry) {
          clearTimeout(entry.timer);
          pending.delete(key);
        }
        accept(socket);
      });
      server.on("tlsClientError", (_error, socket) => socket.destroy());
      return server;
    },
    (socket) => {
      const tls = socket as TLSSocket;
      if (!tls.authorized) throw new Error("Client certificate verification failed");
      const peer = tls.getPeerX509Certificate();
      if (!peer) throw new Error("Client certificate required");
      return options.authenticatePeer(peer);
    },
    "tls+tcp",
  );
}

export interface IrohOptions extends Omit<StreamServerOptions, "host" | "port"> {
  /** Explicit installed executable; never downloaded by this library. */
  bridgePath: string;
  secretKeyFile?: string;
  ephemeral?: boolean;
  noRelay?: boolean;
  issuer: string;
  /** EndpointId is verified by Iroh, then forwarded over an owned private Unix socket. */
  authenticateEndpoint: (endpointId: string) => string | null;
  startupTimeoutMs?: number;
}
export interface IrohServer extends StreamServer {
  endpointId: string;
  directAddress: string;
}
async function stopChild(child: ChildProcess, timeout: number): Promise<void> {
  if (!child.pid || child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGTERM");
  let timer: NodeJS.Timeout | undefined;
  try {
    const exited = once(child, "exit");
    timer = setTimeout(() => child.kill("SIGKILL"), timeout);
    await exited;
  } finally {
    if (timer) clearTimeout(timer);
  }
}
/** Own an Iroh bridge and private Unix upstream. Same-UID processes are trusted. */
export async function serveIroh(service: GrainliftService, options: IrohOptions): Promise<IrohServer> {
  if (process.platform === "win32") throw new Error("Private Unix Iroh hosting requires Linux or macOS");
  if (
    !isAbsolute(options.bridgePath) ||
    !options.issuer ||
    options.issuer.includes("\0") ||
    !!options.secretKeyFile === !!options.ephemeral
  )
    throw new TypeError("Invalid Iroh identity configuration");
  const bound = limits(options);
  const startup = options.startupTimeoutMs ?? 15000;
  if (!Number.isSafeInteger(startup) || startup < 1) throw new TypeError("Invalid bridge startup timeout");
  const directory = await mkdtemp(join(tmpdir(), "grainlift-iroh-"));
  await chmod(directory, 0o700);
  let upstream: StreamServer | undefined;
  let child: ChildProcess | undefined;
  try {
    upstream = await listen(
      service,
      options,
      (accept) => createTcpServer(accept),
      async (socket) => {
        const peer = await readIrohProxyProtocolV2(socket, 1000, 536);
        const owner = options.authenticateEndpoint(peer.endpointId);
        if (!owner) throw new Error("Endpoint is not authorized");
        return new AuthContext(`iroh:${options.issuer}`, true, owner, { endpointId: peer.endpointId });
      },
      "unix",
      join(directory, "worker.sock"),
    );
    const args = [
      "--raw-upstream",
      upstream.endpoint,
      "--discovery-json",
      "--raw-max-connections",
      String(bound.connections),
      "--raw-max-streams",
      String(bound.connections),
      "--raw-drain-timeout",
      String(Math.max(1, Math.ceil(bound.shutdown / 1000))),
    ];
    if (options.secretKeyFile) args.push("--secret-key-file", options.secretKeyFile);
    else args.push("--ephemeral");
    if (options.noRelay) args.push("--no-relay");
    child = spawn(options.bridgePath, args, {
      stdio: ["ignore", "pipe", "ignore"],
      env: { PATH: process.env.PATH, RUST_LOG: "off" },
    });
    const bridge = child;
    const discovery = await new Promise<{ endpoint_id: string; direct_addresses: string[] }>(
      (resolve, reject) => {
        let pending = Buffer.alloc(0);
        const timer = setTimeout(() => done(new Error("Bridge startup timed out")), startup);
        const done = (error?: Error, value?: { endpoint_id: string; direct_addresses: string[] }) => {
          clearTimeout(timer);
          bridge.off("error", failed);
          bridge.off("exit", exited);
          bridge.stdout?.off("data", data);
          if (error) reject(error);
          else resolve(value!);
        };
        const failed = () => done(new Error("Bridge failed to start"));
        const exited = () => done(new Error("Bridge exited before readiness"));
        const data = (chunk: Buffer) => {
          pending = Buffer.concat([pending, chunk]);
          if (pending.length > 16384) {
            done(new Error("Bridge discovery exceeds limit"));
            return;
          }
          const newline = pending.indexOf(10);
          if (newline < 0) return;
          try {
            const value = JSON.parse(pending.subarray(0, newline).toString()) as {
              endpoint_id: string;
              direct_addresses: string[];
            };
            if (
              !/^[a-f0-9]{64}$/.test(value.endpoint_id) ||
              !Array.isArray(value.direct_addresses) ||
              !value.direct_addresses.every((address) => typeof address === "string")
            )
              throw new Error("Invalid discovery");
            done(undefined, value);
          } catch {
            done(new Error("Invalid bridge discovery"));
          }
        };
        bridge.once("error", failed);
        bridge.once("exit", exited);
        bridge.stdout!.on("data", data);
      },
    );
    child.stdout?.resume();
    const directAddress =
      discovery.direct_addresses.find((address) => address.startsWith("127.0.0.1:")) ??
      discovery.direct_addresses[0];
    if (!directAddress) throw new Error("Bridge did not advertise a direct address");
    let closing: Promise<void> | undefined;
    const close = () => {
      closing ??= (async () => {
        try {
          await stopChild(bridge, bound.shutdown);
        } finally {
          try {
            await upstream!.close();
          } finally {
            await rm(directory, { recursive: true, force: true });
          }
        }
      })();
      return closing;
    };
    // An unexpectedly exited bridge must fail closed and release the private listener.
    bridge.once("exit", () => {
      void close().catch(() => {});
    });
    return {
      endpoint: `iroh://${discovery.endpoint_id}`,
      endpointId: discovery.endpoint_id,
      directAddress,
      close,
    };
  } catch (error) {
    if (child) await stopChild(child, bound.shutdown);
    if (upstream) await upstream.close();
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
}
