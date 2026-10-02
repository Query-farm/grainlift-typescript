// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import { once } from "node:events";
import { createServer, type RequestListener, type Server } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import type { AuthenticateFn } from "./auth.js";
import type { GrainliftService, HttpOptions } from "./service.js";

export interface HttpServerOptions {
  port?: number;
  /** Plain HTTP is restricted to a local TLS proxy or local applications. */
  host?: "127.0.0.1" | "::1";
  maxConcurrentRequests?: number;
  requestTimeoutMs?: number;
  shutdownTimeoutMs?: number;
  tls?: { cert: string | Buffer; key: string | Buffer };
  /** VGI-RPC HTTP options, such as `externalStorage` for large requests and results. */
  http?: HttpOptions;
}
export interface RunningServer {
  endpoint: string;
  server: Server;
  close: () => Promise<void>;
}

/** Serve bounded authenticated HTTP on loopback with persistent connections. */
export async function serveHttp(
  service: GrainliftService,
  authenticate: AuthenticateFn,
  options: HttpServerOptions = {},
): Promise<RunningServer> {
  const host = options.host ?? "127.0.0.1";
  if (host !== "127.0.0.1" && host !== "::1") throw new TypeError("Plain HTTP must bind loopback");
  const concurrency = options.maxConcurrentRequests ?? 64;
  const timeout = options.requestTimeoutMs ?? 30_000;
  const shutdownTimeout = options.shutdownTimeoutMs ?? 5000;
  if (
    !Number.isSafeInteger(concurrency) ||
    concurrency < 1 ||
    !Number.isSafeInteger(timeout) ||
    timeout < 1 ||
    !Number.isSafeInteger(shutdownTimeout) ||
    shutdownTimeout < 1
  ) {
    throw new TypeError("Hosting limits must be positive integers");
  }
  const handler = service.httpHandler(authenticate, options.http);
  let active = 0;
  let stopping = false;
  const listener: RequestListener = async (incoming, outgoing) => {
    if (stopping || active >= concurrency) {
      outgoing.writeHead(503, { connection: "close" }).end();
      return;
    }
    active++;
    try {
      const body: Uint8Array[] = [];
      let length = 0;
      for await (const part of incoming) {
        const bytes = part as Buffer;
        length += bytes.length;
        if (length > service.limits.requestBytes) {
          outgoing.writeHead(413, { connection: "close" }).end();
          return;
        }
        body.push(bytes);
      }
      const headers = new Headers();
      for (let i = 0; i < incoming.rawHeaders.length; i += 2) {
        const name = incoming.rawHeaders[i]!;
        if (name.toLowerCase() === "authorization" && headers.has(name)) {
          outgoing.writeHead(401).end();
          return;
        }
        headers.append(name, incoming.rawHeaders[i + 1]!);
      }
      const method = incoming.method ?? "GET";
      const response = await handler(
        new Request(`http://localhost${incoming.url ?? "/"}`, {
          method,
          headers,
          ...(method === "GET" || method === "HEAD" ? {} : { body: Buffer.concat(body) }),
        }),
      );
      outgoing.writeHead(response.status, Object.fromEntries(response.headers));
      if (response.body) {
        for await (const bytes of response.body) {
          if (outgoing.destroyed) break;
          if (!outgoing.write(bytes))
            await new Promise<void>((resolve) => {
              const done = () => {
                outgoing.off("drain", done);
                outgoing.off("close", done);
                resolve();
              };
              outgoing.once("drain", done);
              outgoing.once("close", done);
            });
        }
      }
      outgoing.end();
    } catch {
      if (!outgoing.headersSent) outgoing.writeHead(500);
      outgoing.end();
    } finally {
      active--;
    }
  };
  const configuration = {
    requestTimeout: timeout,
    headersTimeout: Math.min(timeout, 10_000),
    maxHeaderSize: 16_384,
  };
  const server = options.tls
    ? createHttpsServer({ ...configuration, ...options.tls }, listener)
    : createServer(configuration, listener);
  server.keepAliveTimeout = 5000;
  server.maxConnections = concurrency * 2;
  server.listen(options.port ?? 0, host);
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Listener address unavailable");
  let closing: Promise<void> | undefined;
  return {
    endpoint: `${options.tls ? "https" : "http"}://${host === "::1" ? "[::1]" : host}:${address.port}`,
    server,
    close: () => {
      closing ??= (async () => {
        stopping = true;
        let timer: NodeJS.Timeout | undefined;
        const deadline = new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            server.closeAllConnections();
            void service.close().catch(() => {});
            reject(new Error("Graceful shutdown exceeded its deadline"));
          }, shutdownTimeout);
        });
        const drain = new Promise<void>((resolve, reject) =>
          server.close((error) => (error ? reject(error) : resolve())),
        );
        const cleanup = (async () => {
          await service.cancelPending();
          await service.close();
        })();
        try {
          await Promise.race([Promise.all([drain, cleanup]), deadline]);
        } finally {
          if (timer) clearTimeout(timer);
        }
      })();
      return closing;
    },
  };
}
