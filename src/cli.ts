// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
/**
 * Development hosting for a worker: call {@link run} from your own command.
 *
 * The toolkit installs no executable of its own; give each worker its own
 * command, e.g. a `bin` entry or `npm start` script whose main calls `run`.
 */
import { randomBytes } from "node:crypto";
import { readFile } from "node:fs/promises";
import { basename } from "node:path";
import { parseArgs } from "node:util";
import type { Worker } from "./api.js";
import { AuthContext, type AuthenticateFn, authenticateAnonymous, bearerAuthenticateStatic } from "./auth.js";
import { serveHttp } from "./hosting.js";
import { GrainliftService, type ServiceOptions } from "./service.js";
import { serveMutualTls } from "./transports.js";

export const TOKEN_VARIABLE = "GRAINLIFT_TOKEN";
export const ANONYMOUS_PRINCIPAL = "anonymous";
const DEVELOPER = "developer";

export interface RunOptions {
  /** The target clients open (`grainlift.target`); other targets are refused. */
  target: string;
  /** Command-line arguments; defaults to `process.argv.slice(2)`. */
  argv?: readonly string[];
  /** Command name shown by `--help`; defaults to the script's file name. */
  name?: string;
  /** Help text shown by `--help`. */
  description?: string;
  /** Default for `--auth`: `token` (the default) or `anonymous`. */
  auth?: "token" | "anonymous";
  /** Additional service configuration, such as limits; `authorize` is derived from `target`. */
  service?: Omit<ServiceOptions, "authorize">;
  /** Stops the server when aborted, in addition to SIGINT and SIGTERM. */
  signal?: AbortSignal;
}

class UsageError extends Error {}

function usage(name: string, description: string | undefined, auth: string): string {
  return [
    `usage: ${name} [--host {http,mtls}] [--port PORT] [--auth {token,anonymous}]`,
    `       [--tls-cert FILE --tls-key FILE --client-ca FILE --client-uri URI]`,
    ...(description ? ["", description] : []),
    "",
    "options:",
    "  --host {http,mtls}        loopback HTTP (default) or verified TCP/mTLS",
    "  --port PORT               loopback port (default: 8080)",
    "  --auth {token,anonymous}  HTTP access: require a bearer token, or also allow",
    `                            clients without one (default: ${auth})`,
    "  -h, --help                show this help message and exit",
    "",
    "mTLS (--host mtls):",
    "  --tls-cert FILE           server certificate chain (PEM)",
    "  --tls-key FILE            server private key (PEM)",
    "  --client-ca FILE          CA that issues client certificates (PEM)",
    "  --client-uri URI          authorized client certificate URI SAN",
    "",
  ].join("\n");
}

function developmentToken(token: string | undefined): string {
  if (token) return token;
  const generated = randomBytes(24).toString("base64url");
  process.stderr.write(`${TOKEN_VARIABLE} is not set; generated a token for this run:\n`);
  process.stderr.write(`    export ${TOKEN_VARIABLE}=${generated}\n`);
  return generated;
}

function httpAccess(auth: string): AuthenticateFn {
  const token = process.env[TOKEN_VARIABLE];
  const developer = new AuthContext("grainlift", true, DEVELOPER);
  if (auth === "anonymous") {
    process.stdout.write(
      `Anonymous access enabled: clients connect without a token as '${ANONYMOUS_PRINCIPAL}'\n`,
    );
    return authenticateAnonymous(ANONYMOUS_PRINCIPAL, token ? new Map([[token, developer]]) : undefined);
  }
  return bearerAuthenticateStatic(new Map([[developmentToken(token), developer]]));
}

function hasUriSan(subjectAltName: string | undefined, uri: string): boolean {
  // Node quotes SAN entries that contain separators, so accept both spellings.
  const entries = (subjectAltName ?? "").split(", ");
  return entries.includes(`URI:${uri}`) || entries.includes(`URI:${JSON.stringify(uri)}`);
}

async function stopped(signal: AbortSignal | undefined): Promise<void> {
  if (signal?.aborted) return;
  await new Promise<void>((resolve) => {
    const stop = () => {
      process.off("SIGINT", stop);
      process.off("SIGTERM", stop);
      signal?.removeEventListener("abort", stop);
      resolve();
    };
    process.once("SIGINT", stop);
    process.once("SIGTERM", stop);
    signal?.addEventListener("abort", stop, { once: true });
  });
}

/**
 * Serve one worker on loopback for development, choosing the host from the command line.
 *
 * HTTP hosts authenticate with the bearer token in `GRAINLIFT_TOKEN`; when it
 * is unset, a random token is generated and printed for the client to export.
 * With `--auth anonymous`, clients may also connect without a token; use it
 * only for workers that are safe to expose publicly, such as read-only data.
 * The mTLS host authorizes one client certificate URI instead. The promise
 * resolves after SIGINT, SIGTERM or `options.signal` once the server has shut
 * down. Invalid arguments print usage and set `process.exitCode` to 2. Production
 * deployments should configure `GrainliftService`, `serveHttp` or
 * `serveMutualTls` directly with their own credentials and limits.
 */
export async function run(worker: Worker, options: RunOptions): Promise<void> {
  const auth = options.auth ?? "token";
  if (auth !== "token" && auth !== "anonymous") throw new TypeError("auth must be 'token' or 'anonymous'");
  const name = options.name ?? basename(process.argv[1] ?? "grainlift-worker");
  const help = usage(name, options.description, auth);
  let values: Record<string, string | boolean | undefined>;
  let port: number;
  try {
    ({ values } = parseArgs({
      args: [...(options.argv ?? process.argv.slice(2))],
      options: {
        host: { type: "string", default: "http" },
        port: { type: "string", default: "8080" },
        auth: { type: "string", default: auth },
        "tls-cert": { type: "string" },
        "tls-key": { type: "string" },
        "client-ca": { type: "string" },
        "client-uri": { type: "string" },
        help: { type: "boolean", short: "h" },
      },
    }));
    if (values.help) {
      process.stdout.write(help);
      return;
    }
    port = Number(values.port);
    if (!/^[0-9]+$/.test(String(values.port)) || port > 65535) throw new UsageError("invalid --port");
    if (values.host !== "http" && values.host !== "mtls")
      throw new UsageError("--host must be 'http' or 'mtls'");
    if (values.auth !== "token" && values.auth !== "anonymous")
      throw new UsageError("--auth must be 'token' or 'anonymous'");
    if (
      values.host === "mtls" &&
      !(values["tls-cert"] && values["tls-key"] && values["client-ca"] && values["client-uri"])
    )
      throw new UsageError("--host mtls requires --tls-cert, --tls-key, --client-ca and --client-uri");
  } catch (error) {
    process.stderr.write(`${help}\n${name}: error: ${(error as Error).message}\n`);
    process.exitCode = 2;
    return;
  }
  const target = options.target;
  const service = new GrainliftService(worker, {
    ...options.service,
    authorize: (_principal, requested) => requested === target,
  });
  let server: { endpoint: string; close(): Promise<void> };
  try {
    if (values.host === "mtls") {
      const uri = String(values["client-uri"]);
      server = await serveMutualTls(service, {
        port,
        cert: await readFile(String(values["tls-cert"])),
        key: await readFile(String(values["tls-key"])),
        ca: await readFile(String(values["client-ca"])),
        authenticatePeer: (certificate) => {
          if (!hasUriSan(certificate.subjectAltName, uri))
            throw new Error("Client certificate is not authorized");
          return new AuthContext("mtls", true, DEVELOPER);
        },
      });
    } else {
      server = await serveHttp(service, httpAccess(String(values.auth)), { port });
    }
  } catch (error) {
    await service.close();
    throw error;
  }
  process.stdout.write(`Grainlift target '${target}' listening on ${server.endpoint}\n`);
  await stopped(options.signal);
  await server.close();
}
