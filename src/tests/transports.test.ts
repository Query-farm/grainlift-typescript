// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { once } from "node:events";
import { chmod, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { tcpConnect } from "@query-farm/vgi-rpc";
import { AdbcError, Connection, Statement } from "../api.js";
import { batch, Field, Int64, Schema } from "../arrow.js";
import { AuthContext } from "../auth.js";
import { GrainliftService } from "../service.js";
import { serveIroh, serveMutualTls, serveTcp } from "../transports.js";
import { decodeRecord, encodeRecord } from "../wire.js";

const identity = new AuthContext("local", true, "alice");
function fixture(pause: Promise<void> = Promise.resolve(), releaseError?: Error) {
  let releases = 0;
  const schema = new Schema([new Field("value", new Int64(), false)]);
  class TestStatement extends Statement {
    override async executeUpdate(): Promise<bigint> {
      await pause;
      return 1n;
    }
    override async execute() {
      return {
        schema,
        batches: [batch(schema, { value: [1n] }), batch(schema, { value: [2n] })],
        close: () => {
          releases++;
          if (releaseError) throw releaseError;
        },
      };
    }
  }
  class TestConnection extends Connection {
    override async newStatement() {
      return new TestStatement();
    }
  }
  const service = new GrainliftService({ open: async () => new TestConnection() }, { authorize: () => true });
  return { service, releases: () => releases };
}
function address(endpoint: string) {
  const url = new URL(endpoint);
  return { host: url.hostname, port: Number(url.port) };
}
async function deadline<T>(value: Promise<T>, milliseconds = 1000): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      value,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Test deadline exceeded")), milliseconds);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
test("plain TCP rejects routable binds and anonymous configured identities", async () => {
  const { service } = fixture();
  try {
    await assert.rejects(serveTcp(service, identity, { host: "0.0.0.0" }), /loopback/);
    await assert.rejects(serveTcp(service, AuthContext.anonymous()), /authenticated/);
  } finally {
    await service.close();
  }
});
test("TCP socket admission rejects excess peers and recovers after disconnect", async () => {
  const { service } = fixture();
  const host = await serveTcp(service, identity, { maxConnections: 1 });
  const first = connect(address(host.endpoint));
  first.on("error", () => {});
  await once(first, "connect");
  const second = connect(address(host.endpoint));
  second.on("error", () => {});
  try {
    await deadline(once(second, "close"));
  } finally {
    first.destroy();
    second.destroy();
    await host.close();
  }
});
test("idle sockets expire without leaving Arrow readers blocking shutdown", async () => {
  const { service } = fixture();
  const host = await serveTcp(service, identity, { idleTimeoutMs: 30, shutdownTimeoutMs: 100 });
  const socket = connect(address(host.endpoint));
  socket.on("error", () => {});
  await deadline(once(socket, "close"));
  await deadline(host.close());
});
test("connection byte budget is enforced below, at and above its exact boundary", async () => {
  for (const length of [7, 8, 9]) {
    const { service } = fixture();
    const host = await serveTcp(service, identity, { maxInputBytesPerConnection: 8 });
    const socket = connect(address(host.endpoint));
    socket.on("error", () => {});
    const prefix = Buffer.from([255, 255, 255, 255, 100, 0, 0, 0, 0]);
    let closed = false;
    socket.on("close", () => {
      closed = true;
    });
    socket.write(prefix.subarray(0, length));
    if (length > 8) await deadline(once(socket, "close"));
    else {
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
      assert.equal(closed, false);
    }
    await host.close();
  }
});
test("incomplete TLS handshakes time out and are also included in bounded shutdown", async () => {
  const directory = await mkdtemp(join(tmpdir(), "grainlift-tls-test-"));
  try {
    execFileSync(
      "openssl",
      [
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        join(directory, "key.pem"),
        "-out",
        join(directory, "cert.pem"),
        "-subj",
        "/CN=localhost",
        "-days",
        "1",
      ],
      { stdio: "ignore" },
    );
    const cert = await readFile(join(directory, "cert.pem"));
    const key = await readFile(join(directory, "key.pem"));
    for (const handshakeTimeoutMs of [40, 5000]) {
      const { service } = fixture();
      const host = await serveMutualTls(service, {
        ca: cert,
        cert,
        key,
        handshakeTimeoutMs,
        shutdownTimeoutMs: 200,
        authenticatePeer: () => identity,
      });
      const socket = connect(address(host.endpoint));
      socket.on("error", () => {});
      try {
        await once(socket, "connect");
        if (handshakeTimeoutMs === 40) await deadline(once(socket, "close"));
        await deadline(host.close());
      } finally {
        socket.destroy();
        await host.close();
      }
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
test("stock VGI client reuses TCP and preserves exact nested response records", async () => {
  const { service } = fixture();
  const host = await serveTcp(service, identity);
  const target = address(host.endpoint);
  const client = tcpConnect(target.host, target.port);
  try {
    const response = await client.call("open_connection", {
      request: encodeRecord(
        "OpenConnectionRequest",
        { target: "default", database_options: [], connection_options: [] },
        4096,
      ),
    });
    const session = decodeRecord("SessionResponse", response!.result, 4096);
    for (let index = 0; index < 3; index++) {
      const created = await client.call("new_statement", session);
      const statement = decodeRecord("StatementResponse", created!.result, 4096);
      await client.call("close_statement", statement);
    }
    await client.call("close_connection", session);
    assert.equal(service.snapshot().sessions, 0);
  } finally {
    client.close();
    await host.close();
  }
});
test("abrupt TCP producer disconnect releases its live result iterator", async () => {
  const fixtureValue = fixture();
  const host = await serveTcp(fixtureValue.service, identity);
  const target = address(host.endpoint);
  const client = tcpConnect(target.host, target.port);
  try {
    const response = await client.call("open_connection", {
      request: encodeRecord(
        "OpenConnectionRequest",
        { target: "default", database_options: [], connection_options: [] },
        4096,
      ),
    });
    const session = decodeRecord("SessionResponse", response!.result, 4096);
    const created = await client.call("new_statement", session);
    const statement = decodeRecord("StatementResponse", created!.result, 4096);
    const executed = await client.call("execute", statement);
    const result = decodeRecord("ExecuteResponse", executed!.result, 4096);
    const stream = await client.stream("read_result", {
      ...session,
      result_id: result.result_id,
      sequence: 0n,
    });
    await stream[Symbol.asyncIterator]().next();
    client.close();
    await new Promise<void>((resolve) => setTimeout(resolve, 30));
    assert.equal(fixtureValue.releases(), 1);
    assert.equal(fixtureValue.service.snapshot().results, 0);
  } finally {
    client.close();
    await host.close();
  }
});
test("Iroh requires explicit executable and exactly one service key policy", async () => {
  const { service } = fixture();
  try {
    await assert.rejects(
      serveIroh(service, {
        bridgePath: "vgi-iroh-bridge",
        ephemeral: true,
        issuer: "test",
        authenticateEndpoint: () => "alice",
      }),
      /configuration/,
    );
    await assert.rejects(
      serveIroh(service, {
        bridgePath: "/does/not/exist",
        issuer: "test",
        authenticateEndpoint: () => "alice",
      }),
      /configuration/,
    );
  } finally {
    await service.close();
  }
});
test("producer disconnect defers cleanup until another statement's callback leaves the session gate", async () => {
  let resume: (() => void) | undefined;
  const pause = new Promise<void>((resolve) => {
    resume = resolve;
  });
  const f = fixture(pause);
  const host = await serveTcp(f.service, identity);
  const target = address(host.endpoint);
  const client = tcpConnect(target.host, target.port);
  try {
    const response = await client.call("open_connection", {
      request: encodeRecord(
        "OpenConnectionRequest",
        { target: "default", database_options: [], connection_options: [] },
        4096,
      ),
    });
    const session = decodeRecord("SessionResponse", response!.result, 4096);
    const created = await client.call("new_statement", session);
    const statement = decodeRecord("StatementResponse", created!.result, 4096);
    const second = await client.call("new_statement", session);
    const other = decodeRecord("StatementResponse", second!.result, 4096);
    const executed = await client.call("execute", statement);
    const result = decodeRecord("ExecuteResponse", executed!.result, 4096);
    const stream = await client.stream("read_result", {
      ...session,
      result_id: result.result_id,
      sequence: 0n,
    });
    await stream[Symbol.asyncIterator]().next();
    const active = f.service.withIdentity(identity, () => f.service.invoke("execute_update", other));
    await new Promise<void>((resolve) => setImmediate(resolve));
    client.close();
    await new Promise<void>((resolve) => setTimeout(resolve, 25));
    assert.equal(f.releases(), 0);
    resume!();
    await active;
    assert.equal(f.releases(), 1);
    assert.equal(f.service.snapshot().results, 0);
  } finally {
    resume?.();
    client.close();
    await host.close();
  }
});

test("Iroh bridge failure before readiness closes the service and reaps the child", async () => {
  const directory = await mkdtemp(join(tmpdir(), "grainlift-bridge-test-"));
  const script = join(directory, "bridge.mjs");
  const report = join(directory, "report.json");
  const f = fixture();
  try {
    await writeFile(
      script,
      `#!${process.execPath}\nimport {writeFileSync} from 'node:fs';\nwriteFileSync(${JSON.stringify(report)}, JSON.stringify({pid:process.pid,upstream:process.argv[process.argv.indexOf('--raw-upstream')+1]}));\nprocess.exit(7);\n`,
    );
    await chmod(script, 0o700);
    await assert.rejects(
      serveIroh(f.service, {
        bridgePath: script,
        ephemeral: true,
        issuer: "test",
        authenticateEndpoint: () => "alice",
        startupTimeoutMs: 1000,
        shutdownTimeoutMs: 100,
      }),
      /before readiness/,
    );
    const details = JSON.parse(await readFile(report, "utf8")) as { pid: number; upstream: string };
    assert.throws(() => process.kill(details.pid, 0));
    await assert.rejects(stat(new URL(details.upstream).pathname), { code: "ENOENT" });
    await assert.rejects(
      f.service.withIdentity(identity, () =>
        f.service.invoke("open_connection", {
          target: "default",
          database_options: [],
          connection_options: [],
        }),
      ),
    );
  } finally {
    await f.service.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Iroh upstream requires its private preamble and bridge exit fails closed", async () => {
  const directory = await mkdtemp(join(tmpdir(), "grainlift-bridge-test-"));
  const script = join(directory, "bridge.mjs");
  const report = join(directory, "report.json");
  const f = fixture();
  let running: Awaited<ReturnType<typeof serveIroh>> | undefined;
  try {
    await writeFile(
      script,
      `#!${process.execPath}\nimport {writeFileSync} from 'node:fs';\nwriteFileSync(${JSON.stringify(report)}, JSON.stringify({pid:process.pid,upstream:process.argv[process.argv.indexOf('--raw-upstream')+1]}));\nconsole.log(JSON.stringify({endpoint_id:'a'.repeat(64),direct_addresses:['127.0.0.1:1']}));\nsetInterval(()=>{},1000);\n`,
    );
    await chmod(script, 0o700);
    running = await serveIroh(f.service, {
      bridgePath: script,
      ephemeral: true,
      issuer: "test",
      authenticateEndpoint: () => "alice",
      shutdownTimeoutMs: 100,
    });
    const details = JSON.parse(await readFile(report, "utf8")) as { pid: number; upstream: string };
    const path = new URL(details.upstream).pathname;
    assert.equal((await stat(dirname(path))).mode & 0o777, 0o700);
    assert.equal((await stat(path)).mode & 0o777, 0o600);
    const socket = connect(path);
    socket.on("error", () => {});
    socket.write(Buffer.alloc(16));
    await deadline(once(socket, "close"));
    process.kill(details.pid, "SIGTERM");
    await new Promise<void>((resolve) => setTimeout(resolve, 50));
    await running.close();
    await assert.rejects(stat(path), { code: "ENOENT" });
    assert.equal(f.service.snapshot().sessions, 0);
  } finally {
    await running?.close();
    await f.service.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("raw cancellation cleanup cannot leak downstream messages through VGI hook logging", async () => {
  const f = fixture(Promise.resolve(), new AdbcError("private downstream detail", "io"));
  const host = await serveTcp(f.service, identity);
  const target = address(host.endpoint);
  const client = tcpConnect(target.host, target.port);
  const messages: unknown[][] = [];
  const debug = console.debug;
  console.debug = (...args: unknown[]) => {
    messages.push(args);
  };
  try {
    const response = await client.call("open_connection", {
      request: encodeRecord(
        "OpenConnectionRequest",
        { target: "default", database_options: [], connection_options: [] },
        4096,
      ),
    });
    const session = decodeRecord("SessionResponse", response!.result, 4096);
    const created = await client.call("new_statement", session);
    const statement = decodeRecord("StatementResponse", created!.result, 4096);
    const executed = await client.call("execute", statement);
    const result = decodeRecord("ExecuteResponse", executed!.result, 4096);
    const stream = await client.stream("read_result", {
      ...session,
      result_id: result.result_id,
      sequence: 0n,
    });
    await stream[Symbol.asyncIterator]().next();
    await (stream as unknown as { cancel(): Promise<void> }).cancel();
    assert.equal(f.releases(), 1);
    assert.deepEqual(messages, []);
  } finally {
    console.debug = debug;
    client.close();
    await host.close();
  }
});
