// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { request } from "node:http";
import { test } from "node:test";
import { Connection, Statement } from "../api.js";
import { AuthContext } from "../auth.js";
import { serveHttp } from "../hosting.js";
import { GrainliftService } from "../service.js";

const identity = new AuthContext("test", true, "alice");
test("hosting shutdown cancels active backend work before waiting for drain", async () => {
  let release: (() => void) | undefined;
  const paused = new Promise<void>((resolve) => {
    release = resolve;
  });
  class BusyStatement extends Statement {
    override async executeUpdate(): Promise<bigint> {
      await paused;
      return 1n;
    }
    override async cancel(): Promise<void> {
      release!();
    }
  }
  class BusyConnection extends Connection {
    override async newStatement(): Promise<Statement> {
      return new BusyStatement();
    }
  }
  const service = new GrainliftService({ open: async () => new BusyConnection() }, { authorize: () => true });
  const host = await serveHttp(service, () => identity, { shutdownTimeoutMs: 1000 });
  const active = service.withIdentity(identity, async () => {
    const session = await service.invoke("open_connection", {
      target: "default",
      database_options: [],
      connection_options: [],
    });
    const statement = await service.invoke("new_statement", session);
    return service.invoke("execute_update", statement);
  });
  await new Promise<void>((resolve) => setImmediate(resolve));
  await host.close();
  await active;
  assert.equal(service.snapshot().sessions, 0);
});

test("hosting rejects excess concurrent request bodies before buffering", async () => {
  const service = new GrainliftService({ open: async () => new Connection() }, { authorize: () => true });
  const host = await serveHttp(service, () => identity, {
    maxConcurrentRequests: 1,
    shutdownTimeoutMs: 1000,
  });
  const first = request(host.endpoint, { method: "POST", headers: { "content-length": "1000" } });
  first.on("error", () => {});
  first.flushHeaders();
  first.write("x");
  await new Promise<void>((resolve) => setTimeout(resolve, 25));
  try {
    const response = await fetch(host.endpoint, { method: "POST", body: "x" });
    assert.equal(response.status, 503);
  } finally {
    first.destroy();
    await host.close();
  }
});

test("hosting forces stuck sockets closed by the shutdown deadline", async () => {
  const service = new GrainliftService({ open: async () => new Connection() }, { authorize: () => true });
  const host = await serveHttp(service, () => identity, { shutdownTimeoutMs: 40 });
  const pending = request(host.endpoint, { method: "POST", headers: { "content-length": "1000" } });
  pending.on("error", () => {});
  pending.flushHeaders();
  pending.write("x");
  await new Promise<void>((resolve) => setTimeout(resolve, 15));
  const start = Date.now();
  await assert.rejects(host.close(), /shutdown exceeded/);
  assert.ok(Date.now() - start < 1000);
  pending.destroy();
  assert.equal(service.snapshot().sessions, 0);
});
