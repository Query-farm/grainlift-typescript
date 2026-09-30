// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { Connection, type OpenOptions, type QueryResult, Statement } from "../api.js";
import { batch, Field, Int64, Schema } from "../arrow.js";
import {
  ANONYMOUS_DOMAIN,
  AuthContext,
  type AuthenticateFn,
  authenticateAnonymous,
  bearerAuthenticateStatic,
} from "../auth.js";
import { serveHttp } from "../hosting.js";
import { GrainliftService } from "../service.js";
import { connect, query } from "./http-client.js";

const TOKEN = "anonymous-test-token-123456";
const alice = new AuthContext("bearer", true, "alice");
const schema = new Schema([new Field("value", new Int64(), false)]);

class TestStatement extends Statement {
  override async setSqlQuery(): Promise<void> {}
  override async execute(): Promise<QueryResult> {
    return { schema, batches: [batch(schema, { value: [1n, 2n] }), batch(schema, { value: [3n] })] };
  }
}
class TestConnection extends Connection {
  override async newStatement(): Promise<Statement> {
    return new TestStatement();
  }
}
function fixture(authenticate: AuthenticateFn) {
  const opened: OpenOptions[] = [];
  const service = new GrainliftService(
    {
      open: async (options) => {
        opened.push(options);
        return new TestConnection();
      },
    },
    { authorize: () => true },
  );
  return { service, opened, handler: service.httpHandler(authenticate) };
}

test("an anonymous-only service needs no credentials, even across continuations", async () => {
  const f = fixture(authenticateAnonymous("public"));
  const client = connect(f.handler);
  try {
    const { batches } = await query(client);
    assert.deepEqual(batches, [[1, 2], [3]]);
    assert.deepEqual(
      f.opened.map((o) => o.principal),
      ["public"],
    );
    assert.ok(client.paths.some((path) => path.endsWith("/read_result/exchange")));
  } finally {
    client.close();
    await f.service.close();
  }
});

test("a token-only service still rejects missing credentials", async () => {
  const f = fixture(bearerAuthenticateStatic(new Map([[TOKEN, alice]])));
  const client = connect(f.handler);
  try {
    await assert.rejects(query(client));
    assert.equal(f.service.snapshot().sessions, 0);
  } finally {
    client.close();
    await f.service.close();
  }
});

test("a wrong token is rejected, not downgraded to anonymous", async () => {
  const f = fixture(authenticateAnonymous("public", new Map([[TOKEN, alice]])));
  try {
    for (const bad of ["Bearer wrong-token-000000000000", "", "Basic x"]) {
      const client = connect(f.handler, bad);
      await assert.rejects(query(client));
      client.close();
    }
    assert.equal(f.service.snapshot().sessions, 0);
  } finally {
    await f.service.close();
  }
});

test("anonymous and token principals are isolated", async () => {
  const f = fixture(authenticateAnonymous("public", new Map([[TOKEN, alice]])));
  const token = connect(f.handler, `Bearer ${TOKEN}`);
  const anonymous = connect(f.handler);
  try {
    const own = await query(token);
    const shared = await query(anonymous);
    assert.deepEqual(own.batches, [[1, 2], [3]]);
    assert.deepEqual(shared.batches, [[1, 2], [3]]);
    assert.deepEqual(f.opened.map((o) => o.principal).sort(), ["alice", "public"]);
    await assert.rejects(anonymous.call("new_statement", { session_id: own.session_id }), /not_found/);
    await assert.rejects(token.call("new_statement", { session_id: shared.session_id }), /not_found/);
  } finally {
    token.close();
    anonymous.close();
    await f.service.close();
  }
});

test("an anonymous continuation cannot be resumed with a token", async () => {
  const f = fixture(authenticateAnonymous("public", new Map([[TOKEN, alice]])));
  const client = connect(f.handler);
  try {
    const { session_id } = await client.call("open_connection", {
      target: "default",
      database_options: [],
      connection_options: [],
    });
    const { statement_id } = await client.call("new_statement", { session_id });
    await client.call("set_sql_query", { session_id, statement_id, sql: "query" });
    const { result_id } = await client.call("execute", { session_id, statement_id });
    const stream = await client.stream("read_result", { session_id, result_id, sequence: 0n });
    const batches = stream[Symbol.asyncIterator]();
    assert.equal((await batches.next()).value.length, 2);
    client.authorization = `Bearer ${TOKEN}`;
    await assert.rejects(batches.next());
  } finally {
    client.close();
    await f.service.close();
  }
});

test("token identities cannot claim the anonymous principal or domain", async () => {
  const rotated = new Map([[TOKEN, alice]]);
  const f = fixture(authenticateAnonymous("public", (request) => bearerAuthenticateStatic(rotated)(request)));
  try {
    for (const identity of [
      new AuthContext("bearer", true, "public"),
      new AuthContext(ANONYMOUS_DOMAIN, true, "alice"),
    ]) {
      rotated.set(TOKEN, identity);
      const client = connect(f.handler, `Bearer ${TOKEN}`);
      await assert.rejects(query(client));
      client.close();
    }
    assert.equal(f.service.snapshot().sessions, 0);
  } finally {
    await f.service.close();
  }
});

test("access configuration is validated", () => {
  assert.throws(() => authenticateAnonymous(""), /Invalid anonymous principal/);
  assert.throws(() => authenticateAnonymous("x".repeat(1025)), /Invalid anonymous principal/);
  assert.throws(
    () => authenticateAnonymous("public", new Map([[TOKEN, new AuthContext("bearer", true, "public")]])),
    /must differ/,
  );
});

test("serveHttp serves anonymous clients over a real socket", async () => {
  const service = new GrainliftService({ open: async () => new TestConnection() }, { authorize: () => true });
  const host = await serveHttp(service, authenticateAnonymous("public"));
  const client = connect(host.endpoint);
  try {
    assert.deepEqual((await query(client)).batches, [[1, 2], [3]]);
  } finally {
    client.close();
    await host.close();
  }
});
