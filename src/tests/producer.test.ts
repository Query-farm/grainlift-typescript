// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { AdbcError, Connection, QueryResult, ResultProducer, Statement } from "../api.js";
import { batch, field, int64, schema as makeSchema, type RecordBatch } from "../arrow.js";
import { AuthContext, bearerAuthenticateStatic } from "../auth.js";
import { GrainliftService } from "../service.js";
import { connect } from "./http-client.js";

const identity = new AuthContext("test", true, "alice");
const schema = makeSchema([field("n", int64(), false), field("total", int64(), false)]);
const isStatus =
  (status: string, message?: string) =>
  (error: unknown): boolean =>
    error instanceof AdbcError &&
    error.status === status &&
    (message === undefined || JSON.parse(error.message).message.includes(message));

/** Emit `perBatch` numbers per call with a running total that spans batches. */
class RunningTotal extends ResultProducer {
  next = 0n;
  total = 0n;
  constructor(
    readonly stop: bigint,
    readonly perBatch = 2n,
  ) {
    super();
  }
  produce(): RecordBatch | null {
    if (this.next >= this.stop) return null;
    const n: bigint[] = [];
    const total: bigint[] = [];
    for (; this.next < this.stop && n.length < this.perBatch; this.next++) {
      this.total += this.next;
      n.push(this.next);
      total.push(this.total);
    }
    return batch(schema, { n, total });
  }
}
ResultProducer.register("tests:RunningTotal", RunningTotal);

/** Carry an oversized state field. */
class Padded extends ResultProducer {
  padding = "x".repeat(1024);
  produce(): null {
    return null;
  }
}
ResultProducer.register("tests:Padded", Padded);

class ProducerStatement extends Statement {
  sql = "";
  closed = 0;
  override async setSqlQuery(sql: string): Promise<void> {
    this.sql = sql;
  }
  override async execute(): Promise<QueryResult> {
    const result = QueryResult.fromProducer(
      schema,
      this.sql === "padded" ? new Padded() : new RunningTotal(BigInt(this.sql)),
    );
    return { ...result, close: () => void this.closed++ };
  }
}
class ProducerConnection extends Connection {
  statement = new ProducerStatement();
  override async newStatement(): Promise<Statement> {
    return this.statement;
  }
}
function service(limits = {}) {
  const connection = new ProducerConnection();
  return {
    connection,
    service: new GrainliftService({ open: async () => connection }, { authorize: () => true, limits }),
  };
}
async function execute(value: GrainliftService, sql: string) {
  const { session_id } = await value.invoke("open_connection", {
    target: "default",
    database_options: [],
    connection_options: [],
  });
  const { statement_id } = await value.invoke("new_statement", { session_id });
  await value.invoke("set_sql_query", { session_id, statement_id, sql });
  const { result_id } = await value.invoke("execute", { session_id, statement_id });
  return { session_id, result_id };
}
const totals = (value: RecordBatch | null) => Array.from(value?.getChild("total") ?? [], Number);

test("HTTP resumes producers from continuation tokens without retaining batches", async () => {
  const { service: value, connection } = service();
  const token = "producer-test-token-123456";
  const handler = value.httpHandler(bearerAuthenticateStatic(new Map([[token, identity]])));
  const client = connect(handler, `Bearer ${token}`);
  try {
    const { session_id } = await client.call("open_connection", {
      target: "default",
      database_options: [],
      connection_options: [],
    });
    const { statement_id } = await client.call("new_statement", { session_id });
    await client.call("set_sql_query", { session_id, statement_id, sql: "5" });
    const { result_id } = await client.call("execute", { session_id, statement_id });
    const stream = await client.stream("read_result", { session_id, result_id, sequence: 0n });
    const received: number[][] = [];
    for await (const rows of stream) received.push(rows.map((row) => Number(row.total)));
    assert.deepEqual(received.flat(), [0, 1, 3, 6, 10]);
    // Each later batch arrives through a continuation-token exchange.
    assert.ok(client.paths.filter((path) => path.endsWith("/read_result/exchange")).length >= 2);
    const sessions = (value as unknown as { sessions: Map<string, { results: Map<string, unknown> }> })
      .sessions;
    const result = sessions.get(String(session_id))!.results.get(String(result_id)) as {
      previous?: unknown;
      ended: boolean;
      sequence: bigint;
    };
    assert.equal(result.previous, undefined);
    assert.equal(result.ended, true);
    assert.equal(result.sequence, 3n);
    assert.equal(connection.statement.closed, 1);
  } finally {
    client.close();
    await value.close();
  }
});

test("replay re-produces the previous batch from token state; older sequences fail", async () => {
  const { service: value } = service();
  try {
    await value.withIdentity(identity, async () => {
      const handles = await execute(value, "5");
      const initial = await value.initResult({ ...handles, sequence: 0n });
      assert.equal(typeof initial.producer, "string");
      const first = await value.next({ ...initial });
      const retry = { ...initial };
      assert.deepEqual(totals(await value.next(retry)), totals(first));
      const second = { ...retry, sequence: 1n };
      assert.deepEqual(totals(await value.next(second)), [3, 6]);
      await assert.rejects(value.next({ ...initial }), isStatus("invalid_arguments", "sequence"));
      await assert.rejects(
        value.initResult({ ...handles, sequence: 1n }),
        isStatus("invalid_arguments", "continuation tokens"),
      );
      const third = { ...second, sequence: 2n };
      assert.deepEqual(totals(await value.next(third)), [10]);
      const end = { ...third, sequence: 3n };
      assert.equal(await value.next({ ...end }), null);
      assert.equal(await value.next({ ...end }), null);
    });
  } finally {
    await value.close();
  }
});

test("in-memory iteration matches token resumption", async () => {
  const result = QueryResult.fromProducer(schema, new RunningTotal(5n));
  const seen: number[][] = [];
  for await (const value of result.batches) seen.push(totals(value)!);
  assert.deepEqual(seen, [[0, 1], [3, 6], [10]]);
  const producer = new RunningTotal(3000n, 1024n);
  await producer.produce();
  const resumed = ResultProducer.decode(producer.encode());
  assert.ok(resumed instanceof RunningTotal);
  assert.deepEqual({ ...resumed }, { ...producer });
  assert.deepEqual(totals(await resumed.produce()), totals(await producer.produce()));
});

test("oversized producer state is rejected before any token is issued", async () => {
  const { service: value, connection } = service({ producerStateBytes: 512 });
  try {
    await value.withIdentity(identity, async () => {
      await assert.rejects(execute(value, "padded"), isStatus("invalid_data", "state exceeds"));
    });
    assert.equal(connection.statement.closed, 1);
  } finally {
    await value.close();
  }
});

test("only registered producer types decode", () => {
  const encoded = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
  for (const payload of [
    encoded({ type: "tests:Unknown", state: {} }),
    encoded({ type: "tests:RunningTotal", state: [] }),
    encoded({ state: {} }),
    new TextEncoder().encode("not json"),
  ])
    assert.throws(() => ResultProducer.decode(payload), isStatus("invalid_data", "Unknown result producer"));
  class Unregistered extends ResultProducer {
    produce(): null {
      return null;
    }
  }
  assert.throws(() => new Unregistered().encode(), isStatus("invalid_data", "not registered"));
  class Unserializable extends ResultProducer {
    when = new Date();
    produce(): null {
      return null;
    }
  }
  ResultProducer.register("tests:Unserializable", Unserializable);
  assert.throws(() => new Unserializable().encode(), isStatus("invalid_data", "not serializable"));
  assert.throws(() => ResultProducer.register("tests:RunningTotal", Padded), TypeError);
  const polluted = ResultProducer.decode(
    new TextEncoder().encode('{"type":"tests:Padded","state":{"__proto__":{"produce":1},"padding":"y"}}'),
  );
  assert.ok(polluted instanceof Padded);
  assert.equal(Object.getPrototypeOf(polluted), Padded.prototype);
});

test("a producer fetch for a released result reports not_found", async () => {
  const { service: value } = service();
  try {
    await value.withIdentity(identity, async () => {
      const handles = await execute(value, "1");
      const cursor = await value.initResult({ ...handles, sequence: 0n });
      await value.invoke("close_result", handles);
      await assert.rejects(value.next(cursor), isStatus("not_found"));
    });
  } finally {
    await value.close();
  }
});
