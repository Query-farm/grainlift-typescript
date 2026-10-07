// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import {
  AdbcError,
  Connection,
  type ObjectFilters,
  type OpenOptions,
  type OptionKind,
  type OptionValue,
  type PartitionResult,
  type QueryResult,
  Statement,
  type StatisticsFilters,
  type TableIdentifier,
  type Worker,
} from "../api.js";
import {
  batch,
  binary,
  bool,
  encodeBatch,
  encodeSchema,
  field,
  int64,
  schema as makeSchema,
  type RecordBatch,
  type Schema,
} from "../arrow.js";
import { AuthContext } from "../auth.js";
import { GrainliftService } from "../service.js";
import { CONTRACT, decodeRecord, encodeRecord, optionToWire } from "../wire.js";

const identity = new AuthContext("test", true, "alice");
const resultSchema = makeSchema([field("value", int64(), false)]);
const one = () => batch(resultSchema, { value: [9007199254740993n] });
const bindingSchema = makeSchema([field("batch_ipc", binary(), false), field("finish", bool(), false)]);
const frame = (payload: Uint8Array, finish: boolean) =>
  batch(bindingSchema, { batch_ipc: [payload], finish: [finish] });
const isStatus =
  (status: string) =>
  (error: unknown): boolean =>
    error instanceof AdbcError && error.status === status;

class CompleteStatement extends Statement {
  calls: string[] = [];
  values = new Map<string, OptionValue>();
  bound: readonly RecordBatch[] = [];
  plan: Uint8Array = new Uint8Array();
  closed = 0;
  resultClosed = 0;
  override async setSqlQuery(sql: string): Promise<void> {
    this.calls.push(sql);
  }
  override async setSubstraitPlan(plan: Uint8Array): Promise<void> {
    this.plan = plan;
  }
  override async prepare(): Promise<void> {
    this.calls.push("prepare");
  }
  override async bind(_schema: Schema, value: RecordBatch): Promise<void> {
    this.bound = [value];
  }
  override async bindStream(_schema: Schema, values: readonly RecordBatch[]): Promise<void> {
    this.bound = values;
  }
  override async execute(): Promise<QueryResult> {
    return {
      schema: resultSchema,
      batches: [one(), one()],
      rowsAffected: 2n,
      close: () => {
        this.resultClosed++;
      },
    };
  }
  override async executeUpdate(): Promise<bigint> {
    return 9007199254740993n;
  }
  override async executeSchema(): Promise<Schema> {
    return resultSchema;
  }
  override async getParameterSchema(): Promise<Schema> {
    return resultSchema;
  }
  override async executePartitions(): Promise<PartitionResult> {
    return { schema: resultSchema, partitions: [new Uint8Array([0, 255, 1])], rowsAffected: 1n };
  }
  override async setOption(key: string, value: OptionValue): Promise<void> {
    this.values.set(key, value);
  }
  override async getOption(key: string, _kind: OptionKind): Promise<OptionValue> {
    return this.values.get(key)!;
  }
  override async cancel(): Promise<void> {
    this.calls.push("cancel");
  }
  override async close(): Promise<void> {
    this.closed++;
  }
}
class CompleteConnection extends Connection {
  statement = new CompleteStatement();
  calls: string[] = [];
  values = new Map<string, OptionValue>();
  filters: unknown;
  descriptor: Uint8Array = new Uint8Array();
  closed = 0;
  override async newStatement(): Promise<Statement> {
    return this.statement;
  }
  override async commit(): Promise<void> {
    this.calls.push("commit");
  }
  override async rollback(): Promise<void> {
    this.calls.push("rollback");
  }
  override async cancel(): Promise<void> {
    this.calls.push("cancel");
  }
  override async setOption(key: string, value: OptionValue): Promise<void> {
    this.values.set(key, value);
  }
  override async getOption(key: string, _kind: OptionKind): Promise<OptionValue> {
    return this.values.get(key)!;
  }
  override async getInfo(codes: bigint[] | null): Promise<QueryResult> {
    this.filters = codes;
    return this.statement.execute();
  }
  override async getObjects(filters: ObjectFilters): Promise<QueryResult> {
    this.filters = filters;
    return this.statement.execute();
  }
  override async getTableSchema(table: TableIdentifier): Promise<Schema> {
    this.filters = table;
    return resultSchema;
  }
  override async getTableTypes(): Promise<QueryResult> {
    return this.statement.execute();
  }
  override async getStatisticNames(): Promise<QueryResult> {
    return this.statement.execute();
  }
  override async getStatistics(filters: StatisticsFilters): Promise<QueryResult> {
    this.filters = filters;
    return this.statement.execute();
  }
  override async readPartition(descriptor: Uint8Array): Promise<QueryResult> {
    this.descriptor = descriptor;
    return this.statement.execute();
  }
  override async close(): Promise<void> {
    this.closed++;
  }
}
class CompleteWorker implements Worker {
  connections: CompleteConnection[] = [];
  options: OpenOptions[] = [];
  async open(options: OpenOptions): Promise<Connection> {
    this.options.push(options);
    const connection = new CompleteConnection();
    this.connections.push(connection);
    return connection;
  }
}
async function fixture(limits: ConstructorParameters<typeof GrainliftService>[1]["limits"] = {}) {
  const worker = new CompleteWorker();
  const service = new GrainliftService(worker, {
    authorize: (_, target) => target === "default",
    limits,
    allowedConnectionOptions: new Set(["counter"]),
  });
  const call = (name: string, request: Record<string, unknown>) =>
    service.withIdentity(identity, () => service.invoke(name, request));
  const { session_id } = await call("open_connection", {
    target: "default",
    database_options: [],
    connection_options: [],
  });
  const { statement_id } = await call("new_statement", { session_id });
  return {
    service,
    worker,
    call,
    handles: { session_id, statement_id },
    connection: worker.connections[0]!,
    statement: worker.connections[0]!.statement,
  };
}

test("registers every authoritative method and preserves exact typed records", async () => {
  const f = await fixture();
  try {
    assert.equal(f.service.methodNames().length, 31);
    assert.deepEqual(f.service.methodNames(), CONTRACT.methods.map((m) => m.name).sort());
    const value = { value: optionToWire(9007199254740993n, "int") };
    assert.deepEqual(decodeRecord("ValueResponse", encodeRecord("ValueResponse", value, 4096), 4096), value);
  } finally {
    await f.service.close();
  }
});

test("open_connection advertises each backend's explicit or unknown statistics support", async () => {
  for (const supported of [false, true, null]) {
    class DeclaredConnection extends CompleteConnection {
      override statisticsSupported(): boolean | null {
        return supported;
      }
      override statisticNamesSupported(): boolean | null {
        return supported;
      }
    }
    const service = new GrainliftService(
      { open: async () => new DeclaredConnection() },
      {
        authorize: () => true,
      },
    );
    try {
      const response = await service.withIdentity(identity, () =>
        service.invoke("open_connection", {
          target: "default",
          database_options: [],
          connection_options: [],
        }),
      );
      const decoded = decodeRecord("SessionResponse", encodeRecord("SessionResponse", response, 4096), 4096);
      assert.equal(decoded.statistics_supported, supported);
      assert.equal(decoded.statistic_names_supported, supported);
    } finally {
      await service.close();
    }
  }
});

test("dispatches preparation, schema, update, Substrait, transactions and cancellation", async () => {
  const f = await fixture();
  try {
    await f.call("set_sql_query", { ...f.handles, sql: "select ?" });
    await f.call("prepare", f.handles);
    const bytes = new Uint8Array([0, 255, 128, 1]);
    await f.call("set_substrait_plan", { ...f.handles, payload: bytes });
    assert.deepEqual(f.statement.plan, bytes);
    assert.deepEqual(await f.call("execute_update", f.handles), { rows_affected: 9007199254740993n });
    for (const method of ["execute_schema", "get_parameter_schema"]) {
      assert.deepEqual((await f.call(method, f.handles)).schema_ipc, encodeSchema(resultSchema));
    }
    for (const method of ["commit", "rollback", "cancel_connection", "cancel_statement"])
      await f.call(method, f.handles);
    assert.deepEqual(f.connection.calls, ["commit", "rollback", "cancel"]);
    assert.deepEqual(f.statement.calls, ["select ?", "prepare", "cancel"]);
  } finally {
    await f.service.close();
  }
});

test("option APIs preserve string, binary, bigint, and double values", async () => {
  const f = await fixture();
  try {
    for (const [kind, value] of [
      ["string", "hello"],
      ["bytes", new Uint8Array([0, 255])],
      ["int", 9007199254740993n],
      ["double", 1.25],
    ] as const) {
      for (const scope of ["connection", "statement"]) {
        await f.call(`set_${scope}_option`, {
          ...f.handles,
          key: "counter",
          value: optionToWire(value, kind),
        });
        assert.deepEqual(
          await f.call(`get_${scope}_option`, { ...f.handles, key: "counter", value_type: kind }),
          { value: optionToWire(value, kind) },
        );
      }
    }
  } finally {
    await f.service.close();
  }
});

test("binding stages bounded batches, acknowledges replay, finishes once, then executes", async () => {
  const f = await fixture();
  try {
    await f.service.withIdentity(identity, async () => {
      const cursor = await f.service.initBind(
        { ...f.handles, schema_ipc: encodeSchema(resultSchema) },
        false,
      );
      const input = frame(encodeBatch(one()), false);
      await f.service.pushBind(cursor, input);
      await f.service.pushBind(cursor, input);
      await assert.rejects(f.service.invoke("execute", f.handles), isStatus("invalid_state"));
      cursor.sequence++;
      await f.service.pushBind(cursor, frame(new Uint8Array(), true));
      await f.service.pushBind(cursor, frame(new Uint8Array(), true));
      assert.equal(f.statement.bound.length, 1);
      assert.equal(f.statement.bound[0]!.getChild("value")!.get(0), 9007199254740993n);
      await f.service.invoke("execute", f.handles);
    });
  } finally {
    await f.service.close();
  }
});

test("bind_stream accepts multiple and empty streams without losing schema", async () => {
  const f = await fixture();
  try {
    await f.service.withIdentity(identity, async () => {
      for (const count of [2, 0]) {
        const cursor = await f.service.initBind(
          { ...f.handles, schema_ipc: encodeSchema(resultSchema) },
          true,
        );
        for (let i = 0; i < count; i++) {
          await f.service.pushBind(cursor, frame(encodeBatch(one()), false));
          cursor.sequence++;
        }
        await f.service.pushBind(cursor, frame(new Uint8Array(), true));
        assert.equal(f.statement.bound.length, count);
      }
    });
  } finally {
    await f.service.close();
  }
});

test("metadata hooks preserve empty versus absent filters and create pull cursors", async () => {
  const f = await fixture();
  try {
    for (const codes of [null, [], [0n, 4294967295n]]) {
      const result = await f.call("get_info", { ...f.handles, codes });
      assert.deepEqual(f.connection.filters, codes);
      assert.equal(typeof result.result_id, "string");
    }
    const filters = {
      ...f.handles,
      depth: 3n,
      catalog: "",
      db_schema: null,
      table_name: "%",
      table_types: [],
      column_name: null,
    };
    await f.call("get_objects", filters);
    assert.deepEqual(f.connection.filters, filters);
    for (const method of ["get_table_types", "get_statistic_names"])
      assert.equal(typeof (await f.call(method, f.handles)).result_id, "string");
    const table = { ...f.handles, catalog: null, db_schema: "", table_name: "table" };
    await f.call("get_table_schema", table);
    assert.deepEqual(f.connection.filters, table);
    const stats = { ...table, approximate: true };
    await f.call("get_statistics", stats);
    assert.deepEqual(f.connection.filters, stats);
    await assert.rejects(
      f.call("get_info", { ...f.handles, codes: [1n << 32n] }),
      isStatus("invalid_arguments"),
    );
  } finally {
    await f.service.close();
  }
});

test("partition tokens retain descriptors and reject tampering and cross-principal access", async () => {
  const f = await fixture();
  try {
    const result = await f.call("execute_partitions", f.handles);
    const payload = (result.partitions as Uint8Array[])[0]!;
    await f.call("read_partition", { ...f.handles, payload });
    assert.deepEqual(f.connection.descriptor, new Uint8Array([0, 255, 1]));
    const broken = payload.slice();
    broken[0] = broken[0]! ^ 1;
    await assert.rejects(
      f.call("read_partition", { ...f.handles, payload: broken }),
      isStatus("invalid_arguments"),
    );
    await f.service.withIdentity(new AuthContext("test", true, "bob"), async () => {
      const other = await f.service.invoke("open_connection", {
        target: "default",
        database_options: [],
        connection_options: [],
      });
      await assert.rejects(
        f.service.invoke("read_partition", { ...other, payload }),
        isStatus("invalid_arguments"),
      );
    });
  } finally {
    await f.service.close();
  }
});

test("pull results replay one batch and release exactly once on exhaustion", async () => {
  const f = await fixture();
  try {
    await f.service.withIdentity(identity, async () => {
      const result = await f.service.invoke("execute", f.handles);
      const cursor = await f.service.initResult({ ...f.handles, ...result, sequence: 0n });
      const first = await f.service.next(cursor);
      assert.equal(await f.service.next(cursor), first);
      cursor.sequence = 2n;
      await assert.rejects(f.service.next(cursor), isStatus("invalid_arguments"));
      cursor.sequence = 1n;
      await f.service.next(cursor);
      cursor.sequence = 2n;
      assert.equal(await f.service.next(cursor), null);
      assert.equal(f.statement.resultClosed, 1);
      assert.equal(await f.service.next(cursor), null);
    });
  } finally {
    await f.service.close();
  }
  assert.equal(f.statement.resultClosed, 1);
  assert.deepEqual(f.service.snapshot(), { sessions: 0, statements: 0, results: 0, uploads: 0 });
});

test("cross-principal and cross-domain handles never reach the backend", async () => {
  const f = await fixture();
  try {
    for (const other of [new AuthContext("test", true, "bob"), new AuthContext("other", true, "alice")]) {
      await assert.rejects(
        f.service.withIdentity(other, () => f.service.invoke("execute", f.handles)),
        isStatus("not_found"),
      );
    }
    await assert.rejects(f.service.invoke("execute", f.handles), isStatus("unauthenticated"));
  } finally {
    await f.service.close();
  }
});

test("session, statement and result quotas are enforced at boundary", async () => {
  const f = await fixture({
    sessions: 1,
    sessionsPerPrincipal: 1,
    statementsPerSession: 1,
    resultsPerSession: 1,
  });
  try {
    await assert.rejects(
      f.call("open_connection", { target: "default", database_options: [], connection_options: [] }),
      isStatus("invalid_state"),
    );
    await assert.rejects(f.call("new_statement", f.handles), isStatus("invalid_state"));
    await f.call("get_table_types", f.handles);
    await assert.rejects(f.call("get_table_types", f.handles), isStatus("invalid_state"));
  } finally {
    await f.service.close();
  }
});

test("binding rejects immediately above byte and batch boundaries and cleans upload", async () => {
  const encoded = encodeBatch(one());
  for (const limit of [encoded.length - 1, encoded.length, encoded.length + 1]) {
    const f = await fixture({ bindBytes: limit, bindBatches: 1 });
    try {
      await f.service.withIdentity(identity, async () => {
        const cursor = await f.service.initBind(
          { ...f.handles, schema_ipc: encodeSchema(resultSchema) },
          true,
        );
        if (limit < encoded.length) {
          await assert.rejects(
            f.service.pushBind(cursor, frame(encoded, false)),
            isStatus("invalid_arguments"),
          );
        } else {
          await f.service.pushBind(cursor, frame(encoded, false));
          cursor.sequence++;
          await assert.rejects(
            f.service.pushBind(cursor, frame(encoded, false)),
            isStatus("invalid_arguments"),
          );
        }
        assert.equal(f.service.snapshot().uploads, 0);
      });
    } finally {
      await f.service.close();
    }
  }
});

test("bound batches over the batch limit or not Arrow are client-visible errors", async () => {
  const limit = 8192;
  const encoded = encodeBatch(
    batch(resultSchema, { value: Array.from({ length: 2000 }, (_, i) => BigInt(i)) }),
  );
  assert.ok(encoded.length > limit);
  // A bind turn may be as large as the request limit, so both are below it.
  const f = await fixture({ batchBytes: limit, requestBytes: limit });
  try {
    await f.service.withIdentity(identity, async () => {
      for (const [payload, message] of [
        [encoded, new RegExp(`bound batch is ${encoded.length} bytes; the limit is ${limit}`)],
        [new Uint8Array(32), /not a valid Arrow IPC stream/],
      ] as const) {
        const cursor = await f.service.initBind(
          { ...f.handles, schema_ipc: encodeSchema(resultSchema) },
          true,
        );
        await assert.rejects(f.service.pushBind(cursor, frame(payload, false)), (error: unknown) => {
          assert.ok(error instanceof AdbcError && error.status === "invalid_arguments", String(error));
          assert.match(error.message, message);
          return true;
        });
      }
    });
  } finally {
    await f.service.close();
  }
});

test("a bound batch that fits the request limit is accepted above batchBytes", async () => {
  // The driver sizes bind turns by the HTTP request limit alone.
  const encoded = encodeBatch(
    batch(resultSchema, { value: Array.from({ length: 2000 }, (_, i) => BigInt(i)) }),
  );
  const f = await fixture({ batchBytes: 8192, requestBytes: encoded.length * 2 });
  try {
    await f.service.withIdentity(identity, async () => {
      const cursor = await f.service.initBind({ ...f.handles, schema_ipc: encodeSchema(resultSchema) }, true);
      await f.service.pushBind(cursor, frame(encoded, false));
      cursor.sequence++;
      await f.service.pushBind(cursor, frame(new Uint8Array(), true));
      assert.equal(f.statement.bound[0]!.numRows, 2000);
    });
  } finally {
    await f.service.close();
  }
});

test("authoritative options reject collisions and copy mutable configured bytes", async () => {
  const worker = new CompleteWorker();
  const secret = new Uint8Array([1, 2, 3]);
  const service = new GrainliftService(worker, {
    authorize: () => true,
    databaseOptions: new Map([["secret", secret]]),
  });
  secret[0] = 9;
  try {
    await service.withIdentity(identity, async () => {
      const request = { target: "default", database_options: [], connection_options: [] };
      await service.invoke("open_connection", request);
      assert.deepEqual(worker.options[0]!.databaseOptions.get("secret"), new Uint8Array([1, 2, 3]));
      (worker.options[0]!.databaseOptions.get("secret") as Uint8Array)[0] = 8;
      await service.invoke("open_connection", request);
      assert.deepEqual(worker.options[1]!.databaseOptions.get("secret"), new Uint8Array([1, 2, 3]));
      await assert.rejects(
        service.invoke("open_connection", {
          ...request,
          database_options: [{ key: "secret", value: optionToWire("replace", "string") }],
        }),
        isStatus("unauthorized"),
      );
    });
  } finally {
    await service.close();
  }
});

test("ADBC errors preserve repeated binary details and arbitrary exceptions are sanitized", async () => {
  const error = new AdbcError("client-visible", "invalid_data", {
    sqlstate: "22000",
    vendorCode: -42,
    details: [
      ["key", new Uint8Array([0])],
      ["key", new Uint8Array([255])],
    ],
  });
  const wire = JSON.parse(error.message);
  assert.deepEqual(wire.details, [
    ["key", "AA=="],
    ["key", "/w=="],
  ]);
  assert.equal(wire.vendor_code, -42);
  const f = await fixture();
  try {
    f.statement.execute = async () => {
      throw new Error("secret credentials and SQL");
    };
    await assert.rejects(
      f.call("execute", f.handles),
      (failure: unknown) =>
        failure instanceof AdbcError && failure.status === "internal" && !failure.message.includes("secret"),
    );
  } finally {
    await f.service.close();
  }
});

test("busy shutdown cleans idle sessions and eventually cleans a completing callback", async () => {
  const f = await fixture();
  let resume: (() => void) | undefined;
  const wait = new Promise<void>((resolve) => {
    resume = resolve;
  });
  f.statement.executeUpdate = async () => {
    await wait;
    return 1n;
  };
  const active = f.call("execute_update", f.handles);
  await new Promise<void>((resolve) => setImmediate(resolve));
  await assert.rejects(f.service.close(), isStatus("timeout"));
  resume!();
  await active;
  assert.equal(f.service.snapshot().sessions, 0);
  assert.equal(f.connection.closed, 1);
});

test("negative affected rows and oversized result registration close backend results", async () => {
  const f = await fixture();
  try {
    f.statement.execute = async () => ({
      schema: resultSchema,
      batches: [one()],
      rowsAffected: -2n,
      close: () => {
        f.statement.resultClosed++;
      },
    });
    await assert.rejects(f.call("execute", f.handles), isStatus("invalid_data"));
    assert.equal(f.statement.resultClosed, 1);
    assert.equal(f.service.snapshot().results, 0);
  } finally {
    await f.service.close();
  }
});

test("idle reaping closes abandoned statements and results", async () => {
  const f = await fixture({ idleMs: 20 });
  await f.call("execute", f.handles);
  await new Promise<void>((resolve) => setTimeout(resolve, 80));
  assert.equal(f.service.snapshot().sessions, 0);
  assert.equal(f.statement.resultClosed, 1);
  assert.equal(f.connection.closed, 1);
  await f.service.close();
});

test("independent principals keep interleaved results isolated after one closes", async () => {
  const worker = new CompleteWorker();
  const service = new GrainliftService(worker, {
    authorize: () => true,
    limits: { sessions: 2, sessionsPerPrincipal: 1 },
  });
  const bob = new AuthContext("test", true, "bob");
  const call = (who: AuthContext, method: string, request: Record<string, unknown>) =>
    service.withIdentity(who, () => service.invoke(method, request));
  const open = (who: AuthContext) =>
    call(who, "open_connection", { target: "default", database_options: [], connection_options: [] });
  try {
    const aliceSession = await open(identity);
    const bobSession = await open(bob);
    const aliceStatement = await call(identity, "new_statement", aliceSession);
    const bobStatement = await call(bob, "new_statement", bobSession);
    const aliceResult = await call(identity, "execute", aliceStatement);
    const bobResult = await call(bob, "execute", bobStatement);
    const aliceRequest = { ...aliceSession, ...aliceResult, sequence: 0n };
    const bobRequest = { ...bobSession, ...bobResult, sequence: 0n };
    await assert.rejects(
      service.withIdentity(bob, () => service.initResult(aliceRequest)),
      isStatus("not_found"),
    );
    const aliceCursor = await service.withIdentity(identity, () => service.initResult(aliceRequest));
    const bobCursor = await service.withIdentity(bob, () => service.initResult(bobRequest));
    const [aliceFirst, bobFirst] = await Promise.all([
      service.withIdentity(identity, () => service.next(aliceCursor)),
      service.withIdentity(bob, () => service.next(bobCursor)),
    ]);
    assert.equal(aliceFirst?.numRows, 1);
    assert.equal(bobFirst?.numRows, 1);
    await call(identity, "close_connection", aliceSession);
    assert.deepEqual(service.snapshot(), { sessions: 1, statements: 1, results: 1, uploads: 0 });
    assert.equal(worker.connections[0]!.statement.resultClosed, 1);
    assert.equal(worker.connections[1]!.statement.resultClosed, 0);
    bobCursor.sequence = 1n;
    assert.equal((await service.withIdentity(bob, () => service.next(bobCursor)))?.numRows, 1);
    bobCursor.sequence = 2n;
    assert.equal(await service.withIdentity(bob, () => service.next(bobCursor)), null);
    assert.equal(worker.connections[1]!.statement.resultClosed, 1);
    await call(bob, "close_connection", bobSession);
    assert.deepEqual(service.snapshot(), { sessions: 0, statements: 0, results: 0, uploads: 0 });
  } finally {
    await service.close();
  }
});

test("bounded session churn releases partial results without disturbing a live reader", async () => {
  const worker = new CompleteWorker();
  const service = new GrainliftService(worker, {
    authorize: () => true,
    limits: { sessions: 2, sessionsPerPrincipal: 1 },
  });
  const bob = new AuthContext("test", true, "bob");
  const call = (who: AuthContext, method: string, request: Record<string, unknown>) =>
    service.withIdentity(who, () => service.invoke(method, request));
  const open = (who: AuthContext) =>
    call(who, "open_connection", { target: "default", database_options: [], connection_options: [] });
  try {
    const observer = await open(identity);
    const observerStatement = await call(identity, "new_statement", observer);
    const observerResult = await call(identity, "execute", observerStatement);
    const observerCursor = await service.withIdentity(identity, () =>
      service.initResult({ ...observer, ...observerResult, sequence: 0n }),
    );
    assert.equal((await service.withIdentity(identity, () => service.next(observerCursor)))?.numRows, 1);
    for (let index = 0; index < 64; index++) {
      const session = await open(bob);
      await assert.rejects(open(bob), isStatus("invalid_state"));
      const statement = await call(bob, "new_statement", session);
      const result = await call(bob, "execute", statement);
      const cursor = await service.withIdentity(bob, () =>
        service.initResult({ ...session, ...result, sequence: 0n }),
      );
      assert.equal((await service.withIdentity(bob, () => service.next(cursor)))?.numRows, 1);
      await call(bob, "close_connection", session);
      assert.deepEqual(service.snapshot(), { sessions: 1, statements: 1, results: 1, uploads: 0 });
      assert.equal(worker.connections[index + 1]!.closed, 1);
      assert.equal(worker.connections[index + 1]!.statement.resultClosed, 1);
    }
    observerCursor.sequence = 1n;
    assert.equal((await service.withIdentity(identity, () => service.next(observerCursor)))?.numRows, 1);
    await call(identity, "close_connection", observer);
  } finally {
    await service.close();
  }
  assert.equal(worker.connections.length, 65);
  assert.ok(worker.connections.every((connection) => connection.closed === 1));
  assert.deepEqual(service.snapshot(), { sessions: 0, statements: 0, results: 0, uploads: 0 });
});
