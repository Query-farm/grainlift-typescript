// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import { inspect } from "node:util";
import { httpConnect } from "@query-farm/vgi-rpc";
import { Connection, type QueryResult, Statement } from "../api.js";
import {
  batch,
  binary,
  encodeBatch,
  encodeSchema,
  field,
  schema as makeSchema,
  type RecordBatch,
  type Schema,
} from "../arrow.js";
import { AuthContext } from "../auth.js";
import { serveHttp } from "../hosting.js";
import { GrainliftService } from "../service.js";
import { ExternalStorageConfig } from "../storage.js";
import { CONTRACT, decodeRecord, encodeRecord } from "../wire.js";

const credentials = { accessKeyId: "AK", secretAccessKey: "secret-canary" };

test("presigning matches AWS's documented example", async () => {
  const storage = new ExternalStorageConfig({
    endpoint: "https://s3.amazonaws.com",
    bucket: "examplebucket",
    region: "us-east-1",
    accessKeyId: "AKIAIOSFODNN7EXAMPLE",
    secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    virtualHostedStyle: true,
  });
  assert.equal(
    await storage.presign("GET", "test.txt", new Date("2013-05-24T00:00:00Z"), 86400),
    "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
      "&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" +
      "&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host" +
      "&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404",
  );
});

test("path-style URLs name the bucket, and only its objects are accepted", async () => {
  const storage = new ExternalStorageConfig({
    endpoint: "https://s3.amazonaws.com",
    bucket: "examplebucket",
    ...credentials,
  });
  const url = await storage.presign("PUT", "grainlift/a b.arrow");
  assert.ok(url.startsWith("https://s3.amazonaws.com/examplebucket/grainlift/a%20b.arrow?"), url);
  storage.validate("https://s3.amazonaws.com/examplebucket/grainlift/x.arrow?sig=1");
  for (const bad of [
    "https://s3.amazonaws.com/otherbucket/x.arrow",
    "http://s3.amazonaws.com/examplebucket/x.arrow",
    "https://169.254.169.254/examplebucket/x",
    "not a url",
  ])
    assert.throws(() => storage.validate(bad), bad);
});

test("settings are validated, credentials default to the environment, and the secret never shows", () => {
  for (const options of [
    { endpoint: "ftp://example.com", bucket: "b" },
    { endpoint: "https://example.com?x=1", bucket: "b" },
    { endpoint: "https://example.com", bucket: " " },
    { endpoint: "https://example.com", bucket: "b", urlTtlSeconds: 0 },
    { endpoint: "https://example.com", bucket: "b", urlTtlSeconds: 604801 },
    { endpoint: "https://example.com", bucket: "b", thresholdBytes: 0 },
  ])
    assert.throws(() => new ExternalStorageConfig({ ...options, ...credentials }), TypeError);
  const saved = { id: process.env.AWS_ACCESS_KEY_ID, secret: process.env.AWS_SECRET_ACCESS_KEY };
  try {
    delete process.env.AWS_ACCESS_KEY_ID;
    delete process.env.AWS_SECRET_ACCESS_KEY;
    assert.throws(
      () => new ExternalStorageConfig({ endpoint: "https://example.com", bucket: "b" }),
      /credentials/,
    );
    process.env.AWS_ACCESS_KEY_ID = "ENVKEY";
    process.env.AWS_SECRET_ACCESS_KEY = "secret-canary";
    const storage = new ExternalStorageConfig({ endpoint: "https://example.com", bucket: "b" });
    assert.equal(storage.accessKeyId, "ENVKEY");
    assert.equal(storage.region, "auto");
    assert.equal(storage.urlTtlSeconds, 900);
    assert.equal(storage.thresholdBytes, 1024 * 1024);
    assert.equal(storage.maxUploadBytes, 256 * 1024 * 1024);
    for (const rendered of [JSON.stringify(storage), String(storage), inspect(storage, { showHidden: true })])
      assert.ok(!rendered.includes("secret-canary"), rendered);
  } finally {
    for (const [name, value] of [
      ["AWS_ACCESS_KEY_ID", saved.id],
      ["AWS_SECRET_ACCESS_KEY", saved.secret],
    ] as const) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
});

const ROW_BYTES = 300 * 1024;
const REQUEST_LIMIT = 64 * 1024;
const blobSchema = makeSchema([field("v", binary(), false)]);
const blob = (seed: number) => Uint8Array.from({ length: ROW_BYTES }, (_, i) => (i % 251) ^ seed);

class BlobStatement extends Statement {
  constructor(private readonly received: Uint8Array[][]) {
    super();
  }
  private sql = "";
  override async setSqlQuery(sql: string): Promise<void> {
    this.sql = sql;
  }
  override async bindStream(_schema: Schema, values: readonly RecordBatch[]): Promise<void> {
    this.received.push(values.flatMap((value) => [...value.getChild("v")!] as Uint8Array[]));
  }
  override async execute(): Promise<QueryResult> {
    assert.equal(this.sql, "blobs");
    return { schema: blobSchema, batches: [0, 1, 2].map((seed) => batch(blobSchema, { v: [blob(seed)] })) };
  }
}

/** A bucket that keeps PUTs, serves GETs, and rejects any URL it did not sign. */
async function bucket(storage: () => ExternalStorageConfig) {
  const objects = new Map<string, Buffer>();
  const counts = { put: 0, get: 0, rejected: 0 };
  const server = createServer(async (request, response) => {
    const url = new URL(request.url!, `http://${request.headers.host}`);
    const date = url.searchParams.get("X-Amz-Date") ?? "";
    const key = url.pathname.replace(/^\/test-bucket\//, "");
    const when = new Date(
      `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}T${date.slice(9, 11)}:${date.slice(11, 13)}:${date.slice(13, 15)}Z`,
    );
    const expected = await storage()
      .presign(
        request.method as "GET" | "PUT",
        decodeURIComponent(key),
        when,
        Number(url.searchParams.get("X-Amz-Expires")),
      )
      .catch(() => "");
    if (expected !== `http://${request.headers.host}${request.url}`) {
      counts.rejected++;
      response.writeHead(403).end();
      return;
    }
    if (request.method === "PUT") {
      const parts: Buffer[] = [];
      for await (const part of request) parts.push(part as Buffer);
      objects.set(key, Buffer.concat(parts));
      counts.put++;
      response.writeHead(200).end();
    } else {
      counts.get++;
      const body = objects.get(key);
      if (body) response.writeHead(200, { "content-length": body.length }).end(body);
      else response.writeHead(404).end();
    }
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const endpoint = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  return { endpoint, objects, counts, close: () => new Promise((resolve) => server.close(resolve)) };
}

test("binds over the request limit are uploaded and large results come from the bucket", async () => {
  let storage: ExternalStorageConfig | undefined;
  const s3 = await bucket(() => storage!);
  storage = new ExternalStorageConfig({
    endpoint: s3.endpoint,
    bucket: "test-bucket",
    prefix: "grainlift/",
    thresholdBytes: 64 * 1024,
    ...credentials,
  });
  const received: Uint8Array[][] = [];
  class BlobConnection extends Connection {
    override async newStatement(): Promise<Statement> {
      return new BlobStatement(received);
    }
  }
  const service = new GrainliftService(
    { open: async () => new BlobConnection() },
    { authorize: () => true, limits: { requestBytes: REQUEST_LIMIT, batchBytes: 1 << 20 } },
  );
  const identity = new AuthContext("test", true, "alice");
  const host = await serveHttp(service, () => identity, {
    shutdownTimeoutMs: 1000,
    http: { externalStorage: storage },
  });
  const rpc = httpConnect(host.endpoint, {
    // The test bucket is plain HTTP on loopback.
    externalLocation: {
      storage: { upload: async () => assert.fail("client uploads only requests") },
      urlValidator: () => {},
    },
  });
  const call = async (method: string, request: Record<string, unknown>) => {
    const spec = CONTRACT.methods.find((m) => m.name === method)!;
    const response = await rpc.call(
      method,
      spec.request_record ? { request: encodeRecord(spec.request_record, request, 1 << 20) } : request,
    );
    return decodeRecord(spec.response_record!, response!.result, 1 << 20);
  };
  try {
    const { session_id } = await call("open_connection", {
      target: "default",
      database_options: [],
      connection_options: [],
    });
    const { statement_id } = await call("new_statement", { session_id });

    // One bind turn carrying a batch several times the request limit.
    const rows = [blob(7), blob(9)];
    const bind = await rpc.stream("bind_stream", {
      session_id,
      statement_id,
      schema_ipc: encodeSchema(blobSchema),
    });
    await bind.exchange([{ batch_ipc: encodeBatch(batch(blobSchema, { v: rows })), finish: false }]);
    await bind.exchange([{ batch_ipc: new Uint8Array(), finish: true }]);
    bind.close();
    assert.deepEqual(received, [rows]);
    const uploads = s3.counts.put;
    assert.ok(uploads >= 1, "the bind was not uploaded");

    await call("set_sql_query", { session_id, statement_id, sql: "blobs" });
    const { result_id } = await call("execute", { session_id, statement_id });
    const values: Uint8Array[] = [];
    for await (const output of await rpc.stream("read_result", { session_id, result_id, sequence: 0n }))
      for (const row of output) values.push(row.v as Uint8Array);
    assert.deepEqual(values, [0, 1, 2].map(blob));
    assert.ok(s3.counts.put >= uploads + 3, "results were not stored");
    assert.ok(s3.counts.get >= 4, "results were not fetched from the bucket");
    assert.equal(s3.counts.rejected, 0);
    assert.ok([...s3.objects.keys()].every((key) => /^grainlift\/[0-9a-f-]{36}\.arrow$/.test(key)));
  } finally {
    rpc.close?.();
    await host.close();
    await s3.close();
  }
});
