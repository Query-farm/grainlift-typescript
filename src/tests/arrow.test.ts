// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import test from "node:test";
import {
  CompressionType,
  compressionRegistry,
  Dictionary,
  Int32,
  RecordBatchStreamWriter,
  Table,
  tableToIPC,
} from "@query-farm/apache-arrow";
import {
  batch,
  decodeBatch,
  decodeSchema,
  encodeBatch,
  encodeSchema,
  Field,
  Int64,
  retainedBytes,
  Schema,
  sameSchema,
  Utf8,
} from "../arrow.js";

const simpleSchema = new Schema([new Field("value", new Int64(), false)]);
const simpleBatch = () => batch(simpleSchema, { value: [1n, 2n] });

test("one complete batch round-trips at the encoded size boundary", () => {
  const bytes = encodeBatch(simpleBatch());
  assert.throws(() => decodeBatch(bytes, bytes.length - 1));
  for (const limit of [bytes.length, bytes.length + 1]) {
    const value = decodeBatch(bytes, limit);
    assert.equal(value.numRows, 2);
    assert.equal(value.getChild("value")?.get(1), 2n);
    assert.ok(sameSchema(value.schema, simpleSchema));
  }
});

test("legacy Arrow stream framing remains accepted", () => {
  const bytes = new RecordBatchStreamWriter({ writeLegacyIpcFormat: true })
    .writeAll([simpleBatch()])
    .toUint8Array(true);
  assert.equal(decodeBatch(bytes, bytes.length).numRows, 2);
});

test("truncation, multiple batches and concatenated streams are rejected", () => {
  const bytes = encodeBatch(simpleBatch());
  for (let removed = 1; removed <= 16; removed++) {
    assert.throws(() => decodeBatch(bytes.subarray(0, bytes.length - removed), bytes.length));
  }
  for (const tail of [bytes, new Uint8Array(4), bytes.subarray(bytes.length - 8)]) {
    const appended = new Uint8Array(bytes.length + tail.length);
    appended.set(bytes);
    appended.set(tail, bytes.length);
    assert.throws(() => decodeBatch(appended, appended.length));
  }
  const multiple = tableToIPC(new Table(simpleSchema, [simpleBatch(), simpleBatch()]), "stream");
  assert.throws(() => decodeBatch(multiple, multiple.length));
  const malformed = bytes.slice();
  new DataView(malformed.buffer).setInt32(4, -2, true);
  assert.throws(() => decodeBatch(malformed, malformed.length));
});

test("dictionary batches remain supported", () => {
  const schema = new Schema([new Field("value", new Dictionary(new Utf8(), new Int32()), false)]);
  const bytes = encodeBatch(batch(schema, { value: ["a", "b", "a"] }));
  const value = decodeBatch(bytes, bytes.length);
  assert.equal(value.numRows, 3);
  assert.equal(value.getChild("value")?.get(2), "a");
});

test("compressed record and dictionary messages are rejected before codec decode", () => {
  const previous = compressionRegistry.get(CompressionType.ZSTD);
  let decoded = 0;
  compressionRegistry.set(CompressionType.ZSTD, {
    // Deliberately ineffective: Arrow stores each original buffer with the
    // standard -1 marker, while advertising compression at message level.
    encode: (data) => new Uint8Array([40, 181, 47, 253, 0, 0, ...data]),
    decode: (data) => {
      decoded++;
      return data;
    },
  });
  try {
    const dictionary = new Schema([new Field("value", new Dictionary(new Utf8(), new Int32()), false)]);
    for (const value of [simpleBatch(), batch(dictionary, { value: ["a", "b"] })]) {
      const bytes = new RecordBatchStreamWriter({ compressionType: CompressionType.ZSTD })
        .writeAll([value])
        .toUint8Array(true);
      assert.throws(() => decodeBatch(bytes, bytes.length), /Nested IPC compression/);
    }
    assert.equal(decoded, 0);
  } finally {
    compressionRegistry.set(CompressionType.ZSTD, previous ?? {});
  }
});

test("retained bytes account for full backing buffers of sliced batches", () => {
  const value = batch(simpleSchema, { value: Array.from({ length: 4096 }, (_, i) => BigInt(i)) });
  assert.equal(retainedBytes(value.slice(0, 1)), retainedBytes(value));
  assert.ok(retainedBytes(value.slice(0, 1)) >= 4096 * 8);
});

test("schema IPC uses Arrow Message APIs and preserves metadata", () => {
  const schema = new Schema(
    [new Field("value", new Utf8(), true, new Map([["field", "value"]]))],
    new Map([["schema", "value"]]),
  );
  const bytes = encodeSchema(schema);
  assert.throws(() => decodeSchema(bytes, bytes.length - 1));
  assert.ok(sameSchema(decodeSchema(bytes, bytes.length), schema));
  assert.throws(() => decodeSchema(encodeBatch(simpleBatch()), 1024 * 1024));
});
