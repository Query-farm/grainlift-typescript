// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import test from "node:test";
// arrow-js only crafts raw IPC bytes here (legacy framing, codecs, several
// batches); everything handed to grainlift is built through its facade, which
// is arrow-js under Node.js.
import {
  type RecordBatch as ArrowRecordBatch,
  type Schema as ArrowSchema,
  CompressionType,
  compressionRegistry,
  Message,
  RecordBatchStreamWriter,
  Table,
  tableToIPC,
} from "@query-farm/apache-arrow";
import {
  arrowBackend,
  batch,
  decodeBatch,
  decodeSchema,
  dictionary,
  encodeBatch,
  encodeSchema,
  field,
  int32,
  int64,
  schema as makeSchema,
  type RecordBatch,
  sameSchema,
  utf8,
} from "../arrow.js";

const native = (value: RecordBatch) => value as unknown as ArrowRecordBatch;
const simpleSchema = makeSchema([field("value", int64(), false)]);
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
    .writeAll([native(simpleBatch())])
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
  const multiple = tableToIPC(
    new Table(simpleSchema as unknown as ArrowSchema, [native(simpleBatch()), native(simpleBatch())]),
    "stream",
  );
  assert.throws(() => decodeBatch(multiple, multiple.length));
  const malformed = bytes.slice();
  new DataView(malformed.buffer).setInt32(4, -2, true);
  assert.throws(() => decodeBatch(malformed, malformed.length));
});

test("dictionary batches remain supported", () => {
  const schema = makeSchema([field("value", dictionary(int32(), utf8()), false)]);
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
    const dictionarySchema = makeSchema([field("value", dictionary(int32(), utf8()), false)]);
    for (const value of [simpleBatch(), batch(dictionarySchema, { value: ["a", "b"] })]) {
      const bytes = new RecordBatchStreamWriter({ compressionType: CompressionType.ZSTD })
        .writeAll([native(value)])
        .toUint8Array(true);
      assert.throws(() => decodeBatch(bytes, bytes.length), /Nested IPC compression/);
    }
    assert.equal(decoded, 0);
  } finally {
    compressionRegistry.set(CompressionType.ZSTD, previous ?? {});
  }
});

test("schema IPC is one bare Arrow message and preserves metadata", () => {
  assert.equal(arrowBackend, "arrow-js");
  const schema = makeSchema(
    [field("value", utf8(), true, new Map([["field", "value"]]))],
    new Map([["schema", "value"]]),
  );
  const bytes = encodeSchema(schema);
  // The same unframed message the Rust driver reads with root_as_message.
  assert.ok(Message.decode(bytes).isSchema());
  assert.throws(() => decodeSchema(bytes, bytes.length - 1));
  assert.ok(sameSchema(decodeSchema(bytes, bytes.length), schema));
  assert.throws(() => decodeSchema(encodeBatch(simpleBatch()), 1024 * 1024));
});
