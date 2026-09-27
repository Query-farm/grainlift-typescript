// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

import type { Data } from "@query-farm/apache-arrow";
import {
  Binary,
  Bool,
  ByteStream,
  Field,
  Float64,
  Int64,
  List,
  Message,
  MessageReader,
  makeData,
  RecordBatch,
  Schema,
  Struct,
  Table,
  tableFromIPC,
  tableToIPC,
  Utf8,
  vectorFromArray,
} from "@query-farm/apache-arrow";

export { Binary, Bool, Field, Float64, Int64, List, RecordBatch, Schema, Struct, Utf8 };

/** Build a batch with explicit physical Arrow types and nullability. */
export function batch(schema: Schema, values: Record<string, unknown[]>): RecordBatch {
  const first = schema.fields[0];
  const length = first ? (values[first.name]?.length ?? 0) : 0;
  const children = schema.fields.map((f) => {
    const column = values[f.name];
    if (!column || column.length !== length) throw new Error("Mismatched column length");
    const data = vectorFromArray(column, f.type).data[0];
    if (!data) throw new Error("Missing Arrow column data");
    return data;
  });
  return new RecordBatch(schema, makeData({ type: new Struct(schema.fields), length, children }));
}

/** A complete uncompressed Arrow IPC stream containing exactly one batch. */
export function encodeBatch(value: RecordBatch): Uint8Array {
  return tableToIPC(new Table(value.schema, [value]), "stream");
}

/** Count unique backing allocations, including dictionaries and sliced buffers. */
export function retainedBytes(value: RecordBatch): number {
  const buffers = new Set<ArrayBufferLike>();
  const visit = (data: Data): void => {
    for (const buffer of [data.valueOffsets, data.values, data.nullBitmap, data.typeIds]) {
      if (buffer) buffers.add(buffer.buffer);
    }
    for (const child of data.children) visit(child);
    for (const chunk of data.dictionary?.data ?? []) visit(chunk);
  };
  visit(value.data);
  let total = 0;
  for (const buffer of buffers) total += buffer.byteLength;
  return total;
}

/** Track library consumption without interpreting FlatBuffer field layouts. */
class ExactByteStream extends ByteStream {
  consumed = 0;
  constructor(private readonly bytes: Uint8Array) {
    super(bytes);
  }
  override read(size?: number | null): Uint8Array | null {
    if (
      size != null &&
      (!Number.isSafeInteger(size) || size < 0 || size > this.bytes.length - this.consumed)
    ) {
      throw new Error("Truncated or invalid IPC framing");
    }
    const value = super.read(size);
    this.consumed += value?.byteLength ?? 0;
    return value;
  }
}

function validateStream(bytes: Uint8Array): void {
  const source = new ExactByteStream(bytes);
  const reader = new MessageReader(source);
  let schemas = 0;
  let batches = 0;
  while (true) {
    const message = reader.readMessage();
    if (message === null) break;
    if (!Number.isSafeInteger(message.bodyLength) || message.bodyLength < 0)
      throw new Error("Invalid IPC body length");
    if (message.isSchema()) {
      if (schemas++ !== 0 || batches !== 0 || message.bodyLength !== 0)
        throw new Error("Unexpected IPC schema");
      message.header();
    } else if (message.isRecordBatch() || message.isDictionaryBatch()) {
      if (schemas !== 1) throw new Error("IPC schema must precede batches");
      const header = message.isRecordBatch() ? message.header() : message.header().data;
      if (header.compression != null) throw new Error("Nested IPC compression is not supported");
      if (message.isRecordBatch() && ++batches > 1) throw new Error("Expected one IPC batch");
    } else throw new Error("Unexpected IPC message");
    reader.readMessageBody(message.bodyLength);
  }
  if (source.consumed !== bytes.length || schemas !== 1 || batches !== 1) {
    throw new Error("Expected one complete IPC stream and one batch");
  }
}

/** Read one complete IPC record; reject truncation, concatenation and trailing data. */
export function decodeBatch(bytes: Uint8Array, limit: number): RecordBatch {
  if (bytes.byteLength > limit || bytes.byteLength < 16) throw new Error("Invalid IPC size");
  validateStream(bytes);
  // A nested field can be a tiny slice of a much larger request allocation.
  const table = tableFromIPC(bytes.slice());
  const value = table.batches[0];
  if (table.batches.length !== 1 || !value) throw new Error("Expected one IPC batch");
  if (retainedBytes(value) > limit) throw new Error("Decoded IPC buffers exceed configured limit");
  return value;
}

/** Grainlift schema messages omit stream framing; compression belongs to VGI. */
export function encodeSchema(schema: Schema): Uint8Array {
  return Message.encode(Message.from(schema));
}

/** Frame a raw Arrow schema message for the standard Arrow reader. */
export function decodeSchema(bytes: Uint8Array, limit: number): Schema {
  if (!bytes.length || bytes.length > limit) throw new Error("Invalid schema size");
  const message = Message.decode(bytes);
  if (!message.isSchema() || message.bodyLength !== 0) throw new Error("Expected Arrow schema message");
  return message.header();
}

/** Stable structural comparison including nested types, metadata and nullability. */
export function sameSchema(left: Schema, right: Schema): boolean {
  function describe(schema: Schema): string {
    const field = (f: Field): unknown => [
      f.name,
      f.nullable,
      f.type.toString(),
      [...f.metadata].sort(),
      f.type.children?.map(field) ?? [],
    ];
    return JSON.stringify([schema.fields.map(field), [...schema.metadata].sort()]);
  }
  return describe(left) === describe(right);
}
