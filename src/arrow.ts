// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

import type { VgiBatch, VgiDataType, VgiField, VgiSchema } from "@query-farm/vgi-rpc/arrow";
// Arrow through VGI-RPC's facade: arrow-js under Node.js, flechette under
// Cloudflare Workers (the `workerd`/`worker` export conditions). Batches and
// schemas are whatever the active backend builds, so they flow into VGI-RPC
// unchanged; code here only uses the facade, never a backend directly.
import * as arrow from "@query-farm/vgi-rpc/arrow";

export type RecordBatch = VgiBatch;
export type Schema = VgiSchema;
export type Field = VgiField;
export type DataType = VgiDataType;

export {
  binary,
  bool,
  dateDay,
  decimal,
  dictionary,
  durationMicro,
  field,
  fixedSizeBinary,
  float32,
  float64,
  int8,
  int16,
  int32,
  int64,
  largeBinary,
  largeUtf8,
  list,
  map,
  nullType,
  schema,
  struct,
  timeMicro,
  timestampMicro,
  uint8,
  uint16,
  uint32,
  uint64,
  utf8,
} from "@query-farm/vgi-rpc/arrow";

/** The Arrow implementation in use: "arrow-js" (Node.js) or "flechette" (Workers). */
export const arrowBackend = arrow.backend.name;

/** Build a batch with explicit physical Arrow types and nullability. */
export function batch(schema: Schema, values: Record<string, unknown[]>): RecordBatch {
  const first = schema.fields[0];
  const length = first ? (values[first.name]?.length ?? 0) : 0;
  for (const f of schema.fields) {
    const column = values[f.name];
    if (!column || column.length !== length) throw new Error("Mismatched column length");
  }
  return arrow.batchFromColumns(schema, values as Record<string, unknown[]> as Record<string, never[]>);
}

/** A complete uncompressed Arrow IPC stream containing exactly one batch. */
export function encodeBatch(value: RecordBatch): Uint8Array {
  return arrow.serializeBatch(value);
}

// ----- IPC framing -----------------------------------------------------------
//
// Validation reads the stream's own framing and the few FlatBuffer fields it
// needs, so it behaves the same whichever backend decodes the payload.

const CONTINUATION = 0xffffffff;
enum HeaderType {
  Schema = 1,
  DictionaryBatch = 2,
  RecordBatch = 3,
}

class Table {
  private readonly vtable: number;
  private readonly vtableSize: number;
  constructor(
    private readonly view: DataView,
    readonly position: number,
  ) {
    this.vtable = position - view.getInt32(position, true);
    this.vtableSize = view.getUint16(this.vtable, true);
  }
  /** Absolute position of field `index`, or 0 when absent. */
  private field(index: number): number {
    const entry = 4 + index * 2;
    if (entry + 2 > this.vtableSize) return 0;
    const offset = this.view.getUint16(this.vtable + entry, true);
    return offset ? this.position + offset : 0;
  }
  uint8(index: number): number {
    const at = this.field(index);
    return at ? this.view.getUint8(at) : 0;
  }
  int64(index: number): bigint {
    const at = this.field(index);
    return at ? this.view.getBigInt64(at, true) : 0n;
  }
  table(index: number): Table | null {
    const at = this.field(index);
    return at ? new Table(this.view, at + this.view.getUint32(at, true)) : null;
  }
}

interface MessageInfo {
  header: number;
  bodyLength: number;
  compressed: boolean;
}

function readMessage(bytes: Uint8Array): MessageInfo {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes.byteLength < 8) throw new Error("Invalid IPC message");
  const message = new Table(view, view.getUint32(0, true));
  const header = message.uint8(1);
  const bodyLength = message.int64(3);
  if (bodyLength < 0n || bodyLength > BigInt(Number.MAX_SAFE_INTEGER))
    throw new Error("Invalid IPC body length");
  // RecordBatch.compression is field 3; a DictionaryBatch wraps one in field 1.
  const batch = header === HeaderType.DictionaryBatch ? message.table(2)?.table(1) : message.table(2);
  const compressed = header !== HeaderType.Schema && !!batch && batch.table(3) !== null;
  return { header, bodyLength: Number(bodyLength), compressed };
}

/** Walk a whole stream; reject truncation, trailing data and anything but schema + one batch. */
function validateStream(bytes: Uint8Array): void {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let position = 0;
  let schemas = 0;
  let batches = 0;
  for (;;) {
    if (position + 4 > bytes.length) throw new Error("Truncated IPC stream");
    let metadataLength = view.getInt32(position, true);
    position += 4;
    if (metadataLength === -1 || view.getUint32(position - 4, true) === CONTINUATION) {
      if (position + 4 > bytes.length) throw new Error("Truncated IPC stream");
      metadataLength = view.getInt32(position, true);
      position += 4;
    }
    if (metadataLength === 0) break;
    if (metadataLength < 0 || position + metadataLength > bytes.length)
      throw new Error("Invalid IPC framing");
    const message = readMessage(bytes.subarray(position, position + metadataLength));
    position += metadataLength;
    if (message.header === HeaderType.Schema) {
      if (schemas++ !== 0 || batches !== 0 || message.bodyLength !== 0)
        throw new Error("Unexpected IPC schema");
    } else if (message.header === HeaderType.RecordBatch || message.header === HeaderType.DictionaryBatch) {
      if (schemas !== 1) throw new Error("IPC schema must precede batches");
      if (message.compressed) throw new Error("Nested IPC compression is not supported");
      if (message.header === HeaderType.RecordBatch && ++batches > 1)
        throw new Error("Expected one IPC batch");
    } else throw new Error("Unexpected IPC message");
    if (position + message.bodyLength > bytes.length) throw new Error("Truncated IPC body");
    position += message.bodyLength;
  }
  if (position !== bytes.length || schemas !== 1 || batches !== 1) {
    throw new Error("Expected one complete IPC stream and one batch");
  }
}

/** Read one complete IPC record; reject truncation, concatenation and trailing data. */
export function decodeBatch(bytes: Uint8Array, limit: number): RecordBatch {
  if (bytes.byteLength > limit || bytes.byteLength < 16) throw new Error("Invalid IPC size");
  validateStream(bytes);
  // A copy, so a nested field never retains a much larger request allocation;
  // the decoded batch is then bounded by `limit`.
  return arrow.deserializeBatch(bytes.slice());
}

/** Grainlift schema messages omit stream framing; compression belongs to VGI. */
export function encodeSchema(value: Schema): Uint8Array {
  const stream = arrow.serializeSchema(value);
  const view = new DataView(stream.buffer, stream.byteOffset, stream.byteLength);
  if (view.getUint32(0, true) !== CONTINUATION) throw new Error("Unexpected IPC schema framing");
  const length = view.getInt32(4, true);
  return stream.slice(8, 8 + length);
}

/** Frame a raw Arrow schema message as a stream for the backend's reader. */
export function decodeSchema(bytes: Uint8Array, limit: number): Schema {
  if (!bytes.length || bytes.length > limit) throw new Error("Invalid schema size");
  const message = readMessage(bytes);
  if (message.header !== HeaderType.Schema || message.bodyLength !== 0)
    throw new Error("Expected Arrow schema message");
  const padded = (bytes.length + 7) & ~7;
  const stream = new Uint8Array(8 + padded + 8);
  const view = new DataView(stream.buffer);
  view.setUint32(0, CONTINUATION, true);
  view.setInt32(4, padded, true);
  stream.set(bytes, 8);
  view.setUint32(8 + padded, CONTINUATION, true);
  return arrow.deserializeSchema(stream);
}

/** Stable structural comparison including nested types, metadata and nullability. */
export function sameSchema(left: Schema, right: Schema): boolean {
  return describeSchema(left) === describeSchema(right);
}

function sortedMetadata(metadata: Map<string, string> | null | undefined): [string, string][] {
  return [...(metadata ?? [])].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
}

function describeType(type: DataType): unknown {
  // Both backends follow the Arrow FlatBuffer type ids and parameters; only
  // the signedness property is spelled differently.
  const t = type as unknown as Record<string, unknown>;
  const children = (t.children as Field[] | undefined)?.map(describeField) ?? [];
  const dictionary = t.dictionary ? describeType(t.dictionary as DataType) : null;
  const indices = t.indices ? describeType(t.indices as DataType) : null;
  return [
    type.typeId,
    t.bitWidth ?? null,
    t.isSigned ?? t.signed ?? null,
    t.precision ?? null,
    t.scale ?? null,
    t.unit ?? null,
    t.timezone ?? null,
    t.byteWidth ?? null,
    t.listSize ?? t.stride ?? null,
    children,
    dictionary,
    indices,
  ];
}

function describeField(f: Field): unknown {
  return [f.name, f.nullable, describeType(f.type), sortedMetadata(f.metadata)];
}

function describeSchema(value: Schema): string {
  return JSON.stringify([value.fields.map(describeField), sortedMetadata(value.metadata)], (_key, v) =>
    typeof v === "bigint" ? v.toString() : v,
  );
}
