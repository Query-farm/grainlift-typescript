// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

import { AdbcError, invalid, type OptionKind, type OptionValue } from "./api.js";
import {
  field as arrowField,
  schema as arrowSchema,
  batch,
  binary,
  bool,
  type DataType,
  decodeBatch,
  encodeBatch,
  type Field,
  float64,
  int64,
  list,
  type Schema,
  sameSchema,
  struct,
  utf8,
} from "./arrow.js";
import contract from "./contract.json" with { type: "json" };

interface FieldSpec {
  name: string;
  nullable: boolean;
  metadata: Record<string, string>;
  type: string | { list: FieldSpec } | { struct: FieldSpec[] };
}
interface SchemaSpec {
  fields: FieldSpec[];
  metadata: Record<string, string>;
}
export interface MethodSpec {
  name: string;
  kind: "unary" | "producer" | "exchange";
  request: SchemaSpec;
  request_record: string | null;
  response: SchemaSpec | null;
  response_record: string | null;
  input: SchemaSpec | null;
}
export const CONTRACT = contract as {
  format_version: number;
  protocol_name: string;
  protocol_version: string;
  records: Record<string, SchemaSpec>;
  methods: MethodSpec[];
};
function field(spec: FieldSpec): Field {
  let type: DataType;
  if (typeof spec.type === "string") {
    const primitive: Record<string, () => DataType> = {
      string: utf8,
      binary,
      int64,
      bool,
      float64,
    };
    const make = primitive[spec.type];
    if (!make) throw new Error("Unknown protocol physical type");
    type = make();
  } else if ("list" in spec.type) type = list(field(spec.type.list));
  else type = struct(spec.type.struct.map(field));
  return arrowField(spec.name, type, spec.nullable, new Map(Object.entries(spec.metadata)));
}
export function schema(spec: SchemaSpec): Schema {
  return arrowSchema(spec.fields.map(field), new Map(Object.entries(spec.metadata)));
}
export function recordSchema(name: string): Schema {
  const spec = CONTRACT.records[name];
  if (!spec) throw new Error("Unknown protocol record");
  return schema(spec);
}

function materialize(value: unknown, spec: FieldSpec): unknown {
  if (value == null) {
    if (!spec.nullable) invalid("Null required field");
    return null;
  }
  if (typeof spec.type !== "string") {
    if ("list" in spec.type) {
      const item = spec.type.list;
      const values = Array.from(value as Iterable<unknown>);
      return values.map((v) => {
        if (v == null) invalid("Null list item");
        return materialize(v, item);
      });
    }
    const row = value as Record<string, unknown>;
    return Object.fromEntries(spec.type.struct.map((f) => [f.name, materialize(row[f.name], f)]));
  }
  if (spec.type === "int64" && typeof value !== "bigint") invalid("Integer must be exact bigint");
  if (spec.type === "string" && (typeof value !== "string" || value.includes("\0"))) invalid("Invalid text");
  if (spec.type === "binary" && !(value instanceof Uint8Array)) invalid("Invalid binary field");
  if (spec.type === "bool" && typeof value !== "boolean") invalid("Invalid boolean field");
  if (spec.type === "float64" && typeof value !== "number") invalid("Invalid double field");
  return value;
}

/** Strict typed nested request decoding without compression or coercion. */
export function decodeRecord(name: string, bytes: Uint8Array, limit: number): Record<string, unknown> {
  try {
    const spec = CONTRACT.records[name]!;
    const value = decodeBatch(bytes, limit);
    if (value.numRows !== 1 || !sameSchema(value.schema, schema(spec))) invalid("Unexpected control schema");
    return Object.fromEntries(
      spec.fields.map((f) => [f.name, materialize(value.getChild(f.name)?.get(0), f)]),
    );
  } catch (error) {
    if (error instanceof AdbcError) throw error;
    invalid("Malformed control record");
  }
}
export function encodeRecord(name: string, value: Record<string, unknown>, limit: number): Uint8Array {
  const spec = CONTRACT.records[name]!;
  const validated = Object.fromEntries(spec.fields.map((f) => [f.name, [materialize(value[f.name], f)]]));
  const bytes = encodeBatch(batch(schema(spec), validated));
  if (bytes.length > limit) throw new AdbcError("Response exceeds configured limit", "invalid_data");
  return bytes;
}
export function optionKind(value: unknown): OptionKind {
  if (value !== "string" && value !== "bytes" && value !== "int" && value !== "double")
    invalid("Invalid option type");
  return value;
}
export function optionToWire(value: OptionValue, kind: OptionKind): Record<string, unknown> {
  const result: Record<string, unknown> = {
    kind,
    string_value: null,
    bytes_value: null,
    int_value: null,
    double_value: null,
  };
  result[`${kind}_value`] = value;
  optionFromWire(result);
  return result;
}
export function optionFromWire(value: unknown): OptionValue {
  const row = value as Record<string, unknown>;
  const kind = optionKind(row.kind);
  const selected = row[`${kind}_value`];
  for (const key of ["string", "bytes", "int", "double"]) {
    if (key !== kind && row[`${key}_value`] != null) invalid("Multiple option payloads");
  }
  if (
    (kind === "string" && typeof selected !== "string") ||
    (kind === "bytes" && !(selected instanceof Uint8Array)) ||
    (kind === "int" && (typeof selected !== "bigint" || selected < -(1n << 63n) || selected >= 1n << 63n)) ||
    (kind === "double" && typeof selected !== "number")
  )
    invalid("Invalid option payload");
  return selected as OptionValue;
}
export function text(value: unknown, nonempty = false): string {
  if (typeof value !== "string" || value.includes("\0") || (nonempty && !value)) invalid("Invalid text");
  return value;
}
export function options(value: unknown): Map<string, OptionValue> {
  if (!Array.isArray(value)) invalid("Expected option list");
  const result = new Map<string, OptionValue>();
  for (const item of value as Record<string, unknown>[]) {
    const key = text(item.key, true);
    if (result.has(key)) invalid("Duplicate option key");
    result.set(key, optionFromWire(item.value));
  }
  return result;
}
