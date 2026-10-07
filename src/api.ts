// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import { jsonStateSerializer } from "@query-farm/vgi-rpc";
import type { RecordBatch, Schema } from "./arrow.js";

export type Status =
  | "unknown"
  | "not_implemented"
  | "not_found"
  | "already_exists"
  | "invalid_arguments"
  | "invalid_state"
  | "invalid_data"
  | "integrity"
  | "internal"
  | "io"
  | "cancelled"
  | "timeout"
  | "unauthenticated"
  | "unauthorized";
const statuses: ReadonlySet<string> = new Set([
  "unknown",
  "not_implemented",
  "not_found",
  "already_exists",
  "invalid_arguments",
  "invalid_state",
  "invalid_data",
  "integrity",
  "internal",
  "io",
  "cancelled",
  "timeout",
  "unauthenticated",
  "unauthorized",
]);

/** An explicitly client-visible ADBC failure. Arbitrary exceptions are sanitized. */
export class AdbcError extends Error {
  readonly errorKind: string;
  constructor(
    message: string,
    readonly status: Status = "unknown",
    options: {
      sqlstate?: string;
      vendorCode?: number;
      details?: Iterable<readonly [string, Uint8Array]>;
    } = {},
  ) {
    const sqlstate = options.sqlstate ?? "00000";
    const vendor = options.vendorCode ?? 0;
    if (
      !statuses.has(status) ||
      sqlstate.length !== 5 ||
      [...sqlstate].some((character) => character.charCodeAt(0) > 127) ||
      !Number.isInteger(vendor) ||
      vendor < -(2 ** 31) ||
      vendor >= 2 ** 31
    ) {
      throw new TypeError("Invalid ADBC error fields");
    }
    super(
      JSON.stringify({
        status,
        message,
        sqlstate: [...Buffer.from(sqlstate, "ascii")],
        vendor_code: vendor,
        details: [...(options.details ?? [])].map(([k, v]) => [k, Buffer.from(v).toString("base64")]),
      }),
    );
    if (Buffer.byteLength(this.message) > 64 * 1024) throw new TypeError("ADBC error exceeds 64 KiB");
    this.errorKind = `adbc.${status}`;
  }
}
export function unsupported(): never {
  throw new AdbcError("Operation is not implemented", "not_implemented");
}
export function invalid(message = "Invalid argument"): never {
  throw new AdbcError(message, "invalid_arguments");
}

/** Exact ADBC option types. Integer options are always bigint, never number. */
export type OptionValue = string | Uint8Array | bigint | number;
export type OptionKind = "string" | "bytes" | "int" | "double";
export interface QueryResult {
  schema: Schema;
  batches: AsyncIterable<RecordBatch> | Iterable<RecordBatch>;
  rowsAffected?: bigint | null;
  /** Called once when the cursor completes, fails, expires or is closed. */
  close?: () => void | Promise<void>;
  /** Serializable state behind `batches`, when built with `QueryResult.fromProducer`. */
  producer?: ResultProducer;
}
export const QueryResult = Object.freeze({
  /**
   * Build a result whose state is carried by a serializable producer. The
   * service reads the producer's encoded state, never `batches`; `batches`
   * drives the same producer in memory for direct callers such as tests.
   */
  fromProducer(schema: Schema, producer: ResultProducer, rowsAffected?: bigint | null): QueryResult {
    return {
      schema,
      batches: producer.batches(),
      producer,
      ...(rowsAffected === undefined ? {} : { rowsAffected }),
    };
  },
});

type ProducerType = abstract new (...args: never[]) => ResultProducer;
const producerTypes = new Map<string, ProducerType>();
const producerNames = new Map<ProducerType, string>();
const BIGINT_MARKER = "__bigint__:";
function serializable(value: unknown, depth = 0): boolean {
  if (depth > 32) return false;
  if (value === null || typeof value === "boolean" || typeof value === "bigint") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (typeof value === "string") return !value.startsWith(BIGINT_MARKER);
  if (Array.isArray(value)) return value.every((item) => serializable(item, depth + 1));
  if (typeof value === "object" && Object.getPrototypeOf(value) === Object.prototype)
    return Object.values(value).every((item) => serializable(item, depth + 1));
  return false;
}

/**
 * Serializable result state that produces one batch per call.
 *
 * An alternative to a batch iterator: subclass it with fields that hold
 * everything needed to produce the rest of the result, register the subclass
 * once with {@link ResultProducer.register}, and return it with
 * `QueryResult.fromProducer`. Over HTTP the service serializes the producer
 * into the sealed continuation token after every batch, so no iterator, cursor
 * or replay batch is retained in server memory between fetches, and a retried
 * fetch re-produces its batch from the token's state. Other transports carry
 * the same state in their in-memory cursor.
 *
 * The state is the instance's own enumerable fields; they must be JSON values
 * or `bigint` (strings, finite numbers, booleans, null, arrays and plain
 * objects). Keep sockets, files and backend cursors out of the state; results
 * that need them should use an iterator instead. Decoding restores the fields
 * onto the registered class's prototype without calling its constructor. The
 * encoded state is bounded by `Limits.producerStateBytes`.
 */
export abstract class ResultProducer {
  /** Register a producer class under a stable name so encoded state can be restored. */
  static register(name: string, type: ProducerType): void {
    if (typeof name !== "string" || !name || Buffer.byteLength(name) > 1024)
      throw new TypeError("Invalid result producer name");
    const existing = producerTypes.get(name);
    if ((existing && existing !== type) || (producerNames.has(type) && producerNames.get(type) !== name))
      throw new TypeError("Result producer is already registered under another name");
    producerTypes.set(name, type);
    producerNames.set(type, name);
  }

  /** Return the next batch and advance the state, or null at end of result. */
  abstract produce(): RecordBatch | null | Promise<RecordBatch | null>;

  /** Serialize the registered type name and the instance's fields. */
  encode(): Uint8Array {
    const type = producerNames.get(this.constructor as ProducerType);
    if (type === undefined) throw new AdbcError("Result producer is not registered", "invalid_data");
    const state = { ...this };
    if (!serializable(state))
      throw new AdbcError("Result producer state is not serializable", "invalid_data");
    return jsonStateSerializer.serialize({ type, state });
  }

  /** Restore a producer serialized by {@link ResultProducer.encode}; only registered types decode. */
  static decode(payload: Uint8Array): ResultProducer {
    let decoded: unknown;
    try {
      decoded = jsonStateSerializer.deserialize(payload);
    } catch {
      decoded = undefined;
    }
    const { type, state } = (decoded ?? {}) as { type?: unknown; state?: unknown };
    const producerType = typeof type === "string" ? producerTypes.get(type) : undefined;
    if (
      producerType === undefined ||
      !serializable(state) ||
      state === null ||
      typeof state !== "object" ||
      Array.isArray(state)
    )
      throw new AdbcError("Unknown result producer", "invalid_data");
    const producer = Object.create(producerType.prototype) as ResultProducer;
    for (const [key, value] of Object.entries(state))
      Object.defineProperty(producer, key, { value, writable: true, enumerable: true, configurable: true });
    return producer;
  }

  /** Drive the producer in memory until it is exhausted. */
  async *batches(): AsyncGenerator<RecordBatch, void, undefined> {
    for (let value = await this.produce(); value !== null; value = await this.produce()) yield value;
  }
}
export interface PartitionResult {
  schema: Schema;
  partitions: Uint8Array[];
  rowsAffected?: bigint | null;
}
export interface ObjectFilters {
  depth: bigint;
  catalog: string | null;
  db_schema: string | null;
  table_name: string | null;
  table_types: string[] | null;
  column_name: string | null;
}
export interface TableIdentifier {
  catalog: string | null;
  db_schema: string | null;
  table_name: string;
}
export interface StatisticsFilters {
  catalog: string | null;
  db_schema: string | null;
  table_name: string | null;
  approximate: boolean;
}
export interface OpenOptions {
  target: string;
  principal: string;
  databaseOptions: ReadonlyMap<string, OptionValue>;
  connectionOptions: ReadonlyMap<string, OptionValue>;
}

/** Override supported operations. Defaults preserve honest ADBC NOT_IMPLEMENTED. */
export class Statement {
  async setSqlQuery(_sql: string): Promise<void> {
    unsupported();
  }
  async setSubstraitPlan(_plan: Uint8Array): Promise<void> {
    unsupported();
  }
  async prepare(): Promise<void> {
    unsupported();
  }
  async bind(_schema: Schema, _batch: RecordBatch): Promise<void> {
    unsupported();
  }
  async bindStream(_schema: Schema, _batches: readonly RecordBatch[]): Promise<void> {
    unsupported();
  }
  async execute(): Promise<QueryResult> {
    unsupported();
  }
  async executeUpdate(): Promise<bigint | null> {
    unsupported();
  }
  async executeSchema(): Promise<Schema> {
    unsupported();
  }
  async getParameterSchema(): Promise<Schema> {
    unsupported();
  }
  async executePartitions(): Promise<PartitionResult> {
    unsupported();
  }
  async setOption(_key: string, _value: OptionValue): Promise<void> {
    unsupported();
  }
  async getOption(_key: string, _kind: OptionKind): Promise<OptionValue> {
    unsupported();
  }
  async cancel(): Promise<void> {
    unsupported();
  }
  async close(): Promise<void> {}
}

/** One backend connection, serialized by the service except cancellation. */
export class Connection {
  /** Explicit support for this session; null leaves dispatch to the backend. */
  statisticsSupported(): boolean | null {
    return null;
  }
  statisticNamesSupported(): boolean | null {
    return null;
  }
  async newStatement(): Promise<Statement> {
    unsupported();
  }
  async setOption(_key: string, _value: OptionValue): Promise<void> {
    unsupported();
  }
  async getOption(_key: string, _kind: OptionKind): Promise<OptionValue> {
    unsupported();
  }
  async commit(): Promise<void> {
    unsupported();
  }
  async rollback(): Promise<void> {
    unsupported();
  }
  async cancel(): Promise<void> {
    unsupported();
  }
  async getInfo(_codes: bigint[] | null): Promise<QueryResult> {
    unsupported();
  }
  async getObjects(_filters: ObjectFilters): Promise<QueryResult> {
    unsupported();
  }
  async getTableSchema(_table: TableIdentifier): Promise<Schema> {
    unsupported();
  }
  async getTableTypes(): Promise<QueryResult> {
    unsupported();
  }
  async getStatisticNames(): Promise<QueryResult> {
    unsupported();
  }
  async getStatistics(_filters: StatisticsFilters): Promise<QueryResult> {
    unsupported();
  }
  async readPartition(_descriptor: Uint8Array): Promise<QueryResult> {
    unsupported();
  }
  async close(): Promise<void> {}
}
export interface Worker {
  open(options: OpenOptions): Promise<Connection>;
}
export interface Limits {
  sessions: number;
  sessionsPerPrincipal: number;
  statementsPerSession: number;
  resultsPerSession: number;
  requestBytes: number;
  batchBytes: number;
  bindBytes: number;
  bindBatches: number;
  schemaBytes: number;
  sqlBytes: number;
  partitions: number;
  partitionBytes: number;
  /** Maximum encoded ResultProducer state carried in a continuation token. */
  producerStateBytes: number;
  idleMs: number;
}
export const defaultLimits: Readonly<Limits> = Object.freeze({
  sessions: 128,
  sessionsPerPrincipal: 16,
  statementsPerSession: 64,
  resultsPerSession: 64,
  requestBytes: 8 * 1024 * 1024,
  batchBytes: 16 * 1024 * 1024,
  bindBytes: 64 * 1024 * 1024,
  bindBatches: 1024,
  schemaBytes: 1024 * 1024,
  sqlBytes: 1024 * 1024,
  partitions: 1024,
  partitionBytes: 1024 * 1024,
  producerStateBytes: 64 * 1024,
  idleMs: 300_000,
});
