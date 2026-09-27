// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
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
  idleMs: 300_000,
});
