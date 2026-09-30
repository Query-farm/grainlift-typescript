// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash, createHmac, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import type { Socket } from "node:net";
import { Transform } from "node:stream";
import {
  createHttpHandler,
  jsonStateSerializer,
  Protocol,
  serveStream,
  TransportKind,
  AuthContext as VgiAuthContext,
} from "@query-farm/vgi-rpc";
import {
  AdbcError,
  type Connection,
  defaultLimits,
  invalid,
  type Limits,
  type ObjectFilters,
  type OptionValue,
  type QueryResult,
  ResultProducer,
  type Statement,
  type StatisticsFilters,
  type TableIdentifier,
  type Worker,
} from "./api.js";
import {
  batch,
  decodeBatch,
  decodeSchema,
  encodeBatch,
  encodeSchema,
  type RecordBatch,
  retainedBytes,
  Schema,
  sameSchema,
} from "./arrow.js";
import { AuthContext, type AuthenticateFn } from "./auth.js";
import {
  CONTRACT,
  decodeRecord,
  encodeRecord,
  optionFromWire,
  optionKind,
  options,
  optionToWire,
  recordSchema,
  schema,
  text,
} from "./wire.js";

interface ResultState {
  query: QueryResult;
  iterator: AsyncIterator<RecordBatch> | Iterator<RecordBatch>;
  /** Initial encoded producer (base64); later states travel only in cursors. */
  producer?: string;
  sequence: bigint;
  previous?: RecordBatch;
  ended: boolean;
  released: boolean;
}
interface SocketScope {
  results: Map<string, { session: Session; id: string }>;
  uploads: Map<string, { session: Session; statement: StatementState; id: string }>;
}
interface Upload {
  id: string;
  schema: Schema;
  stream: boolean;
  batches: RecordBatch[];
  bytes: number;
  sequence: bigint;
  previousDigest?: string;
  finished: boolean;
}
interface StatementState {
  backend: Statement;
  result?: string;
  upload?: Upload;
}
interface Session {
  owner: string;
  target: string;
  connection: Connection;
  statements: Map<string, StatementState>;
  results: Map<string, ResultState>;
  touched: number;
  busy: boolean;
  cleanup: Map<string, () => Promise<void>>;
}
interface Cursor {
  session_id: string;
  result_id: string;
  sequence: bigint;
  /** Encoded ResultProducer state (base64) for producer results, sealed into continuation tokens. */
  producer?: string;
  __outputSchema?: Schema;
}
interface BindCursor {
  session_id: string;
  statement_id: string;
  upload_id: string;
  sequence: bigint;
}
export interface ServiceOptions {
  limits?: Partial<Limits>;
  /** Called before connection allocation. Unlisted targets/principals must be rejected. */
  authorize: (principal: string, target: string) => boolean;
  databaseOptions?: ReadonlyMap<string, OptionValue>;
  connectionOptions?: ReadonlyMap<string, OptionValue>;
  allowedDatabaseOptions?: ReadonlySet<string>;
  allowedConnectionOptions?: ReadonlySet<string>;
}
function copyOption(value: OptionValue): OptionValue {
  return value instanceof Uint8Array ? value.slice() : value;
}
function copyOptions(values?: ReadonlyMap<string, OptionValue>): Map<string, OptionValue> {
  return new Map([...(values ?? [])].map(([key, value]) => [text(key, true), copyOption(value)]));
}

/** Grainlift's full typed ADBC lifecycle, independent of individual backend capabilities. */
export class GrainliftService {
  private readonly protocol: Protocol;
  readonly limits: Readonly<Limits>;
  private readonly sessions = new Map<string, Session>();
  private readonly pending = new Map<string, number>();
  private readonly auth = new AsyncLocalStorage<AuthContext>();
  private readonly socketScope = new AsyncLocalStorage<SocketScope>();
  private readonly partitionKey = randomBytes(32);
  private readonly timer: NodeJS.Timeout;
  private closed = false;
  private opening = 0;
  private readonly config: ServiceOptions;

  constructor(
    private readonly worker: Worker,
    config: ServiceOptions,
  ) {
    this.limits = Object.freeze({ ...defaultLimits, ...config.limits });
    for (const value of Object.values(this.limits)) {
      if (!Number.isSafeInteger(value) || value <= 0)
        throw new TypeError("Limits must be positive safe integers");
    }
    this.config = {
      ...config,
      databaseOptions: copyOptions(config.databaseOptions),
      connectionOptions: copyOptions(config.connectionOptions),
      allowedDatabaseOptions: new Set(config.allowedDatabaseOptions),
      allowedConnectionOptions: new Set(config.allowedConnectionOptions),
    };
    this.protocol = this.buildProtocol();
    this.timer = setInterval(
      () => {
        void this.reap();
      },
      Math.min(this.limits.idleMs, 1000),
    );
    this.timer.unref();
  }

  /** Build a Fetch handler. Authentication is mandatory on every request and continuation. */
  httpHandler(
    authenticate: AuthenticateFn,
    extra: { prefix?: string; compressionLevel?: number | null } = {},
  ): (request: Request) => Promise<Response> {
    const handler = createHttpHandler(this.protocol, {
      ...extra,
      authenticate: () => {
        const auth = this.auth.getStore() ?? AuthContext.anonymous();
        return new VgiAuthContext(auth.domain, auth.authenticated, auth.principal, auth.claims);
      },
      maxRequestBytes: this.limits.requestBytes,
      maxDecompressedRequestBytes: this.limits.requestBytes,
      maxResponseBytes: this.limits.batchBytes + this.limits.schemaBytes + 65536,
      tokenTtl: Math.ceil(this.limits.idleMs / 1000),
      stateSerializer: {
        serialize: (state: unknown) => {
          const { __outputSchema: _ignored, ...plain } = state as Record<string, unknown>;
          return jsonStateSerializer.serialize(plain);
        },
        deserialize: (bytes: Uint8Array) => jsonStateSerializer.deserialize(bytes),
      },
      enableLandingPage: false,
      enableDescribePage: false,
      enableNotFoundPage: false,
    });
    return async (request) => {
      let identity: AuthContext;
      try {
        identity = await authenticate(request);
      } catch {
        return new Response(null, { status: 401 });
      }
      if (!identity.authenticated || identity.principal === null) return new Response(null, { status: 401 });
      return this.auth.run(identity, () => handler(request));
    };
  }

  /** Invoke within a verified identity; intended for transport adapters and direct tests. */
  withIdentity<T>(identity: AuthContext, action: () => Promise<T>): Promise<T> {
    return this.auth.run(identity, action);
  }
  /** Serve stock VGI stream framing under an already verified transport identity. */
  async serveSocket(socket: Socket, identity: AuthContext, inputBudget: number): Promise<void> {
    if (!Number.isSafeInteger(inputBudget) || inputBudget < 1)
      throw new TypeError("Invalid connection budget");
    let consumed = 0;
    const input = new Transform({
      highWaterMark: 65536,
      transform(chunk: Buffer, _encoding, callback) {
        consumed += chunk.length;
        callback(consumed > inputBudget ? new Error("Connection input budget exceeded") : null, chunk);
      },
    });
    // The lifecycle owner closes the peer on input failure; never log input or errors.
    input.on("error", () => socket.destroy());
    const closed = () => input.destroy(new Error("Stream closed"));
    socket.once("close", closed);
    const scope: SocketScope = { results: new Map(), uploads: new Map() };
    await this.withIdentity(identity, () =>
      this.socketScope.run(scope, async () => {
        this.owner();
        try {
          socket.pipe(input);
          await serveStream(this.protocol, {
            readable: input,
            writable: socket,
            transportKind: TransportKind.TCP,
          });
        } finally {
          socket.off("close", closed);
          input.destroy();
          for (const { session, id } of scope.results.values()) {
            await this.deferCleanup(session, `result:${id}`, () => this.dropResult(session, id));
          }
          for (const { session, statement, id } of scope.uploads.values()) {
            await this.deferCleanup(session, `upload:${id}`, async () => {
              if (statement.upload?.id === id && !statement.upload.finished) statement.upload = undefined;
            });
          }
        }
      }),
    );
  }
  /** Registered versioned wire method names, suitable for contract parity checks. */
  methodNames(): string[] {
    return this.protocol.methodNames();
  }
  private owner(): string {
    const auth = this.auth.getStore();
    if (!auth?.authenticated || auth.principal === null)
      throw new AdbcError("Authentication required", "unauthenticated");
    return JSON.stringify([auth.domain, auth.principal]);
  }
  private session(id: unknown): Session {
    const session = this.sessions.get(text(id, true));
    if (!session || session.owner !== this.owner())
      throw new AdbcError("Session is unavailable", "not_found");
    session.touched = Date.now();
    return session;
  }
  private statement(session: Session, id: unknown): StatementState {
    const state = session.statements.get(text(id, true));
    if (!state) throw new AdbcError("Statement is unavailable", "not_found");
    return state;
  }
  private async guard<T>(action: () => Promise<T>): Promise<T> {
    try {
      if (this.closed) throw new AdbcError("Service is closed", "invalid_state");
      this.owner();
      return await action();
    } catch (error) {
      if (error instanceof AdbcError) throw error;
      throw new AdbcError("Worker operation failed", "internal");
    }
  }
  private async locked<T>(session: Session, action: () => Promise<T>): Promise<T> {
    if (session.busy) throw new AdbcError("Session is busy", "invalid_state");
    session.busy = true;
    try {
      return await action();
    } finally {
      try {
        for (const [key, cleanup] of session.cleanup) {
          session.cleanup.delete(key);
          try {
            await cleanup();
          } catch {
            /* One failure must not prevent other cleanup. */
          }
        }
        if (this.closed) {
          for (const [id, current] of this.sessions)
            if (current === session) await this.dropSession(id, session);
        }
      } finally {
        session.busy = false;
        session.touched = Date.now();
      }
    }
  }
  private async deferCleanup(session: Session, key: string, cleanup: () => Promise<void>): Promise<void> {
    if (session.busy) {
      session.cleanup.set(key, cleanup);
      return;
    }
    try {
      await this.locked(session, cleanup);
    } catch {
      /* Transport cleanup never logs backend failures. */
    }
  }
  private checkSchema(value: Schema): Uint8Array {
    const bytes = encodeSchema(value);
    if (bytes.length > this.limits.schemaBytes)
      throw new AdbcError("Schema exceeds configured limit", "invalid_data");
    return bytes;
  }
  private async release(result: ResultState): Promise<void> {
    if (result.released) return;
    result.released = true;
    try {
      await result.iterator.return?.();
    } finally {
      await result.query.close?.();
    }
  }
  private async dropResult(session: Session, id: string): Promise<void> {
    const result = session.results.get(id);
    if (!result) return;
    session.results.delete(id);
    result.previous = undefined;
    for (const statement of session.statements.values())
      if (statement.result === id) statement.result = undefined;
    await this.release(result);
  }
  private async dropStatement(session: Session, id: string): Promise<void> {
    const statement = session.statements.get(id);
    if (!statement) return;
    session.statements.delete(id);
    statement.upload = undefined;
    try {
      if (statement.result) await this.dropResult(session, statement.result);
    } finally {
      await statement.backend.close();
    }
  }
  private async dropSession(id: string, session: Session): Promise<void> {
    this.sessions.delete(id);
    const failures: unknown[] = [];
    for (const key of [...session.statements.keys()]) {
      try {
        await this.dropStatement(session, key);
      } catch (error) {
        failures.push(error);
      }
    }
    for (const key of [...session.results.keys()]) {
      try {
        await this.dropResult(session, key);
      } catch (error) {
        failures.push(error);
      }
    }
    try {
      await session.connection.close();
    } catch (error) {
      failures.push(error);
    }
    if (failures.length) throw new AdbcError("Backend cleanup failed", "internal");
  }
  private async reap(): Promise<void> {
    for (const [id, session] of this.sessions) {
      if (!session.busy && Date.now() - session.touched >= this.limits.idleMs) {
        session.busy = true;
        try {
          await this.dropSession(id, session);
        } catch {
          /* Other sessions still need cleanup. */
        }
      }
    }
  }
  /** Stop accepting calls and close idle handles. Busy callbacks require cooperative completion. */
  async close(): Promise<void> {
    this.closed = true;
    clearInterval(this.timer);
    const busy = this.opening || [...this.sessions.values()].some((s) => s.busy);
    const results = await Promise.allSettled(
      [...this.sessions].filter(([, s]) => !s.busy).map(([id, s]) => this.dropSession(id, s)),
    );
    if (results.some((r) => r.status === "rejected"))
      throw new AdbcError("Backend cleanup failed", "internal");
    if (busy) throw new AdbcError("Shutdown is waiting for backend callbacks", "timeout");
  }
  /** Ask active backends to cancel; implementations must cooperate to interrupt work. */
  async cancelPending(): Promise<void> {
    const callbacks: Promise<unknown>[] = [];
    for (const session of this.sessions.values()) {
      if (!session.busy) continue;
      callbacks.push(session.connection.cancel());
      for (const statement of session.statements.values()) callbacks.push(statement.backend.cancel());
    }
    await Promise.allSettled(callbacks);
  }
  /** Bounded counts only; never includes SQL, values, credentials or backend messages. */
  snapshot(): { sessions: number; statements: number; results: number; uploads: number } {
    return {
      sessions: this.sessions.size,
      statements: [...this.sessions.values()].reduce((n, s) => n + s.statements.size, 0),
      results: [...this.sessions.values()].reduce((n, s) => n + s.results.size, 0),
      uploads: [...this.sessions.values()].reduce(
        (n, s) => n + [...s.statements.values()].filter((v) => v.upload && !v.upload.finished).length,
        0,
      ),
    };
  }
  private async register(
    session: Session,
    getQuery: () => Promise<QueryResult>,
    statement?: StatementState,
  ): Promise<Record<string, unknown>> {
    if (statement?.upload && !statement.upload.finished)
      throw new AdbcError("Binding is incomplete", "invalid_state");
    if (statement?.result) await this.dropResult(session, statement.result);
    if (session.results.size >= this.limits.resultsPerSession)
      throw new AdbcError("Result limit reached", "invalid_state");
    const query = await getQuery();
    let iterator: AsyncIterator<RecordBatch> | Iterator<RecordBatch>;
    try {
      const iterable = query.batches;
      iterator =
        Symbol.asyncIterator in iterable ? iterable[Symbol.asyncIterator]() : iterable[Symbol.iterator]();
    } catch (error) {
      await query.close?.();
      throw error;
    }
    const result: ResultState = { query, iterator, sequence: 0n, ended: false, released: false };
    try {
      if (query.producer !== undefined) result.producer = this.encodeProducer(query.producer);
      const schema_ipc = this.checkSchema(query.schema);
      const rows_affected = this.rows(query.rowsAffected ?? null);
      const result_id = randomUUID();
      encodeRecord("ExecuteResponse", { result_id, schema_ipc, rows_affected }, this.limits.batchBytes);
      session.results.set(result_id, result);
      if (statement) statement.result = result_id;
      return { result_id, schema_ipc, rows_affected };
    } catch (error) {
      await this.release(result);
      throw error;
    }
  }
  private encodeProducer(producer: ResultProducer): string {
    if (!(producer instanceof ResultProducer)) throw new AdbcError("Invalid result producer", "invalid_data");
    const encoded = producer.encode();
    if (encoded.length > this.limits.producerStateBytes)
      throw new AdbcError("Result producer state exceeds configured limit", "invalid_data");
    return Buffer.from(encoded).toString("base64");
  }
  private rows(value: bigint | null): bigint | null {
    if (value !== null && (typeof value !== "bigint" || value < -1n || value >= 1n << 63n)) {
      throw new AdbcError("Invalid affected row count", "invalid_data");
    }
    return value;
  }
  private async open(request: Record<string, unknown>): Promise<Record<string, unknown>> {
    const owner = this.owner();
    const target = text(request.target, true);
    const principal = this.auth.getStore()!.principal!;
    if (!this.config.authorize(principal, target))
      throw new AdbcError("Target is unavailable", "unauthorized");
    const db = options(request.database_options);
    const conn = options(request.connection_options);
    const merge = (
      given: Map<string, OptionValue>,
      fixed: ReadonlyMap<string, OptionValue>,
      allowed: ReadonlySet<string>,
    ) => {
      for (const key of given.keys())
        if (fixed.has(key) || !allowed.has(key))
          throw new AdbcError("Option is not caller configurable", "unauthorized");
      return copyOptions(new Map([...given, ...fixed]));
    };
    const databaseOptions = merge(db, this.config.databaseOptions!, this.config.allowedDatabaseOptions!);
    const connectionOptions = merge(
      conn,
      this.config.connectionOptions!,
      this.config.allowedConnectionOptions!,
    );
    const own =
      [...this.sessions.values()].filter((s) => s.owner === owner).length + (this.pending.get(owner) ?? 0);
    if (
      this.sessions.size + this.opening >= this.limits.sessions ||
      own >= this.limits.sessionsPerPrincipal
    ) {
      throw new AdbcError("Session limit reached", "invalid_state");
    }
    this.opening++;
    this.pending.set(owner, (this.pending.get(owner) ?? 0) + 1);
    try {
      const connection = await this.worker.open({ target, principal, databaseOptions, connectionOptions });
      if (this.closed) {
        await connection.close();
        throw new AdbcError("Service is closed", "invalid_state");
      }
      const session_id = randomUUID();
      try {
        encodeRecord("SessionResponse", { session_id }, this.limits.batchBytes);
      } catch (error) {
        await connection.close();
        throw error;
      }
      this.sessions.set(session_id, {
        owner,
        target,
        connection,
        statements: new Map(),
        cleanup: new Map(),
        results: new Map(),
        touched: Date.now(),
        busy: false,
      });
      return { session_id };
    } finally {
      this.opening--;
      const count = this.pending.get(owner)! - 1;
      if (count) this.pending.set(owner, count);
      else this.pending.delete(owner);
    }
  }

  /** Dispatch typed control records. Prefer the registered protocol for wire serving. */
  async invoke(method: string, request: Record<string, unknown>): Promise<Record<string, unknown>> {
    return this.guard(async () => {
      if (method === "open_connection") return this.open(request);
      const session = this.session(request.session_id);
      if (method === "cancel_connection") {
        await session.connection.cancel();
        return { ok: true };
      }
      if (method === "cancel_statement") {
        await this.statement(session, request.statement_id).backend.cancel();
        return { ok: true };
      }
      return this.locked(session, async () => {
        const ok = { ok: true };
        const connection = session.connection;
        if (method === "close_connection") {
          await this.dropSession(text(request.session_id), session);
          return ok;
        }
        if (method === "new_statement") {
          if (session.statements.size >= this.limits.statementsPerSession)
            throw new AdbcError("Statement limit reached", "invalid_state");
          const backend = await connection.newStatement();
          const statement_id = randomUUID();
          try {
            encodeRecord(
              "StatementResponse",
              { session_id: request.session_id, statement_id },
              this.limits.batchBytes,
            );
          } catch (error) {
            await backend.close();
            throw error;
          }
          session.statements.set(statement_id, { backend });
          return { session_id: request.session_id, statement_id };
        }
        if (method === "close_result") {
          const id = text(request.result_id, true);
          if (!session.results.has(id)) throw new AdbcError("Result is unavailable", "not_found");
          await this.dropResult(session, id);
          return ok;
        }
        if (method === "commit" || method === "rollback") {
          await connection[method]();
          return ok;
        }
        if (method === "set_connection_option") {
          const key = text(request.key, true);
          if (this.config.connectionOptions!.has(key) || !this.config.allowedConnectionOptions!.has(key))
            throw new AdbcError("Option is not caller configurable", "unauthorized");
          await connection.setOption(key, optionFromWire(request.value));
          return ok;
        }
        if (method === "get_connection_option") {
          const kind = optionKind(request.value_type);
          return { value: optionToWire(await connection.getOption(text(request.key, true), kind), kind) };
        }
        if (method === "get_info") {
          const codes = request.codes as bigint[] | null;
          if (
            codes !== null &&
            (!Array.isArray(codes) || codes.some((v) => typeof v !== "bigint" || v < 0n || v >= 1n << 32n))
          )
            invalid("Invalid information codes");
          return this.register(session, () => connection.getInfo(codes));
        }
        if (method === "get_objects") {
          if (typeof request.depth !== "bigint" || request.depth < 0n || request.depth > 3n)
            invalid("Invalid object depth");
          return this.register(session, () => connection.getObjects(request as unknown as ObjectFilters));
        }
        if (method === "get_table_schema")
          return {
            schema_ipc: this.checkSchema(
              await connection.getTableSchema(request as unknown as TableIdentifier),
            ),
          };
        if (method === "get_table_types") return this.register(session, () => connection.getTableTypes());
        if (method === "get_statistic_names")
          return this.register(session, () => connection.getStatisticNames());
        if (method === "get_statistics")
          return this.register(session, () =>
            connection.getStatistics(request as unknown as StatisticsFilters),
          );
        if (method === "read_partition") {
          const descriptor = this.openPartition(request.payload as Uint8Array, session);
          return this.register(session, () => connection.readPartition(descriptor));
        }
        const statement = this.statement(session, request.statement_id);
        const backend = statement.backend;
        if (method === "close_statement") {
          await this.dropStatement(session, text(request.statement_id));
          return ok;
        }
        if (method === "set_sql_query" || method === "set_substrait_plan") {
          const value = method === "set_sql_query" ? text(request.sql) : (request.payload as Uint8Array);
          if (Buffer.byteLength(value) > this.limits.sqlBytes) invalid("Statement exceeds configured limit");
          if (statement.result) await this.dropResult(session, statement.result);
          statement.upload = undefined;
          if (typeof value === "string") await backend.setSqlQuery(value);
          else await backend.setSubstraitPlan(value);
          return ok;
        }
        if (method === "prepare") {
          await backend.prepare();
          return ok;
        }
        if (method === "execute") return this.register(session, () => backend.execute(), statement);
        if (statement.upload && !statement.upload.finished)
          throw new AdbcError("Binding is incomplete", "invalid_state");
        if (method === "execute_update") {
          if (statement.result) await this.dropResult(session, statement.result);
          return { rows_affected: this.rows(await backend.executeUpdate()) };
        }
        if (method === "execute_schema") {
          if (statement.result) await this.dropResult(session, statement.result);
          return { schema_ipc: this.checkSchema(await backend.executeSchema()) };
        }
        if (method === "get_parameter_schema")
          return { schema_ipc: this.checkSchema(await backend.getParameterSchema()) };
        if (method === "execute_partitions") {
          if (statement.result) await this.dropResult(session, statement.result);
          const result = await backend.executePartitions();
          const schema_ipc = this.checkSchema(result.schema);
          if (result.partitions.length > this.limits.partitions)
            throw new AdbcError("Too many partitions", "invalid_data");
          const partitions: Uint8Array[] = [];
          let total = 0;
          for (const descriptor of result.partitions) {
            const token = this.sealPartition(descriptor, session);
            total += token.length;
            if (total > this.limits.partitionBytes)
              throw new AdbcError("Partitions exceed configured limit", "invalid_data");
            partitions.push(token);
          }
          return { rows_affected: this.rows(result.rowsAffected ?? null) ?? -1n, schema_ipc, partitions };
        }
        if (method === "set_statement_option") {
          await backend.setOption(text(request.key, true), optionFromWire(request.value));
          return ok;
        }
        if (method === "get_statement_option") {
          const kind = optionKind(request.value_type);
          return { value: optionToWire(await backend.getOption(text(request.key, true), kind), kind) };
        }
        throw new AdbcError("Unknown operation", "not_implemented");
      });
    });
  }

  private sealPartition(descriptor: Uint8Array, session: Session): Uint8Array {
    if (!(descriptor instanceof Uint8Array) || descriptor.length > this.limits.partitionBytes)
      throw new AdbcError("Partition exceeds configured limit", "invalid_data");
    const payload = encodeRecord(
      "PartitionClaims",
      {
        version: 1n,
        expires_at_ms: BigInt(Date.now() + this.limits.idleMs),
        owner: JSON.stringify([session.owner, session.target]),
        descriptor,
      },
      this.limits.partitionBytes,
    );
    const signature = createHmac("sha256", this.partitionKey).update(payload).digest();
    return Buffer.concat([signature, payload]);
  }
  private openPartition(token: Uint8Array, session: Session): Uint8Array {
    if (!(token instanceof Uint8Array) || token.length < 32 || token.length > this.limits.partitionBytes)
      invalid("Invalid partition token");
    const signature = createHmac("sha256", this.partitionKey).update(token.subarray(32)).digest();
    if (!timingSafeEqual(signature, token.subarray(0, 32))) invalid("Invalid partition token");
    const claims = decodeRecord("PartitionClaims", token.subarray(32), this.limits.partitionBytes);
    if (
      claims.version !== 1n ||
      claims.owner !== JSON.stringify([session.owner, session.target]) ||
      (claims.expires_at_ms as bigint) < BigInt(Date.now())
    )
      invalid("Invalid partition token");
    return claims.descriptor as Uint8Array;
  }

  async initResult(request: Record<string, unknown>): Promise<Cursor> {
    return this.guard(async () => {
      const session = this.session(request.session_id);
      const result = session.results.get(text(request.result_id, true));
      if (!result) throw new AdbcError("Result is unavailable", "not_found");
      this.socketScope
        .getStore()
        ?.results.set(text(request.result_id), { session, id: text(request.result_id) });
      // Stock VGI normalizes safe scalar int64 values to number, retaining
      // bigint outside the safe range. Restore exact bigint at the boundary.
      const sequence =
        typeof request.sequence === "number" && Number.isSafeInteger(request.sequence)
          ? BigInt(request.sequence)
          : request.sequence;
      if (result.producer !== undefined && sequence !== 0n)
        invalid("Producer results resume from continuation tokens");
      if (
        typeof sequence !== "bigint" ||
        sequence < 0n ||
        (sequence !== result.sequence && sequence !== result.sequence - 1n)
      )
        invalid("Invalid result sequence");
      return {
        session_id: text(request.session_id),
        result_id: text(request.result_id),
        sequence,
        ...(result.producer === undefined ? {} : { producer: result.producer }),
        __outputSchema: result.query.schema,
      };
    });
  }
  /**
   * Fetch exactly one batch. Iterator results retain only the immediately
   * preceding replay batch; producer results retain nothing and advance
   * `cursor.producer` to the state for the following fetch.
   */
  async next(cursor: Cursor): Promise<RecordBatch | null> {
    return this.guard(async () => {
      const session = this.session(cursor.session_id);
      return this.locked(session, async () => {
        const result = session.results.get(cursor.result_id);
        if (!result) throw new AdbcError("Result is unavailable", "not_found");
        if (result.producer !== undefined) return this.nextProduced(session, result, cursor);
        if (cursor.sequence === result.sequence - 1n && result.previous) return result.previous;
        if (cursor.sequence !== result.sequence) invalid("Invalid result sequence");
        if (result.ended) return null;
        try {
          const next = await result.iterator.next();
          if (next.done) {
            result.ended = true;
            await this.release(result);
            return null;
          }
          const value = next.value;
          this.checkBatch(result, value);
          result.previous = value;
          result.sequence++;
          return value;
        } catch (error) {
          await this.dropResult(session, cursor.result_id);
          throw error;
        }
      });
    });
  }
  private checkBatch(result: ResultState, value: RecordBatch): void {
    if (!sameSchema(value.schema, result.query.schema))
      throw new AdbcError("Result schema changed", "invalid_data");
    if (retainedBytes(value) > this.limits.batchBytes || encodeBatch(value).length > this.limits.batchBytes)
      throw new AdbcError("Batch exceeds configured limit", "invalid_data");
  }
  /** Resume a producer from cursor state; replaying the previous sequence re-produces its batch. */
  private async nextProduced(
    session: Session,
    result: ResultState,
    cursor: Cursor,
  ): Promise<RecordBatch | null> {
    if (cursor.sequence !== result.sequence && cursor.sequence !== result.sequence - 1n)
      invalid("Invalid result sequence");
    if (result.ended && cursor.sequence === result.sequence) return null;
    let value: RecordBatch | null;
    let advanced: string;
    try {
      if (typeof cursor.producer !== "string") throw new AdbcError("Unknown result producer", "invalid_data");
      const producer = ResultProducer.decode(Buffer.from(cursor.producer, "base64"));
      value = await producer.produce();
      if (value === null) {
        result.ended = true;
        await this.release(result);
        return null;
      }
      this.checkBatch(result, value);
      advanced = this.encodeProducer(producer);
    } catch (error) {
      await this.dropResult(session, cursor.result_id);
      throw error;
    }
    if (cursor.sequence + 1n > result.sequence) result.sequence = cursor.sequence + 1n;
    cursor.producer = advanced;
    return value;
  }
  async initBind(request: Record<string, unknown>, stream: boolean): Promise<BindCursor> {
    return this.guard(async () => {
      const session = this.session(request.session_id);
      return this.locked(session, async () => {
        const statement = this.statement(session, request.statement_id);
        let schema: Schema;
        try {
          schema = decodeSchema(request.schema_ipc as Uint8Array, this.limits.schemaBytes);
        } catch {
          invalid("Invalid binding schema");
        }
        if (statement.result) await this.dropResult(session, statement.result);
        const upload_id = randomUUID();
        this.socketScope.getStore()?.uploads.set(upload_id, { session, statement, id: upload_id });
        statement.upload = {
          id: upload_id,
          schema,
          stream,
          batches: [],
          bytes: 0,
          sequence: 0n,
          finished: false,
        };
        return {
          session_id: text(request.session_id),
          statement_id: text(request.statement_id),
          upload_id,
          sequence: 0n,
        };
      });
    });
  }
  async pushBind(cursor: BindCursor, frame: RecordBatch): Promise<void> {
    return this.guard(async () => {
      const session = this.session(cursor.session_id);
      return this.locked(session, async () => {
        const statement = this.statement(session, cursor.statement_id);
        const upload = statement.upload;
        if (!upload || upload.id !== cursor.upload_id)
          throw new AdbcError("Binding is unavailable", "not_found");
        const digest = createHash("sha256").update(encodeBatch(frame)).digest("hex");
        if (cursor.sequence === upload.sequence - 1n && digest === upload.previousDigest) return;
        if (cursor.sequence !== upload.sequence || upload.finished) invalid("Invalid binding sequence");
        try {
          const bindingMethod = CONTRACT.methods.find((m) => m.name === "bind")!;
          if (frame.numRows !== 1 || !sameSchema(frame.schema, schema(bindingMethod.input!)))
            invalid("Expected one binding frame");
          const payload = frame.getChild("batch_ipc")?.get(0) as Uint8Array;
          const finish = frame.getChild("finish")?.get(0);
          if (!(payload instanceof Uint8Array) || typeof finish !== "boolean")
            invalid("Invalid binding frame");
          if (finish) {
            if (payload.length) invalid("Finish frame must have no payload");
            if (!upload.stream && upload.batches.length !== 1) invalid("Bind requires exactly one batch");
            if (upload.stream) await statement.backend.bindStream(upload.schema, upload.batches);
            else await statement.backend.bind(upload.schema, upload.batches[0]!);
            upload.batches = [];
            upload.finished = true;
          } else {
            if (
              upload.bytes + payload.length > this.limits.bindBytes ||
              upload.batches.length >= this.limits.bindBatches ||
              (!upload.stream && upload.batches.length !== 0)
            )
              invalid("Binding exceeds configured limit");
            const value = decodeBatch(payload, this.limits.batchBytes);
            if (!sameSchema(value.schema, upload.schema)) invalid("Binding schema changed");
            const retained = Math.max(payload.length, retainedBytes(value));
            if (upload.bytes + retained > this.limits.bindBytes) invalid("Binding exceeds configured limit");
            upload.batches.push(value);
            upload.bytes += retained;
          }
          upload.previousDigest = digest;
          upload.sequence++;
        } catch (error) {
          statement.upload = undefined;
          throw error;
        }
      });
    });
  }
  private buildProtocol(): Protocol {
    const protocol = new Protocol(CONTRACT.protocol_name, { protocolVersion: CONTRACT.protocol_version });
    for (const method of CONTRACT.methods) {
      if (method.kind === "unary") {
        protocol.unary(method.name, {
          params: schema(method.request),
          result: schema(method.response!),
          handler: async (params) => {
            const request = method.request_record
              ? decodeRecord(method.request_record, params.request as Uint8Array, this.limits.requestBytes)
              : params;
            const value = await this.invoke(method.name, request);
            return { result: encodeRecord(method.response_record!, value, this.limits.batchBytes) };
          },
        });
      } else if (method.kind === "producer") {
        protocol.producer<Cursor>(method.name, {
          params: schema(method.request),
          outputSchema: new Schema([]),
          init: (params) => this.initResult(params),
          produce: async (cursor, out) => {
            const value = await this.next(cursor);
            if (value) {
              out.emit(value);
              cursor.sequence++;
            } else {
              this.socketScope.getStore()?.results.delete(cursor.result_id);
              out.finish();
            }
          },
          onCancel: async (cursor) => {
            try {
              const session = this.session(cursor.session_id);
              await this.deferCleanup(session, `result:${cursor.result_id}`, () =>
                this.dropResult(session, cursor.result_id),
              );
            } catch {
              /* VGI logs hook failures; cancellation must not expose backend errors. */
            }
          },
        });
      } else {
        protocol.exchange<BindCursor>(method.name, {
          params: schema(method.request),
          inputSchema: schema(method.input!),
          outputSchema: schema(method.response!),
          init: (params) => this.initBind(params, method.name === "bind_stream"),
          exchange: async (cursor, input, out) => {
            await this.pushBind(cursor, input as RecordBatch);
            cursor.sequence++;
            out.emit(batch(recordSchema("OkResponse"), { ok: [true] }));
          },
          onCancel: async (cursor) => {
            try {
              const session = this.session(cursor.session_id);
              await this.deferCleanup(session, `upload:${cursor.upload_id}`, async () => {
                const statement = this.statement(session, cursor.statement_id);
                if (statement.upload?.id === cursor.upload_id && !statement.upload.finished)
                  statement.upload = undefined;
              });
            } catch {
              /* Cancellation cleanup never logs backend messages. */
            }
          },
        });
      }
    }
    return protocol;
  }
}
