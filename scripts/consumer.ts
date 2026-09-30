// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import { AuthContext, Connection, GrainliftService, QueryResult, ResultProducer, Schema, Statement,
  authenticateAnonymous, bearerAuthenticateStatic, serveHttp, type RecordBatch, type Worker } from "@query-farm/grainlift";
import { run } from "@query-farm/grainlift/cli";

class Empty extends ResultProducer {
  done = false;
  produce(): RecordBatch | null { return null; }
}
ResultProducer.register("consumer:Empty", Empty);
class BackendStatement extends Statement {
  override async execute(): Promise<QueryResult> { return QueryResult.fromProducer(new Schema([]), new Empty()); }
}
class BackendConnection extends Connection {
  override async newStatement(): Promise<Statement> { return new BackendStatement(); }
}
const worker: Worker = { open: async () => new BackendConnection() };
const service = new GrainliftService(worker, { authorize: () => true });
const identity = new AuthContext("bearer", true, "example");
const handler = service.httpHandler(bearerAuthenticateStatic(new Map([["test-token-123456", identity]])));
const anonymous = service.httpHandler(authenticateAnonymous("anonymous", new Map([["test-token-123456", identity]])));
void handler;
void anonymous;
void serveHttp;
void run;
void service.close();
