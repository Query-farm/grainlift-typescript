// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import { AuthContext, Connection, GrainliftService, Statement, bearerAuthenticateStatic, serveHttp,
  type QueryResult, type Worker } from "@query-farm/grainlift";

class BackendStatement extends Statement {
  override async execute(): Promise<QueryResult> { throw new Error("fixture only"); }
}
class BackendConnection extends Connection {
  override async newStatement(): Promise<Statement> { return new BackendStatement(); }
}
const worker: Worker = { open: async () => new BackendConnection() };
const service = new GrainliftService(worker, { authorize: () => true });
const identity = new AuthContext("bearer", true, "example");
const handler = service.httpHandler(bearerAuthenticateStatic(new Map([["test-token-123456", identity]])));
void handler;
void serveHttp;
void service.close();
