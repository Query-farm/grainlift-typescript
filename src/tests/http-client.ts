// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
// A stock VGI HTTP client whose Authorization header tests may change between requests.
import { httpConnect } from "@query-farm/vgi-rpc";
import { CONTRACT, decodeRecord, encodeRecord } from "../wire.js";

export interface Client {
  authorization: string | undefined;
  call(method: string, request: Record<string, unknown>): Promise<Record<string, unknown>>;
  stream(
    method: string,
    params: Record<string, unknown>,
  ): ReturnType<ReturnType<typeof httpConnect>["stream"]>;
  paths: string[];
  close(): void;
}

/** Connect to a Fetch handler in process, or to a URL. */
export function connect(
  target: string | ((request: Request) => Promise<Response>),
  authorization?: string,
): Client {
  const paths: string[] = [];
  const state = { authorization };
  const rpc = httpConnect(typeof target === "string" ? target : "http://grainlift.test", {
    fetch: async (input, init) => {
      const headers = new Headers(init?.headers);
      headers.delete("authorization");
      if (state.authorization !== undefined) headers.set("authorization", state.authorization);
      const request = new Request(input, { ...init, headers });
      paths.push(new URL(request.url).pathname);
      return typeof target === "string" ? fetch(request) : target(request);
    },
  });
  return {
    get authorization() {
      return state.authorization;
    },
    set authorization(value) {
      state.authorization = value;
    },
    paths,
    async call(method, request) {
      const spec = CONTRACT.methods.find((m) => m.name === method)!;
      const params = spec.request_record
        ? { request: encodeRecord(spec.request_record, request, 65536) }
        : request;
      const response = await rpc.call(method, params);
      return decodeRecord(spec.response_record!, response!.result, 65536);
    },
    stream: (method, params) => rpc.stream(method, params),
    close: () => rpc.close?.(),
  };
}

/** Open a session, run `sql`, and read every batch's first column through continuation tokens. */
export async function query(
  client: Client,
  sql = "query",
): Promise<{ session_id: unknown; batches: number[][] }> {
  const { session_id } = await client.call("open_connection", {
    target: "default",
    database_options: [],
    connection_options: [],
  });
  const { statement_id } = await client.call("new_statement", { session_id });
  await client.call("set_sql_query", { session_id, statement_id, sql });
  const { result_id } = await client.call("execute", { session_id, statement_id });
  const batches: number[][] = [];
  for await (const rows of await client.stream("read_result", { session_id, result_id, sequence: 0n }))
    batches.push(rows.map((row) => Number(Object.values(row)[0])));
  return { session_id, batches };
}
