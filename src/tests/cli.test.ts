// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { Connection } from "../api.js";
import { type RunOptions, run } from "../cli.js";
import { connect } from "./http-client.js";

const FIXTURE = fileURLToPath(new URL("./cli-fixture.js", import.meta.url));
const worker = { open: async () => new Connection() };

/**
 * Record the command's text output. The test runner reports results through the
 * same streams as binary frames, so those pass through untouched.
 */
function capture(
  original: typeof process.stdout.write,
  record: (text: string) => void,
): typeof process.stdout.write {
  return function (this: NodeJS.WriteStream, chunk: unknown, ...rest: unknown[]) {
    if (typeof chunk !== "string") return Reflect.apply(original, this, [chunk, ...rest]);
    record(chunk);
    return true;
  } as typeof process.stdout.write;
}

interface Running {
  endpoint: string;
  stdout: string;
  stderr: string;
  stop(): Promise<void>;
}

/** Run the development command in process on an ephemeral port, capturing its output. */
async function start(
  argv: string[],
  options: Partial<RunOptions> = {},
  token: string | undefined = undefined,
): Promise<Running> {
  const saved = process.env.GRAINLIFT_TOKEN;
  if (token === undefined) delete process.env.GRAINLIFT_TOKEN;
  else process.env.GRAINLIFT_TOKEN = token;
  const writes = { stdout: process.stdout.write, stderr: process.stderr.write };
  const output = { stdout: "", stderr: "" };
  process.stdout.write = capture(writes.stdout, (text) => {
    output.stdout += text;
  });
  process.stderr.write = capture(writes.stderr, (text) => {
    output.stderr += text;
  });
  const controller = new AbortController();
  let serving: Promise<void>;
  try {
    serving = run(worker, {
      target: "default",
      name: "fixture",
      ...options,
      argv: ["--port", "0", ...argv],
      signal: controller.signal,
    });
    const deadline = Date.now() + 10_000;
    while (!/listening on /.test(output.stdout)) {
      if (Date.now() > deadline) throw new Error("No listening line");
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  } finally {
    process.stdout.write = writes.stdout;
    process.stderr.write = writes.stderr;
    if (saved === undefined) delete process.env.GRAINLIFT_TOKEN;
    else process.env.GRAINLIFT_TOKEN = saved;
  }
  return {
    endpoint: /listening on (\S+)/.exec(output.stdout)![1]!,
    ...output,
    stop: async () => {
      controller.abort();
      await serving;
    },
  };
}
async function opens(endpoint: string, token?: string): Promise<boolean> {
  const client = connect(endpoint, token === undefined ? undefined : `Bearer ${token}`);
  try {
    await client.call("open_connection", { target: "default", database_options: [], connection_options: [] });
    return true;
  } catch {
    return false;
  } finally {
    client.close();
  }
}
/** Run to completion with arguments that never start a server, capturing output and exit code. */
async function finished(argv: string[]) {
  const writes = { stdout: process.stdout.write, stderr: process.stderr.write };
  const output = { stdout: "", stderr: "", code: undefined as typeof process.exitCode };
  process.stdout.write = capture(writes.stdout, (text) => {
    output.stdout += text;
  });
  process.stderr.write = capture(writes.stderr, (text) => {
    output.stderr += text;
  });
  const exitCode = process.exitCode;
  try {
    await run(worker, { target: "default", name: "fixture", description: "Fixture worker", argv });
    output.code = process.exitCode;
  } finally {
    process.stdout.write = writes.stdout;
    process.stderr.write = writes.stderr;
    process.exitCode = exitCode;
  }
  return output;
}

test("token mode without GRAINLIFT_TOKEN generates one and prints the export line", async () => {
  const running = await start([]);
  try {
    const token = /export GRAINLIFT_TOKEN=(\S+)/.exec(running.stderr)?.[1];
    assert.ok(token);
    assert.match(running.stdout, /Grainlift target 'default' listening on http:\/\/127\.0\.0\.1:\d+/);
    assert.equal(await opens(running.endpoint, token), true);
    assert.equal(await opens(running.endpoint), false);
  } finally {
    await running.stop();
  }
});

test("an exported GRAINLIFT_TOKEN is served as-is", async () => {
  const running = await start([], {}, "chosen-token-0123456789");
  try {
    assert.doesNotMatch(running.stderr, /export GRAINLIFT_TOKEN/);
    assert.equal(await opens(running.endpoint, "chosen-token-0123456789"), true);
    assert.equal(await opens(running.endpoint, "wrong-token-0123456789"), false);
  } finally {
    await running.stop();
  }
});

test("--auth anonymous needs no token and generates none", async () => {
  const running = await start(["--auth", "anonymous"]);
  try {
    assert.match(running.stdout, /Anonymous access enabled: clients connect without a token as 'anonymous'/);
    assert.doesNotMatch(running.stderr, /GRAINLIFT_TOKEN/);
    assert.equal(await opens(running.endpoint), true);
    assert.equal(await opens(running.endpoint, "wrong-token-0123456789"), false);
  } finally {
    await running.stop();
  }
});

test("an anonymous default also accepts an exported token; --auth token restores the requirement", async () => {
  const token = "chosen-token-0123456789";
  const anonymous = await start([], { auth: "anonymous" }, token);
  try {
    assert.equal(await opens(anonymous.endpoint), true);
    assert.equal(await opens(anonymous.endpoint, token), true);
  } finally {
    await anonymous.stop();
  }
  const required = await start(["--auth", "token"], { auth: "anonymous" }, token);
  try {
    assert.equal(await opens(required.endpoint), false);
    assert.equal(await opens(required.endpoint, token), true);
  } finally {
    await required.stop();
  }
});

test("help, invalid options and incomplete mTLS options", async () => {
  const help = await finished(["--help"]);
  assert.equal(help.code, undefined);
  assert.match(help.stdout, /usage: fixture/);
  assert.match(help.stdout, /--auth \{token,anonymous\}/);
  assert.match(help.stdout, /Fixture worker/);
  for (const argv of [
    ["--host", "mtls"],
    ["--auth", "none"],
    ["--host", "waitress"],
    ["--port", "x"],
    ["--bogus"],
  ]) {
    const failed = await finished(argv);
    assert.equal(failed.code, 2, argv.join(" "));
    assert.match(failed.stderr, /fixture: error: /);
  }
  await assert.rejects(run(worker, { target: "default", auth: "none" as "token", argv: [] }), /auth must be/);
});

test("a command built on run() serves until SIGTERM and exits cleanly", async () => {
  const child = spawn(process.execPath, [FIXTURE, "--port", "0", "--auth", "anonymous"]);
  let stdout = "";
  child.stdout.on("data", (data: Buffer) => {
    stdout += data;
  });
  try {
    const deadline = Date.now() + 20_000;
    while (!/listening on /.test(stdout)) {
      assert.equal(child.exitCode, null);
      assert.ok(Date.now() < deadline, "No listening line");
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.equal(await opens(/listening on (\S+)/.exec(stdout)![1]!), true);
    child.kill("SIGTERM");
    const [code] = await once(child, "exit");
    assert.equal(code, 0);
  } finally {
    if (child.exitCode === null) child.kill("SIGKILL");
  }
});
