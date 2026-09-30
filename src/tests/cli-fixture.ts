// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
// A development command built on run(), spawned by cli.test.ts.
import { Connection } from "../api.js";
import { run } from "../cli.js";

run({ open: async () => new Connection() }, { target: "default", name: "fixture" }).catch(
  (error: unknown) => {
    process.stderr.write(`${(error as Error).message}\n`);
    process.exitCode = 1;
  },
);
