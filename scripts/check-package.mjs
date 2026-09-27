// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const npm = process.env.npm_execpath;
if (!npm) throw new Error("Run through npm run check:package");
const directory = mkdtempSync(join(tmpdir(), "grainlift-consumer-"));
const runNpm = (args) => execFileSync(process.execPath, [npm, ...args], { encoding: "utf8" });
try {
  const packed = JSON.parse(runNpm(["pack", "--json", "--pack-destination", directory]));
  writeFileSync(join(directory, "package.json"), JSON.stringify({ type: "module", private: true }));
  runNpm(["install", "--prefix", directory, "--no-audit", "--no-fund", join(directory, packed[0].filename), "@types/node@22.19.15"]);
  writeFileSync(join(directory, "consumer.ts"), readFileSync("scripts/consumer.ts"));
  writeFileSync(join(directory, "tsconfig.json"), JSON.stringify({
    compilerOptions: { target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext", strict: true,
      noUncheckedIndexedAccess: true, noEmit: true }, include: ["consumer.ts"],
  }));
  execFileSync(process.execPath, [resolve("node_modules/typescript/bin/tsc"), "-p", directory], { stdio: "inherit" });
  execFileSync(process.execPath, ["--input-type=module", "-e", "await import('@query-farm/grainlift')"],
    { cwd: directory, stdio: "inherit" });
  process.stdout.write("Packed SDK imports and typechecks without consumer workarounds\n");
} finally { rmSync(directory, { recursive: true, force: true }); }
