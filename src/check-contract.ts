// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

// Deliberately fail if the canonical checkout is missing: this gate never skips.
const canonical = process.env.GRAINLIFT_CONTRACT ?? "../grainlift/validation/conformance/contract.json";
assert.deepEqual(
  await readFile(canonical),
  await readFile("src/contract.json"),
  "Grainlift protocol snapshot differs from the authoritative contract",
);
process.stdout.write("Grainlift protocol snapshot matches authoritative contract\n");
