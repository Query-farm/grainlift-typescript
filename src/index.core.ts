// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

// Runtime-agnostic entry: the service, its Fetch handler and Arrow helpers.
// Cloudflare Workers resolve the package to this module (the `workerd`/`worker`
// export conditions) with flechette as the Arrow backend; it needs the
// `nodejs_compat` compatibility flag (AsyncLocalStorage, node:crypto, Buffer).
// The Node.js host and the TCP, mTLS and Iroh transports are in `index.ts`.
export * from "./api.js";
export * from "./arrow.js";
export {
  ANONYMOUS_DOMAIN,
  AuthContext,
  type AuthenticateFn,
  authenticateAnonymous,
  bearerAuthenticateStatic,
} from "./auth.js";
export { GrainliftService, type HttpOptions, type ServiceOptions } from "./service.js";
export {
  ExternalStorageConfig,
  type ExternalStorageHttpOptions,
  type ExternalStorageOptions,
} from "./storage.js";
export { CONTRACT, decodeRecord, encodeRecord, recordSchema } from "./wire.js";
