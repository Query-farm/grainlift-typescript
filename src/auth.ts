// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
import { timingSafeEqual } from "node:crypto";

/** Verified identity; authentication domains are part of handle ownership. */
export class AuthContext {
  constructor(
    readonly domain: string,
    readonly authenticated: boolean,
    readonly principal: string | null,
    readonly claims: Record<string, unknown> = {},
  ) {}
  static anonymous(): AuthContext {
    return new AuthContext("", false, null);
  }
}
export type AuthenticateFn = (request: Request) => AuthContext | Promise<AuthContext>;

/** Static bearer authenticator for examples and controlled deployments. */
export function bearerAuthenticateStatic(tokens: ReadonlyMap<string, AuthContext>): AuthenticateFn {
  const entries = [...tokens].map(([key, auth]) => {
    if (Buffer.byteLength(key) < 16) throw new TypeError("Bearer tokens require at least 16 bytes");
    return [Buffer.from(key), auth] as const;
  });
  return (request) => {
    const header = request.headers.get("authorization");
    if (!header?.startsWith("Bearer ")) return AuthContext.anonymous();
    const supplied = Buffer.from(header.slice(7));
    for (const [token, auth] of entries) {
      if (supplied.length === token.length && timingSafeEqual(supplied, token)) return auth;
    }
    return AuthContext.anonymous();
  };
}
