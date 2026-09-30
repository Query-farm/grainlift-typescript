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

/** Authentication domain of anonymous identities, distinct from every token domain. */
export const ANONYMOUS_DOMAIN = "grainlift.anonymous";

/**
 * Opt in to anonymous HTTP access, alone or alongside bearer tokens.
 *
 * Anonymous access is for services that are safe to expose without
 * credentials, such as read-only data. Requests without an Authorization
 * header act as `principal` in the {@link ANONYMOUS_DOMAIN} domain, so their
 * sessions and continuation tokens stay separate from token principals; all
 * anonymous clients share that principal, so grant it only public, read-only
 * capabilities. A request that presents credentials is passed to `tokens`,
 * and one that fails is rejected, never downgraded to anonymous. Token
 * identities may not use the anonymous principal name or domain.
 *
 * @param principal Principal for requests without credentials.
 * @param tokens Bearer secrets mapped to identities, or an authenticator for requests that carry credentials.
 */
export function authenticateAnonymous(
  principal: string,
  tokens?: ReadonlyMap<string, AuthContext> | AuthenticateFn,
): AuthenticateFn {
  if (typeof principal !== "string" || !principal || Buffer.byteLength(principal) > 1024)
    throw new TypeError("Invalid anonymous principal");
  if (tokens instanceof Map && [...tokens.values()].some((auth) => auth.principal === principal))
    throw new TypeError("The anonymous principal must differ from every token principal");
  const credentials = typeof tokens === "function" ? tokens : tokens && bearerAuthenticateStatic(tokens);
  const anonymous = new AuthContext(ANONYMOUS_DOMAIN, true, principal);
  return async (request) => {
    if (request.headers.get("authorization") === null) return anonymous;
    const identity = credentials ? await credentials(request) : AuthContext.anonymous();
    if (identity.principal === principal || identity.domain === ANONYMOUS_DOMAIN)
      return AuthContext.anonymous();
    return identity;
  };
}
