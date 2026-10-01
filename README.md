# Grainlift TypeScript

Build server-side ADBC workers in TypeScript. Ordinary ADBC applications keep
using `adbc-driver-grainlift`; this package supplies the backend framework.
It carries typed Arrow records over [VGI-RPC](https://github.com/Query-farm/vgi-rpc-typescript)
and connects to the ordinary [Grainlift ADBC driver](https://github.com/Query-farm/grainlift).
It runs on Node.js 22 or newer, and on Cloudflare Workers (see
[Cloudflare Workers](#cloudflare-workers)).

## Status

This is a prerelease SDK. The package name is `@query-farm/grainlift`, but it is
**not published to npm**. Depend on a pinned commit through npm's GitHub
support (`"@query-farm/grainlift": "github:Query-farm/grainlift-typescript#<commit>"`);
the `prepare` script builds `dist/` during installation.

The toolkit implements all 31 methods in Grainlift 0.4.0, including
connections, statements, transactions, preparation, single-batch and stream
binding, ingestion through standard statement options plus execute-update,
metadata/statistics, partition descriptors, Substrait, typed options, and
cancellation hooks. Backend classes explicitly return ADBC `NOT_IMPLEMENTED`
for operations that the backend has not implemented. This is not a claim that
every backend supports every ADBC feature or that ADBC conformance is certified.

HTTP, HTTPS, TCP, mTLS, and Iroh have passed the shared native-driver suite.
See [recorded validation](VALIDATION.md) for the tested versions, scope, and
remaining release work, and [GitHub Actions](https://github.com/Query-farm/grainlift-typescript/actions)
for hosted CI runs.

## Quickstart

The [hello-world example](https://github.com/Query-farm/grainlift-hello-world-typescript)
is the place to start: a complete read-only service with three queries, served
anonymously on loopback and queried from Haybarn/DuckDB or the Node.js ADBC
driver manager.

```sh
git clone https://github.com/Query-farm/grainlift.git
(cd grainlift && cargo build --locked -p adbc-driver-grainlift)
git clone https://github.com/Query-farm/grainlift-hello-world-typescript.git
cd grainlift-hello-world-typescript
npm ci
npm start
```

## Build a backend

Subclass the provided connection and statement classes and override supported
operations. This skeleton shows authentication and hosting; its base statement
returns `NOT_IMPLEMENTED` until you provide a query implementation.

```typescript
import {
  AuthContext, Connection, GrainliftService, Statement,
  bearerAuthenticateStatic, serveHttp,
} from "@query-farm/grainlift";

// Subclass Connection/Statement and override your backend's supported methods.
class MyConnection extends Connection {
  override async newStatement(): Promise<Statement> {
    return new Statement(); // Replace with your Statement implementation.
  }
}
const service = new GrainliftService({ open: async () => new MyConnection() }, {
  authorize: (principal, target) => principal === "application" && target === "default",
});
const token = process.env.GRAINLIFT_TOKEN!;
const authenticate = bearerAuthenticateStatic(new Map([
  [token, new AuthContext("bearer", true, "application")],
]));
const server = await serveHttp(service, authenticate);
// server.endpoint is a loopback URL; await server.close() during shutdown.
```

### Arrow types and batches

Schemas and batches come from VGI-RPC's Arrow facade, re-exported here: arrow-js
under Node.js, [flechette](https://github.com/uwdata/flechette) under Cloudflare
Workers. Build them with the factory functions rather than a backend's classes,
so the same backend code runs on both:

```typescript
import { batch, field, int64, schema, utf8 } from "@query-farm/grainlift";

const people = schema([field("id", int64(), false), field("name", utf8(), true)]);
const rows = batch(people, { id: [1n, 2n], name: ["Ada", null] });
```

Decoded values are plain JavaScript: `bigint` for 64-bit integers, strings,
`Uint8Array`, objects for structs and arrays for lists. Decimals arrive
unscaled (`4.5` as `45n` at scale 1).

### Results: iterators and producers

`Statement.execute` returns a `QueryResult`: a schema plus any iterable or async
iterable of batches, with an optional `close` callback for resources such as a
database cursor. The iterator lives in server memory until the result is
exhausted or released.

When the remaining work fits in a small serializable state, return a
`ResultProducer` instead. Its own fields (JSON values and `bigint`) are the
entire resumable state and `produce()` returns the next batch or `null`:

```typescript
import { batch, QueryResult, ResultProducer, type RecordBatch } from "@query-farm/grainlift";

class Countdown extends ResultProducer {
  constructor(public remaining: number) { super(); }
  produce(): RecordBatch | null {
    if (this.remaining === 0) return null;
    return batch(schema, { n: [BigInt(this.remaining--)] });
  }
}
ResultProducer.register("my-service:Countdown", Countdown); // once, under a stable name
// In Statement.execute():
return QueryResult.fromProducer(schema, new Countdown(10));
```

The service encodes the initial state at execute time. Over HTTP the encoded
state rides in the `read_result` cursor, which VGI seals into the encrypted
continuation token after every batch; the server keeps no iterator and no replay
batch. A retried fetch of the previous sequence re-produces its batch from the
token; older sequences fail with `INVALID_ARGUMENT`. Only registered classes
decode (others are `INVALID_DATA`), and decoding restores fields without calling
the constructor. Raw-stream transports carry the same state in their in-memory
cursor. `Limits.producerStateBytes` (64 KiB) bounds the encoded state.

### Anonymous access and development hosting

Token authentication is the default. Services that are safe to expose without
credentials, such as read-only public data, can opt in to anonymous HTTP access:

```typescript
import { authenticateAnonymous } from "@query-farm/grainlift";
// Requests without an Authorization header act as "anonymous"; tokens still work.
const server = await serveHttp(service, authenticateAnonymous("anonymous", tokens));
```

`tokens` is optional and takes the same map as `bearerAuthenticateStatic` (or
any authenticator). A presented credential that fails is rejected, never
downgraded to anonymous. Anonymous identities use the separate
`grainlift.anonymous` authentication domain, so their sessions and continuation
tokens cannot be used by a token principal, and token identities may not claim
the anonymous principal name.

For development, `run()` from `@query-farm/grainlift/cli` gives a worker its own
command with `--host http|mtls`, `--port` (default 8080), `--auth
token|anonymous` and mTLS certificate flags. HTTP uses the token in
`GRAINLIFT_TOKEN`, generating and printing one when it is unset. The toolkit
installs no executable of its own:

```typescript
import { run } from "@query-farm/grainlift/cli";
await run(new MyWorker(), { target: "default", auth: "token" });
```

## Contract and backend responsibilities

`src/contract.json` is copied byte-for-byte from Grainlift's canonical
`validation/conformance/contract.json`, generated from the Rust protocol types.
Run `npm run check:contract` with a sibling Grainlift checkout, or set
`GRAINLIFT_CONTRACT` to its canonical artifact path. This command fails on drift
or a missing source; the checked-in snapshot does not update itself.
All physical schemas, method names, nullability and nested list/struct fields
come from that artifact. Named control records use strict single-row Arrow IPC
envelopes. Embedded schema messages use Arrow's public `Message` APIs; embedded
control/binding IPC is uncompressed. VGI owns transport compression.

Integer options, row counts and metadata codes use `bigint`, preserving values
above 2^53. Nullable metadata filters retain the distinction between `null`, an
empty string and an empty list. Use `AdbcError` to supply client-visible status,
SQLSTATE, vendor code and ordered binary details; other exceptions are replaced
with a generic internal error. Do not put credentials into client-visible errors.

Override every operation your backend supports; unavailable operations must
remain `NOT_IMPLEMENTED`. Metadata batches must conform to the relevant ADBC
schemas. Backends own transaction semantics and bind/ingestion behavior. The SDK
does not emulate missing database operations. Returned Arrow batches must remain
immutable while held by the SDK. A result's optional `close` callback is invoked
once on exhaustion, explicit close, failure, expiry or service shutdown.

## Hosting and limits

The supported adapters reuse the published VGI framing and connection reuse:

| Adapter | Identity and deployment boundary |
| --- | --- |
| `serveHttp` | Bearer/application authentication on every HTTP request; optional `tls: {cert, key}` serves HTTPS directly. |
| `serveTcp` | Loopback only; requires an explicit local identity. It trusts local callers, without pretending to authenticate remote peers. |
| `serveMutualTls` | TLS 1.2 or newer, mandatory CA-verified client certificate, then application authorization through `authenticatePeer`. |
| `serveIroh` | Raw Iroh QUIC through an explicitly installed `vgi-iroh-bridge` 0.27.3; authorization uses the verified EndpointId. |

`httpHandler` can also be mounted in another Node HTTP/HTTPS host, which must
enforce socket/admission limits itself. Its options pass CORS (`corsOrigins`)
and OAuth discovery (`oauthResourceMetadata`) through to VGI-RPC: a request
without an accepted identity gets VGI-RPC's standard 401, with the OAuth
`WWW-Authenticate` challenge when configured, while CORS preflights, `/health`
and the OAuth metadata need no credentials. Only `serveMutualTls` permits routable
bind addresses. Clients must verify the server certificate and hostname; no
adapter disables certificate verification.

Iroh hosting owns the bridge process and a private Unix socket (directory 0700,
socket 0600). The socket requires VGI's canonical PROXY-v2 EndpointId preamble;
the issuer is selected locally. Processes running under the same OS account are
part of this trust boundary. There is no publicly reachable upstream TCP port
that accepts forwarded identity. Linux and macOS are supported; this Unix
adapter does not support Windows. The executable is never downloaded at runtime.
Production uses `secretKeyFile`; `ephemeral: true` is explicitly for examples and
tests. An unexpected bridge exit closes its upstream listener. Direct-path and
relay behavior follows the installed bridge; tests use direct paths without relays.

Sessions and all child handles are principal/domain scoped and process-local.
Deployments require affinity to the owning process; restart invalidates sessions,
continuation tokens and signed partition tokens. A service authorizer gates
targets. Configured database/connection options are authoritative: callers cannot
overwrite those keys; additional options require explicit allowlists.

Defaults: 128 sessions, 16 per principal, 64 statements and 64 results per session,
8 MiB request, 16 MiB batch, 1 MiB schema/SQL, 64 MiB staged binding, 1024 binding
batches, 1024 partitions with 1 MiB combined tokens, 64 KiB producer state, and
5 minutes idle expiry. All are configurable positive finite limits. Iterator
results retain at most one replay batch per cursor; producer results retain none. Binding is explicitly bounded staging, not unbounded streaming;
the backend receives input only after a validated finish frame. Retained Arrow
backing buffers count toward batch/binding limits. Partition tokens expire and
are authenticated to the principal, target and service instance.

The HTTP adapter bounds active requests (64) and sockets (128), header size,
request body size and request intake time. Shutdown stops admission, requests
backend cancellation, drains requests, and closes handles. After 5 seconds it
destroys remaining sockets and reports failure. Backends must cooperate with
`cancel` to interrupt work; JavaScript cannot forcibly stop a blocked native
callback or CPU loop. Run untrusted/blocking backends under a process supervisor
with memory/CPU limits. Application byte quotas do not replace process isolation
or promise pre-allocation safety for every malformed Arrow payload.

Stream adapters bound connections (64), socket inactivity (30 seconds), TLS
handshake progress (5 seconds), shutdown (5 seconds), and cumulative input per
connection (128 MiB). The input budget is deliberately a **connection-lifetime
budget**, not a per-request budget. Exhaustion closes that socket and can surface
an ADBC `IO` error; applications must recreate their ADBC connection. This does
not provide transparent reconnect or replay. Operators configure the lifetime
budget with `maxInputBytesPerConnection`. The application request, batch, and
binding limits also remain enforced. Arrow decoding can allocate based on metadata before all bytes
arrive, so transport caps still do not substitute for process memory limits.

Peer disconnect does not by itself prove a session is abandoned: independent
HTTP calls and replay may follow. Raw-stream disconnect releases its active
result cursor and incomplete binding upload; the connection's ADBC session is
still usable through another authenticated transport connection. Explicit
handle cleanup, stream cancellation, idle expiry and shutdown reclaim state.
There is no hard per-operation execution deadline for backend callbacks.
No SQL, values, tokens or raw backend exceptions
are logged by this toolkit.

## Cloudflare Workers

Under the `workerd`/`worker` export conditions the package resolves to a
runtime-agnostic entry: `GrainliftService`, `httpHandler`, authentication and
the Arrow helpers, with flechette as the Arrow backend. The Node.js host and
the TCP, mTLS and Iroh transports are not part of it. It needs the
`nodejs_compat` compatibility flag (AsyncLocalStorage, `node:crypto`, `Buffer`).

Sessions, statements and result iterators live in memory between requests, so
host the service in one Durable Object and forward every request to it; when
the object is evicted, sessions end and the driver opens new ones (autocommit
work only). The [Cloudflare example](https://github.com/Query-farm/grainlift-typescript-cloudflare-example)
serves a D1 database this way, with Google sign-in.

## Testing

From this repository, with Node.js 22 or newer:

```sh
npm ci
npm run check
npm test
npm run check:package
```

For contract parity, also check out `Query-farm/grainlift` as `../grainlift` and
run `npm run check:contract`, or set `GRAINLIFT_CONTRACT` to the canonical JSON
path. `check:package` installs a packed tarball into an isolated consumer and
checks strict compilation and runtime imports.

Tests cover positive optional backend hooks, Arrow IPC framing/compression
rejection, exact integers, ownership, quotas, replay, binding cleanup, result
cleanup and shutdown, plus producer results resumed through HTTP continuation
tokens, anonymous access and the development command-line host. They also keep two principals' result streams live at
once and cycle 64 short-lived sessions through a two-session quota while
checking cleanup. The shared native ADBC/wire suite in Grainlift is the
cross-language interoperability gate. See [the behavior matrix](docs/COVERAGE.md)
for the relationship between these tests and the older Python/Rust suites.
Production load/soak validation remains separate work.

`npm run check` uses strict TypeScript (including unchecked-index checking) and
Biome. The source tsconfig maps VGI 0.25.4's private Arrow declaration alias to
its published declarations, avoiding accidental typechecking of upstream source.
Our public declarations do not expose that alias or VGI types; consumers need no
tsconfig workaround. The separate example is compiled as a strict consumer.

The [CI workflow](.github/workflows/ci.yml) tests Node 22/24 on Linux/macOS,
including packaging and contract parity. The separate example tests its queries
through the native driver and Haybarn. Recorded EC2 results and hosted CI runs
are distinct evidence; see the links above for each.

## Documentation

- [Validation evidence and limitations](VALIDATION.md)
- [Behavior coverage and differences from Python/Rust](docs/COVERAGE.md)
- [Executable TypeScript example](https://github.com/Query-farm/grainlift-hello-world-typescript)
- [Grainlift native ADBC driver, protocol, and shared validation](https://github.com/Query-farm/grainlift)
- [Backend API](src/api.ts), [service configuration](src/service.ts), and [transport options](src/transports.ts)

## License

Licensed under [Apache License 2.0](LICENSE).
