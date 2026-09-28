# Grainlift TypeScript

Build server-side ADBC workers in TypeScript. Ordinary ADBC applications keep
using `adbc-driver-grainlift`; this package supplies the backend framework.
It carries typed Arrow records over [VGI-RPC](https://github.com/Query-farm/vgi-rpc-typescript)
and connects to the ordinary [Grainlift ADBC driver](https://github.com/Query-farm/grainlift).
Node.js 22 or newer is required.

## Status

This is a prerelease SDK. The package name is `@query-farm/grainlift`, but it is
**not published to npm**. Build from source with the sibling checkout below.

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

Use sibling checkouts so the example can resolve its source dependency:

```sh
git clone https://github.com/Query-farm/grainlift-typescript.git
git clone https://github.com/Query-farm/grainlift-hello-world-typescript.git
cd grainlift-typescript
npm ci
npm run build
cd ../grainlift-hello-world-typescript
npm ci
npm run build
export GRAINLIFT_HELLO_TOKEN="$(node -e 'process.stdout.write(require("node:crypto").randomBytes(32).toString("hex"))')"
node dist/main.js --transport http --port 0
```

The example prints its endpoint as JSON. Keep stdin open while using the server;
a newline, EOF, SIGINT, or SIGTERM initiates shutdown. Connect with the native
Grainlift ADBC driver, target `default`, the configured bearer token, and
`autocommit=True`, then execute `QUERY`. See the
[example README](https://github.com/Query-farm/grainlift-hello-world-typescript#readme)
for workload controls and other transports.

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
enforce socket/admission limits itself. Only `serveMutualTls` permits routable
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
batches, 1024 partitions with 1 MiB combined tokens, and 5 minutes idle expiry.
All are configurable positive finite limits. Results retain at most one replay
batch per cursor. Binding is explicitly bounded staging, not unbounded streaming;
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
cleanup and shutdown. They also keep two principals' result streams live at
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
including packaging and contract parity. The separate example runs the shared
native-driver suite across all five transports. Recorded EC2 results and hosted
CI runs are distinct evidence; see the links above for each.

## Documentation

- [Validation evidence and limitations](VALIDATION.md)
- [Behavior coverage and differences from Python/Rust](docs/COVERAGE.md)
- [Executable TypeScript example](https://github.com/Query-farm/grainlift-hello-world-typescript)
- [Grainlift native ADBC driver, protocol, and shared validation](https://github.com/Query-farm/grainlift)
- [Backend API](src/api.ts), [service configuration](src/service.ts), and [transport options](src/transports.ts)

## License

Licensed under [Apache License 2.0](LICENSE).
