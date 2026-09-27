# Validation evidence

The results below were recorded on an EC2 test host, not a developer workstation.
Runtime: Node.js 22.22.2 on Linux aarch64, TypeScript 5.9.3,
`@query-farm/vgi-rpc` 0.25.4, Arrow 21.1.1, and the released
`vgi-iroh-bridge` 0.27.3. Native-driver validation uses ordinary ADBC handles.

The shared transport matrix passed on 2026-09-26:

| Transport | Passed tests | Scope |
| --- | ---: | --- |
| HTTP | 60 | Native ABI plus independent control-schema, lifecycle and ownership checks |
| HTTPS | 61 | HTTP cases plus server certificate validation |
| TCP | 52 | Native persistent streams, lifecycle, exact control schemas and versions; explicit local identity |
| mTLS | 61 | TCP cases plus verified certificate identities, cross-owner denial, CA and hostname rejection |
| Iroh | 12 | Native raw QUIC queries/reuse, two permitted EndpointIds and denied identity |

Tests inapplicable to a transport were deselected, not counted as passed. The
native driver SHA-256 was
`6ed0ce017a0043e30bdf0178ebb0d0897c099e0abf03f5506b07fe2ffb52639e`.
The [Grainlift repository](https://github.com/Query-farm/grainlift/tree/main/validation/conformance/results)
stores the shared JUnit evidence and environment under
`validation/conformance/results/`. The final matrix includes the cancellation
privacy regression fix.

The SDK's 38 tests additionally cover positive optional backend hooks, exact bigint,
IPC validation, byte and admission boundaries, cleanup, explicit TLS handshake
timeouts, private Iroh upstream permissions and bridge lifecycle. A privacy
regression ensures cancellation cleanup cannot reach upstream debug logging
with a downstream error message. The separate example has two workload tests.
Strict TypeScript, Biome, canonical contract parity, and an isolated installed
npm tarball consumer are separate gates.

This evidence establishes interoperability and tested lifecycle/security
behavior. It is not long-running load/soak evidence, real-backend certification,
Windows Iroh support, or evidence that hosted CI has run. Consult
[GitHub Actions](https://github.com/Query-farm/grainlift-typescript/actions)
for hosted workflow results. See [the README](README.md#testing) for the commands
to reproduce the SDK gates and the
[example README](https://github.com/Query-farm/grainlift-hello-world-typescript#testing)
for the shared native-driver gate. The SDK is prerelease and has not been
published to npm.
