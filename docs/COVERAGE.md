# Behavior coverage

Test counts are not interchangeable across SDKs. Python's older suite splits
many cases into individual parametrized tests and includes Python-specific
hosting, isolation and release tooling. TypeScript groups several operations in
one test and has a different host/runtime. A smaller count is not proof of
equivalent coverage, nor should cases be duplicated just to match a number.

| Behavior | TypeScript evidence | Shared native-driver evidence |
| --- | --- | --- |
| All 31 methods and exact Arrow records | Canonical contract parity; strict typed-record tests | Method registration, version and malformed-schema rejection |
| Statement lifecycle and result pulls | Replay, cursor cleanup, byte limits, exact bigint | Repeated queries, partial close, independent handles, recoverable errors |
| Preparation, updates and transactions | Positive hook dispatch, row-count validation | Synthetic example intentionally returns `NOT_IMPLEMENTED` |
| Binding and ingestion input | Positive single/stream binding, finish/replay, byte/batch limits | Optional hooks are not a substitute for a real ingestion backend gate |
| Options, metadata and Substrait | Four exact option types, null/empty filters, metadata cursors, exact binary plan | Native query fixture does not certify every metadata schema |
| Partitioned results | Signed token round trip, tampering and cross-principal rejection | Backend-specific partition behavior needs backend integration tests |
| Principal ownership | Different principals and authentication domains | HTTP credentials, verified mTLS identities, Iroh EndpointId allowlist |
| HTTP/HTTPS | Admission, bounded body intake, cancellation and shutdown | Actual ADBC query/error/reuse plus certificate validation |
| TCP/mTLS | Stock VGI client, admission, idle timeout, lifetime byte boundary, TLS handshake timeout, disconnect cleanup | Actual ADBC persistent streams, certificate/hostname/ownership rejection |
| Iroh | Strict identity configuration and owned bridge lifecycle | Actual native ADBC QUIC streams, two allowed identities and denied identity |
| Packaging | Installed npm tarball strict compilation and runtime import | Independent executable process adapter |

The shared suite selects applicable behaviors per transport. Deselected HTTP
token tests on mTLS/Iroh are replaced by certificate/EndpointId checks; they are
not reported as passed. Raw TCP deliberately has a single configured local
identity and cannot provide per-client authentication.

Remaining release work includes long running load/soak evidence, real downstream
backend integration, relay-path deployment testing, and deployment supervision.
Node cannot forcibly interrupt blocking JavaScript or native callbacks. The SDK
does not currently provide Python's isolated-worker hosting facilities or a
Windows Iroh listener. These are explicit differences, not covered by passing
the shared synthetic workload.
