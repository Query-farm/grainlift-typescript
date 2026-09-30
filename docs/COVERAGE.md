# Behavior coverage

Test counts are not interchangeable across SDKs. Python's older suite splits
many cases into individual parametrized tests and includes Python-specific
hosting, isolation and release tooling. TypeScript groups several operations in
one test and has a different host/runtime. A smaller count is not proof of
equivalent coverage, nor should cases be duplicated just to match a number.

| Behavior | TypeScript evidence | Shared native-driver evidence |
| --- | --- | --- |
| All 31 methods and exact Arrow records | Canonical contract parity; strict typed-record tests | Method registration, version and malformed-schema rejection |
| Statement lifecycle and result pulls | Replay, cursor cleanup, byte limits, exact bigint, interleaved live results across principals | Repeated queries, partial close, independent handles, recoverable errors |
| Preparation, updates and transactions | Positive hook dispatch, row-count validation | The hello-world example prepares queries through DuckDB/Haybarn `adbc_scanner`; updates and transactions are not exercised natively |
| Binding and ingestion input | Positive single/stream binding, finish/replay, byte/batch limits | Optional hooks are not a substitute for a real ingestion backend gate |
| Options, metadata and Substrait | Four exact option types, null/empty filters, metadata cursors, exact binary plan | Native query fixture does not certify every metadata schema |
| Partitioned results | Signed token round trip, tampering and cross-principal rejection | Backend-specific partition behavior needs backend integration tests |
| Principal ownership | Different principals and authentication domains, cross-owner result denial while both clients remain active | HTTP credentials, verified mTLS identities, Iroh EndpointId allowlist |
| Bounded session churn | 64 open/query/partial-close cycles at a two-session quota while another result remains live; backend close counts and retained handles checked each cycle | Native independent-client and session-churn tests in the shared worker suite |
| HTTP/HTTPS | Admission, bounded body intake, cancellation and shutdown | Actual ADBC query/error/reuse plus certificate validation |
| TCP/mTLS | Stock VGI client, admission, idle timeout, lifetime byte boundary, TLS handshake timeout, disconnect cleanup | Actual ADBC persistent streams, certificate/hostname/ownership rejection |
| Iroh | Strict identity configuration and owned bridge lifecycle | Actual native ADBC QUIC streams, two allowed identities and denied identity |
| Packaging | Installed npm tarball strict compilation and runtime import | Independent executable process adapter |

The shared native-driver column records runs of Grainlift's
`validation/conformance` suite against the former synthetic TypeScript worker.
That worker has been replaced by the
[hello-world example](https://github.com/Query-farm/grainlift-hello-world-typescript),
and no TypeScript CI job runs the shared suite any longer; treat that column as
historical until this SDK adds its own native-driver fixture. The example's CI
exercises HTTP, anonymous and token access, and SQL through `adbc_scanner` with
the native driver.

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
