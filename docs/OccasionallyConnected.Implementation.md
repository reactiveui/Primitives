# OccasionallyConnected implementation

The normative feature specification is [ReactiveUI.Primitives.OccasionallyConnected.md](ReactiveUI.Primitives.OccasionallyConnected.md).
Implementation PRs target `OccasionallyConnected`. The feature is incomplete until all v1 gates below pass.

## Delivery stages

| Stage | Scope | Required behavioral evidence |
| --- | --- | --- |
| 1 | Core identities, options, models and contracts | Unicode and size boundaries, immutable options, invalid configuration and capability combinations |
| 2 | Allowlisted JSON serialization and versioning | Canonical-byte hashes, unknown contracts, upcast chains, corrupt payloads, AOT registration |
| 3 | Builder and context lifecycle | Structural validation, identity compatibility, concurrent start/stop and restart |
| 4 | Sequenced local publishing | Atomic optimistic commit, cancellation boundaries, serialized notifications and observer isolation |
| 5 | Bounded queues and observer input | Count and byte limits, every overflow policy, durable records never dropped |
| 6 | Retry, batching and loopback transport | Lost ACKs, stable operation IDs, virtual time, circuit transitions and fairness |
| 7 | Remote receive and inbox | Deduplication, durable cursors, post-commit ACKs and snapshot recovery |
| 8 | Server authorization and idempotency | Authenticated identity, replay authorization, atomic effects and original duplicate results |
| 9 | SQLite persistence | Transaction conformance, child-process crash points, leases, migration and compaction |
| 10 | HTTP protocol | Version fixtures, bounded parsing, nonce replay protection, partial and ambiguous results |
| 11 | Exactly-once effect | Capability negotiation, retention expiry and explicit downgrade |
| 12 | DI and release verification | Options validation, samples, packaging, API, trim/AOT, security, soak and performance |

Each stage may use multiple small PRs. Tests precede behavior changes and use TUnit assertions exclusively.
Every stage requires zero build/analyzer warnings and 100% line and branch coverage for its new executable code;
the user-required coverage gate supersedes the lower percentages in specification section 17.4.
No suppression or coverage exclusion is added to meet these gates. Coverage totals alone do not establish the
durability, ordering or security invariants: the scenario and conformance suites must also pass.

## Integration decisions

- Public API baselines use the repository's current `PublicAPI/<tfm>/PublicAPI.txt` format and PublicApiSharp analyzer.
  The specification's shipped/unshipped filenames describe an older mechanism; API changes remain reviewable per TFM.
- Core targets the centrally defined `LibraryTargetFrameworks`; tests use `TestTargetFrameworks`.
- The six v1 packages are Core, the lean runtime, Server, Storage.Sqlite, Transport.Http and DependencyInjection.
  The specification explicitly schedules the separate Hosting and Reactive variants and other adapters after v1.
- New capabilities remain unadvertised until their conformance suite passes.
- Feature tests use `src/occasionally-connected.testconfig.json`, with no source, function or attribute exclusions.
  The existing preview-package warning suppression does not apply to the new package family.
- `StartPosition.FromSequence` denotes a non-negative server sequence interpreted by the adapter, never a local
  client sequence. Cursor values remain opaque and are limited to 4096 UTF-8 bytes without normalization.

## Contract decisions to establish before dependent implementation

The design repeats `ClientIdentity` with different constructor shapes, leaves several policy interfaces implicit,
and requires recovery transactions beyond the abbreviated adapter snippets. Record the final semantics alongside
the stage introducing each contract, with executable tests. Do not silently weaken the normative guarantees to
fit a snippet.

## Verification record

### Stage 1a: stream identities and start positions

- Executable RED: `StreamId` stub failed 16 of 28 tests; `StartPosition` stub failed 6 of 12 tests.
- GREEN: 40 tests passed on each of net8.0, net9.0, net10.0 and net11.0 (160 executions).
- Release coverage, independently inspected through Mtpunittestmcp: 66/66 lines and 58/58 branches on each modern TFM.
- Release builds and API baseline checks passed all eight library TFMs with zero warnings; NuGet packing passed.
- No new suppression or source/function/attribute coverage exclusion was added.
- Logs and reports are generated under `artifacts/occasionally-connected`; CI retains fresh reports for each OS/TFM.

The installed .NET 11 SDK's native `dotnet test` handshake returned exit 5 for this TUnit application. Building the
test project and executing its DLL directly runs the same Microsoft.Testing.Platform/TUnit application successfully.
The coverage workflow uses that invocation on Windows, Linux and macOS, with an explicit 100% line/branch gate.
PR [#192](https://github.com/reactiveui/Primitives/pull/192) passed all twelve feature coverage jobs on Windows,
Linux and macOS across the four modern frameworks. Full-solution CI builds remain a separate check.

### Stage 1b: publishing, subscription and observer input options

- Added operation/subscription identities and immutable options with structural validation.
- Durable publishing rejects dropping policies; exactly-once publishing requires durability; synchronous observer
  input rejects blocking backpressure. Custom policies require explicit registration support.
- Priority validation accepts configured inclusive bounds, with a default of -10 through 10.
- Agent tests preceded implementation; root review added independent custom-policy, durable-guarantee and
  per-operation override cases. Disabling validation caused 29 tests to fail.
- GREEN: 90 tests passed on each modern framework (360 executions).
- Release coverage independently inspected through Mtpunittestmcp: 152/152 lines and 104/104 branches per framework.

### Stage 1c: batching configuration

- Added immutable count, encoded-byte, dwell-time and per-stream in-flight limits with positive-value validation.
- Defaults match the design: 100 operations, 1 MiB, 50 ms and one in-flight batch per stream.
- Executable RED: the compilable validation stub failed eight of the ten new tests.
- GREEN: 100 tests passed on each modern framework (400 executions).
- Release coverage independently inspected through Mtpunittestmcp: 165/165 lines and 112/112 branches per framework.
- Runtime batching and enforcement of smaller negotiated server limits remain pending.

### Stage 6a: standalone endpoint circuit breaker

- Added the lean runtime project and a deterministic, thread-safe circuit breaker with an injected `TimeProvider`.
- Five consecutive transient failures open an endpoint for 30 seconds by default. Only one concurrent caller gets
  the half-open probe. A successful handshake resets the circuit; a failed or abandoned probe reopens it.
- Late failures do not extend an already-open deadline. Failure counts and extreme UTC deadlines cannot overflow.
- Root review expanded the worker's six-test handoff, removed unreachable state branches, and completed the gate.
  Ignoring the configured threshold caused seven tests to fail.
- GREEN: 106 Core tests plus 15 runtime tests passed per modern framework (484 executions).
- Mtpunittestmcp confirmed Core coverage of 173/173 lines and 118/118 branches, and runtime coverage of 57/57 lines
  and 20/20 branches, on each modern framework.
- Engine integration, retry persistence and endpoint fault classification remain pending.

### Stage 6b: standalone retry policy

- Added persisted retry state and deterministic decorrelated jitter with injected time and randomness.
- Server retry hints are lower bounds. Scheduling stops at the retry-age or calendar limit, rather than overflowing
  a deadline or retrying immediately. A backwards clock cannot increase the configured remaining-age budget.
- Authentication retries require an explicit renewed credential version and allow one immediate retry per version.
  Permanent failures stop; invalid persisted state and negative server delays are rejected.
- Root review added executable regression tests before fixing twelve failures in the initial handoff, including
  overflow, missing renewal evidence, invalid state and retry-age boundaries.
- GREEN: 120 Core tests plus 49 runtime tests passed per modern framework (676 executions).
- Mtpunittestmcp confirmed Core coverage of 216/216 lines and 128/128 branches, and runtime coverage of 139/139 lines
  and 72/72 branches, on each modern framework. All eight library targets build with zero warnings and errors.
- The synchronization engine must persist decisions, respect stored due times after restart, and only invoke retry
  for an operation whose delivery guarantee permits another attempt. That integration remains pending.

### Stage 2a: allowlisted payload serialization

- Added owned payload envelopes, schema registration, explicit contiguous upcasting and a source-generated JSON adapter.
- Payload hashes cover the exact stored UTF-8 bytes. Metadata, schema, type, encoded size and hash validation precede
  deserialization; each upcast output is revalidated. Cancellation is observed after an upcaster returns.
- Registry snapshots isolate a serializer from later registrations and registered JSON metadata is frozen. Reference
  preservation and polymorphic root metadata are rejected. Missing, ambiguous and backwards upcast chains fail explicitly.
- The JSON writer distinguishes its bounded scratch requests from the exact encoded payload limit. Scratch allocation is
  capped at six times the payload limit plus 4 KiB, accounting for JSON escaping and fixed writer requests; committed bytes
  cannot exceed the configured limit. Envelope length is available without allocating a payload copy.
- Root executable RED cases exposed exact-size rejection, missing-hash handling and cancellation after upcasting before
  the fixes. Boundary tests also cover large escaped payloads, immutable byte ownership and extreme schema-version gaps.
- GREEN: 128 Core tests plus 110 runtime tests passed per modern framework (952 executions).
- Mtpunittestmcp confirmed 100% lines and branches: Core 242/242 lines and 128/128 branches; runtime 339/339 lines on
  net8/net9/net10 and 338/338 on net11, with 178/178 branches on each. All eight library targets build without warnings.
- Transport parsing limits, encrypted persistence, quarantine integration and engine projection remain later stages.

### Stage 2b: bounded admission and durable capacity options

- Added an internal FIFO admission queue with count and byte limits, bounded waiting producers, and reserved control
  capacity. Block, reject, eligible oldest/newest drops and custom decisions are supported; custom policies may also block.
- Custom callbacks run outside locks and their decisions are revalidated before commit. Cancellation before admission
  preserves existing work; cancellation after admission cannot replace a committed result. Callback registration and
  registration disposal run outside the queue lock. Cancelling a waiting producer drains eligible successors.
- Data and control producers retain order within their class. Control traffic can use its reserve behind blocked data.
  Byte arithmetic cannot overflow and eviction never invokes payload equality inside the lock. Disposal releases queued data.
- Added finite outbox/inbox count and byte defaults of 10,000 entries and 64 MiB, plus 1,000 blocked outbox publishers.
  These are implementation defaults; all limits must be positive.
- Worker regression tests exposed six queue failures. Root tests then exposed cancellation, disposal and missing custom
  blocking behavior before correction. Disabling the capacity validators caused ten tests to fail.
- GREEN: 142 Core tests plus 161 runtime tests passed per modern framework (1,212 executions).
- Mtpunittestmcp confirmed 100% lines and branches on each target: Core 267/267 lines and 142/142 branches; runtime
  623/623 lines on net8, 617/617 on net9/net10 and 616/616 on net11, with 302/302 branches each. All eight library builds pass.
- Outbox transactions, observer dispatch and engine scheduling must integrate these primitives in subsequent stages.

The implemented identity, configuration, policy, serialization and admission stages are verified. Adapter capability negotiation,
remaining contracts and integrated runtime/durability stages are still incomplete. Passing option validation alone does not establish a
delivery guarantee or establish that a custom policy preserves durable work; the runtime must enforce both.
