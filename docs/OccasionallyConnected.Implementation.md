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

### Stage 1d: context, retention, security and diagnostics configuration

- Added immutable context configuration with required nested option records and a complete shared default instance.
  Startup is explicit by default, with four concurrent streams, three conflict-resolution rounds and priorities -10 to 10.
- Retention defaults are one day for terminal outbox records, seven days for inbox deduplication, server idempotency and
  snapshots, thirty days for dead letters, and hourly compaction. Retention never authorizes deleting required rebuild data.
- Security limits default to 1 MiB payloads, 2 MiB messages and 4 MiB decompressed messages, with bounded metadata and
  JSON depth. Nonce retention covers the configured replay window. Runtime adapters must enforce these validated limits.
- Diagnostics default to no identifiers, 10% activity sampling, an 80% high-water mark, and both 256-fault and 64 KiB queue
  bounds. Hashed identifiers require explicit opt-in. Authenticated encryption at rest can be required explicitly at startup.
- Root review rejected the initial analyzer-failing draft, verified the repaired source, and independently changed the
  high-water comparison to accept 100%; the boundary test failed before the correct implementation was restored.
- GREEN: 182 Core TUnit tests passed on each modern framework (728 executions). Mtpunittestmcp confirmed 388/388 lines
  and 190/190 branches on each target. All eight Core library targets build with zero warnings and errors.
- Context startup, encrypted storage, replay-cache enforcement and bounded diagnostic emission remain runtime work.

### Stage 2c: protocol, transaction and typed projection contracts

- Added immutable operation, batch, remote-event, recovery, lease, status and conflict models. Caller-owned collections
  are copied so later list or metadata changes cannot alter a prepared operation or canonical conflict decision.
- Defined effective persisted operation policy, lease-owned attempt barriers, restart-visible terminal/retry lookups and
  optimistic snapshot revisions. Store contracts specify atomic rejection of stale revisions and invalid batch results.
  Late cancellation must return an already committed local result; it must not make durable work appear rolled back.
- Added a batch-result validator for correlation, exact membership, valid operation fields, one stream, configured priority
  limits and increasing client sequences. Gaps from terminal operations remain valid. Negative retry hints are rejected.
- Added typed projection and conflict resolver contracts. `ApplyRemote` receives validated, upcast `TInput` alongside event
  metadata. Reducers are deterministic and pure; returned state becomes observable only after a successful transaction.
- Explicit cancellation-free overloads forward through extension methods, preserving arguments, results and failures.
- Root behavioral RED tests exposed descending-sequence and negative-retry acceptance. Four conflict ownership/null-input
  tests failed against the initial constructors before defensive copying was implemented. Type-specific test repairs were
  reviewed rather than accepting analyzer-failing drafts or relying on coverage percentages alone.
- GREEN: 257 Core TUnit tests passed on each modern framework (1,028 executions). Mtpunittestmcp confirmed 772/772 lines
  and 274/274 branches on each target. All eight Core library targets build with zero warnings and errors.
- These contracts do not implement durable storage, network synchronization or server effects; their implementations and
  capability conformance tests remain subsequent stages. Diagnostic classification and emission are separate slices.

### Stage 2d: capability negotiation

- Added internal startup negotiation that intersects authenticated peer offers with transport support and known runtime
  features. It selects protocol 1.0, rejects incompatible major versions, and intersects local and peer count/byte limits.
  Without batch support, the operation limit is one; unknown optional feature bits are never advertised by the runtime.
- Durable publishing requires atomic local commit. At-least-once publishing requires server idempotency. Exactly-once
  additionally requires atomic remote apply, durable inbox, atomic server effect/acknowledgement and receive acknowledgements.
- Cursor recovery, concurrent draining, multiple writer processes and required encryption each validate the corresponding
  capabilities. A peer-required inbox retention window needs both sufficient configured retention and a durable inbox.
- Exposed the effective exactly-once window as the shorter client inbox/server idempotency retention. Other delivery
  guarantees clear this field, including an untrusted peer claim. Missing or invalid required retention fails negotiation.
- Executable TDD produced 35 failures against the initial stub. A further failing regression exposed a peer inbox-retention
  promise without durable inbox storage; the corrected validator rejects it before synchronization.
- GREEN: 257 Core and 198 runtime tests passed on each modern framework (1,820 executions). Mtpunittestmcp confirmed
  Core 772/772 lines and 274/274 branches; runtime 699/699 lines on net8, 693/693 on net9/net10, and 692/692 on net11,
  with 348/348 branches on each. All eight Core and runtime library targets build without warnings or errors.
- Adapter conformance, authenticated handshakes, context startup integration and runtime guarantee-expiry enforcement
  remain subsequent work. Negotiation validates declared capabilities; it does not itself implement those guarantees.

### Stage 2e: isolated observer notifications

- Added an internal dispatcher with independent subscription queues bounded by item count and estimated bytes. The owning
  stream lane supplies publication order; each subscription drains serially on an asynchronous scheduler outside queue locks.
- Latest-state overflow can coalesce to the newest state. Event overflow always disconnects with a typed overflow error,
  even when the subscription permits state coalescing. Accepted data precedes completion/error notification.
- Disposal clears queued data and pending terminal notifications. A callback already claimed by a drain may start or return
  after disposal; queued notifications are not claimed afterward. Observer failures disconnect only the affected subscriber.
- Scheduler rejection clears the affected subscription immediately and returns a typed result without invoking observers,
  diagnostic reporters or trace listeners on the publisher stack. Reporter failures set a fixed-size health flag and cannot
  escape through a secondary diagnostic callback. Thread-pool queue rejection is handled explicitly.
- Agent RED regressions exposed disposal-after-terminal, event coalescing and throwing diagnostic listener failures.
  Root review removed a nullable suppression and ineffective exception observation code; an executable health-flag regression
  failed before the flag was implemented. Tests were reorganized under the production types they exercise.
- GREEN: all 226 runtime TUnit tests passed on each modern framework (904 executions). Mtpunittestmcp confirmed 966/966
  lines on net8, 955/955 on net9/net10 and 954/954 on net11, with 434/434 branches on every target. All eight runtime library
  targets build with zero warnings/errors. The unchanged Core package retains its independently verified 100% gate.
- Integration with the stream lane, source bridges and bounded engine diagnostic emitter remains subsequent work.

### Stage 2f: typed diagnostic classification

- Completed the fault model with category, severity and transient-status fields. Categories distinguish configuration,
  storage, serialization, transport, authentication, authorization, protocol, capacity, conflict, observer and invariant faults.
  Severity ranges from information through critical. Normal offline state alone is not a fault.
- Existing constructor calls remain valid and default to a non-transient internal-invariant error; emitters set explicit
  classification through immutable init properties. Optional stream/operation identities and local exceptions are retained.
- Added validation for stable nonblank codes, non-null messages, defined classifications and non-default optional identities.
  This is structural validation; diagnostic queue bounds, privacy filtering, metrics and emission remain runtime work.
- Seven executable negative cases failed before classification/identity validation was implemented. All 268 Core TUnit
  tests then passed on each modern framework (1,072 executions). Mtpunittestmcp confirmed 794/794 lines and 290/290
  branches on each target; all eight Core library targets build with zero warnings and errors.

### Stage 2g: typed stream definitions

- Added immutable typed stream definitions with required stream identity, projection and input/state contract identifiers.
  Input schema, state schema and snapshot format versions default to one and must remain positive.
- Validates optional subscription, publication and observer-input settings together. Nested stream identities must match;
  explicitly supplied subscription identities must agree. Custom-policy support and configured publication priority bounds
  flow into nested validation. Wire contract identifiers are validated without normalization.
- Root review expanded tests for durable subscription identity placement, isolated custom policies, null contracts,
  invalid priority ranges and forbidden blocking observer bridges. Inverting the stream-identity comparison produced two
  executable failures before restoration, confirming the tests detect cross-stream configuration errors.
- GREEN: all 289 Core TUnit tests passed on each modern framework (1,156 executions). Mtpunittestmcp confirmed
  846/846 lines and 314/314 branches on each target. All eight Core library targets build with zero warnings/errors.
- Durable default subscription identity resolution and context caching/startup remain runtime integration work.

### Stage 3a: atomic local commit kernel

- Added an internal per-stream transaction kernel that requires successful recovery and rejects overlapping asynchronous
  calls immediately. It holds no state lock across serialization, projection or store calls; a later bounded lane will own it.
- Serializes and decodes input before projection so mutable caller input cannot diverge from the persisted operation.
  Commits the operation, next sequence and optimistic snapshot through the atomic store contract with an expected revision.
  Visible state advances only after a valid receipt; cancellation after durable commit still returns that receipt.
- Recovery validates identity, cursor, counters and snapshot contract/format, and permits older payload schemas through
  the serializer's upcast path. Pending operations already represented in the snapshot are not projected a second time.
  Failed recovery clears readiness; malformed or null commit receipts poison the kernel against further use.
- Executable agent regressions exposed missing snapshot payload validation and stale readiness after failed recovery.
  Root review replaced unfinished timed concurrency tests with explicitly released/awaited gates and added commit/restart
  and stale-writer recovery tests using an adapter test double that enforces sequence/revision checks.
- GREEN: all 263 runtime TUnit tests pass on each modern framework (1,052 executions). Mtpunittestmcp confirms
  1208/1208 lines on net8 and 1196/1196 on net9/net10/net11, with 510/510 branches on each. All eight runtime
  library targets build with zero warnings/errors. The unchanged Core package retains its independent 100% gate.
- This stage implements the transaction orchestration kernel. Concrete durable-store conformance, bounded ordered admission,
  context lifecycle, remote apply and notification integration remain subsequent work.

### Stage 3b: durable subscription identity contract

- Added store lookup/creation of a stable subscription identifier scoped to the initialized store and stream. Existing
  identifiers survive omitted preferences; explicit mismatches fail without changing the mapping. Concurrent calls use
  the first committed mapping, and cancellation cannot delete an existing or concurrently committed identity.
- Added the explicit no-cancellation overload and tests for exact argument/result forwarding, omitted preferences and
  adapter failure propagation. Root mutation testing replaced a supplied preference with null and observed an executable
  failure before restoring the implementation. The local-commit test double explicitly rejects this unused adapter member.
- All 291 Core tests and 263 runtime tests pass on each modern framework. Mtpunittestmcp confirms Core coverage of
  847/847 lines and 314/314 branches on each target; runtime coverage remains 100% for lines and branches.
  All eight Core library targets build with zero warnings and errors.
- This defines the adapter contract. Persistent mapping implementations, concurrent store conformance and engine startup
  integration remain subsequent work; forwarding tests do not establish durable storage behavior.

### Stage 3c: typed capacity failures

- Added `QueueCapacityExceededException` for rejection before persistence. An explicit `CanFitWhenEmpty` hint distinguishes
  an operation that might fit after draining from one that cannot fit the configured empty queue. Standard exception
  constructors default to no automatic wait; the hint does not guarantee future admission.
- Preserves messages and wrapped causes, rejects null messages, and includes the standard exception constructors required
  by repository analyzers. Root review added the wrapped null-message regression and checked standard constructor messages.
- Inverting the capacity hint produced three executable TUnit failures before restoration. All 296 Core tests pass on
  each modern framework; Mtpunittestmcp confirms 856/856 lines and 318/318 branches on every target.
  All eight Core library targets build with zero warnings and errors.
- Queue/store use of this failure and the bounded wait-for-capacity path remain subsequent integration work.

### Stage 3d: server hub convenience overloads

- Added the no-cancellation overloads for applying operations and subscribing to remote batches. Both forward the exact
  supplied arguments with `CancellationToken.None`, preserving results, enumerable identity and unwrapped failures.
- Four TUnit tests exercise forwarding, ordered nonempty enumeration and failures during invocation or enumeration.
  Root mutation testing substituted a copied client identity and observed a failing reference assertion before restoration.
- All 300 Core tests pass on each modern framework. Mtpunittestmcp confirms 858/858 lines and 318/318 branches on each
  target. All eight Core library targets build with zero warnings and errors. These overloads add no server implementation.

### Stage 3e: atomic remote receive kernel

- Added remote batch application to the same exclusive transaction owner as local commits and recovery. Validates stream,
  event identities, bounded Unicode cursors and input contracts before taking one owned inbox lookup snapshot. Only new
  events are decoded and projected, preserving their received order.
- Persists the filtered inbox batch, cursor and projected snapshot together using the expected revision. Visible state
  changes only after the adapter returns an exact valid receipt. Malformed receipts or lookup results poison that instance;
  cancellation after a successful commit still returns the committed result.
- Old duplicate replays cannot roll back the cursor; duplicate-only batches continuing the current cursor still commit
  cursor advancement. Empty batches must preserve cursor continuity. A stale writer must recover before retrying.
- Root review added tests for each receipt invariant, failed storage followed by retry and restart, overlapping local and
  remote transactions, cancellation after serialization and repeated inbox lookup identifiers. Inverting deduplication
  caused 11 executable failures before restoration. All 298 runtime tests pass on each modern target, with 100% matching
  line and branch coverage (602 branches per target). All eight runtime library targets build without warnings or errors.
- Transport acknowledgements, bounded receive admission and observer publication remain subsequent integration work.

### Stage 4a: SQLite subscription identity persistence

- Added the SQLite library and test projects with an internal file-backed identity component. A serializable transaction
  persists the first subscription assigned to each store partition and stream. Reopening reuses that mapping, competing
  explicit identities reject the loser, and independent streams can share an explicitly chosen subscription identifier.
- Validates schema ownership and definitions before changing journal mode, verifies WAL and FULL synchronous settings,
  uses parameterized SQL and closes each connection. Rejects unsupported encryption requirements before plaintext writes,
  malformed stored identities, invalid initialization and attempts to switch an initialized instance's partition.
- Real file tests cover close/reopen, concurrent instances, malformed schemas, cancellation behind a database writer and
  lifecycle failures. Root restored the invalid cross-stream uniqueness constraint and observed an executable regression
  before restoring the implementation. All 37 tests pass on each modern target with 100% matching line and branch coverage
  (78 branches per target). All eight library targets build cleanly and appear in the generated package.
- Uses Microsoft.Data.Sqlite 10.0.12 with SQLitePCLRaw.bundle_e_sqlite3 2.1.13 to obtain the corrected native-asset packaging
  described in [SQLitePCLRaw #678](https://github.com/ericsink/SQLitePCL.raw/issues/678). API tracking and runtime dependency
  assets remain enabled. Root verified separate coverage reports for each target and the packed dependency groups.
- This is the identity persistence component, not the complete local store adapter. Outbox/inbox transactions, leases,
  compaction, encryption, migrations beyond schema v1 and process-crash conformance remain subsequent work.

### Stage 3f: typed remote facade contracts

- Added decoded remote messages, subscription and publishing interfaces, and explicit no-cancellation/default-input
  extension overloads. Publication follows the chosen durability policy; subscribers receive locally committed messages.
- TUnit tests verify message metadata, subscription forwarding, exact argument/result identity, ordered delivery and
  unwrapped failures. These contract fakes do not establish runtime durability or bounded admission conformance.
- Root review added bridge failure and metadata assertions. Replacing omitted input options with a new instance produced
  an executable failure before restoration. All 308 Core tests pass on each modern framework with 100% matching coverage
  (866 lines and 318 branches per target). All eight Core library targets build without warnings or errors.
- Concrete remote facades and their transaction, queue and lifecycle integration remain subsequent work.
### Stage 3g: local-first stream facade contracts

- Added the two-type local-first stream interface, its single-type alias and explicit publish/start/stop convenience
  overloads. The contracts describe committed local replay, remote delivery and producer-local input notifications.
- Tests verify separate state/input types, reference input and receipt identity, exact options and cancellation forwarding,
  lifecycle calls and unwrapped failures. These contract fakes do not establish concrete stream runtime behavior.
- Root replaced cancellation forwarding with `CancellationToken.None` and observed an executable regression before
  restoration. All 315 Core tests pass on each modern framework; Mtpunittestmcp confirms 871/871 lines and 318/318 branches
  on every target. All eight Core library targets build with zero warnings and errors.
- Concrete stream startup, shutdown, bounded publication and transaction integration remain subsequent work.

### Stage 3h: context facade contract

- Added the context interface connecting its synchronization engine, lifecycle state stream and typed stream factory.
  Parameterless lifecycle extensions preserve the underlying asynchronous operation and use no cancellation token.
- Tests exercise the interface surface, exact lifecycle call counts, incomplete operations and unwrapped failures.
  Root redirected startup to shutdown and observed three executable failures before restoring the implementation.
- All 319 Core tests pass on each modern framework with 873/873 lines and 318/318 branches covered. All eight Core
  library targets build with zero warnings and errors. These interface tests do not establish concrete context behavior.
- Context ownership, compatible stream caching and integrated startup/shutdown remain subsequent implementation work.

### Stage 4b: SQLite atomic local commit component

- Added an internal SQLite transaction component that commits the outbox operation, optimistic snapshot and next client
  sequence together. Exact repeated operation IDs return the original receipt; changed intent is rejected using a
  canonical fingerprint that remains valid after later snapshots replace the original state.
- Shared schema validation supports transactional identity-v1 migration to local-commit-v2. Recovery checks snapshot
  cursors, pending sequences and subscription identities, and fails closed on inconsistent persisted data. Counter
  overflow is rejected before writes. Store partitions scope operation IDs and all related rows.
- Writer waits are finite and cancellation-aware. Ownership is checked before persistent durability settings change,
  and every operational connection applies foreign-key enforcement and FULL synchronous writes. Application clocks run
  outside the storage gate. Required encryption still fails before plaintext creation.
- Real SQLite tests cover reopening, competing writers, trigger-induced rollback, migration, corruption and duplicate
  intent. Six executable regressions exposed missing recovery/overflow checks before root fixes. Root replaced a
  helper-only settings assertion with SQL probes from actual write connections; forcing unsafe settings failed the test.
- All 82 tests pass on each modern framework. Mtpunittestmcp confirms 898/898 lines on net8 and 892/892 on net9-net11,
  with 244/244 branches on every target. All eight library targets build with zero warnings and errors.
- This remains an internal local commit component. Remote inbox transactions, leases, retention, encryption, the bounded
  asynchronous adapter worker and process-crash conformance are still pending; no complete adapter capability is advertised.

The implemented identity, configuration, policy, serialization, admission, protocol, negotiation, observer, fault-model, stream-definition and transaction-kernel stages are verified. Adapter conformance,
remaining facade contracts and integrated runtime/durability stages are still incomplete. Passing option validation alone does not establish a
delivery guarantee or establish that a custom policy preserves durable work; the runtime must enforce both.
