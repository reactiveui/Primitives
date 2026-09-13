# OccasionallyConnected implementation

The normative feature specification is [ReactiveUI.Primitives.OccasionallyConnected.md](ReactiveUI.Primitives.OccasionallyConnected.md).
Implementation remains on the local `OccasionallyConnected` feature branch. Nothing is pushed until all v1 work is complete and verified; publication will use one final PR.

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

Stages are integrated through reviewed local commits. Tests precede behavior changes and use TUnit assertions exclusively.
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

## Local consolidation — 2026-09-12

OccasionallyConnected is now the sole local feature branch. All 61 prior CP_* branch heads were checked for ancestry and merged where necessary, then their local branch refs were deleted. Detached worktrees preserve tracked and untracked drafts; none were deleted. The SQLite remote draft subsequently completed independent review as stage 4c. No push or PR is permitted until the complete feature is implemented and verified; the eventual publication is one final PR.

Conflict resolution retained the current Core APIs, SQLite registration, implementation ledger and newer dependency pins. It retained coverage collector18.11.2 and the TUnit cancellation-token CI repair. Old reconciliation branches contributed history without removing newer feature work. The consolidated solution at 4f2873e passed its full Release build with zero warnings and errors. Core319, runtime298 and SQLite82 tests passed on each modern target with matching 100% line and branch coverage. Existing Primitives tests also passed on all four targets.

### Stage 4c: SQLite atomic remote apply component

- Schema version three adds a partitioned inbox and migrates exact version-one and version-two stores transactionally.
  A frozen historical version-two SQL fixture verifies migration independently of current schema construction.
- Applying selected remote events commits inbox identifiers, the projected snapshot and the original batch cursor in
  one transaction. Revision/cursor mismatches and duplicate races reject the entire transaction. Remote application
  preserves local client sequences and original receipts for previously committed local operations.
- Inbox lookups query only candidate identifiers. Retention timestamps record local application time, including events
  received long after their server commit time. Root added an executable regression for this timestamp distinction.
- Root verified 97 TUnit tests on each net8-net11 target with 100% line and branch coverage: 1067 lines on net8,
  1059 on other targets and 290 branches on every target. All eight library targets build without warnings or errors.
  Tests include reopen, historical migration, mixed cursors, rollback, corruption, cancellation and partition isolation.
- This remains an internal component. Leases, retry barriers, retention, encryption, the public asynchronous adapter
  and process-crash conformance remain incomplete. No full adapter capability is advertised.

### Stage 3i: persisted status and synchronization waiting

- Added the engine's persisted operation-status lookup contract and explicit cancellation overload. Root verified
  exact operation-ID forwarding with an executable mutation regression. All 321 Core tests pass on each modern
  target with 874/874 lines and 318/318 branches covered; all eight library targets build without warnings or errors.
- Added public synchronization waits with system or injected clocks. Subscription precedes persisted lookup, covering
  completion during lookup and recovery of an already persisted result. Conflicts remain pending; terminal failures,
  timeouts, cancellation and exhausted status sources finish the wait and release its subscription.
- Timeout does not wait for an adapter cancellation callback or stop durable synchronization. A pending lookup may
  finish independently, with errors observed. Executable regressions exposed premature completion and a timeout
  blocked by an adapter callback before the implementation was corrected.
- Root verified all 321 runtime tests on each modern target with 100% line and branch coverage: 1444 lines on net8,
  1432 on net9/net10, 1425 on net11, and 624 branches throughout. All eight library targets build without warnings
  or errors. Concrete engine integration remains subsequent work.

### Stage 4d: bounded synchronous SQLite worker

- Added an internal FIFO worker with one dedicated execution thread and immediate admission limits covering the
  active command plus queued commands and caller-declared retained bytes. Queued cancellation releases capacity;
  cancellation after a committed receipt does not replace that receipt.
- Concurrent disposal callers share completion while queued work is drained and the active command finishes.
  Root removed a test-only production callback and verified an executable capacity-boundary regression.
- All 105 SQLite tests pass on each modern target, with 1249 lines on net8, 1241 on other targets, and 324 branches
  fully covered. All eight library targets build without warnings or errors. Wiring the worker into the public
  asynchronous adapter remains subsequent work.

### Stage 4e: durable SQLite outbox leases

- Schema version four persists lease identifiers, expiry and original batch membership, with transactional migrations
  from all earlier schemas. A frozen version-three fixture verifies historical compatibility.
- Synchronous acquisition selects one contiguous stream prefix within operation and payload-byte limits before reading
  payloads. Active or oversized heads cannot be bypassed within a stream. Unfiltered selection scans stream heads with
  constant retained memory and cancellation checks. Reclaim touches only selected expired rows.
- Renewal and release validate complete membership; stale owners cannot act on surviving subsets after partial reclaim.
  Root reproduced an early-renewal defect and changed renewal to extend the existing expiry. Corrupted identifiers,
  inconsistent expiry, rejected SQL writes and invalid migrations fail without partial mutation.
- Root verified 130 TUnit tests on each modern target with 100% SQLite line and branch coverage: 1558 lines on net8,
  1547 on net9-net11 and 380 branches throughout. All eight library targets build without warnings or errors.
- The byte limit covers stored payload bytes, not metadata or transport framing. The public adapter, current ownership
  checks at the upload attempt barrier, retry/status transitions, retention, encryption and crash conformance remain
  subsequent work; this component alone does not establish a complete delivery guarantee.

### Stage 3j: shared lifecycle transitions

- Added an internal coordinator with one driver and bounded shared startup/cleanup state. Concurrent callers join the
  active transition; accepted opposite intent survives cancellation of the caller waiting for it. Application callbacks
  run outside the state lock. Disposal prevents restart and joins cleanup.
- Failed startup requires cleanup before retry. Cleanup failures stop automatic progress and permit an explicit retry.
  Shared failures are observed even if every waiting caller cancels. Root made callback-result application and selection
  of the next transition atomic to prevent an overlapping startup request from waiting indefinitely.
- Root verified 338 TUnit tests on each modern target with 100% runtime line and branch coverage: 1600 lines on net8,
  1585 on net9/net10, 1584 on net11 and 722 branches throughout. All eight library targets build without warnings or
  errors. A deliberate stop-intent mutation failed the cancellation regression; restoring the implementation passed.
- This remains an internal component. Concrete context, engine and stream lifecycle integration remain subsequent work.
- A further concurrency regression exercises 64 clients immediately requesting startup again across 64 resource
  lifetimes. Restoring the old gap between callback completion and next-transition selection caused an executable
  timeout. The corrected implementation passes this regression and the full four-target suite with unchanged coverage.

### Stage 8a: bounded authenticated nonce registry

- Added the Server package and its tests to the solution. Its internal registry scopes nonce fingerprints by authenticated
  tenant and client, rejects changed request bytes or timestamps, and admits identical retries without allocating another
  retained record. Canonical request size, key lengths, record count and encoded key/hash/timestamp bytes are bounded.
- Retention lasts through both the configured minimum interval and the full freshness interval of a future timestamp.
  Inclusive expiry boundaries, clock rollback and timestamp overflow preserve replay protection. Expired records reclaim
  capacity; unexpired entries are never evicted to admit another nonce. Application clocks run outside the registry lock.
- Root reproduced premature reuse after freshness expiry but before configured retention ended, then added the retention
  parameter and corrected expiry calculation. A separate mutation placing the clock callback inside the lock failed a
  cross-thread regression. Tests also cover exact byte reclamation, concurrency, scope isolation and caller buffer changes.
- All 23 server TUnit tests pass on each modern target with 100% line and branch coverage: 111 lines on net8, 110 on
  net9-net11 and 54 branches throughout. All eight library targets build without warnings or errors.
- This registry is process-local and stores fingerprints rather than protocol responses. The complete server still needs
  authorization, transactional effect/idempotency storage, response replay and restart protection before any corresponding
  capability can be advertised. The retained byte counter measures encoded records, not exact managed heap consumption.

### Stage 4f: durable SQLite operation state and upload barrier

- Schema version five persists operation status, attempts and retry scheduling, with transactional migration and a frozen
  version-four fixture. Initial local commits persist queued state in the same transaction as their receipt and snapshot.
- The attempt barrier checks complete current lease ownership and samples expiry after acquiring the SQLite writer
  transaction. At-most-once operations with an existing attempt cannot receive permission to send again. Remote results
  must match the original lease identifier and exact operation membership before any status changes are committed.
- Recovery preserves unresolved conflicts, expired guarantees and ambiguous operations while leasing keeps blocked stream
  heads in place. Missing or invalid persisted status fails recovery instead of silently omitting local intent.
- Root reproduced seven database regressions, including ignored state writes granting send permission or returning a local
  receipt, then required successful state persistence before transaction completion. SQLite triggers verify rollback.
- All 166 SQLite TUnit tests pass on each modern target with 100% line and branch coverage: 2008 lines on net8,
  1994 on net9-net11 and 505 branches throughout. All eight library targets build without warnings or errors.
- Public adapter integration, compaction, encryption and crash conformance remain subsequent work. These synchronous
  internal operations require the bounded worker adapter to coordinate admission and drain operations during disposal.

### Stage 4g: transactional SQLite compaction

- Added internal compaction with bounded candidate batches in one SQLite write transaction. Terminal outbox and
  dead-letter records use separate retention windows; unresolved stream history, active leases and the current snapshot
  producer remain protected. Inbox retention uses local receipt timestamps and runs independently of the outbox budget.
- The advisory byte target measures remaining encoded outbox payload and metadata bytes. Reclaimed bytes do not represent
  SQLite file shrinkage. Inbox removal is counted in records and reports zero encoded outbox bytes.
- Root rejected inbox starvation when an inbox-only store already met its byte target. A fresh agent reproduced the
  failure before correcting it. Root reviewed the correction and added multi-batch restart and invalid-budget tests.
- All 181 SQLite TUnit tests pass on each modern target with 100% line and branch coverage: 2241 lines on net8,
  2226 on net9-net11 and 547 branches throughout. All eight library targets build without warnings or errors.
- Public adapter and scheduler integration, encryption and process-crash conformance remain subsequent work.
### Stage 3k: bounded in-memory store reference

- Added an internal process-local store with atomic snapshot/intent and inbox/cursor updates, canonical duplicate receipts,
  contiguous stream leases, retry due times, status lookup and an at-most-once attempt barrier. Upload acknowledgements
  leave receive cursors unchanged. Application clock callbacks execute outside the store gate.
- Admission counts retained records and deterministic encoded data, including identities, metadata, payloads, snapshots,
  inbox keys, leases and retry/status records. Capacity changes are checked before mutation and released on compaction.
  These counters do not measure exact managed heap consumption.
- Root reproduced and corrected false durability advertising, cursor advancement on upload acknowledgements, ignored
  retention, clock callbacks under the gate, retry violations, incorrect compaction targets, and unsupported schemas/types.
  Further tests exercise malformed input, immutable duplicate intent, lease ownership/renewal and stream-scoped recovery.
- All 391 runtime TUnit tests pass on each modern target with 100% line and branch coverage: 2342 lines on net8,
  2312 on net9/net10, 2311 on net11 and 1086 branches throughout. All eight library targets build without warnings or errors.
- The adapter is internal and ephemeral. Its inbox is capacity-bounded but does not yet prune by age. Engine transitions
  for dead letters and expired guarantees, explicit reconciliation, public construction and integration remain tracked work.
  Terminal compaction preserves snapshots and unresolved stream history; this component makes no restart durability claim.
### Stage 3l: in-memory inbox retention

- Inbox entries now retain local receipt timestamps within the encoded byte budget. Compaction prunes expired entries
  independently of the terminal history cutoff and advisory byte target, preserving current snapshots and receive cursors.
- Unresolved stream intent protects its inbox. Stream selection and the exact retention boundary are honored; pruning
  releases record and encoded-byte capacity so subsequent remote commits can be admitted.
- Root first reproduced the full-store regression with no expired entries reclaimed, then implemented receipt-time
  retention. All 393 runtime TUnit tests pass on each modern target with 100% line and branch coverage: 2365 lines on
  net8, 2335 on net9/net10, 2334 on net11 and 1102 branches throughout. All eight library targets build cleanly.
### Stage 3m: explicit durable local commit negotiation

- Added a store capability distinguishing persisted local receipts and snapshots from atomic in-memory updates.
  Durable publishing now requires both atomicity and durable local commits; exactly-once retains its additional inbox
  and remote apply requirements. The in-memory reference continues to advertise only its process-local capabilities.
- Root reproduced negotiation incorrectly accepting the in-memory store with the default durable policy. The regression
  now rejects that configuration and still permits volatile publishing against the same store.
- All 395 runtime TUnit tests pass on each modern target with MTP-confirmed 100% line and branch coverage: 2365 lines on
  net8, 2335 on net9/net10, 2334 on net11 and 1102 branches throughout. All eight library targets build without warnings
  or errors. Core API baselines include the new capability on every target.
### Stage 3n: bounded owned inbox lookup inputs

- Inbox lookups capture caller candidates once before evaluating inbox membership. Caller list callbacks execute outside
  the store gate; lookups retain an atomic view of inbox membership using the owned snapshot under the gate.
- A separate transient query budget uses the configured record and encoded-byte limits to reserve candidate and result
  buffers before allocation. Concurrent captures share this budget; cancellation, validation and caller exceptions release
  reservations. Returned results transfer to callers. The counters measure logical encoded data, not managed heap bytes.
- Root first reproduced incorrect identifiers from repeated caller reads and indexing before oversized-input rejection.
  Additional tests block a real caller indexer while querying independent store state, exhaust count and byte capacity
  concurrently, and verify cancellation/error reclamation, empty input and invalid counts.
- All 401 runtime TUnit tests pass on each modern target with MTP-confirmed 100% line and branch coverage: 2392 lines on
  net8, 2361 on net9/net10, 2360 on net11 and 1112 branches throughout. All eight library targets build cleanly.
### Stage 3o: bounded fair stream scheduling

- Added an internal scheduler with one pending head per stream, per-operation priority, stream weight and bounded aging.
  Smooth weighted credit survives successive heads, while pending updates preserve waiting age. Due or inflight heads
  cannot be bypassed within their stream. Equal scores use current head priority and then registration order.
- Admission bounds stream count and logical metadata bytes, including actual UTF-8 stream identifiers. Completion and
  removal reclaim capacity. Fresh acquisition objects enforce ownership; stale or forged tokens cannot release a head.
  Clock callbacks execute outside the scheduler lock, and concurrent acquisition never returns the same head twice.
- Root reviewed the algorithm and corrected earlier fixed-priority, caller-declared sizing and token-identity proposals.
  Additional tests use a blocked application clock and concurrent scheduler reads, and verify forged-token rejection.
- All 427 runtime TUnit tests pass on each modern target with MTP-confirmed 100% line and branch coverage: 2552 lines on
  net8, 2516 on net9/net10, 2515 on net11 and 1190 branches throughout. All eight library targets build cleanly.
- This component schedules bounded metadata only. Concrete engine integration, transport dispatch and end-to-end fairness
  remain subsequent work; encoded metadata counters do not measure exact managed heap consumption.

### Stage 4h: public bounded SQLite adapter and acknowledged crash receipt

- Added the public SQLite local-store adapter by composing the synchronous transactional backend with its bounded worker.
  Public options configure command count, logical input bytes, retention and the clock. Required schema versions are
  positive minimum requirements; future versions and unsupported authenticated encryption fail before initialization.
- Event lookup reserves bounded capture capacity before copying caller identifiers once outside the capture gate. The
  worker and capture stages have separate finite budgets. Input sizing checks each addition against capacity, including
  payloads, metadata and retry state. Disposal closes both admissions, rejects queued commands and joins active work.
- The adapter advertises atomic local/remote commits, durable local commits and inbox records, and leased outbox support.
  It does not advertise encryption at rest or multi-process coordination. All eight public API baselines are enabled.
- Root strengthened causal capacity tests, verified retry scheduling after reopen, replaced timing-based disposal checks
  with explicit worker entry signals, and tested cancellation and ownership when lease enumeration ends early.
- A real child-process test commits through the public adapter and publishes a signal only after the receipt returns.
  The parent terminates its own child, reopens the database and verifies the operation, snapshot, subscription, sequence
  and original duplicate receipt. Replaying the operation does not apply optimistic state twice.
- All 206 SQLite TUnit tests pass on each modern target with MTP-confirmed 100% line and branch coverage: 2498 lines on
  net8, 2482 on net9/net10, 2483 on net11 and 599 branches throughout. All eight library targets build cleanly.
- This crash test covers an acknowledged receipt followed by process termination. The remaining crash-point matrix,
  disk-full and corruption cases, encryption, process ownership and engine integration remain tracked work. Logical byte
  accounting does not measure exact managed heap consumption or physical SQLite file size.

### Stage 5b: server receive acknowledgement contract

- Added explicit-cancellation and convenience acknowledgement overloads to the server hub contract. Successful completion
  means an authorized acknowledgement was persisted or an identical duplicate was already persisted; failures propagate.
- Root reviewed the API and strengthened tests for deferred completion, immediate and deferred failure, cancellation
  identity, exact arguments and single invocation. The extension preserves the hub's asynchronous operation.
- All 325 Core TUnit tests pass on each modern target with MTP-confirmed 100% line and branch coverage: 875 lines and
  318 branches throughout. All eight Core library targets build with zero warnings and errors.
- This stage defines the server boundary. Durable server acknowledgement storage and transport integration remain work.

### Stage 5c: deterministic server write ordering

- Added internal server write stamps and last-writer-wins ordering by server commit instant, ordinal authenticated
  client identity, then operation identity. Equal stamps do not replace one another; time zone offsets do not alter order.
- A failing test first demonstrated incorrect timestamp precedence. Tests now cover both comparison directions, each
  tie-breaker, equivalent instants, and convergence across all arrival permutations of competing writes.
- All 28 Server TUnit tests pass on each modern target with MTP-confirmed 100% line and branch coverage: 116 lines on
  net8, 115 on net9/net10/net11 and 58 branches throughout. All eight Server library targets build cleanly.
- This is an internal ordering primitive. The concrete resolver, authenticated stamp creation and atomic server commit
  integration remain subsequent work; the primitive itself does not authenticate identities or persist timestamps.

### Stage 5d: canonical operation fingerprints

- Added an internal, versioned SHA-256 encoding of authenticated tenant/client scope and complete operation intent,
  including actual payload bytes, all policy fields and ordinally sorted metadata. Diagnostic timestamps are excluded.
- Canonical UTF-8 bytes are counted before payload copies and hash staging; malformed text and oversized operations fail.
  This is a per-operation bound, not a global admission or durability guarantee.
- Root reviewed production encoding and strengthened the independent test encoder with platform binary primitives and
  a sequence using every 64-bit byte. Tests cover fixed vectors, Unicode chunk boundaries, exact size limits, identity
  separation, changed payload with unchanged claimed hash and policy changes.
- All 42 Server TUnit tests pass in Release on net8/net9/net10/net11. MTP confirms 100% matching Server line and branch
  coverage (245 lines on net8, 244 on the other targets, 80 branches). All eight library targets build without warnings
  or errors. Journal integration and durable duplicate-response replay remain subsequent work.

### Stage 4i: default SQLite writer ownership

- The public adapter captures its full database path at construction and acquires an exclusive sidecar handle on its
  worker before initialization. New-owner initialization failure releases the handle; failed reinitialization retains
  an existing owner. Disposal drains admitted work and captures before releasing ownership.
- Tests exercise competing adapters, a live competing child process, kill/reopen, failed backend initialization while
  the failed adapter stays alive, and relative-path capture in an isolated child process. Known network and reparse paths
  are rejected; unsupported aliasing deployments are documented without advertising multi-process coordination.
- Root added a failing regression for redirected sidecars, then applied the same reparse validation to the sidecar.
  Root also removed thread-pool continuation dependence from the existing writer-wait cancellation test after a loaded
  run demonstrated its cancellation could arrive after the bounded timeout. Production timeout behavior is unchanged.
- All 220 SQLite TUnit tests pass on net8/net9/net10/net11 in Release, with MTP-confirmed 100% matching package line and
  branch coverage (2564/2548/2548/2549 lines and 623 branches). All eight library targets build without warnings or errors.
- Encryption, durable capacity enforcement, the complete crash-point matrix and engine integration remain work.

### Stage 6a: bounded HTTP retry hints

- Added the HTTP transport project and an internal Retry-After parser for a single bounded header value. It accepts
  platform-representable delay-seconds and HTTP dates against a caller-sampled instant; past dates produce zero.
  Invalid, multiple, oversized and out-of-range values remain absent hints. The raw limit is 128 characters and
  delay-seconds use the platform parser's Int32 range; this helper does not schedule or classify retries.
- Root removed redundant parsing and strengthened tests with valid values exactly at and beyond the length boundary,
  newline injection, multiple date values and equivalent observed instants with different offsets.
- All 18 HTTP TUnit tests pass in Release on net8/net9/net10/net11. MTP confirms 100% matching package line and branch
  coverage (11 lines and 12 branches). All eight library targets build without warnings or errors.
- The authenticated HTTP adapter, bounded response decoding and receive protocol remain subsequent work.

### Stage 5e: atomic process-local server commit journal

- Added a bounded internal journal that atomically records canonical state, terminal per-operation replay results,
  conflicts, events, cursor and revision. Tenant/stream/client keys isolate replay; stale revisions and duplicate races
  reject the entire prepared plan before effects become visible. It does not advertise durable server idempotency.
- Prepared collections are owned and count-bounded. Retained logical bytes include payloads, response metadata, write
  stamps and the final cursor even after event rows expire. Per-call capture bounds do not claim global admission limits.
- Root review required fixes for integer widening, capacity arithmetic and retained cursor accounting. Executed failing
  regressions demonstrated overflow and undercounting before correction. Root added a blocked clock/compaction test
  proving callbacks do not hold the gate and a later retention watermark governs the eventual commit.
- All 81 Server TUnit tests pass in Release on net8/net9/net10/net11 with MTP-confirmed 100% matching package line and
  branch coverage (747/743/743/743 lines and 300 branches). All eight library targets build without warnings or errors.
- Authorization, concrete server effects/resolvers, durable journal storage, receive/ACK integration and global admission
  remain subsequent work. This journal is explicitly process-local.

### Stage 5f: client-scoped remote event origin

- Added immutable origin correlation containing the client identity and operation identifier. Optional event origins
  must match the existing causal operation, and the server journal rejects origins belonging to another client.
- Identity validation checks the length bound before UTF-8 validation and preserves ordinal Unicode identity. Journal
  byte accounting includes origin text. This data does not authenticate a caller; trusted server and transport code
  must establish its provenance.
- Root reviewed validation ordering, equality isolation and boundary tests, then independently ran all 334 Core and
  83 Server tests on net8/net9/net10/net11. MTP confirms 100% matching line and branch coverage: Core 891 lines and
  328 branches; Server 752/748/748/748 lines and 306 branches. All eight library targets build without warnings or errors.
- Client reconciliation and durable origin transport/persistence remain subsequent work.

### Stage 3: durable commit capability enforcement

- The local committer now requires both atomic local commit and durable local commit capabilities before accepting a
  durable operation. A real in-memory adapter regression failed before this fix because it returned a durable receipt.
- Tests verify rejection leaves the pending queue, snapshot, sequence and visible state unchanged, and exercise each
  incomplete capability combination before a store mutation can run.
- All 431 runtime TUnit tests pass in Release on net8/net9/net10/net11, with MTP-confirmed 100% matching package line
  and branch coverage (2553/2517/2517/2516 lines and 1192 branches). All eight library targets build without warnings
  or errors. Volatile publishing and full engine integration remain subsequent work.

### Stage 3: bounded FIFO batch selection

- Added a pure internal planner for the next ordered batch prefix. It applies the smaller local and negotiated count
  and byte ceilings, includes caller-supplied encoded envelope costs, and uses subtraction to avoid overflow.
- Partial batches wait for the caller-sampled monotonic dwell deadline. Full batches and prefixes blocked by the next
  item's size flush immediately. An oversized head is reported without skipping it. Only a bounded prefix is inspected;
  transport encoding, queue ownership, timers and in-flight coordination remain engine/transport responsibilities.
- Root completed the draft after the initial agent stopped on analyzer errors. Five executed tests failed against the
  compiling stub, then passed after implementation. Root added limit, sequence-gap and maximum-integer tests.
- All 454 runtime TUnit tests pass in Release on net8/net9/net10/net11 with MTP-confirmed 100% matching package line
  and branch coverage (2587/2551/2551/2550 lines and 1220 branches). All eight library targets build without warnings
  or errors. The planner is ready for engine integration; it does not claim an implemented upload pipeline.

### Stage 4: authoritative snapshots and client identity binding

- Local snapshots now retain separate authoritative and optimistic payloads under the same revision and cursor.
  An omitted authoritative mutation preserves the existing value; a recovered null remains unknown. Original duplicate
  operation intent includes authoritative mutation presence and content, independently of the current snapshot.
- SQLite schema six adds transactional sidecars and migrates historical schemas without inventing authoritative state.
  Tests use an independent schema-five fixture and verify pending operations, inbox entries and leases survive migration.
  A rejected first client binding on historical pending work rolls back the migration before a legacy reopen succeeds.
- Both stores support ordinal client identity binding. A first binding requires a pristine partition; reopening an
  existing binding with another client or without its identity fails. Empty subscription mappings remain compatible.
  Root added an executed failing regression for malformed SQLite binding values before fixing their interpretation.
- Logical admission includes authoritative payloads and client identities. Root verified an executed failing public
  SQLite regression for a large Unicode event origin, then integrated its byte-accounting fix.
- Root independently reviewed and corrected the implementation, retained existing compaction timestamp validation,
  and removed remaining null-forgiving fixtures using public reflection or typed delegates without suppressions.
- Release TUnit suites pass on net8/net9/net10/net11: Core 335, runtime 466, SQLite 244 tests per target. MTP confirms
  100% matching package line and branch coverage: Core 891 lines/328 branches; runtime 2644/2608/2608/2607 lines and
  1258 branches; SQLite 2820/2804/2804/2805 lines and 697 branches. All eight affected library targets build with
  zero warnings and errors. These component checks do not establish complete client reconciliation or synchronization.

### Stage 3: complete remote operation groups

- Added copied operation-completion declarations to remote batches without changing their existing constructor.
  Each declaration names an exact client operation and all of its event IDs; an empty declaration represents an
  accepted operation with no emitted events. These records do not authenticate the server or order opaque cursors.
- The validator bounds events, completions and total declared IDs before allocating lookup structures. It rejects
  duplicate origins/events, missing or foreign IDs, mixed streams, and partially declared operation effects.
  Legacy events without an origin are allowed but cannot establish local operation inclusion.
- Root took over after the agent made no source changes. An executed regression failed against the initial validator
  because a two-event operation could declare only its first event complete. The corrected implementation passes
  this case, exact limits, ordinal/canonical-Unicode identity separation, null boundaries and owned-collection tests.
- All 369 Core TUnit tests pass in Release on net8/net9/net10/net11. MTP confirms 100% matching line and branch coverage
  (935 lines, 372 branches) on each target. All eight Core library targets build with zero warnings and errors.
- Store inclusion, receive paging and committer integration remain required; the DTO and validator do not implement
  reconciliation or authenticate a completion declaration by themselves.

### Stage 3: atomic volatile publishing

- The local committer now accepts explicitly volatile policies when the store advertises atomic local commit.
  Durable policies still require both atomic and durable local commit; volatile publishing never grants a durable
  capability or silently changes the persisted operation policy.
- Root added an executed failing public in-memory adapter regression before changing the capability gate. Tests also
  cover volatile publishing through a durable store and rejection by stores that lack atomic commits.
- All 469 runtime TUnit tests pass in Release on net8/net9/net10/net11, with MTP-confirmed 100% matching package line
  and branch coverage (2646/2610/2610/2609 lines, 1258 branches). All eight runtime library targets build with zero
  warnings and errors. Observer admission and the complete publish/receive pipeline remain subsequent work.

### Stage 5f: durable SQLite server commit journal

- Added an independent schema-one SQLite journal for atomic server state, event rows and complete terminal operation
  replay. Competing instances use transactional revision checks; duplicate requests preserve the original receipt.
- Retention keeps the durable clock and event-sequence high-water marks. Reopening with smaller configured limits
  rejects oversized retained history before reconstructing replay payloads, including growth by another instance.
- Durable reads validate raw SQLite storage classes rather than accepting provider coercions. Real database tests cover
  corrupted rows, competing commits, trigger-induced rollback, tenant/client isolation and zero-event acceptance.
- Process termination tests verify acknowledged journal writes survive reopening and an uncommitted raw SQLite
  transaction does not. The full application crash matrix remains a later integration gate.
- Root review added two executed failing capacity regressions, then verified all 113 Server TUnit tests on
  net8/net9/net10/net11. MTP reports 100% matching package line and branch coverage (1551/1547/1547/1547 lines,
  474 branches); all eight Server library targets build in Release with zero warnings or errors.
- This internal journal does not authorize callers, implement the server hub or advertise end-to-end guarantees.
### Stage 4: atomic upload-result reconciliation

- Added a bounded store transaction that commits complete lease results, lease release and optimistic snapshot
  replacement together. Snapshot revisions fence competing work; authoritative payloads and receive cursors remain
  unchanged. Rejections contradicting prior authoritative inclusion fail atomically.
- The stream committer rebuilds from an isolated authoritative checkpoint and replays surviving operations in client
  sequence order. Accepted operations remain replay-visible until receive inclusion; rejected edits disappear without
  attempting to invert application mutations. Malformed store receipts poison the committer before further writes.
- Both local stores implement this transaction. Status-only rejection fails when an authoritative snapshot requires
  reconciliation. SQLite captures caller collections within finite limits and prepares the receipt before commit.
- Application-style tests close and reopen SQLite before and after a mixed accept/reject response and verify replacement
  state, revision, durable operation status and replay membership. Additional cases cover mutable projections, retryable
  work, later echoes, cancellation timing, stale leases, failed transactions and invalid receipts.
- Root independently reviewed the agents' code, extracted shared payload comparison, removed an unreachable nullable
  branch and added missing receipt tests. A merged coverage report was insufficient; each framework was checked separately.
- Integrated Release suites pass on net8/net9/net10/net11: Core 371, runtime 550 and SQLite 295 tests each. MTP confirms
  100% matching line and branch coverage: Core 938 lines/372 branches; runtime 2911/2874/2874/2868 lines/1420 branches;
  SQLite 3073/3057/3057/3059 lines/775 branches. All eight affected library targets build without warnings or errors.
- Engine orchestration, terminal local failure handling and complete application integration remain subsequent work.
### Stage 3: bounded loopback reference transport

- Added a public loopback transport that forwards an explicitly bound client identity to a caller-owned server hub.
  Request and subscription admission is finite; acknowledgement capacity remains available when data slots are full.
- Receive validation binds each batch to the requested stream and checks cursor continuity before exposing the same
  validated batch instance. Redelivery at the current cursor remains a duplicate candidate for durable inbox validation;
  it cannot rewind the transport cursor or bypass the store's checks for previously applied effects.
- Subscription shutdown cancels active work, disposes paused upstream enumerators and releases admission even when a
  cancellation callback or upstream disposal fails. Terminal moves close their admission lane before cleanup starts.
- Root independently reviewed the hardening draft and added an executed lost-ACK redelivery regression before fixing
  cursor admission. Continuous and resumed subscriptions both preserve the next valid cursor chain.
- All 635 runtime TUnit tests pass in Release on net8/net9/net10/net11. MTP confirms 100% matching line and branch
  coverage (3412/3368/3368/3360 lines and 1634/1634/1634/1638 branches). All eight runtime targets build cleanly.
- The adapter does not own the supplied hub or implement its durable authorization, subscription or acknowledgement store.