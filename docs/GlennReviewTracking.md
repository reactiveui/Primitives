# Glenn review tracking

## Audit snapshot

Reviewed and tested on **4 October 2026** against PR [#229](https://github.com/reactiveui/Primitives/pull/229).
The PR and local HEAD were `9a65c2dde097cd4bee446dafd09dc710ac347ae1`.
This audit also includes the current **uncommitted working tree**.
Those changes are not yet evidence of what the remote PR contains.

The numbered tasks come from the supplied 33-item list. Its source is
[Glenn's general review](https://github.com/reactiveui/Primitives/pull/229#pullrequestreview-5375687233),
submitted on 1 October against `f625d79c2225938acd330463e02fefcdda81bf9d`.
The original [release membership thread](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826255)
is also part of the acceptance criteria.

This tracker records source, workflow and test evidence. The verification section distinguishes fresh local runs
from checks on the committed PR head. Follow-up implementation closes the server connection reuse gap.
Local capacity accounting and selected-key metadata reads now have passing regression and measurement evidence.
Final conformance and stable/prerelease package checks pass. No review threads or PR settings were changed.

## Status summary

Items **1–15 are complete for the support scope stated below**.
Their remedies have source, regression and workflow evidence. Items 11–15 rely heavily on uncommitted work.
Completion does not mean reviewer approval, a published release, real-device certification or green remote CI.

Status meanings:

- **Complete:** the requested remedy is verified for its stated support scope. Release execution and device proof remain distinct.
- **Verified in working tree:** relevant fresh tests pass for the stated support scope. The changes are not yet on the PR head.
- **Partial:** useful changes are present, but a concrete part of the requested remedy remains.
- **Open:** keep the task in the backlog. This audit does not certify its completion.

At inspection, the current PR head had passing stable/prerelease package gates, both native Mobile host legs,
the complete native package check, AOT, all host builds, Sonar and the crash matrix.
Windows .NET 8 coverage had failed. All other coverage matrix legs had passed.
This is a dated snapshot, not a claim that CI is green. See the [live checks](https://github.com/reactiveui/Primitives/pull/229/checks).
These checks do not verify uncommitted changes.

## Items 1–15

| Item | Status | Result and support limits |
| --- | --- | --- |
| 1. Release solution membership | Verified | Core, runtime, Server and Storage.Sqlite are in the full release filter. The caller and shared release workflow pack that filter. Both local version-policy gates verify the complete feature dependency graph. No release was published. |
| 2. Stable-version packaging | Complete | Stable packages omit .NET 11 preview assets and dependency groups. Prerelease packages retain them. Both fresh package gates pass. |
| 3. CRDT convergence | Complete | Equal or missing stamps use deterministic value ordering. Configurable element bounds no longer conflict with a hidden 4,096-element ceiling. |
| 4. CRDT tombstone reclamation | Complete, conditional contract | Causal checkpoints remove covered tombstones without accepting stale deleted dots. The caller must prove contiguous observed prefixes and persist the checkpoint safely. |
| 5. Delivery and deduplication retention | Complete, finite guarantee | Operation proofs and receive history have separate retention. Expired exactly-once guarantees become explicit before another send. This is not indefinite exactly-once delivery. |
| 6. Supported Mobile native heads | Complete | Host builds and package composition cover .NET 10 Android, Windows, iOS and Mac Catalyst. Real-device behavior remains a separate validation task. |
| 7. Legitimate mobile filesystem paths | Complete | Physical parent aliases share ownership. The most specific containing mount controls network policy. Final database and sidecar links remain restricted. |
| 8. Reinstall and backup restore | Complete, fail-closed policy | Reinstall creates a fresh identity/key. Matching backups preserve identity. Missing or mismatched secure state fails without silently discarding pending data. Older matching backups still need an external freshness check. |
| 9. Encryption and rollback guarantees | Complete by narrowing claims | Records bind authentication to their context. Documentation and tests explicitly exclude standalone whole-file freshness and deletion protection. An external protected checkpoint is required for those threats. |
| 10. WebSocket receive progress and faults | Complete | Bounded event admission does not block acknowledgement reception. Terminal receive faults reach pending and future requests with the original classification. |
| 11. Reusable storage conformance kit | Verified in working tree | The reusable kit covers shipped store implementations on applicable targets. Full suites pass on .NET 8–11, including browser and native process-kill cases. Capabilities stay narrowed where evidence is absent. |
| 12. SQLite connection and statement reuse | Verified in working tree | Both local SQLite and the server journal retain owned connections and bounded prepared-statement caches. Full server suites pass on .NET 8 and 9. |
| 13. SQLite read/query costs | Verified in working tree | Protected point reads, derived-key reuse, indexed server selection, transactional capacity counters and selected-key metadata batches replace warm history scans and N+1 reads. Cold migration and external-change integrity validation still scan records on purpose. |
| 14. Server polling and database scheduling | Verified in working tree | Empty polls avoid write reservations. Bounded workers run database work. Shared wakeup epochs notify all subscribers of one hub. Separate hubs/processes still need fallback polling. |
| 15. Filesystem journal persistence | Verified in working tree | Incremental transaction frames reduce whole-state writes. Recovery drops only an incomplete final frame. Complete corrupt frames fail closed. Platform flush guarantees remain item 16. |

### 1–2. Release and stable packages

Evidence: [release filter](../src/ReactiveUI.Primitives.slnf),
[release workflow](../.github/workflows/release.yml),
[pack targets](../src/Directory.Build.targets),
[package workflow](../.github/workflows/occasionally-connected-packages.yml), and
[release package validation](../tools/OccasionallyConnected.Ci/Packages.Release.cs).

Pack resolves MinVer before choosing stable assets. It gives NuGet a filtered restore-model copy.
Normal build and test restore assets retain .NET 11. This does not suppress preview warnings globally.
[PackagesTests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Tests/PackagesTests.cs)
contains stable package checks. A passing package gate is not the same as a completed signed release.

The authenticated source check of the
[shared release workflow](https://github.com/reactiveui/actions-common/blob/f3c785e851ac6dcca18672a9515d68d03d7fc3d9/.github/workflows/workflow-common-release.yml)
confirms that its `solutionFile` input reaches `dotnet pack`.
The caller supplies the full `ReactiveUI.Primitives.slnf`, not the smaller gate filter.
The shared workflow adds the Uno filter through MSBuild and uploads the combined unsigned package feed.
This checks the actual release path without creating a release or publishing packages.

### 3–4. CRDT merge and reclamation

Evidence: [merge functions](../src/ReactiveUI.Primitives.OccasionallyConnected.Core/Crdt/CrdtFunctions.cs),
[bounds](../src/ReactiveUI.Primitives.OccasionallyConnected.Core/Crdt/CrdtBounds.cs), and
[OR-set checkpoint logic](../src/ReactiveUI.Primitives.OccasionallyConnected.Core/Crdt/CrdtFunctions.ORSet.cs).

[Register tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Core.Tests/CrdtFunctionsTests.LwwRegister.cs)
cover commutative, associative and idempotent ties, including canonical timestamps.
[OR-set tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Core.Tests/CrdtFunctionsTests.ORSet.cs)
include a 6,000-element merge, repeated removal cycles, stale replay, concurrent dots,
unobserved prefixes, codec round trips and checkpoint merge laws.

Reclamation uses causal knowledge, not elapsed age. A caller must prove that each checkpoint prefix is fully observed.
It must persist and synchronize the checkpoint atomically. Reset identities must not reuse old dot sequences.

### 5. Retention and guarantees

Evidence: [journal retention options](../src/ReactiveUI.Primitives.OccasionallyConnected.Server/ServerCommitJournalOptions.cs),
[receive history handling](../src/ReactiveUI.Primitives.OccasionallyConnected.Server/ServerReceivePageOperations.cs), and
[upload guarantee handling](../src/ReactiveUI.Primitives.OccasionallyConnected/SyncEngine.Upload.Guarantees.cs).

Receive history cannot outlive operation replay proofs.
[In-memory receive tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Server.Tests/InMemoryServerCommitJournalTests.ReceivePages.cs)
and [SQLite receive tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Server.Tests/SqliteServerCommitJournalTests.ReceivePages.cs)
check that receive-history expiry preserves operation replay.
[Upload retention tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Tests/SyncEngineTests.Upload.Retention.cs)
include `ExactlyOnceLostAckAfterDaysOfflineExpiresBeforeAnotherSend`.
Fallback to a weaker guarantee requires explicit opt-in.

### 6–8. Native packages, paths and installation recovery

Evidence: [native Mobile workflow](../.github/workflows/occasionally-connected-mobile.yml),
[native package tool](../tools/OccasionallyConnected.Ci/MobileNativePackage.cs),
[Mobile API baselines](../src/ReactiveUI.Primitives.OccasionallyConnected.Mobile/PublicAPI/),
[SQLite path ownership](../src/ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite/SqliteSingleWriterOwnership.cs), and
[installation policy](../src/ReactiveUI.Primitives.OccasionallyConnected.Mobile/MobileInstallation.cs).

The release workflow replaces the Windows-only Mobile package with the composed package before signing.
Supported stable native assets are .NET 10. Preview native heads require matching workloads.
[Packaging tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests/MobileSqliteStorageTests.Packaging.cs)
check API baselines and native convenience APIs.

[Ownership tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests/SqliteLocalStoreAdapterTests.Ownership.cs)
cover parent aliases, retargeted aliases, nested mounts and network policy.
[Installation tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests/MobileSqliteStorageTests.Installations.cs)
cover app-data aliases, retained secure storage after reinstall, matching backups, database-only backups,
missing markers, mismatched secure state and orphan recovery sidecars.

Host alias tests exercise the mechanism needed for iOS `/var` to `/private/var` paths.
They do not replace device tests. Recovery refuses unsafe state rather than silently deleting encrypted pending records.
The [Mobile README](../src/ReactiveUI.Primitives.OccasionallyConnected.Mobile/README.md) defines the backup policy.

### 9. Encryption threat model

The [SQLite README](../src/ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite/README.md)
separates authenticated records from whole-file freshness and completeness.
[Tampering tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests/SqliteLocalStoreAdapterTests.EncryptionTampering.cs)
cover row substitution and bound-column changes.
[Backup tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests/SqliteLocalStoreAdapterTests.EncryptionBackups.cs)
demonstrate that deletion and a valid older encrypted backup require external proof.

Do not describe these tests as proving anti-rollback protection.
Choosing whole-database encryption is still an architecture decision in item 32.

### 10. WebSocket progress

Evidence: [WebSocket adapter](../src/ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets/WebSocketRemoteTransportAdapter.cs),
[progress tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets.Tests/WebSocketRemoteTransportAdapterTests.Progress.cs), and
[send tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets.Tests/WebSocketRemoteTransportAdapterTests.Sending.cs).

Real-socket regressions cover full count/byte event lanes, acknowledgement progress, saved-cursor resume,
malformed/closed receivers, pending and future requests, cancellation and disposal.
Transport credentials and keep-alive configuration remain item 21.

### 11. Storage conformance

The [source kit](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests/LocalStoreConformance.cs)
is included in the Core package.
[Fixtures](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests/ILocalStoreAdapterTests.Fixture.cs)
cover InMemory, plaintext/encrypted SQLite, FileSystem, LiteDB, BliteDB and IndexedDB where applicable.
The suite checks atomic failure, cancellation, deduplication, lease/client ownership, restart,
snapshot recovery and advertised capabilities.

[Crash tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests/ILocalStoreAdapterTests.Crash.cs),
[corruption tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests/ILocalStoreAdapterTests.Corruption.cs), and
[browser tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests/ILocalStoreAdapterTests.Browser.cs)
add process-kill and browser restart cases.
The suite rejects unproven multi-process coordination. Preserve its capability limits.
All applicable providers pass the fresh full runs for this working-tree revision.
The Windows crash tests join the signaled writer and wait for exclusive file access before reopening.
Only Windows sharing or lock violations retry within the existing shutdown deadline.
TUnit regressions prove release waiting, cancellation and propagation of other failures.
The tests still require competing-writer rejection and preserved committed state after an abrupt kill.
No profiler is disabled and no corruption or durability assertion is skipped.

### 12–13. SQLite reuse and query costs

Evidence: [local commit store](../src/ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite/SqliteLocalCommitStore.cs),
[connection scope](../src/ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite/SqliteStoreConnectionScope.cs),
[shared database owner](../src/OccasionallyConnected.Sqlite.Shared/SqliteDatabase.cs), and
[statement owner](../src/OccasionallyConnected.Sqlite.Shared/SqliteStatement.cs).
The local connection is reused. Prepared handles are reset and bindings cleared before reuse.
The idle cache holds at most 128 statements and excludes SQL above 16 KiB.
Cleanup removes operation cancellation and retires damaged connections.

[Connection reuse tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests/SqliteLocalCommitStoreTests.ConnectionReuse.cs)
and [statement tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests/SqliteStatementTests.cs)
cover warm reuse, cleanup faults, bindings, cache bounds, schema changes and constraints.
The [server journal](../src/ReactiveUI.Primitives.OccasionallyConnected.Server/SqliteServerCommitJournal.cs)
retains its initialized connection. Its [ownership gate](../src/ReactiveUI.Primitives.OccasionallyConnected.Server/SqliteServerCommitJournal.Connection.cs)
covers complete transaction and cursor lifetimes. Independent workers keep bounded admission.
Disposal closes admission, drains workers and releases the native connection.
[Server connection tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Server.Tests/SqliteServerCommitJournalTests.Connection.cs)
cover warm reuse, rollback recovery, canceled admission, sibling worker serialization and disposal/reopening.
The full server suite passed 597 tests on each of .NET 8 and 9. An independent source review found no actionable defects.

[Point-read tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests/SqliteLocalStoreAdapterTests.PointReads.cs)
measure selected-row authentication over 64/512/1,024-row histories, warm reads, key resolutions and allocations.
Server selection, event keys, indexes and transactional metrics are present in the working tree.
[Index tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Server.Tests/SqliteServerCommitJournalTests.Indexes.cs)
cover targeted queries and replay selection.

Warm `ReadUsage` in
[SqliteOutboxCapacitySql.cs](../src/ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite/SqliteOutboxCapacitySql.cs)
reads one transactional usage row. Migration backfills per-operation charges once.
Triggers update charges and totals in the same transaction as operation state changes.
Encrypted charges carry bounded authenticated proofs. Null, oversized or substituted proofs fail closed.
Initialization and detected external changes validate the complete accounting state before trusting it.
These cold integrity scans are intentional. They are not part of each warm admission.

[Accounting tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests/SqliteLocalCommitStoreTests.OutboxAccounting.cs)
cover migration, rollback, state changes, deletion, retry resurrection, snapshots and tampering.
[Metadata tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests/SqliteLocalCommitStoreTests.Metadata.cs)
measure native executions, not just cached statement preparation.
Capacity reads use 14 SQLite virtual-machine steps at both 1 and 128 retained operations.
Recovery uses three metadata SELECTs at both sizes. A selected lease uses one at both sizes.
The full SQLite suites pass on .NET 8 and 9: 619 passed, three privilege-dependent link tests skipped, zero failed per target.
MTP reports 98.95% line and 96.66% branch coverage for the SQLite production assembly on each target.

### 14. Server scheduling and notifications

Evidence: [empty offer reads](../src/ReactiveUI.Primitives.OccasionallyConnected.Server/SqliteServerCommitJournal.EmptyOffers.cs),
[bounded database scheduling](../src/ReactiveUI.Primitives.OccasionallyConnected.Server/SqliteServerCommitJournal.Scheduling.cs), and
[hub wakeup epochs](../src/ReactiveUI.Primitives.OccasionallyConnected.Server/ServerStreamHub.cs).

[Scheduling tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Server.Tests/SqliteServerCommitJournalTests.Scheduling.cs)
cover cancellation, bounded admission and disposal/drain.
[Broadcast tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Server.Tests/ServerStreamHubTests.Broadcast.cs)
cover all idle subscribers and receive gaps.
[Batching tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Server.Tests/SqliteServerCommitJournalTests.Batching.cs)
cover fewer physical commits, identical replay results and transaction rollback.
Broadcast scope is one hub. Independent hubs and processes retain fallback polling.

### 15. Incremental filesystem journal

Evidence: [delta model](../src/ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem/FileSystemJournalDelta.cs),
[journal persistence](../src/ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem/FileSystemLocalStoreAdapter.Journal.cs), and
[frame recovery](../src/ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem/FileSystemJournalHelpers.cs).

[Journal tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests/FileSystemLocalStoreAdapterTests.Journal.cs)
cover bounded linear growth, incomplete final frames, complete corruption and legacy compaction.
[Fault tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests/FileSystemLocalStoreAdapterTests.JournalFaults.cs)
and [replay tests](../src/tests/ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests/FileSystemLocalStoreAdapterTests.JournalReplay.cs)
cover crash prefixes, cancellation, append rollback and invalid state replacements.
This completes the write-amplification remedy at source level. It does not close platform flush work in item 16.

## Verification on 4 October 2026

Fresh local Release builds use the neutral target overrides required by `CLAUDE.md`.
They retain the applicable .NET 11 test targets. The builds listed below report zero warnings and errors.
Tests use TUnit assertions. MTP inspects the coverage reports.

| Suite | Target | Passed | Skipped | Failed |
| --- | --- | --- | --- | --- |
| Core | .NET 8 | 590 | 0 | 0 |
| Runtime | .NET 8 | 1,710 | 4 | 0 |
| Server | .NET 8 and 9, each | 597 | 0 | 0 |
| SQLite | .NET 8 and 9, each | 619 | 3 | 0 |
| FileSystem | .NET 8 | 146 | 0 | 0 |
| WebSockets | .NET 8 | 26 | 0 | 0 |
| Mobile | .NET 10 | 63 | 1 | 0 |
| Conformance | .NET 8 and 9, each | 135 | 4 | 0 |
| Conformance | .NET 10 and 11, each | 147 | 4 | 0 |

The skipped cases require unavailable link privileges, platform capabilities or a composed native package artifact.
Conformance skips only transport cases that do not apply to the advertised streaming capability.
They do not skip a failing assertion. Runtime and some provider runs preceded the final bounded charge-proof change.
The final full SQLite suites cover that change. The package consumers also rebuild the frozen production source.
Final conformance runs include the crash-test handle-lifetime fix and its new regressions.
The .NET 10 and 11 runs use an installed Chromium browser for real IndexedDB commit and restart checks.
No browser case is skipped.

Local coverage reports are under `src/TestResults`. These generated files are not committed:

| Evidence | Report directory |
| --- | --- |
| Core | `items1-15-core-net8` |
| Runtime | `items1-15-runtime-net8-final` |
| Server | `glenn-item12-server-net8-final`, `glenn-item12-server-net9-final` |
| SQLite | `item13-sqlite-net8-frozen`, `item13-sqlite-net9-frozen` |
| FileSystem | `items1-15-filesystem-net8` |
| WebSockets | `items1-15-websockets-net8` |
| Mobile | `items1-15-mobile-net10-frozen` |
| Conformance | `items1-15-conformance-net8.0-frozen` through `items1-15-conformance-net11.0-frozen` |

For a test suite, build from `src` in Release for the chosen target.
Set `LangVersion=preview` and clear `AndroidPrimitivesTargetFrameworks`, `ApplePrimitivesTargetFrameworks`
and `MobilePlatformTargetFrameworks` for neutral validation.
Run the built test assembly with `--coverage --coverage-output-format cobertura --results-directory <directory>`.
For browser cases on this host, set `RXUI_CONFORMANCE_BROWSER` to the installed Chromium executable.
The browser driver supports this explicit path. It does not change machine settings.

The fresh stable gate at version `0.1.0` passes in
`artifacts/items1-15-stable-frozen`.
It packs 20 production and dependency packages. It passes 32 determinism checks and
460, 66 and 30 symbol, Source Link and framework checks across the three package groups.
A clean install passes all 19 sample checks on each of .NET 8, 9, 10, 11 and .NET Framework 4.6.2, 4.7.2, 4.8 and 4.8.1.
The separate AOT check is not repeated by this local gate. It passes on the committed PR head.
The fresh prerelease gate at `0.1.0-glenn-review.1` also passes in
`artifacts/items1-15-prerelease-frozen`.
It passes 32 determinism checks and 520, 84 and 48 checks across the same three package groups.
All eight clean consumer targets pass all 19 sample checks again.
Stable packages omit preview assets and dependency groups. The prerelease packages retain them.

The packed sample now snapshots its concurrent diagnostic counters before sorting.
TUnit regressions cover empty logs, ordered counts and concurrent reads and updates.
The package tool disables reusable MSBuild nodes so a completed pack round does not retain PDB locks into the next round.
Neither change lowers a package gate or a test assertion.

The committed PR head has passing native host and package-composition checks:
[Windows and macOS native builds](https://github.com/reactiveui/Primitives/actions/runs/37158665769),
[stable and prerelease package checks](https://github.com/reactiveui/Primitives/actions/runs/37158665861), and
[AOT consumer](https://github.com/reactiveui/Primitives/actions/runs/37158665790).
Those host results do not certify real devices or the uncommitted working tree.
The remaining non-CodeQL failure is
[Windows .NET 8 coverage](https://github.com/reactiveui/Primitives/actions/runs/37158665870/job/111307279537).
Do not mark remote CI green from local results.

## Items 16–33: retained backlog

These remain open in [RemainingTasks.md](RemainingTasks.md).
The acceptance notes below preserve the supplied list. They are not a new exhaustive audit of each feature.

| Item | Status | Acceptance still to record |
| --- | --- | --- |
| 16. Platform durability | Open | Apple full flush, directory synchronization, network policy and bounded Windows replacement retries. |
| 17. LiteDB capacity and AOT | Open | Safe default document limits and a downstream trimmed/native AOT consumer result. |
| 18. Clock safety | Open | Monotonic elapsed time, persisted-deadline restart policy and forward/backward clock and replay-skew tests. |
| 19. Browser storage limits | Open | Persistence request, observable denial/quota/eviction and rejection of unsupported blocking SQLite on WebAssembly. |
| 20. Background lifecycle | Open | Distinct hidden/frozen/terminated behavior, mobile background time and Doze with cancellation/drain semantics. |
| 21. Transport options and liveness | Open | Authentication, proxy, certificates, keep-alive and bounded empty HTTP response polling. |
| 22. SignalR registration isolation | Open | Prove that unrelated hubs keep their serialization settings. Current registration still calls global `AddJsonProtocol`. |
| 23. Replay proofs and deployment | Open | No temporary proof values on the wire, secret handling, bounded replay state and multi-node behavior. |
| 24. Payload work and equality | Open | Measured copy/parse/hash reductions that retain ownership, integrity and structural equality. |
| 25. Scheduling and waiting | Open | Reuse suitable repository primitives and remove avoidable per-subscriber work and arbitrary wait limits. Item 14 is only one part. |
| 26. Public contracts/defaults | Open | Constructible defaults, effective options, interface-safe extensions and consistent TFM APIs/baselines. |
| 27. DI, startup and health | Open | Nonblocking resolution, useful startup exceptions and unhealthy status after terminal failure. |
| 28. Options and observability | Open | Agreed binding/validation, host-controlled sampling, structured logs and metric conventions. |
| 29. Release/test integration | Open | Symbols, supported test parallelism, package metadata, scoped gates and justification for unrelated changes. |
| 30. PR and release claims | Open | Reconcile the supplied 16-package/21-suite baseline with current inventory. Separate coverage measures and state retention/encryption limits. |
| 31. Public API decisions | Open decision | Agree naming, provider-neutral Core, internal seams, terminology and GUID-v7 defaults as one deliberate revision. |
| 32. Architecture and first release | Open decision | Agree package scope, arbitrary store adapters, journal/encryption boundaries and neutral transport. Record reasoned choices for alternative wire protocols and compression. |
| 33. Final review reconciliation | Open | Map every thread to a tested fix, verified explanation or agreed deferral. Complete applicable gates before resolving threads. |

## Next updates

1. Preserve and review the current uncommitted changes. Record their eventual commit SHA here.
2. Retain the connection ownership, transaction and cancellation regressions for item 12.
3. Retain the accounting invariants and native execution measurements for item 13.
4. Rerun relevant TUnit and package gates when these source files change. Keep the dated evidence above.
5. Record any eventual signed release execution. Keep host checks separate from device checks.
6. Update this dated CI snapshot after the new changes reach the PR head.
7. Reconcile review threads without treating source presence as reviewer approval.

When an item changes, retain its original number. Record the source commit, evidence and remaining limits.
