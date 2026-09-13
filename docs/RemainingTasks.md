# OccasionallyConnected remaining tasks

Audit date: 13 September 2026. Implementation reference: `OccasionallyConnected` at GitHub-signed commit `055142f863c3c347765bb84f150ddfa2b96c6f93`, containing the reviewed endpoint and HTTP negotiation changes.

The feature is **incomplete and is not yet ready for an end-to-end application**. The committed contracts, storage, serialization, server components and CRDT example provide a substantial foundation. The concrete synchronization engine, application construction API, DI integration and complete HTTP recovery path still require implementation or integration.

This report compares the [design specification](ReactiveUI.Primitives.OccasionallyConnected.md), committed source, isolated worktree drafts, recorded failing tests and verified coverage reports. Passing coverage measures implemented code; it does not measure missing features. No overall completion percentage is claimed.

## Local consolidation status

- `OccasionallyConnected` is the master feature branch. Consolidation was performed locally. On 13 September the user authorized GitHub's authenticated signed-commit API for reviewed changes after local GPG signing timed out. Unfinished implementation remains local; no new `CP_*` branches or PRs are part of this workflow.
- Signed commit `da8636c8819788f898cfbe9f043eaa1a44b730ea` merged 197 accepted files covering protocol/server work, trusted principals, recovery contracts and the initial ResilienceLab example.
- Signed commit `484fa490101c09682dd629d9b82811dfdea08c92` merged 73 reviewed quarantine/recovery source and API files.
- The only remaining local branch names are `main` and `OccasionallyConnected`. Local `CP_*` source branches have already been deleted. Remaining task worktrees use detached HEADs.
- **127 completed registered worktrees have been physically removed** across the consolidation batches, including the temporary final-integration and accepted HTTP endpoint worktrees. Seven unfinished task worktrees remain. The endpoint archive contains 2,388 source/evidence files with verified SHA-256 hashes; all 156 changed donor source paths were checked against the feature branch before removal. No completed registered donor worktree remains.
- Before removal, the orchestrator verified ancestry, current changes, absolute paths and archived evidence. The first batch alone preserved 2,256 evidence files. Archives and removal manifests are under local `artifacts/occasionally-connected/completed-worktrees/`.
- Two **unregistered residual directories** remain: `Primitives-oc-example-lab` and `Primitives-oc-completion-proof9704de1`. Their completed source was verified against the feature branch and their non-build source/evidence archived. Automatic approval review rejected native recursive directory deletion as "blocked by policy". They are not remaining registered worktrees; their physical cleanup is blocked.
- The independent review covered all remaining inactive `Primitives-*-*` siblings, including CI trees. Older uncommitted copies were compared with current source, reachable historical versions, relocated types and split tests. Superseded copies were retired without overwriting newer fixes. The completed SQLite crash reproduction task contributed archived diagnostics, not a claimed fix; the intermittent failure remains below.
- The user's pre-existing `src/ReactiveUI.Primitives.slnx` changes have been preserved byte-for-byte during consolidation and the report merge.

## Accepted implementation and evidence

| Section | Accepted status | Verification and limits |
| --- | --- | --- |
| Core contracts and validation | Committed, including trusted identity and bounded snapshot/quarantine contracts | Latest integrated Core suite: 524 tests on each of .NET 8-11; 100% matching-package line and branch coverage. |
| Runtime building blocks and typed local stream facade | Committed, including local commits, reconciliation, queues, diagnostics and quarantine behavior | Latest integrated Runtime suite: 909 tests per modern framework; 100% matching-package coverage. The concrete engine and public construction API remain outstanding. |
| SQLite local storage and crash recovery | Committed, including bounded persisted-payload reads, corruption quarantine, dead-letter preservation, lease identity and process-crash tests | 402 tests per modern framework; 100% line and branch coverage. Core, Runtime and SQLite each built all eight library targets in Release with zero warnings/errors. |
| Server operation processing, CRDTs and stream hub | Accepted component integrations committed | Authorization, server ordering, idempotency, CRDT resolution and receive/ACK tests exist. Snapshot offers are still isolated. An intermittent SQLite crash-reopen failure remains unresolved. |
| HTTP client protocol, codec and portable server endpoint | Root-reviewed endpoint and atomic capability negotiation correction committed with a valid GitHub signature | Combined suite: 374 passing tests on each modern framework, with 100% matching-package line/branch coverage. HTTP library builds all eight targets with zero warnings/errors. Includes bounded request admission, independent ACK capacity, cancellation/timer draining, strict routing, body limits and borrowed-hub ownership. Replay/session authentication and snapshot recovery integration remain outstanding. |
| ResilienceLab `crdt-loopback` | Committed runnable example | 62 tests and 100% line/branch coverage on .NET 8-11; actual 23-case demo passed on all four. Orchestrator independently reran the integrated .NET 8 example. This scenario uses two in-memory loopback clients and explicit receive acknowledgements. |

The supported library matrix is `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`. TUnit execution/coverage uses the four modern frameworks. Example applications target those four modern frameworks and are non-packable.

## Required implementation and integration

| Priority | Work item | Current finding | Completion requirement |
| --- | --- | --- | --- |
| P0 | Finish and integrate the concrete synchronization engine | `SyncEngine` exists in the isolated draft. Upload/dead-letter race tests exposed double-release accounting. Root review corrected a separate receive test: inclusion evidence removes local replay, not pending upload work. A weak-session recovered exactly-once test also failed after a send occurred. | Verify authoritative pending snapshots against actual storage semantics, inclusion followed by upload acknowledgement, and fail-closed capability checks before the attempt barrier. Complete lifecycle/wake behavior and the full coverage/framework gates. |
| P0 | Public builder and context | No concrete `OccasionallyConnectedBuilder` or `OccasionallyConnectedContext` was found in committed production source. | Implement validated construction, bounded typed stream registry, complete definition compatibility, once-only initialization, offline-first publication, observable AutoStart failures and single-owner disposal. Preserve borrowed dependency ownership for DI. |
| P0 | Production synchronous input bridge | The user approved bounded owned-input capture on 13 September; a dedicated implementation task has started. No production implementation is accepted yet. | Reserve count/byte capacity before copying mutable input; reject synchronous `Block`; exercise all supported overflow policies and preserve durable work. Connect the bridge to the context/facade. |
| P0 | HTTP replay, session binding and request authentication | Isolated replay/session implementation remains unmerged. Latest recorded full run passed 280 tests but covered 2195/2215 lines and 763/798 branches; lifecycle tests were subsequently added. | Complete bounded session/replay retention, canonical request MAC verification, duplicate waiters, cancellation, expiry/clock rollback, secret retirement and capacity accounting. Integrate endpoint admission and client signing; prove replay does not duplicate effects. |
| P0 | Server snapshot views and durable offers | Memory/SQLite journal draft has coherent views and persisted subscription generations. Latest recorded full Server run passed 456 tests but was below complete branch coverage; further source changes await verification. | Complete view/proof validation and transactional offers/ACKs, integrate the materializer and `IServerSnapshotRecoveryHub`, and test mutations/compaction between view capture, offer and acknowledgement. |
| P0 | Atomic client snapshot recovery | Public recovery contracts are committed. The implementation scan found only test doubles implementing `ILocalSnapshotRecoveryStore`, `IRemoteSnapshotRecoverySession` and `IServerSnapshotRecoveryHub`. | Implement the memory/SQLite atomic recovery transaction and connect it through HTTP, engine and projection reconstruction. Preserve pending work, identity, cursor, inbox, terminal outcomes and quarantine guarantees under crashes and retries. |
| P1 | Microsoft.Extensions dependency injection | The v1 DependencyInjection project is absent. A reviewed design proposal is preserved locally. | Add compatible central package pins, validated options, singleton context and named streams, source-generated schema registration, correct borrowed ownership, redacted bounded logging and visible startup failures. Verify actual provider lifetimes and disposal. |
| P1 | Complete the four example applications | Only the initial ResilienceLab scenario is committed; two applications are isolated drafts and the collaboration client is absent. | Deliver the application matrix below through public APIs, include runnable instructions and solution/CI wiring, and validate freshly packed-package consumers. |
| P1 | Resolve the intermittent Server crash test | A combined Server run produced SQLite Error 10 when reopening after a killed writer. Subsequent focused and repeated full runs passed, so no root cause or fix was established. | Capture the extended SQLite error and process/file state, diagnose the race and prove the correction. Passing retries alone do not close this issue. |
| P1 | Final conformance and release-readiness gates | Component tests are extensive; the complete assembled application and package matrix have not passed. | Run the fault/security/compatibility/performance matrix below against the final local branch and its freshly packed packages. |
| P1 | Retire the remaining implementation worktrees as they finish | Completed registered donor worktrees have been removed. Eight isolated implementation worktrees remain unfinished. | Verify each completed section, integrate it locally, archive evidence and immediately remove its worktree. The two unregistered residual directories have a separate policy-blocked physical cleanup item. |

## Example application matrix

| Application | Current state | Remaining demonstration and validation |
| --- | --- | --- |
| `OccasionallyConnected.DurableOutbox` | Isolated SQLite example draft; latest .NET 11 run passed 68/68 tests with 982/1003 lines and 348/370 branches covered | Complete meaningful simulation/status and serialization coverage, attempt-overflow handling, actual demo execution and all four runnable targets before integration. The no-authoritative-snapshot rejection and denied-attempt regressions now pass. |
| `OccasionallyConnected.Collaboration.Server` | Isolated ASP.NET/SQLite server draft with development authentication, activity merge and four CRDT registrations | Latest full .NET 8: 40/40 passed, with 806/920 lines and 248/322 branches covered. The expanded 62-test source includes socket publish/receive/ACK/restart. It now incorporates the root-verified HTTP negotiation fix and restores the atomic capability declaration; that assembled example still awaits validation. Finish behavior coverage and all-framework gates before claiming the delivery-guarantee matrix. |
| `OccasionallyConnected.Collaboration.Client` | Not implemented | Build a public-context client with durable local identity, offline startup, optimistic changes, reconnect, operation status, persisted subscription resume, conflict reconciliation and bounded input/observer behavior. Run two independent SQLite clients against the real server. |
| `OccasionallyConnected.ResilienceLab` | Initial CRDT loopback scenario committed and verified | Add the planned runtime/network/durable failure scenarios: dropped ACKs, duplicate/reordered delivery, retry/backoff, capability downgrade, backpressure, slow observers, corruption/quarantine, restart and retention-gap recovery. |

The server example currently demonstrates development-token authentication. Production host authentication/authorization composition and secure transport must be documented and exercised without presenting those development credentials as a production configuration.

## Final acceptance matrix

- [ ] Run two independent durable clients and the real HTTP server through offline publication, reconnect, restart and eventual convergence.
- [ ] Exercise each supported delivery guarantee at the design's crash points: serialization, local commit, enqueue notification, upload, server apply before ACK, local ACK commit, remote apply, inbox/notification, compaction and migration.
- [ ] Verify exactly-once **effect within declared retention**, capability downgrade/fail-closed behavior and ambiguous outcomes without claiming unlimited exactly-once delivery.
- [ ] Verify all producer paths (`PublishAsync`, observer bridge and `stream.Input`) under count/byte limits, cancellation, concurrent producers and applicable buffer strategies.
- [ ] Verify subscription identity/cursor continuity, duplicate suppression, atomic snapshot recovery, pending replay order and projection/notification consistency after restart.
- [ ] Exercise tenant/session substitution, stale/replayed requests, payload/hash/schema corruption, oversized requests/responses, expired credentials, clock skew and redacted diagnostics.
- [ ] Complete reusable storage and transport conformance, protocol golden fixtures and supported migration/version combinations.
- [ ] Measure representative throughput, allocation, large-outbox recovery, compaction and slow-observer isolation; complete a bounded soak scenario.
- [ ] Compile and run public API examples against freshly packed NuGet packages; complete trimmed and NativeAOT publish/run checks where supported.
- [ ] Run the final solution build, API compatibility and required OS/framework CI matrix. GitHub CI has not been rerun for these local-only commits.
- [ ] Preserve zero new suppressions/exclusions and only TUnit assertions. Require 100% line and branch coverage for handwritten feature code on each applicable modern target. As explicitly approved on 13 September, report framework-generated serializer coverage separately while keeping all coverage collection enabled and retaining full-package totals. Async state machines mapped to handwritten source remain part of the handwritten requirement.
- [ ] Wire all completed packages/examples/tests into the solution and CI while preserving the user's existing solution-file edits.
- [ ] Merge every accepted section into local `OccasionallyConnected`, archive evidence, remove completed worktrees and confirm no obsolete source branch remains.
- [ ] Prepare the single final PR only after the complete feature passes its gates and publication is authorized under the user's local-only instruction.

## Evidence, process and next execution order

Detailed local evidence is preserved under `artifacts/occasionally-connected/`, including completed-worktree archives, source SHA manifests and the durable `CURRENT.md` checkpoint. The original `worktree-cleanup-audit.json` was corrupted during an audit update and is superseded; use the validated versioned audits and root removal manifests.

Earlier work includes disclosed TDD process deviations: some code preceded actual failing-test execution, and an earlier CRDT experiment temporarily removed/reapplied a fix. Those histories cannot be represented as strict red-before-green development. Current repairs preserve compiling behavioral RED evidence before the corresponding fix; compiler/fixture failures are classified separately.

Proceed in this order:

1. Complete the engine accounting/lifecycle work, HTTP endpoint/replay and server snapshot-offer gates in their retained work areas. Retire each area immediately after verified integration.
2. Implement the public context/input composition and atomic end-to-end snapshot recovery, then the DI package.
3. Complete the collaboration client, integrate both application drafts and expand ResilienceLab.
4. Resolve the intermittent Server crash failure and run the assembled application, packed-package and full compatibility/security/performance acceptance matrix; close the remaining worktrees and prepare the final PR.

The design explicitly defers `.Reactive`, `.Hosting`, file-system storage, WebSockets and later platform convenience adapters to v1.x or later. They are outside the v1 completion gate unless scope is explicitly expanded.

## Remaining local worktrees

Seven registered sibling task worktrees remain unfinished after the reviewed HTTP endpoint was committed and its source/evidence archive verified before removal. Each remaining task must pass its acceptance gates before integration and retirement. The orchestrator uses the original `Primitives` checkout on `OccasionallyConnected`.

| Worktree | Concrete remaining task |
| --- | --- |
| `Primitives-oc-memory-snapshot-recovery` | Implement atomic local checkpoint recovery in the in-memory adapter, including pending dispositions, transaction fences and recoverable acknowledgement state. |
| `Primitives-oc-owned-input` | Implement the approved bounded owned-input capture contract and synchronous producer, with capacity reserved before mutable input copying. |
| `Primitives-oc-engine` | Finish authoritative queue outcome accounting and valid race fixtures; verify lifecycle/wake behavior; complete the Runtime coverage/framework matrix and merge the engine. |
| `Primitives-oc-http-replay` | Verify new replay/session lifecycle cases, expiry and bounded secret retention; complete coverage and integrate endpoint/client request authentication. |
| `Primitives-oc-server-snapshot-offers` | Verify the latest coherent-view and durable-offer changes; complete coverage and integrate snapshot materialization, hub recovery and acknowledgement. |
| `Primitives-oc-example-outbox` | 70 tests pass on net11; handwritten lines are 647/647 and branches 233/250. Finish meaningful branch tests and snapshot validation, report generated serializer coverage separately, then verify the framework matrix and integrate. |
| `Primitives-oc-example-server` | Connection regression fixed in the draft; complete socket publication/receive/ACK/restart coverage and the runnable framework matrix, then integrate the ASP.NET/SQLite server example. |

The public builder/context, input bridge, atomic recovery, DI package, collaboration client and final application acceptance work still need implementation or integration as described above; worktree count is not a feature completion measure.

Review evidence and verified source archives are retained locally under `artifacts/occasionally-connected/`: `cleanup-review-a.json`, `cleanup-review-a-nearest-evidence.json`, `cleanup-review-b.json`, `cleanup-review-c-root.json`, and `completed-worktrees/` with per-batch removal manifests. `worktree-cleanup-final.json` records the final physical/registered inventory. Old absolute worktree paths in earlier test logs must be resolved through these archives.

The two policy-blocked residual directories listed above require separate physical cleanup. No alternate deletion method was attempted after either rejection.
