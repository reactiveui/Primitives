# OccasionallyConnected remaining tasks

Updated: 14 September 2026. Audited against `OccasionallyConnected` at `15cb322`, retained worktree drafts and available CI results. The feature is not yet ready for an end-to-end application. Requirements follow [the design document](ReactiveUI.Primitives.OccasionallyConnected.md).

## Implementation and integration

| Priority | Remaining task | Required outcome |
| --- | --- | --- |
| P0 | Obtain passing cross-platform CI | Inspect the [build for `2082e9b`](https://github.com/reactiveui/Primitives/actions/runs/34843877707). All three operating-system jobs failed. Windows reports a concurrent first-client SQLite binding failure and two SignalFromTask cancellation failures. Verify the repaired current-directory child lifecycle and physical temporary-directory fixtures on Linux/macOS. Local Windows validation passes all 403 SQLite and 929 runtime tests on each modern framework; this does not establish cross-platform CI success. Diagnose the separate Windows Server crash-reopen SQLite Error 10 failure if it recurs. Resolve remaining failures without suppression or retry masking and obtain passing Windows/Linux/macOS runs. |
| P0 | Finish and integrate `SyncEngine` | Resolve the latest full .NET 8 run's 12 failures out of 935 tests. Preserve shared transport ownership when an individual receive pump reconnects; root review rejected a draft that disposed the shared session and disabled later uploads. Align upload fixtures with their actual clocks and declared batch capabilities; verify authoritative queue accounting, receive inclusion followed by terminal upload ACK, lifecycle/wake behavior and shared capability validation before prepare/send. Honor negotiated batch limits and retention, including transports without `BatchPush`. Complete the framework and coverage gates before merging. |
| P0 | Implement the public builder and context | Add validated construction, a bounded typed stream registry, complete definition compatibility checks, shared initialization, offline publication, observable AutoStart failures and correct owned/borrowed dependency disposal. Compose the actual engine and facade through public APIs. |
| P0 | Complete bounded synchronous input capture | Fix failed-ticket retention, callback-failure isolation, pump ownership/draining, invalid capture cleanup and supported overflow behavior, including `DropOldest` without evicting active or durable work. Reserve count/bytes before copying mutable input; reject synchronous `Block` and unsupported custom policies. Connect the producer to the facade and prove actual `stream.Input` publication and disposal. |
| P0 | Complete HTTP replay and authentication integration | Finish replay/session lifecycle tests, bounded retention, duplicate waiters, expiry/clock rollback and secret retirement. Integrate canonical request MAC verification with client signing and endpoint admission; prove replay does not duplicate effects. |
| P0 | Integrate server snapshot recovery | Integrate snapshot materialization and `IServerSnapshotRecoveryHub`; exercise mutation and compaction between capture, offer and ACK. |
| P0 | Complete atomic client snapshot recovery | Implement SQLite atomic recovery and connect HTTP recovery, engine orchestration and projection reconstruction. Preserve pending work, identities, cursor, inbox, terminal outcomes and quarantine across failure, restart and retry. |
| P1 | Implement dependency injection | Add the DependencyInjection package, compatible central package pins, validated options, singleton context/named streams, generated schema registration, borrowed ownership, bounded redacted logging and visible startup failures. Verify actual provider lifetimes and disposal. |
| P1 | Align the coverage gate with the approved policy | Update `tools/Test-OccasionallyConnectedCoverage.ps1` and add TUnit regression tests. Require 100% handwritten line/branch coverage; report framework-generated serializer coverage and full-package totals separately. Classify by recognized generated source paths, not class names or suffix alone; async state machines mapped to handwritten source remain gated. Keep all coverage collection enabled, with no suppressions or exclusions. |

## Example applications

| Application | Remaining work |
| --- | --- |
| `OccasionallyConnected.Collaboration.Server` | Verify mounted routes and executable startup against real ASP.NET/SQLite sockets. Complete graceful host cancellation and disposal, options, bounded I/O, authentication and payload behavior coverage; verify all modern targets and integrate. Document production authentication/secure transport separately from development-token setup. |
| `OccasionallyConnected.Collaboration.Client` | Implement a public-context client with durable identity, offline startup, optimistic edits, reconnect/status, persisted subscription resume, conflict reconciliation and bounded input/observers. Exercise two independent SQLite clients against the real server. |
| `OccasionallyConnected.ResilienceLab` | Extend the existing CRDT loopback example with dropped ACKs, duplicate/reordered delivery, retry/backoff, capability downgrade, backpressure, slow observers, corruption/quarantine, restart and retention-gap recovery. Use the actual runtime, network and durable storage paths. |

The integrated DurableOutbox example still requires final freshly packed-package consumer validation. Remaining applications require public-API instructions, solution/CI wiring and packed-package validation.

## Final acceptance

- [ ] Run two independent durable clients against the real HTTP server through offline publication, reconnect, restart and eventual convergence.
- [ ] Exercise the supported delivery guarantees at serialization, local commit, enqueue, upload, server apply/ACK, local ACK commit, remote apply, inbox/notification, compaction and migration crash boundaries.
- [ ] Prove exactly-once effects within declared retention, capability downgrade/fail-closed behavior and explicit ambiguous outcomes.
- [ ] Verify `PublishAsync`, observer bridges and `stream.Input` under concurrent producers, count/byte limits, cancellation and each supported buffer strategy.
- [ ] Verify subscription identity/cursor continuity, duplicate suppression, atomic snapshot recovery, replay order and projection/notification consistency after restart.
- [ ] Exercise tenant/session substitution, stale/replayed requests, corrupt payload/hash/schema, oversized messages, expired credentials, clock skew and redacted diagnostics.
- [ ] Complete reusable storage/transport conformance, protocol golden fixtures and supported migration/version combinations.
- [ ] Measure throughput, allocations, large-outbox recovery, compaction and slow-observer isolation; run a bounded soak scenario.
- [ ] Build libraries for `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48` and `net481`; run applicable TUnit tests, handwritten coverage and example demos on .NET 8–11.
- [ ] Compile/run public examples against freshly packed NuGet packages; verify trimming and NativeAOT where supported.
- [ ] Obtain passing final solution, API compatibility and OS/framework CI gates. Preserve the user's existing solution-file edits when adding completed packages/examples/tests.
- [ ] Verify coverage from each original framework report; do not accept merged reports that lose condition data.
- [ ] For each completed section, independently review and verify it, merge into `OccasionallyConnected`, archive evidence, remove its worktree and delete any obsolete source branch.
- [ ] Prepare the single final PR after the complete feature passes its acceptance gates.

## Worktrees requiring completion

Retain these isolated drafts until their work and verification are complete; do not merge unfinished code merely to remove a worktree.

| Worktree | Remaining section |
| --- | --- |
| `Primitives-oc-engine` | Engine failures, negotiation limits, lifecycle and complete validation. |
| `Primitives-oc-owned-input` | Bounded producer, failure/disposal handling and facade composition. |
| `Primitives-oc-http-replay` | Replay lifecycle validation and client/endpoint integration. |
| `Primitives-oc-server-snapshot-offers` | Offer/view validation, materialization and recovery integration. |
| `Primitives-oc-example-server` | Remaining application behavior, coverage and runnable framework gates. |

Separate physical cleanup remains for the unregistered `Primitives-oc-example-lab`, `Primitives-oc-completion-proof9704de1` and `Primitives-oc-memory-snapshot-recovery` directories. Their source/evidence is archived. The recovery worktree was integrated and unregistered, but Git encountered a Windows path-length error; automatic approval review then blocked removal of its residual directory. Automatic approval review also blocked deletion of the other two directories. These blocks have not been bypassed.
