# OccasionallyConnected remaining tasks

Updated: 23 September 2026, 23:19 UTC. The local feature branch is at signed `b8c4ba256b272839f53947b9374a5be378a6e8f1`, following signed base `814b7bd64434fb301b2d5cf59836e059bd3710eb`; the index is empty. Five completed donors were preserved and retired after verifying all 1,027 archived files. Four source branches were deleted; the detached engine donor had no branch to delete. Five worktrees were retired. Two registered donors remain: collaboration-client and resilience-http. Nothing was pushed.

Runtime's last accepted original full run is 1,517/1,517 with 98.98% line and 97.93% branch coverage. A later full r4 attempt passed 1,501/1,519: 18 existing coverage-script tests timed out waiting for spawned PowerShell processes at about 10.5 seconds. No assertions failed, and the cause is unproven. The two added failure-path tests and `PrepareReceive` invariant refactor built in r10, and both focused tests passed. Causal fault waits now require an r11 rebuild. ResilienceLab released slot 2, so the isolated runtime gate is pending.

ResilienceLab passes 100/100 with 99.35% line and 92.60% branch coverage; additional runnable scenarios remain. The client passes 59/59 with 97.04% line and 89.77% branch coverage; focused watch cancellation and database-release checks pass, while actual Ctrl+C work is unbuilt. The feature is not ready for release. Follow [the design](ReactiveUI.Primitives.OccasionallyConnected.md). The current evidence and continuation history are in [CURRENT.md](../artifacts/occasionally-connected/CURRENT.md) and its linked archives.

The reviewed base and subsequent recovery integration are signed locally. Five completed donors have been retired after source-preservation verification. Keep integration local; do not push before final acceptance.

## Remaining integration work

| Priority | Remaining task | Acceptance condition |
| --- | --- | --- |
| P0 | Finish shared-session renewal | Complete the combined lifecycle and original coverage gates. Prove concurrent stale responses share one renewal, repeated expiry without durable progress stops, authentication failures remain terminal, and operation IDs, retry anchors, and cursors survive renewal. Cover at-most-once ambiguity, capability downgrade, cancellation, retired-session disposal, and bounded shutdown. |
| P0 | Finish context diagnostics | Add the remaining reachable scheduler-rejection test and complete original coverage. Preserve one-time typed commits, accurate global queue totals and publication metrics, bounded deferred observer delivery, scheduler routing, slow/throwing observer isolation, overflow, and disposal behavior. |
| P0 | Complete snapshot recovery acceptance | The paired observer, persisted attempt and timestamp, reason-byte limits, and post-commit read-failure checks pass in the signed recovery integration. Finish the framework matrix, remaining original coverage, and application dependency retests. Preserve the accurately labeled restored-defect RED evidence. |
| P0 | Complete public outbox and producer acceptance | Verify the combined context against atomic memory/SQLite admission. Cover shared count/byte limits, blocked publisher limits, cancellation refunds, cross-stream wakeups after durable capacity release, late release after unregister, each supported volatile policy, and observer/input producer paths. Preserve durable work and truthful queue diagnostics. |
| P1 | Finish convenience API integration | The paired public state/queue notification regression now has a genuine RED/GREEN result, and its hook is included in the signed recovery integration. Complete the final behavior matrix and coverage gates. Preserve `ObservePending` reaching zero, `WhereSynchronized` after ACK, replay, mutable-state isolation, wrapper lifecycle, and bounded observer ownership. |
| P0 | Finish HTTP replay application tests | The collaboration client passes the original .NET 8 suite (59/59) with clean analyzer output and 97.04% line and 89.77% branch coverage. Real HTTP, restart/reopen, capacity, canonical stale-merge, and recovery behaviors pass. Failure coverage and the final recovery dependency retest remain. Preserve the reviewed HTTP integration. |
| P0 | Make analyzer dependencies reproducible | SST2338 and PSH1021 fixes currently use local analyzer packages. Arrange separately authorized publication from the analyzer repository and use released dependencies reproducible in CI. Add no suppressions. |
| P0 | Resolve final Windows CI acceptance | Investigate SQLite extended error 1546 during server recovery and concurrent initial client identity binding if either recurs. Do not mask either with retries or weaker durability. Obtain passing final cross-platform CI when publication is authorized. |

## Example applications

| Application | Remaining task |
| --- | --- |
| `OccasionallyConnected.Collaboration.Client` | The original .NET 8 suite passes 59/59 with clean analyzer output and 97.04% line and 89.77% branch coverage. Real HTTP, restart/reopen, offline convergence, cursor continuity, capacity, canonical stale merge, and CLI checks pass. Complete failure coverage and rerun the final recovery dependency gate. Preserve separate SQLite databases, saved subscription/cursor identity, stable operation IDs, duplicate suppression, cancellation, slow observers, and secret-free CLI diagnostics. |
| `OccasionallyConnected.ResilienceLab` | The latest full .NET 8 run passes 100/100 with 1,821/1,833 lines (99.35%) and 413/446 branches (92.60%). The 32-file `r50freeze` is hash-verified for `full-r9-20260924`. Preserve the built-in-server lost-ACK scenario and add the remaining runnable demonstrations for duplicate/reordered delivery, retry/backoff, capability downgrade, backpressure, slow observers, corruption/quarantine, and retention-gap recovery. |
| All examples | Complete public-API instructions and solution/CI wiring. Build and run against freshly packed packages, including DurableOutbox. Verify trimming and NativeAOT where supported. Complete example TUnit suites and original handwritten coverage on .NET 8–11. |

## Final acceptance

- [ ] Run two independent durable clients through offline publication, live disconnect/reconnect, server restart, client restart, and eventual convergence.
- [ ] Complete the delivery-guarantee failure matrix at serialization, local commit, enqueue, upload, server apply/ACK, local ACK commit, remote apply, notification, compaction, and migration boundaries.
- [ ] Prove exactly-once effects within declared retention, capability downgrade/fail-closed behavior, and explicit ambiguous outcomes.
- [ ] Verify `PublishAsync`, observer bridges, and `stream.Input` under concurrent producers, count/byte limits, cancellation, and every supported buffer strategy.
- [ ] Verify combined subscription/cursor continuity, duplicate suppression, atomic snapshot recovery, replay order, and projection/notification consistency after restart.
- [ ] Exercise tenant/session substitution, stale/replayed requests, corrupt payload/hash/schema, oversized messages, expired credentials, clock skew, and redacted diagnostics through application paths.
- [ ] Complete shared transport conformance and packed-package verification of the reviewed shared storage suite. Run the fixtures in the [compatibility matrix](OccasionallyConnected.Compatibility.md) against final packages.
- [ ] Measure throughput, allocations, large-outbox recovery, compaction, and slow-observer isolation. Run bounded soak and reconnect/retry scenarios.
- [ ] Build libraries for `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`. Complete applicable TUnit tests, public API checks, and solution gates.
- [ ] Verify 100% reachable handwritten line and branch coverage from each original framework report. Report compiler/generated residuals separately against exact tested binaries. Add no suppressions or exclusions. Do not substitute merged reports that lose condition data.
- [ ] Independently review integrated source and evidence, create signed local commits, and retire completed worktrees and obsolete source branches.
- [ ] Prepare the single final PR only after the entire feature passes acceptance.

## Worktrees and local cleanup

Two registered donor worktrees remain. Preserve unfinished source and evidence until each integration is reviewed and signed.

| Worktree | Remaining reason to retain it |
| --- | --- |
| `Primitives-oc-collaboration-client` | Final real application acceptance and observability examples. |
| `Primitives-oc-resilience-http` | Real durable HTTP lost-ACK implementation and tests. |

The engine, public-context, convenience-extensions, dependency-injection, and client-recovery donors are complete. Their 1,027 archived files match the donor sources, and their branches and worktrees were retired after signed integration. The two application donors remain active. No new worktrees are needed.

Physical cleanup remains for five integrated, unregistered directories: `Primitives-oc-coverage-gate`, `Primitives-oc-sqlite-main-compat`, `Primitives-oc-example-lab`, `Primitives-oc-completion-proof9704de1`, and `Primitives-oc-memory-snapshot-recovery`. Their source/evidence is archived. Earlier Git removals encountered Windows path-length errors, and automatic approval review rejected subsequent deletion attempts. Keep this cleanup blocked until an approved path is available. Preserve user stashes and unrelated local changes.
