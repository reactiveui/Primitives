# OccasionallyConnected remaining tasks

Updated: 19 September 2026. Audited against `OccasionallyConnected` at `6a8fec13`, retained worktree drafts and available CI results. The feature is not yet ready for an end-to-end application. Requirements follow [the design document](ReactiveUI.Primitives.OccasionallyConnected.md).

Work resumed on 19 September 2026 from `artifacts/occasionally-connected/CURRENT.md`. Five reviewed sections were signed and merged locally. Their source branches are deleted. Four worktree directories were removed; coverage-gate residual cleanup was blocked. Context coverage, HTTP replay validation and client snapshot recovery are active validation gates.

## Implementation and integration

| Priority | Remaining task | Required outcome |
| --- | --- | --- |
| P0 | Finish validation against the latest core | Publish the reviewed SST2338 correction from `D:/Projects/Github/glennawatson/RoslynCommonAnalyzers` (`CP_fix_sst2338`) and consume a released, reproducible analyzer dependency in CI. Permission for the separate analyzer PR is pending. The .NET 11 runtime, Core and SQLite checks currently use the verified local package. Complete the remaining package/framework validation against the signed local main merge `8df4ff40`. |
| P0 | Obtain passing cross-platform CI | Resolve the Windows failures in [run 35178024748](https://github.com/reactiveui/Primitives/actions/runs/35178024748): server crash recovery reports SQLite extended code 1546 (`SQLITE_IOERR_TRUNCATE`), and concurrent first client identity binding returns an unexpected SQLite exception. Capture the expanded binding diagnostics and establish each cause without retry masking or weaker durability. Confirm the locally verified input scheduling/cleanup corrections in full CI, then obtain a passing complete cross-platform run. |
| P0 | Finish and integrate `SyncEngine` | Close the remaining functional coverage gaps for upload scheduling, retries, queue accounting, receive cancellation and shutdown. The last accepted full net8 run passes 1,148 tests without unobserved exceptions; original runtime coverage is 8,043/8,128 lines and 3,278/3,348 branches. Inflight upload parking and receive retry exhaustion pass their focused regressions. The 183 committer tests and real SQLite mixed-result regression pass after simplifying validated queue reconciliation. New malformed-result, retained-retry and real SQLite reconciliation tests pass individually. The dead-letter notification size fix passes its causal regression and full suite. Then finish all framework gates, integrate the reviewed engine and facade changes, and remove the donor. |
| P0 | Complete the public builder and context | Finish validation of the internal lifecycle-intent extraction and retain the public context integration tests. The latest clean build passes six cancellation-policy tests, the public unexpected-cancellation regression and all 1,179 runtime tests. Original runtime coverage is 8,554/8,675 lines and 3,417/3,523 branches. The component tests retain their preimplementation failure. Verify shared start/stop ownership, cancellation, late stream registration, compatible definitions and owned/borrowed disposal. Complete all original coverage and framework gates before integration. |
| P0 | Complete HTTP replay and authentication integration | Finish the snapshot endpoint, session and bounded codec. Verify limits before materialization, malformed text and domain error mapping, signed custom routes and replay without repeated effects. The latest clean build passes all 680 HTTP tests. The latest focused runs pass 130 codec and 155 endpoint tests; the preceding adapter gate passes 113 tests. Caller cancellation and session/adapter disposal regressions pass. Original HTTP coverage is 4,696/4,724 lines and 1,712/1,795 branches. Close the remaining snapshot validation and replay gaps. Complete original coverage and every target framework gate before integration. |
| P0 | Complete atomic client snapshot recovery | The reviewed SQLite atomic recovery and bounded capture baselines are now integrated. Finish runtime recovery using the reconciled baseline. The responsive-publication test has a preserved failure at the missing recovery call. The inflight test now also has a valid failure at the missing recovery call, after the upload completes and its trigger joins. Correct the reviewed runtime draft before its first implementation gate: register upload completion ownership before external work, publish recovered state inside the serialized stream lane, match disposition bytes by operation identity, and preserve parked work across stop/restart. Verify terminal status notifications and retry ownership. Release upload parking after a durable recovery commit even if its acknowledgement is lost. Reconcile already accepted but not receive-included replay records without applying them twice after recovery or restart. Park uploads and join active ownership before capture; allow local publication during network requests; retry concurrent changes; complete live state after durable commit despite caller cancellation. Add causal tests against real stores, then connect HTTP recovery and projection reconstruction. Preserve pending work, identities, cursor, inbox, terminal outcomes and quarantine across restart. |
| P1 | Complete dependency injection | Verify provider lifetimes, named-stream initialization, typed keys, generated schema registration, borrowed ownership, redacted logging and visible startup failures. The accepted baseline passes 24 net8 tests with original coverage of 280/327 lines and 77/112 branches. Strengthened functional tests remain ungated. The user approved a narrow PSH1021 correction for finalization tests. The analyzer correction passes 30 focused tests and 3,888 PerformanceSharp tests. Close its remaining coverage gaps and validate the local package, including qualified helper calls, escaped method groups and nested executable scopes. Then run the causal fault-observation regression and complete all original coverage and framework gates. Reconcile the latest public context dependency before signed integration. |

## Example applications

| Application | Remaining work |
| --- | --- |
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
| `Primitives-oc-dependency-injection` | Verify the strengthened lifetime, registry, serialization and logging tests. Verify the approved finalization-test analyzer correction, reconcile context, complete coverage and framework gates, then integrate. |
| `Primitives-oc-collaboration-client` | Public-context two-client example and real HTTP/SQLite end-to-end test fixture; source implementation is in progress. |
| `Primitives-oc-client-recovery` | Atomic client runtime recovery orchestration and real-store tests; isolated borrowed dependency baseline recorded for later reconciliation. |
| `Primitives-oc-recovery-intent-roles` | Add bounded, distinct accepted-replay intent validation so snapshot recovery cannot double-apply acknowledged edits. Core proposal and regression tests are under review. |
| `Primitives-oc-public-context` | Close remaining lifecycle and builder coverage gaps after the passing 1,179-test gate; complete all framework verification, then integrate. |
| `Primitives-oc-engine` | Finish scheduling, retry, lifecycle and reconciliation verification; close original coverage gaps and complete every framework gate before integration. |
| `Primitives-oc-http-replay` | Client/endpoint replay authentication integration and end-to-end tests. |

Separate physical cleanup remains for the integrated, unregistered `Primitives-oc-coverage-gate` directory (signed commit `a28276a2`, source branch deleted). Git hit a path-length limit and automatic approval review blocked its recursive deletion. Cleanup also remains for the integrated, unregistered `Primitives-oc-sqlite-main-compat` directory (signed commit `851f15a2`, source branch deleted) and the unregistered `Primitives-oc-example-lab`, `Primitives-oc-completion-proof9704de1` and `Primitives-oc-memory-snapshot-recovery` directories. Their source/evidence is archived. The recovery worktree was integrated and unregistered, but Git encountered a Windows path-length error; automatic approval review then blocked removal of its residual directory. Automatic approval review also blocked deletion of the other two directories. The SQLite worktree likewise hit a Windows path-length error during Git removal, and automatic approval review blocked the follow-up directory deletion. These blocks have not been bypassed.
