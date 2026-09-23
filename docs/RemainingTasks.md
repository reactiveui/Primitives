# OccasionallyConnected remaining tasks

Updated: 23 September 2026. Local feature branch HEAD: `3804675d`. The feature is not ready for release. Follow [the design](ReactiveUI.Primitives.OccasionallyConnected.md). Verification history and continuation notes are in [CURRENT.md](../artifacts/occasionally-connected/CURRENT.md) and its linked archives.

The root verified GPG signatures for `6fd9c4a` (retry enum), `ec373c00` (store conformance), `998660ad` (71-file HTTP integration) `48b9c327` (example authorizer) and `3804675d` (atomic outbox capacity). The store-conformance, HTTP and producer-matrix donor worktrees were retired after exact-source checks. Seven registered donor worktrees remain. Finish and retire the existing donor worktrees, then continue directly on `OccasionallyConnected`; create no new worktrees. Use GPT-6-Astra for orchestration and Reactive Multi-Agent assignments with GPT-6-Sol for complex work and GPT-6-Luna for smaller tasks. Keep integration local and do not push before final acceptance.

## Runtime and package integration

| Priority | Remaining task | Acceptance condition |
| --- | --- | --- |
| P0 | Finish shared-session renewal | Complete combined lifecycle and original coverage gates. Prove concurrent stale responses share one renewal, repeated expiry without durable progress stops, authentication failures remain terminal, and operation IDs, retry anchors and cursors survive renewal. Cover at-most-once ambiguity, capability downgrade, cancellation, retired-session disposal and bounded shutdown. |
| P0 | Finish context diagnostics | The clean r24 gate passed lease controls 3/3, context controls 14/14, and the full suite 1,329/1,329 with 97.3% line coverage. A reconstructed legacy double-count reproduction informed the restored source fix; keep its reconstructed status clear. Add the remaining reachable scheduler-rejection test, then complete original coverage. Preserve one-time typed commits, accurate global queue totals and publication metrics, bounded deferred observer delivery, scheduler routing, slow/throwing observer isolation, overflow and disposal behavior. |
| P0 | Complete snapshot recovery with context | Combined build 7 is clean. The focused engine gate passed 263/263. The original full net8 run passed 1,393 of 1,397 tests, with four failures: two expected global-context failures, a stale observable fixture corrected source-only, and an oversized-reason-code byte-budget bug corrected source-only. Rerun the full suite after fixes. Add the source-ready snapshot-expired-upload-renewal regression and establish its actual RED before accepting it. |
| P0 | Complete public outbox and producer acceptance | Verify the combined context against atomic memory/SQLite admission. Cover shared count/byte limits, blocked publisher limits, cancellation refunds, cross-stream wakeups after durable capacity release, late release after unregister, every supported volatile policy and observer/input producer paths. Preserve durable work and verify truthful queue diagnostics. |
| P1 | Integrate convenience APIs | Source compile fixes are ready, but the actual RED gate has not run. Reconcile helpers with context admission, participants and telemetry. Cover paired state/queue notifications after commits, ACKs, dead-letter changes and snapshot recovery; `ObservePending` reaching zero; `WhereSynchronized` after ACK; replay, mutable-state isolation, wrapper lifecycle and bounded observer ownership. Run combined framework and coverage gates. |
| P1 | Integrate dependency injection | Reconcile its public-context dependency, verify the combined source, create the signed local integration and retire its donor. |
| P0 | Finish HTTP replay application tests | The 71-file HTTP integration is signed and its donor retired. Run the example authorization adapter tests against the complete application suite and close any remaining integration checks. |
| P0 | Make analyzer dependencies reproducible | SST2338 and PSH1021 fixes currently use local analyzer packages. Arrange separately authorized publication from the analyzer repository and use released dependencies reproducible in CI. Add no suppressions. |
| P0 | Resolve final Windows CI acceptance | Investigate SQLite extended error 1546 during server recovery and concurrent initial client identity binding if either recurs. Do not mask either with retries or weaker durability. Obtain passing final cross-platform CI when publication is authorized. |

## Example applications

| Application | Remaining task |
| --- | --- |
| `OccasionallyConnected.Collaboration.Client` | Reconcile the complete public context. Pass real HTTP server-restart, client-reopen, offline convergence and the 10,001st-operation capacity regression using separate SQLite databases. Verify saved subscription/cursor identity, stable operation IDs, duplicate suppression, cancellation and slow observers. Finish CLI state/operation/fault output with secret-free diagnostics. Complete framework coverage and local integration. |
| `OccasionallyConnected.ResilienceLab` | r8 build is active after analyzer fixes. Prove the real HTTP lost-ACK scenario reaches runtime GREEN, including server apply before response loss, pending work across client restart, same-ID retry and one durable server effect. Add runnable real-runtime demonstrations of duplicate/reordered delivery, retry/backoff, capability downgrade, backpressure, slow observers, corruption/quarantine and retention-gap recovery. Test every scenario through the application entry point. |
| All examples | Complete public-API instructions and solution/CI wiring. Build and run against freshly packed packages, including DurableOutbox. Verify trimming and NativeAOT where supported. Complete example TUnit suites and original handwritten coverage on .NET 8–11. |

## Final acceptance

- [ ] Run two independent durable clients through offline publication, live disconnect/reconnect, server restart, client restart and eventual convergence.
- [ ] Complete the delivery-guarantee failure matrix at serialization, local commit, enqueue, upload, server apply/ACK, local ACK commit, remote apply, notification, compaction and migration boundaries.
- [ ] Prove exactly-once effects within declared retention, capability downgrade/fail-closed behavior and explicit ambiguous outcomes.
- [ ] Verify `PublishAsync`, observer bridges and `stream.Input` under concurrent producers, count/byte limits, cancellation and every supported buffer strategy.
- [ ] Verify combined subscription/cursor continuity, duplicate suppression, atomic snapshot recovery, replay order and projection/notification consistency after restart.
- [ ] Exercise tenant/session substitution, stale/replayed requests, corrupt payload/hash/schema, oversized messages, expired credentials, clock skew and redacted diagnostics through application paths.
- [ ] Complete shared transport conformance and packed-package verification of the reviewed shared storage suite. Run the fixtures in the [compatibility matrix](OccasionallyConnected.Compatibility.md) against final packages.
- [ ] Measure throughput, allocations, large-outbox recovery, compaction and slow-observer isolation. Run bounded soak and reconnect/retry scenarios.
- [ ] Build libraries for `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48` and `net481`. Complete applicable TUnit tests, public API checks and solution gates.
- [ ] Verify 100% reachable handwritten line and branch coverage from each original framework report. Report compiler/generated residuals separately against exact tested binaries. Add no suppressions or exclusions. Do not substitute merged reports that lose condition data.
- [ ] Independently review integrated source and evidence, create signed local commits, and retire completed worktrees and obsolete source branches.
- [ ] Prepare the single final PR only after the entire feature passes acceptance.

## Worktrees and local cleanup

Seven registered donor worktrees remain. Preserve unfinished source and evidence until each integration is reviewed and signed.

| Worktree | Remaining reason to retain it |
| --- | --- |
| `Primitives-oc-engine` | Combined renewal behavior, original coverage and integration. |
| `Primitives-oc-public-context` | Global diagnostics fixes, combined runtime verification and signed integration. |
| `Primitives-oc-client-recovery` | Snapshot-expiry regression, combined context tests and cross-feature proof, then integration. |
| `Primitives-oc-dependency-injection` | Context reconciliation, signed integration and retirement. |
| `Primitives-oc-collaboration-client` | Real application acceptance and observability examples. |
| `Primitives-oc-resilience-http` | Real durable HTTP lost-ACK implementation and tests. |
| `Primitives-oc-convenience-extensions` | Combined context integration and meaningful RED/GREEN tests. |

Signing is working. The verified signed commits are listed at the top of this file. Preserve the local-only workflow and do not push before final acceptance.

Physical cleanup remains for five integrated, unregistered directories: `Primitives-oc-coverage-gate`, `Primitives-oc-sqlite-main-compat`, `Primitives-oc-example-lab`, `Primitives-oc-completion-proof9704de1` and `Primitives-oc-memory-snapshot-recovery`. Their source/evidence is archived. Earlier Git removals encountered Windows path-length errors, and automatic approval review rejected subsequent deletion attempts. Keep this cleanup blocked until an approved path is available. Preserve user stashes and unrelated local changes.
