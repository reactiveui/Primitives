# OccasionallyConnected remaining tasks

Updated: 24 September 2026, 00:10 UTC. The integrated local feature is at signed commit `e96e8dbc`. All registered donor worktrees and local `CP_*` branches have been retired after source preservation and signed integration. Only the main checkout on `OccasionallyConnected` remains. Unrelated local files, solution ordering edits, and user stashes are preserved. Nothing was pushed.

The client passes 72/72 tests with 97.57% line and 90.54% branch coverage. ResilienceLab passes 102/102 tests with 99.35% line and 92.83% branch coverage. The server example passes 138/138 tests; its coverage is 988/1,021 lines and 325/344 branches (96.77% and 94.48%). Runtime passes 1,522/1,522 tests with 99.05% line and 98.05% branch coverage; its report has 102 uncovered sequence points and 83 uncovered branches. The feature still needs the acceptance work below.

The final 25-path donor audit is resolved. Joined resolver/domain envelope and provenance assertions pass in the 138-test server suite and are signed. Five old unregistered physical directories remain blocked from deletion by prior automatic review.

## Remaining integration work

| Priority | Remaining task | Acceptance condition |
| --- | --- | --- |
| P0 | Complete original coverage gates | Cover remaining reachable handwritten paths in runtime and applications. Retain original framework reports and exact source/binary evidence for generated or unreachable residuals; add no suppressions or exclusions. |
| P0 | Complete framework and application matrix | Run remaining .NET 8–11, legacy framework, and application dependency checks. Preserve current signed integration and record exact binaries and reports. |
| P0 | Complete packaging and solution gates | Run full solution gates, finish CI integration, verify freshly packed examples (including DurableOutbox), and complete supported trimming/NativeAOT checks. |
| P0 | Make analyzer dependencies reproducible | SST2338 and PSH1021 fixes currently use local analyzer packages. Arrange separately authorized analyzer publication and use released dependencies reproducible in CI. Add no suppressions. |
| P0 | Complete Windows CI acceptance | Investigate SQLite extended error 1546 during server recovery and concurrent initial client identity binding if either recurs. Do not mask failures with retries. Obtain passing final cross-platform CI when publication is authorized. |
| P1 | Complete ResilienceLab demonstrations | Add and verify remaining runnable scenarios for duplicate/reordered delivery, retry/backoff, capability downgrade, backpressure, slow observers, corruption/quarantine, and retention-gap recovery. |
| P1 | Complete additional acceptance scenarios | Cover the remaining delivery, persistence, concurrency, recovery, security, and bounded performance/soak cases listed below. |

## Example applications

| Application | Current evidence and remaining work |
| --- | --- |
| `OccasionallyConnected.Collaboration.Client` | 72/72 tests; 97.57% line and 90.54% branch coverage. Complete remaining failure-path coverage and framework checks. |
| `OccasionallyConnected.ResilienceLab` | 102/102 tests across three runs; 99.35% line and 92.83% branch coverage. Add the remaining runnable demonstrations listed above. |
| Server example | 138/138 tests; 988/1,021 lines and 325/344 branches covered. Complete remaining coverage and framework checks. The changed resolver has 24/24 lines and 18/18 branches covered. |
| Runtime | 1,522/1,522; 99.05% line and 98.05% branch coverage. The frozen runtime residual handoff is hash-verified (SHA-256 `E9939BF194384F5686B080F69BC0F41B1F686BD5C7597DC6A32E9A6832DF2EC3`) at `artifacts/occasionally-connected/root-runtime-integration-20260923/runtime-residual-handoff.md`. |
| All examples | Finish CI integration, public API instructions, packed-package runs, applicable trimming/NativeAOT, and framework matrix checks. Run full solution gates with the registered example projects. |

## Final acceptance

- [ ] Run two independent durable clients through offline publication, live disconnect/reconnect, server restart, client restart, and eventual convergence.
- [ ] Complete the delivery-guarantee failure matrix at serialization, local commit, enqueue, upload, server apply/ACK, local ACK commit, remote apply, notification, compaction, and migration boundaries.
- [ ] Prove exactly-once effects within declared retention, capability downgrade/fail-closed behavior, and explicit ambiguous outcomes.
- [ ] Verify `PublishAsync`, observer bridges, and `stream.Input` under concurrent producers, count/byte limits, cancellation, and each supported buffer strategy.
- [ ] Verify subscription/cursor continuity, duplicate suppression, atomic snapshot recovery, replay order, and projection/notification consistency after restart.
- [ ] Exercise tenant/session substitution, stale/replayed requests, corrupt payload/hash/schema, oversized messages, expired credentials, clock skew, and redacted diagnostics through application paths.
- [ ] Complete shared transport conformance and packed-package verification of the reviewed shared storage suite against the [compatibility matrix](OccasionallyConnected.Compatibility.md).
- [ ] Measure throughput, allocations, large-outbox recovery, compaction, and slow-observer isolation. Run bounded soak and reconnect/retry scenarios.
- [ ] Build libraries for `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`. Complete applicable TUnit tests, public API checks, and solution gates.
- [ ] Verify 100% reachable handwritten line and branch coverage from each original framework report. Report compiler/generated residuals separately against exact tested binaries. Add no suppressions or exclusions, and do not merge reports to hide missing conditions.
- [ ] Review remaining feature changes and evidence and create signed local commits on `OccasionallyConnected`.
- [ ] Prepare the single final PR only after the entire feature passes acceptance.

## Worktrees and local cleanup

No registered donor worktrees or local `CP_*` branches remain. Continue all work on the existing `OccasionallyConnected` checkout. Source archives and verification evidence remain under `artifacts/occasionally-connected`.

Physical cleanup remains blocked for five integrated, unregistered directories: `Primitives-oc-coverage-gate`, `Primitives-oc-sqlite-main-compat`, `Primitives-oc-example-lab`, `Primitives-oc-completion-proof9704de1`, and `Primitives-oc-memory-snapshot-recovery`. Earlier removals encountered Windows path-length errors, and automatic approval review rejected later deletion attempts. Keep these directories and preserve user stashes and unrelated local changes.
