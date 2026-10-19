# Remaining OccasionallyConnected framework work

Updated **19 October 2026** for [PR 229](https://github.com/reactiveui/Primitives/pull/229).
Baseline: `239916db4c52f87f0eacd26b7854e10b43fb5773`. This working revision makes approved breaking changes before release.
See [review reconciliation](GlennReviewReconciliation.md) for all 123 threads.

## Framework scope

This is a framework of components, not a finished synchronization product.
The [original specification](ReactiveUI.Primitives.OccasionallyConnected.md), sections 2, 3, 6.2, 15.1 and 21, sets that boundary.
Applications compose stores, transports, serializers, state reducers, authorization and conflict policies.
The framework owns ordering, bounded work, durable transaction boundaries and observable failure reporting.

Applications own domain rules, credentials, deployment topology, backup policy, operating-system permissions and background scheduling.
Reference adapters must expose the hooks those applications need. They must not silently choose host policy.
Items 1–15 retain their source and regression evidence in [GlennReviewTracking.md](GlennReviewTracking.md).
Source completion, reviewer approval, release publication and device validation are separate states.
The original review-list numbers remain stable.

## Required correctness and contract work

| Item | State | Remaining acceptance | Spec sections |
| --- | --- | --- | --- |
| 16. Platform durability | Open | Add Apple full/checkpoint flush and Unix directory synchronization after replacement. Retry only Windows sharing/lock violations within a finite deadline. Mount-aware SQLite ownership is present; retain tests and validate normal device paths. | 5.1, 7.7, 17.2 |
| 17. LiteDB capacity and AOT | Open | Bound the actual serialized document before durable mutation, including snapshots/history. Verify a clean trimmed/native-AOT consumer and state unsupported targets honestly. | 10.2, 17.0, 17.4 |
| 18. Clock resilience | Open | Use monotonic elapsed time during a process lifetime. Define restart behavior for UTC deadlines. Test clock jumps, leases and recoverable replay skew without weakening checks. | 5.3, 9.3, 12.2, 17.2 |
| 19. Browser storage | Open | Request persistence and report its outcome. Classify quota failure and document eviction risk. Reject unsupported blocking SQLite workers before starting them. | 5.1, 7.7, 12.2, 17.0 |
| 20. Lifecycle policy | Partial | Distinguish hidden, frozen and terminated pages. Expose bounded mobile cancellation/drain policy. Keep OS background scheduling in optional host adapters. | 6.2, 9.1, 17.2 |
| 21. Transport configuration | Partial | Socket configuration runs before connection. Fast empty HTTP polls use a cancellable minimum elapsed interval; data and normal long polls add no wait. Prove WebSocket liveness on supported targets. The configuration hook does not supply a server. | 7.8, 9.3, 12.2 |
| 22. SignalR isolation | Implemented | Retain both registration-order, unrelated hub-option and host-resolver tests. Pair clients/servers using the private carrier protocol. HTTP carrier redesign remains item 32. | 2, 6.2, 7.8, 17.0 |
| 23. Replay protocol | Partial | Unsigned signing input no longer needs a temporary proof. Signed requests retain strict validation and real MAC headers. Bound lookup. Define shared replay-store or single-node support, trusted clock correction and secret handling. | 8.3, 12.1–12.2, 17.2 |
| 24. Payload work/equality | Open | Reduce copies, parsing and hashes while retaining ownership and integrity. Define structural payload equality and measure commit/observer paths. | 5.4, 8.1, 10.4, 17.4 |
| 25. Scheduling/waiting | Partial | Retain prior server scheduling and delay fixes. Remove per-subscriber task hops. Reuse matching repository signals, sequencers and bounded channels. Use keyed operation waiters rather than broadcast observer subscriptions. | 5.4, 10.1–10.4, 17.2 |
| 26. Public contracts/defaults | Partial | Constructible defaults and consistent records are revised. Audit every advertised option for enforcement. Public extensions must work through public interfaces. Validate every TFM baseline and packed consumer. | 7, 13.2, 18.4 |
| 27. DI/startup/health | Partial | Terminal errors latch unhealthy state; observer injection is removed. DI uses container abstractions. Startup logs remain redacted by default; hosts can opt in to the original exception. Replace blocking stream resolution with explicit asynchronous initialization or preinitialized registration. | 9.1, 13.3, 14.4 |
| 28. Configuration/telemetry | Partial | Host sampling, optional meter factories and second-based duration metrics are implemented. Startup exceptions remain opt-in. Add optional binding and eager validation in integration packages. | 12.3, 13.2–13.3, 14.1–14.3 |
| 29. Release/test maintenance | Partial | Verify symbols for the full feed. README selection now uses the package-family prefix. Ignored parallelism keys are removed without changing coverage filters. Scope release gates; justify unrelated span edits. Apply repository analyzer/version policy to CI tools. | 15.3, 17.4, 18.1–18.4 |
| 30. Release claims | Open | Recount packages/public types/suites after revisions. Update PR/release claims from committed evidence only. Distinguish handwritten/generated/aggregate/Sonar coverage. State finite deduplication and encryption limits. | 5.3, 12, 15.1, 18.2 |

## Approved breaking framework revisions

The user approved breaking changes on 13 October 2026. Approval permits deliberate contract changes; it does not complete unimplemented work.

| Item | Revision and remaining acceptance | Spec sections |
| --- | --- | --- |
| 31. Public API | Use plain-English mergeable names, valid defaults and consistent records. Keep the retry-policy extension point, not public concrete resilience or test randomness. Prefer version 7 GUIDs where supported. Finish Maximum limits, Publish/Upload, status records, migrations, unreadable payloads, snapshot builders, server updaters, external ownership and send-attempt names. Move provider-specific failures and sibling-only plumbing out of neutral contracts. Keep mappings, baselines and samples synchronized. | 2, 6.2, 7, 18.3 |
| 32. Composition and packages | Separate SQLite server persistence from neutral coordination. Expose one asynchronous atomic persistence contract, not four unrelated synchronous journals. Mobile/Web must accept any conforming store without requiring a provider package. Reuse neutral server validation/authorization across transports. Replace the SignalR HTTP tunnel with direct operations/streaming or omit it from first release. Decide database encryption/key rotation separately from external rollback proof. Publish the versioned wire contract and vectors before claiming other-language interoperability. | 2, 3, 6.2, 7.7–7.9, 8.3, 12, 15.1 |
| 33. Review reconciliation | Give each thread a tested fix, source-backed explanation or explicit deferral. Track GitHub resolution separately. Retain prior invariants through API changes. Run framework/package/AOT/platform gates on the final committed revision before rereview. | 17, 18, 20 |

## Optional later adapters

These are extension opportunities, not automatic first-release blockers:

- MQTT, gRPC and Server-Sent Events, proved by transport conformance.
- CloudEvents bindings and negotiated binary/compressed content.
- Splat integration and host connectivity, metering and battery policy.
- Browser service workers and Background Sync.
- Apple background tasks and Android WorkManager integration.

Adapters must preserve ordering, cancellation, bounds and capability contracts. Broker delivery guarantees do not replace the application outbox or atomic server idempotency.

## Implementation order

1. The upstream analyzer correction is merged and consumed through version 5.1.4. `MergeableInput` and `MergeableChange` now store one closed case instead of mutable tag/payload bags. Finish the remaining public-contract revision and behavioral regressions across all targets.
2. Separate provider/platform ownership and freeze neutral server persistence.
3. Fix platform durability, clock/replay safety, browser persistence and LiteDB bounds.
4. Reduce hot-path work and finish optional hosting/configuration integration.
5. Reconcile wire interoperability, package inventory, release claims and final review evidence.

Each implemented item requires fresh relevant TUnit results. Package/protocol changes require baseline and clean-consumer checks. A passing unrelated CI job does not close a review thread.
