# ReactiveUI.Primitives.OccasionallyConnected remaining tasks

This list is limited to requirements from [ReactiveUI.Primitives.OccasionallyConnected.md](ReactiveUI.Primitives.OccasionallyConnected.md) that are not complete in the current source and test evidence. Core contracts, validation models, serializers, bounded primitives, retry/circuit-breaker components, capability negotiation, observer dispatch, server journals, prepared transport primitives, conflict provenance, subscription starting positions, upload coordination, and client CRDT contracts have been implemented and are not repeated here.

## Runtime composition

- [x] Implement the public builder, context, stream factory, and synchronization engine composition described in sections 6, 7, 13, and 16. Wire the existing local commit, upload, receive, retry, circuit-breaker, scheduler, observer, and capability components through one lifecycle.
  - The builder configures options with `ConfigureOptions`, which takes a transform of the current options.
  - The engine starts offline when the first connection fails with a transient error. A background loop then reconnects through the retry policy and the endpoint circuit breaker.
  - While disconnected, `SyncStates` reports `Offline`, `Connecting`, or `Faulted` with `RetryAfter` and a stable reason code.
  - `TriggerSyncAsync` starts an immediate reconnect attempt.
  - Evidence: `SyncEngineTests.OfflineStartup*.cs` and `OccasionallyConnectedBuilderTests.OfflineStartup.cs`. The second test starts a SQLite-backed context offline, commits a write, reconnects, and synchronizes the write. The runtime suite passes 1549/1549 on `net8.0`, `net9.0`, and `net10.0`.
- [ ] Complete durable admission, `stream.Input`, `IRemoteObserver<T>`, upload/result/status handling, remote projection, and terminal local-failure routing. Verify count and byte limits, cancellation, disposal, and every supported buffer strategy at the public API boundary.
- [x] Integrate the context with DI/hosting, health, logging, metrics, trace propagation, and graceful shutdown. Keep the core independent of Microsoft.Extensions dependencies.
  - Done: `OccasionallyConnectedHealth.Evaluate` maps a `SyncState` to a health report. The report holds a `Healthy`, `Degraded`, or `Unhealthy` status (section 14.4), counts, age, and a reason code. It needs no Microsoft.Extensions dependency.
  - Done: the `ReactiveUI.Primitives.OccasionallyConnected.Hosting` package. `AddOccasionallyConnectedHosting()` registers a hosted service. The service starts the context with the host and stops it gracefully on shutdown. `AddHealthChecks().AddOccasionallyConnected()` adds a health check that reports status, counts, age, and reason code. Evidence: `ReactiveUI.Primitives.OccasionallyConnected.Hosting.Tests` passes 6/6 on `net8.0` through `net11.0`.
  - Done: the HTTP transport propagates W3C trace context. The client adds `traceparent` and `tracestate` from `Activity.Current` unless the caller already set them. The server endpoint starts an `oc.transport.server` activity whose parent is the incoming context. It ignores repeated, oversized, or malformed headers. Payloads never go into headers or spans. Evidence: `HttpRemoteTransportAdapterTests.TraceContext.cs` and `HttpServerEndpointTests.TraceContext.cs`. The HTTP transport suite passes 775/775 on `net8.0` and `net10.0`.
  - Logging comes from the existing `OccasionallyConnectedLoggerBridge` in the DI package. Metrics come from the core `Meter`. The core package still has no Microsoft.Extensions dependency.

## Server and transport integration

- [ ] Deliver the public server hub and HTTP endpoints. Connect authenticated authorization, subscription admission, initial positions, replay paging, receive offers and acknowledgements, idempotency, canonical cursors, conflict resolution, and durable server effects.
- [ ] Compose CRDT resolvers and domain materializers on the server and prove canonical events, provenance, and rejection behaviour through the public hub.
- [ ] Finish shared store and transport conformance suites for every advertised capability. Include cursor gaps, duplicate and reordered delivery, dropped acknowledgements, partial results, streaming receive, and unsupported-capability startup failures.

## Durability and delivery guarantees

- [ ] Run the complete crash matrix from section 17.2 across SQLite and every supported store path: serialization, local commit, enqueue, upload, server apply/ACK, local ACK commit, remote apply, inbox notification, compaction, migration, disk-full, corruption, and process termination.
- [ ] Prove restartable migrations, ownership coordination, authenticated encryption at rest, quarantine/dead-letter recovery, compaction, and retention without losing data required to rebuild snapshots or resolve pending operations.
- [ ] Complete end-to-end at-most-once, at-least-once, and capability-gated exactly-once-effect behaviour, including retention expiry, explicit downgrade, ambiguous outcomes, and server idempotency. The current components validate capabilities but do not yet prove the complete application path.

## Protocol, security, and compatibility

- [ ] Complete protocol-v1 golden fixtures and cross-version upcast/migration tests for wire envelopes, store schemas, snapshots, cursors, and operation results.
- [ ] Complete application-level security tests for authenticated tenant/client binding, nonce and replay handling, authorization, stale credentials, tampering, path traversal, SQL metacharacters, oversized/deep payloads, decompression limits, and redacted diagnostics.
- [ ] Add adapter-specific protocol fuzzing and verify that transport adapters do not introduce hidden unbounded retries.

## Examples and release gates

- [ ] Run the section 16 samples against the freshly packed public packages produced for every supported target framework. Demonstrate offline startup, optimistic writes, reconnect and restart recovery, conflict reconciliation, `PublishAsync`, observer input, and operation synchronization.
- [ ] Add the remaining ResilienceLab demonstrations for duplicate/reordered delivery, capability downgrade, backpressure, slow observers, corruption/quarantine, and retention-gap recovery. The deterministic retry/backoff demonstration is now implemented and covered by 103 passing TUnit tests.
- [ ] Complete the quality gates in section 17.4: full transition/invariant coverage, mutation testing, child-process crash tests, and bounded throughput/allocation/recovery/compaction/slow-observer soak measurements. The supported framework builds and API baselines have passed; the remaining gates need their independent reports.
- [ ] Complete the remaining release gates from section 18: clean-project pack/install tests, deterministic package comparison, Source Link and symbol-package verification, trimming/NativeAOT smoke tests, SBOM and dependency/license/security scans, and scheduled cross-platform crash/soak/performance jobs. All six feature packages now pack successfully for `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`.

## Final release acceptance

- [ ] Freeze the public API, protocol v1, store schema v1, and compatibility policy only after the preceding gates pass.
- [ ] Produce the preview/RC package set and verify clean-project installation, upgrade/rollback guidance, security reporting, and the single final feature PR.
