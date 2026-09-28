# ReactiveUI.Primitives.OccasionallyConnected remaining tasks

This list is limited to requirements from [ReactiveUI.Primitives.OccasionallyConnected.md](ReactiveUI.Primitives.OccasionallyConnected.md) that are not complete in the current source and test evidence. Core contracts, validation models, serializers, bounded primitives, retry/circuit-breaker components, capability negotiation, observer dispatch, server journals, prepared transport primitives, conflict provenance, subscription starting positions, upload coordination, and client CRDT contracts have been implemented and are not repeated here.

## Runtime composition

- [x] Implement the public builder, context, stream factory, and synchronization engine composition described in sections 6, 7, 13, and 16. Wire the existing local commit, upload, receive, retry, circuit-breaker, scheduler, observer, and capability components through one lifecycle.
  - The builder configures options with `ConfigureOptions`, which takes a transform of the current options.
  - The engine starts offline when the first connection fails with a transient error. A background loop then reconnects through the retry policy and the endpoint circuit breaker.
  - While disconnected, `SyncStates` reports `Offline`, `Connecting`, or `Faulted` with `RetryAfter` and a stable reason code.
  - `TriggerSyncAsync` starts an immediate reconnect attempt.
  - Evidence: `SyncEngineTests.OfflineStartup*.cs` and `OccasionallyConnectedBuilderTests.OfflineStartup.cs`. The second test starts a SQLite-backed context offline, commits a write, reconnects, and synchronizes the write. The runtime suite passes 1549/1549 on `net8.0`, `net9.0`, and `net10.0`.
- [x] Complete durable admission, `stream.Input`, `IRemoteObserver<T>`, upload/result/status handling, remote projection, and terminal local-failure routing. Verify count and byte limits, cancellation, disposal, and every supported buffer strategy at the public API boundary.
  - Done: tests through the public context cover count and byte limits, cancellation before and after commit, disposal, and the rejected strategies. They exercise `PublishAsync`, `stream.Input`, and `IRemoteObserver<T>` (`OccasionallyConnectedBuilderTests.PublicAdmission*.cs` and `OccasionallyConnectedBuilderTests.PublicInput.cs`).
  - Done (section 10.2):
    - `PublishAsync` with `DropOldest` dead-letters the oldest pending non-durable operation of the stream with reason `OC.Overflow.DroppedOldest`. It emits `OC.Stream.OutboxOverflow` and the `oc.queue.overflow` metric, and `Local` excludes the evicted operation.
    - `DropNewest` rejects the new operation and emits the same fault and metric.
    - `Custom` calls an `IBufferOverflowPolicy` registered with `UseBufferOverflowPolicy` on the builder or the DI builder. The policy sees metadata only, and it cannot select a durable, leased, or blocked operation.
    - A `DropOldest` eviction on the input bridge now emits `OC.Stream.InputOverflow`.
  - Evidence: `OccasionallyConnectedBuilderTests.Overflow*.cs`, the DI tests, and the metrics tests. The runtime suite passes 1593/1593 on `net10.0`.
  - Limits:
    - Eviction takes operations only from the publishing stream.
    - Eviction needs a lease on the stream's prefix up to the chosen operation. If the head is in flight, waiting on retry backoff, or blocked, the publish fails with `QueueCapacityExceededException`.
    - Stream definitions still reject `Custom`. Only per-call publish options accept it.
- [x] Integrate the context with DI/hosting, health, logging, metrics, trace propagation, and graceful shutdown. Keep the core independent of Microsoft.Extensions dependencies.
  - Done: `OccasionallyConnectedHealth.Evaluate` maps a `SyncState` to a health report. The report holds a `Healthy`, `Degraded`, or `Unhealthy` status (section 14.4), counts, age, and a reason code. It needs no Microsoft.Extensions dependency.
  - Done: the `ReactiveUI.Primitives.OccasionallyConnected.Hosting` package. `AddOccasionallyConnectedHosting()` registers a hosted service. The service starts the context with the host and stops it gracefully on shutdown. `AddHealthChecks().AddOccasionallyConnected()` adds a health check that reports status, counts, age, and reason code. Evidence: `ReactiveUI.Primitives.OccasionallyConnected.Hosting.Tests` passes 6/6 on `net8.0` through `net11.0`.
  - Done: the HTTP transport propagates W3C trace context. The client adds `traceparent` and `tracestate` from `Activity.Current` unless the caller already set them. The server endpoint starts an `oc.transport.server` activity whose parent is the incoming context. It ignores repeated, oversized, or malformed headers. Payloads never go into headers or spans. Evidence: `HttpRemoteTransportAdapterTests.TraceContext.cs` and `HttpServerEndpointTests.TraceContext.cs`. The HTTP transport suite passes 775/775 on `net8.0` and `net10.0`.
  - Logging comes from the existing `OccasionallyConnectedLoggerBridge` in the DI package. Metrics come from the core `Meter`. The core package still has no Microsoft.Extensions dependency.

## Server and transport integration

- [x] Deliver the public server hub and HTTP endpoints. Connect authenticated authorization, subscription admission, initial positions, replay paging, receive offers and acknowledgements, idempotency, canonical cursors, conflict resolution, and durable server effects.
  - Evidence: public `ServerStreamHub` tests in `ServerStreamHubTests.Idempotency.cs`, `.LastWriterWins.cs`, `.ReplayPaging.cs`, and `.StartPositions.cs`.
    - A repeated operation ID returns the original result, with one domain call and one event, including after SQLite reopens. A changed payload under the same ID is rejected.
    - Last-writer-wins resolves stale writes the same way every time.
    - Replay pages by event count and logical bytes, and resumes from the acknowledged cursor after reopen.
    - `Latest`, `FromSequence`, `FromTimestamp`, and `FromCursor` all start in the right place. Cursors from another tenant or stream, or ahead of the stream, are rejected without leaking events.
  - `HttpRemoteTransportAdapterTests.ServerStreamHub.cs` drives the HTTP adapter through `HttpServerEndpoint` into a SQLite hub, including after the hub reopens. The server suite passes 560/560 and the HTTP suite 776/776 on `net10.0`.
- [x] Compose CRDT resolvers and domain materializers on the server and prove canonical events, provenance, and rejection behaviour through the public hub.
  - Evidence: `ServerStreamHubTests.Crdt.cs`. The tests use a SQLite-backed `ServerStreamHub` through its public methods only, with the public CRDT resolver, version factory, initial-state factory, and domain handler. They prove these five behaviours:
    - Canonical events keep increasing cursors and stay identical after the hub reopens.
    - `Origin` and `CausedByOperationId` match the authenticated client.
    - Hash, payload, contract, and state-kind mismatches are rejected with no event or state change.
    - A snapshot from a CRDT materializer resumes with no duplicates.
    - A duplicate operation replays its original result after the hub reopens.
  - The server test suite passes 545/545 on `net8.0` and `net10.0`.
- [x] Finish shared store and transport conformance suites for every advertised capability. Include cursor gaps, duplicate and reordered delivery, dropped acknowledgements, partial results, streaming receive, and unsupported-capability startup failures.
  - Done: `ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests` runs `IRemoteTransportAdapterTests` across Loopback and HTTP, each against in-memory and SQLite `ServerStreamHub`. A fault-injecting hub produces duplicate, reordered, dropped-ACK, and truncated-result cases.
    - Each case is gated on an advertised capability. A table test checks that every advertised transport flag and store flag has a test.
    - `ILocalStoreAdapterTests.DurableInbox.cs` covers deduplication across a restart.
    - The suite passes 50 cases and skips 6 capability-gated ones on `net8.0` and `net10.0`.
  - Retention gaps reach snapshot recovery. The hub raises `RemoteSubscriptionRetentionGapException` with the request's stream, subscription and cursor and the reason code `server-receive-retention-gap`.
    - Loopback passes the exception through. It also serves snapshot recovery when you set `SnapshotRecoveryHub`.
    - HTTP answers a gap with a bodyless `410 Gone`. The client turns it back into the same exception.
    - Expired, ahead and foreign cursors all get the same answer, so the answer reveals nothing about retained history.
    - `RetentionGapRecoveryScenarioTests` proves that a context over HTTP or Loopback recovers on its own from a SQLite hub whose retention expired, then keeps receiving.
  - The HTTP endpoint surfaces a canceled long-poll as `OperationCanceledException`. It no longer answers with a 500. A cancellation the client did not ask for becomes one ambiguous failure, and the engine retries it with backoff.

## Durability and delivery guarantees

- [ ] Run the complete crash matrix from section 17.2 across SQLite and every supported store path: serialization, local commit, enqueue, upload, server apply/ACK, local ACK commit, remote apply, inbox notification, compaction, migration, disk-full, corruption, and process termination.
  - Done: an internal crash-point seam (`ISqliteCommitFaultPoint`, `ISqliteServerCommitFaultPoint`) lets a child process block inside a SQLite transaction until the parent kills it. The public constructors use a no-op.
    - `WhenWriterProcessDiesAtCommitCheckpoint_ThenReopenHonorsTransactionBoundary` kills the child before and after commit for local commit, attempt barrier, sync result, remote apply, dead-letter, and compaction, for each delivery guarantee.
    - `WhenServerWriterDiesAtCommitCheckpoint_ThenResendAppliesExactlyOnce` kills the server journal's writer.
    - `WhenProcessDiesAtStreamCrashPoint_ThenReopenedStreamHonorsDurableBoundary` covers serialization and inbox-before-notification.
    - The in-memory store skips each durable case with a stated reason.
  - Done: four child-process v8-to-v9 migration cases cover plaintext and encrypted stores before and after commit. A public-adapter SQLite page-limit test proves that `StorageFull` rolls back a commit and remains absent after reopen. The SQLite suite passes 576/576 on each of `net8.0`, `net9.0`, `net10.0`, and `net11.0`.
  - Remaining: transport crashes and the producer-by-buffer-strategy matrix. The scheduled cross-platform crash workflow has been added but has not run in CI.
- [x] Prove restartable migrations, ownership coordination, authenticated encryption at rest, quarantine/dead-letter recovery, compaction, and retention without losing data required to rebuild snapshots or resolve pending operations.
  - Done: authenticated encryption at rest in `SqliteLocalStoreAdapter`, using AES-256-GCM with a random 96-bit nonce per value. The key comes from `ILocalStoreKeyProvider` via HKDF-SHA256.
    - The associated data ties each value to its store, record kind, column, and row keys.
    - Encrypted: payloads, hashes, base versions, fingerprints, metadata values, all cursors, quarantine data, and dead-letter reasons.
    - A value that fails authentication is quarantined as `sqlite-record-authentication-failed` and raises a critical `Security` fault, and its stream fails closed.
    - Opening a plaintext database with a key encrypts it in one restartable transaction. Opening an encrypted database without a key, or with the wrong key, fails closed.
    - `RotateEncryptionKeyAsync` re-encrypts the store under the current key.
    - .NET Framework has no `AesGcm`, so a key provider there throws `PlatformNotSupportedException`.
  - Done: schema 9 authenticates every durable operation-state row and the set of proofs. An encrypted pre-v9 store with state rows fails closed until the caller explicitly trusts it once for upgrade. The newer schema makes older readers fail closed. Tampering, missing or swapped proofs, altered storage classes, oversized values, key rotation, and same-session external writes have focused TUnit coverage.
  - Done: SQLite identity and local-commit initialization record a SHA-256 checksum of their schema definitions and reject schema drift on reopen. The adapter translates `SQLITE_FULL` and `SQLITE_IOERR` into non-transient `DurableStorageException` failures; the stream and engine report `Storage` faults. MTP reports no missed coverage in the checksum and failure translator files.
  - Done: a v8-to-v9 upgrade creates a verified, access-controlled SQLite backup before schema writes. Failed backup creation stops the upgrade. Plaintext and encrypted child-process tests prove both sides of the commit boundary, and a public-adapter disk-full test proves rollback after reopen. The full SQLite TUnit suite passes 576/576 on all four modern .NET targets. The schema checksum detects drift; keyed operation-state proofs provide the authenticity check.
- [x] Complete end-to-end at-most-once, at-least-once, and capability-gated exactly-once-effect behaviour, including retention expiry, explicit downgrade, ambiguous outcomes, and server idempotency. The current components validate capabilities but do not yet prove the complete application path.
  - Done: `OccasionallyConnectedBuilderTests.DeliveryGuarantees*.cs` runs end to end over Loopback and HTTP against a SQLite `ServerStreamHub`. A fault injector drops the push response after the server commits.
    - `AtMostOnce` ends `Ambiguous` with no resend and emits `OC.AtMostOnceAmbiguous`.
    - `AtLeastOnce` and `ExactlyOnce` resend the same operation ID and produce exactly one server effect.
    - With `ExactlyOnceExpiryBehavior.StopAndReport`, an operation that reaches the effective window moves to `GuaranteeExpired` and stays there after reopen. It emits `OC.GuaranteeExpired`, and `AwaitSynchronizedAsync` throws `SyncOperationFailedException`.
    - With `FallbackToAtLeastOnce`, the engine emits `OC.GuaranteeDowngraded` before it resends. The downgrade survives a restart.
  - Public API: `ILocalDeliveryGuaranteeStore` (implemented by the SQLite store), `SyncReasonCodes`, and `SyncOperationFailedException`.

## Protocol, security, and compatibility

- [x] Complete protocol-v1 golden fixtures and cross-version upcast/migration tests for wire envelopes, store schemas, snapshots, cursors, and operation results.
  - Done: canonical fixtures live in `GoldenFixtures/protocol-v1/` folders and are checked into source control.
    - HTTP wire messages: connect, push with every result kind, subscribe, acknowledge, and snapshot recovery. Also the cursor query forms and the status classification.
    - The SQLite schema DDL, plus a populated v1 database that current code must open and recover. An unknown `user_version` fails closed without changing the file.
    - A v1 to v2 to v3 payload upcast chain.
    - Tests: `HttpProtocolCodecTests.Golden*.cs`, `SqliteLocalStoreAdapterTests.GoldenSchema.cs`, and `JsonPayloadSerializerTests.Golden.cs`.
  - The HTTP codec skips unknown JSON members at every level, as section 18.3 requires. It still rejects duplicate members, bad JSON, missing required members and oversize bodies.
  - By design: the protocol has no error body (errors are status codes only) and no binary format. The subscribe query string still rejects unknown keys, because the replay signature covers those keys.
- [x] Complete application-level security tests for authenticated tenant/client binding, nonce and replay handling, authorization, stale credentials, tampering, path traversal, SQL metacharacters, oversized/deep payloads, decompression limits, and redacted diagnostics.
  - Evidence (public entry points only):
    - Forged tenant headers are ignored. Unauthorized streams are denied on push, subscribe, ACK, and snapshot, at both the HTTP endpoint and the hub.
    - Message IDs and bodies changed after signing are rejected.
    - The freshness window is enforced at exactly 5 minutes.
    - After a stale credential, the engine renews the token and retries once. It then reports a permanent `Authentication` fault.
    - The push, ACK, and subscribe routes reject path-traversal and control-character stream IDs, and the hub receives stream IDs in NFC form.
    - SQL metacharacters round-trip safely through the local SQLite store and the SQLite server journal.
    - The endpoint rejects deeply nested JSON and excess metadata. Compressed bodies are rejected before decoding, because the HTTP transport does not support compression.
    - Sentinel secrets never appear in HTTP responses or engine faults.
  - Product fix: upload faults now report `Authentication` and `Authorization` failures as non-transient. They were all reported as transient `Transport` failures.
  - Limitation: the HTTP adapter has no token provider that can report a renewed credential version. Over HTTP, a 401 is always permanent. The renew-and-retry-once path works only with transports that supply a credential version.
- [x] Add adapter-specific protocol fuzzing and verify that transport adapters do not introduce hidden unbounded retries.
  - Done: seeded fuzz tests run with fixed case counts: 13,500 mutated golden-fixture cases for the HTTP codec, 5,000 subscribe queries, 5,000 `HttpServerEndpoint` requests, and 7,000 `LoopbackTransportAdapter` inputs. Only documented exceptions or statuses may appear, every case must finish within 5 seconds, oversized bodies must be rejected while bounded, and any input that decodes must round-trip to identical bytes. Each failure message prints its seed.
  - Fuzzing fixed three bugs: invalid UTF-8 and lone surrogates threw `InvalidOperationException`, the snapshot request decoder accepted metadata the encoder refused, and a null `Policy` caused a `NullReferenceException` in the loopback validator.
  - Retry audit (`*Tests.SingleAttempt.cs`): each HTTP and Loopback operation makes exactly one attempt for every failure class. `Retry-After` reaches the engine; the adapter does not act on it.
  - Note: after an empty response, the HTTP subscription polls again straight away. This is long polling, not retrying, and it stops at the first failure. The server's `EmptyPollDelay` paces it.

## Examples and release gates

- [x] Run the section 16 packed-package sample on the current source for every supported target framework. The clean-consumer sample passes all 19 checks on `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`: offline startup, optimistic and observer writes, reconnect, restart recovery, conflict reconciliation, and operation synchronization. Evidence: `artifacts/oc-packages-v9-precommit-20260928` from the uncommitted v9 source; rerun after the final commit for source-SHA provenance.
- [x] Add the remaining ResilienceLab demonstrations for duplicate/reordered delivery, capability downgrade, backpressure, slow observers, corruption/quarantine, and retention-gap recovery.
  - Scenarios: `duplicate-reordered-delivery`, `capability-downgrade`, `backpressure`, `slow-observers`, `corruption-quarantine`, and `retention-gap-recovery`. Each scenario uses public APIs only and reports expected against actual values. `README.md` lists how to run each one.
  - The lab tests pass 109/109 on `net8.0` and `net10.0`.
  - Limitation: the retention-gap scenario recovers through `ServerStreamHub.GetSnapshotAsync` directly. The loopback transport does not advertise `SnapshotRecovery`, so the engine recovers from a gap by itself only over HTTP.
- [ ] Complete the quality gates in section 17.4: full transition/invariant coverage, mutation testing, child-process crash tests, and bounded throughput/allocation/recovery/compaction/slow-observer soak measurements. The v9 SQLite build has zero warnings across eight library targets, and its TUnit suite passes 576/576 on each modern .NET target. The fresh net10 SQLite report has 98.26% line and 93.12% branch coverage, above the design's 95%/90% package thresholds. The repository's stricter 100% handwritten line/branch gate still fails (112 missed lines and 118 missed branches); targeted tests are in progress. Only the Retry mutation campaign has a measured killed mutant so far. The scheduled cross-platform jobs and the other three mutation campaigns still need runs.
- [x] Produce SBOM and dependency/license/security scan evidence for all seven feature packages. The local supply-chain gate packed seven packages and recorded 53 dependency licenses, zero vulnerable or deprecated findings, an SPDX 2.2 SBOM containing all seven requested versions, and successful validation of all 14 package/symbol files (`artifacts/oc-supply-chain-full-20260928`). The release workflow now requires this gate. A source-commit/package-hash provenance report has been added but still needs a post-change run.
- [ ] Finish the NativeAOT and scheduled release evidence. A Windows CI NativeAOT packed-consumer workflow is wired as a required release prerequisite; local execution still lacks Windows C++ linker libraries, and the CI run has not yet occurred. Separate cross-platform scheduled crash, soak, and performance jobs have been added; focused Windows TUnit soak/performance tests pass, but the scheduled jobs have not yet run. The v9 precommit package gate passed 10 package packs, 14 deterministic comparisons, 364 symbol/Source Link/target-framework checks, clean installation, and all eight sample runs. Trim/NativeAOT was skipped locally; rerun against the final source commit and verify the hosted Windows gate.

## Final release acceptance

- [ ] Freeze the public API, protocol v1, store schema v1, and compatibility policy only after the preceding gates pass.
- [ ] Produce the preview/RC package set and verify clean-project installation, upgrade/rollback guidance, security reporting, and the single final feature PR.
