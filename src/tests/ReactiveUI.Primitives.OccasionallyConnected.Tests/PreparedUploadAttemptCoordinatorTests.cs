// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Time.Testing;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="PreparedUploadAttemptCoordinator"/>.</summary>
public sealed partial class PreparedUploadAttemptCoordinatorTests
{
    /// <summary>The store identity used by durable tests.</summary>
    private const string StoreIdentity = "prepared-upload-tests";

    /// <summary>The client identity used by loopback tests.</summary>
    private const string ClientId = "client-a";

    /// <summary>The tenant hint used by loopback tests.</summary>
    private const string Tenant = "tenant-a";

    /// <summary>The send failure message used by primary-precedence tests.</summary>
    private const string SendFailedMessage = "send failed";

    /// <summary>The release failure message used by cleanup-precedence tests.</summary>
    private const string ReleaseFailedMessage = "release failed";

    /// <summary>The snapshot payload text used by real-store coordinator tests.</summary>
    private const string SnapshotPayloadText = "snapshot-1";

    /// <summary>The schema version required by tests.</summary>
    private const int SchemaVersion = 1;

    /// <summary>The first client sequence.</summary>
    private const int FirstSequence = 1;

    /// <summary>The second client sequence.</summary>
    private const int SecondSequence = 2;

    /// <summary>The prior durable attempt count used by status tests.</summary>
    private const int PriorAttempt = 2;

    /// <summary>The next checked attempt after <see cref="PriorAttempt"/>.</summary>
    private const int AttemptAfterPrior = 3;

    /// <summary>The maximum operation count used by tests.</summary>
    private const int LeaseOperationLimit = 8;

    /// <summary>The default encoded byte ceiling.</summary>
    private const long DefaultEncodedSizeBytes = 4096;

    /// <summary>The smaller encoded byte ceiling.</summary>
    private const long SmallEncodedSizeBytes = 8;

    /// <summary>The lease renewal interval used by renewal tests.</summary>
    private const int RenewalIntervalSeconds = 1;

    /// <summary>The lease renewal duration used by renewal tests.</summary>
    private const int RenewalDurationSeconds = 5;

    /// <summary>The short initial lease duration used by renewal tests.</summary>
    private const int ShortInitialLeaseMilliseconds = 200;

    /// <summary>The wait used by gate-based tests.</summary>
    private static readonly TimeSpan GateTimeout = TimeSpan.FromSeconds(5);

    /// <summary>The fixed timestamp used by operation and status fixtures.</summary>
    private static readonly DateTimeOffset TimestampUtc = new(2026, 9, 13, 1, 2, 3, TimeSpan.Zero);

    /// <summary>The stream used by tests.</summary>
    private static readonly StreamId Stream = new("prepared/upload");

    /// <summary>Verifies successful attempts prepare before barriers, send once, then reconcile without caller cancellation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncPreparesBeforeBarriersSendsOnceAndReconciles()
    {
        var lease = CreateLease(CreateOperation(FirstSequence), CreateOperation(SecondSequence));
        var store = CreateStore(lease);
        store.Statuses[lease.Operations[0].OperationId] = CreateStatus(lease.Operations[0], PriorAttempt);
        var preparer = new RecordingPreparer { BeforePrepareReturns = () => store.Calls.Add("prepare") };
        PreparedUploadReconciliation? reconciled = null;
        var request = CreateRequest(
            lease,
            preparer,
            store,
            (reconciliation, token) =>
            {
                reconciled = reconciliation;
                return token.IsCancellationRequested ? throw new OperationCanceledException(token) : ValueTask.CompletedTask;
            });

        var result = await PreparedUploadAttemptCoordinator.ExecuteAsync(request, CancellationToken.None);

        await Assert.That(result.Sent).IsTrue();
        await Assert.That(result.Reconciled).IsTrue();
        await Assert.That(result.Barriers.Count).IsEqualTo(SecondSequence);
        await Assert.That(result.Barriers[0].Attempt).IsEqualTo(AttemptAfterPrior);
        await Assert.That(result.Barriers[1].Attempt).IsEqualTo(FirstSequence);
        await Assert.That(preparer.PrepareCount).IsEqualTo(FirstSequence);
        await Assert.That(preparer.Prepared?.SendCount).IsEqualTo(FirstSequence);
        await Assert.That(reconciled?.LeaseId).IsEqualTo(lease.LeaseId);
        await Assert.That(reconciled?.Batch.BatchId).IsEqualTo(lease.LeaseId);
        await Assert.That(store.Calls.IndexOf("prepare")).IsLessThan(store.Calls.IndexOf("barrier-1"));
        await Assert.That(store.ReleaseCount).IsEqualTo(0);
    }

    /// <summary>Verifies preparation failures leave durable barriers untouched and still release the owned lease.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncPreparationFailureDoesNotRecordBarrierOrSend()
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var preparer = new RecordingPreparer { PrepareException = new InvalidOperationException("prepare failed") };

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, preparer, store), CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("prepare failed");
        await Assert.That(store.Barriers.Count).IsEqualTo(0);
        await Assert.That(preparer.Prepared?.SendCount ?? 0).IsEqualTo(0);
        await Assert.That(store.ReleaseCount).IsEqualTo(FirstSequence);
    }

    /// <summary>Verifies substituted prepared handles fail before durable barriers.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncMismatchingPreparedBatchFailsBeforeBarrier()
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var substituted = new SyncBatch(Guid.NewGuid(), [CreateOperation(FirstSequence)]);
        var prepared = new RecordingPrepared(substituted, CreateResult(substituted));
        var preparer = new RecordingPreparer { Prepared = prepared };

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, preparer, store), CancellationToken.None).AsTask());

        await Assert.That(store.Barriers.Count).IsEqualTo(0);
        await Assert.That(prepared.SendCount).IsEqualTo(0);
        await Assert.That(prepared.DisposeCount).IsEqualTo(FirstSequence);
    }

    /// <summary>Verifies encoded-size overflow after preparation skips every barrier.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncOversizedPreparedBodyFailsBeforeBarrier()
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var preparer = new RecordingPreparer();
        var request = CreateRequest(lease, preparer, store) with
        {
            Options = CreateOptions() with { MaximumEncodedSizeBytes = SmallEncodedSizeBytes },
        };

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(request, CancellationToken.None).AsTask());

        await Assert.That(store.Barriers.Count).IsEqualTo(0);
        await Assert.That(preparer.Prepared?.SendCount ?? 0).IsEqualTo(0);
    }

    /// <summary>Verifies any denied barrier prevents the whole batch from sending.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncDeniedBarrierPreventsWholeBatchSendAndReconciliation()
    {
        var lease = CreateLease(CreateOperation(FirstSequence), CreateOperation(SecondSequence));
        var store = CreateStore(lease);
        store.DeniedOperation = lease.Operations[1].OperationId;
        var preparer = new RecordingPreparer();
        var reconcileCalls = 0;

        var result = await PreparedUploadAttemptCoordinator.ExecuteAsync(
            CreateRequest(
                lease,
                preparer,
                store,
                (_, _) =>
                {
                    reconcileCalls++;
                    return ValueTask.CompletedTask;
                }),
            CancellationToken.None);

        await Assert.That(result.Sent).IsFalse();
        await Assert.That(result.Reconciled).IsFalse();
        await Assert.That(result.Barriers.Count).IsEqualTo(SecondSequence);
        await Assert.That(result.Barriers[1].MaySend).IsFalse();
        await Assert.That(preparer.Prepared?.SendCount).IsEqualTo(0);
        await Assert.That(reconcileCalls).IsEqualTo(0);
    }

    /// <summary>Verifies partial barrier failure preserves ambiguity and skips send.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncPartialBarrierFailureDoesNotSend()
    {
        var lease = CreateLease(CreateOperation(FirstSequence), CreateOperation(SecondSequence));
        var store = CreateStore(lease);
        store.ThrowOnBarrierIndex = SecondSequence;
        var preparer = new RecordingPreparer();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, preparer, store), CancellationToken.None).AsTask());

        await Assert.That(store.Barriers.Count).IsEqualTo(FirstSequence);
        await Assert.That(preparer.Prepared?.SendCount).IsEqualTo(0);
    }

    /// <summary>Verifies malformed remote responses are not passed to reconciliation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncMalformedRemoteResultDoesNotReconcile()
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var malformed = new RemoteSyncResult(Guid.NewGuid(), [], null, null);
        var preparer = new RecordingPreparer { ResultOverride = malformed };
        var reconcileCalls = 0;

        _ = await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(
                CreateRequest(
                    lease,
                    preparer,
                    store,
                    (_, _) =>
                    {
                        reconcileCalls++;
                        return ValueTask.CompletedTask;
                    }),
                CancellationToken.None).AsTask());

        await Assert.That(preparer.Prepared?.SendCount).IsEqualTo(FirstSequence);
        await Assert.That(reconcileCalls).IsEqualTo(0);
    }

    /// <summary>Verifies caller cancellation after a valid response cannot cancel ordered reconciliation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncCancellationAfterResponseDoesNotCancelReconciliation()
    {
        using var callerCancellation = new CancellationTokenSource();
        var clock = new FakeTimeProvider(TimestampUtc);
        var lease = CreateLease(clock.GetUtcNow().AddSeconds(SecondSequence), CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var preparer = new RecordingPreparer { BeforeSendReturns = callerCancellation.Cancel };
        var observedCancellation = true;
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempt = PreparedUploadAttemptCoordinator.ExecuteAsync(
            CreateRequest(
                lease,
                preparer,
                store,
                ReconcileAfterResponseAsync) with
            {
                Options = CreateOptions() with
                {
                    TimeProvider = clock,
                    LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                    LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
                },
            },
            callerCancellation.Token).AsTask();

        await entered.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        clock.Advance(TimeSpan.FromSeconds(RenewalIntervalSeconds));
        await store.WaitForRenewCountAsync(FirstSequence).WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        _ = release.TrySetResult();
        var result = await attempt.ConfigureAwait(false);

        await Assert.That(result.Sent).IsTrue();
        await Assert.That(result.Reconciled).IsTrue();
        await Assert.That(observedCancellation).IsFalse();
        await Assert.That(store.RenewCount).IsEqualTo(FirstSequence);

        async ValueTask ReconcileAfterResponseAsync(PreparedUploadReconciliation reconciliation, CancellationToken token)
        {
            _ = reconciliation;
            observedCancellation = token.IsCancellationRequested;
            _ = entered.TrySetResult();
            await release.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies blocked reconciliation renews the lease and release waits for reconciliation drain.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncBlockedReconciliationRenewsLeaseAndDrainsBeforeRelease()
    {
        var clock = new FakeTimeProvider(TimestampUtc);
        var lease = CreateLease(clock.GetUtcNow().AddSeconds(SecondSequence), CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var preparer = new RecordingPreparer();
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = CreateRequest(
            lease,
            preparer,
            store,
            async (_, _) =>
            {
                _ = entered.TrySetResult();
                await release.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
            }) with
        {
            Options = CreateOptions() with
            {
                TimeProvider = clock,
                LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
            },
        };
        var attempt = PreparedUploadAttemptCoordinator.ExecuteAsync(request, CancellationToken.None).AsTask();

        await entered.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        clock.Advance(TimeSpan.FromSeconds(RenewalIntervalSeconds));
        await store.Renewed.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        await Assert.That(store.ReleaseCount).IsEqualTo(0);
        _ = release.TrySetResult();
        _ = await attempt.ConfigureAwait(false);

        await Assert.That(store.RenewCount).IsGreaterThan(0);
        await Assert.That(store.ReleaseCount).IsEqualTo(0);
    }

    /// <summary>Verifies repeated renewals keep additive stores within the configured forward horizon.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncLongReconciliationRenewsTowardBoundedHorizon()
    {
        var clock = new FakeTimeProvider(TimestampUtc);
        var lease = CreateLease(clock.GetUtcNow().AddSeconds(SecondSequence), CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var preparer = new RecordingPreparer();
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = CreateRequest(
            lease,
            preparer,
            store,
            async (_, _) =>
            {
                _ = entered.TrySetResult();
                await release.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
            }) with
        {
            Options = CreateOptions() with
            {
                TimeProvider = clock,
                LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
            },
        };
        var attempt = PreparedUploadAttemptCoordinator.ExecuteAsync(request, CancellationToken.None).AsTask();

        await entered.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        for (var expectedRenewals = FirstSequence; expectedRenewals <= AttemptAfterPrior; expectedRenewals++)
        {
            var advanceSeconds = expectedRenewals == FirstSequence ? RenewalIntervalSeconds : RenewalDurationSeconds - RenewalIntervalSeconds;
            clock.Advance(TimeSpan.FromSeconds(advanceSeconds));
            await store.WaitForRenewCountAsync(expectedRenewals).WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
            await Assert.That(store.LeaseExpiresAtUtc).IsLessThanOrEqualTo(clock.GetUtcNow().AddSeconds(RenewalDurationSeconds));
            await Assert.That(store.LeaseExpiresAtUtc).IsGreaterThan(clock.GetUtcNow());
        }

        _ = release.TrySetResult();
        _ = await attempt.ConfigureAwait(false);
        await Assert.That(store.RenewalExtensions.TrueForAll(static extension => extension > TimeSpan.Zero)).IsTrue();
    }

    /// <summary>Verifies an initially short lease renews before long preparation can reach barriers.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncNearExpiredLeaseRenewsBeforeLongPrepare()
    {
        var clock = new FakeTimeProvider(TimestampUtc);
        var lease = CreateLease(clock.GetUtcNow().AddMilliseconds(ShortInitialLeaseMilliseconds), CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        var preparer = new RecordingPreparer { BeforePrepareCompletes = _ => new(store.WaitForRenewCountAsync(FirstSequence)) };

        var result = await PreparedUploadAttemptCoordinator.ExecuteAsync(
            CreateRequest(lease, preparer, store) with
            {
                Options = CreateOptions() with
                {
                    TimeProvider = clock,
                    LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                    LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
                },
            },
            CancellationToken.None);

        await Assert.That(result.Sent).IsTrue();
        await Assert.That(store.RenewCount).IsEqualTo(FirstSequence);
        await Assert.That(store.Calls.IndexOf("renew")).IsLessThan(store.Calls.IndexOf("barrier-1"));
    }

    /// <summary>Verifies successful reconciliation stops renewal before a delayed prepared disposal can tick a consumed lease.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncSuccessfulReconciliationStopsRenewalBeforeDelayedPreparedDisposal()
    {
        var clock = new FakeTimeProvider(TimestampUtc);
        await using var store = await CreateInitializedInMemoryStoreAsync(clock);
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstSequence);
        _ = await store.CommitLocalOperationAsync(operation, new(Stream, CreatePayload(SnapshotPayloadText), FirstSequence), CancellationToken.None);
        var lease = await LeaseOnlyBatchAsync(store, TimeSpan.FromSeconds(SecondSequence));
        TaskCompletionSource disposeEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource finishDispose = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var preparer = new RecordingPreparer
        {
            DisposeAsyncOverride = async () =>
            {
                _ = disposeEntered.TrySetResult();
                await finishDispose.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
            },
        };
        var attempt = PreparedUploadAttemptCoordinator.ExecuteAsync(
            CreateRequest(
                lease,
                preparer,
                store,
                async reconciliation =>
                {
                    _ = await store.ApplySyncResultAsync(
                        reconciliation.LeaseId,
                        reconciliation.Result,
                        [],
                        CancellationToken.None).ConfigureAwait(false);
                }) with
            {
                Options = CreateOptions() with
                {
                    TimeProvider = clock,
                    LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                    LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
                },
            },
            CancellationToken.None).AsTask();

        await disposeEntered.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        AdvanceClock(clock, TimeSpan.FromSeconds(RenewalIntervalSeconds));
        _ = finishDispose.TrySetResult();
        var result = await attempt.ConfigureAwait(false);
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(result.Sent).IsTrue();
        await Assert.That(result.Reconciled).IsTrue();
        await Assert.That(preparer.Prepared?.DisposeCount).IsEqualTo(FirstSequence);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
    }

    /// <summary>Verifies successful reconciliation wins when renewal races after the real store consumes the lease.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncSuccessfulReconciliationOwnsLeaseWhenRenewalRacesAfterCommit()
    {
        var clock = new FakeTimeProvider(TimestampUtc);
        await using var innerStore = await CreateInitializedInMemoryStoreAsync(clock);
        _ = await innerStore.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstSequence);
        _ = await innerStore.CommitLocalOperationAsync(operation, new(Stream, CreatePayload(SnapshotPayloadText), FirstSequence), CancellationToken.None);
        var lease = await LeaseOnlyBatchAsync(innerStore, TimeSpan.FromSeconds(SecondSequence));
        var store = new ObservedStore(innerStore);
        TaskCompletionSource commitApplied = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource finishReconciliation = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempt = PreparedUploadAttemptCoordinator.ExecuteAsync(
            CreateRequest(
                lease,
                new RecordingPreparer(),
                store,
                async reconciliation =>
                {
                    _ = await store.ApplySyncResultAsync(
                        reconciliation.LeaseId,
                        reconciliation.Result,
                        [],
                        CancellationToken.None).ConfigureAwait(false);
                    _ = commitApplied.TrySetResult();
                    await finishReconciliation.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
                }) with
            {
                Options = CreateOptions() with
                {
                    TimeProvider = clock,
                    LeaseRenewalInterval = TimeSpan.FromSeconds(RenewalIntervalSeconds),
                    LeaseRenewalDuration = TimeSpan.FromSeconds(RenewalDurationSeconds),
                },
            },
            CancellationToken.None).AsTask();

        await commitApplied.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        AdvanceClock(clock, TimeSpan.FromSeconds(RenewalIntervalSeconds));
        await store.RenewFailed.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
        _ = finishReconciliation.TrySetResult();
        var result = await attempt.ConfigureAwait(false);
        var status = await innerStore.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(result.Sent).IsTrue();
        await Assert.That(result.Reconciled).IsTrue();
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
    }

    /// <summary>Verifies cleanup failures do not hide a primary send failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncCleanupFailurePreservesPrimarySendException()
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        store.ReleaseException = new InvalidOperationException(ReleaseFailedMessage);
        var preparer = new RecordingPreparer { SendException = new InvalidOperationException(SendFailedMessage) };

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, preparer, store), CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains(SendFailedMessage);
        await Assert.That(store.ReleaseCount).IsEqualTo(FirstSequence);
    }

    /// <summary>Verifies synchronous cleanup throws cannot skip later cleanup and cannot hide the primary exception.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncSynchronousCleanupThrowsStillAttemptsEveryCleanupAndPreservesPrimaryException()
    {
        var lease = CreateLease(CreateOperation(FirstSequence));
        var store = CreateStore(lease);
        store.ReleaseException = new InvalidOperationException(ReleaseFailedMessage);
        var preparer = new RecordingPreparer { SendException = new InvalidOperationException(SendFailedMessage) };
        preparer.DisposeAsyncOverride = static () => throw new InvalidOperationException("dispose failed");

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => PreparedUploadAttemptCoordinator.ExecuteAsync(CreateRequest(lease, preparer, store), CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains(SendFailedMessage);
        await Assert.That(preparer.Prepared?.DisposeCount).IsEqualTo(FirstSequence);
        await Assert.That(store.ReleaseCount).IsEqualTo(FirstSequence);
    }

    /// <summary>Verifies a lost response leaves a real at-most-once SQLite operation unsent on restart.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncLostResponseRestartCannotResendAtMostOnceOperation()
    {
        var directory = SqliteTestDirectory.Create("oc-upload-attempt-");
        try
        {
            var databasePath = Path.Combine(directory.FullName, "local.db");
            var operation = await SeedSqliteOperationAsync(databasePath, CreateAtMostOncePolicy());
            {
                await using var firstLease = await LeaseSqliteBatchAsync(databasePath);
                var failing = new RecordingPreparer { SendException = new InvalidOperationException("response lost") };
                _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
                    () => PreparedUploadAttemptCoordinator.ExecuteAsync(
                        CreateRequest(firstLease.Lease, failing, firstLease.Store),
                        CancellationToken.None).AsTask());
            }

            await using var reopenedStore = await CreateInitializedSqliteStoreAsync(databasePath);
            var retryLease = await TryLeaseOnlyBatchAsync(reopenedStore);

            await Assert.That(retryLease).IsNull();
            var status = await reopenedStore.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
            await Assert.That(status?.Attempt).IsEqualTo(FirstSequence);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Verifies real SQLite, reopen and loopback transport can complete a prepared accepted upload.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecuteAsyncRealSqliteReopenAndLoopbackReconcilesAcceptedUpload()
    {
        var directory = SqliteTestDirectory.Create("oc-upload-attempt-");
        try
        {
            var databasePath = Path.Combine(directory.FullName, "local.db");
            var operation = await SeedSqliteOperationAsync(databasePath, OperationPolicy.Default);
            var hub = new RecordingHub();
            PreparedUploadAttemptResult result;
            {
                await using var store = await CreateInitializedSqliteStoreAsync(databasePath);
                var lease = await LeaseOnlyBatchAsync(store);
                await using var adapter = new LoopbackTransportAdapter(CreateLoopbackOptions(hub));
                await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
                result = await PreparedUploadAttemptCoordinator.ExecuteAsync(
                    CreateRequest(
                        lease,
                        (IRemoteTransportBatchPreparer)session,
                        store,
                        async reconciliation =>
                        {
                            _ = await store.ApplySyncResultAsync(
                                reconciliation.LeaseId,
                                reconciliation.Result,
                                [],
                                CancellationToken.None).ConfigureAwait(false);
                        }),
                    CancellationToken.None);
            }

            await Assert.That(result.Sent).IsTrue();
            await Assert.That(result.Reconciled).IsTrue();
            await Assert.That(hub.ApplyCalls).IsEqualTo(FirstSequence);
            await using var reopened = await CreateInitializedSqliteStoreAsync(databasePath);
            var status = await reopened.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
            await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
            await Assert.That(status?.Attempt).IsEqualTo(FirstSequence);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Creates a default coordinator request.</summary>
    /// <param name="lease">The leased batch.</param>
    /// <param name="preparer">The prepared transport.</param>
    /// <param name="store">The local store.</param>
    /// <param name="reconcile">The reconciliation callback.</param>
    /// <returns>The prepared upload attempt request.</returns>
    private static PreparedUploadAttemptRequest CreateRequest(
        LeasedOperationBatch lease,
        IRemoteTransportBatchPreparer preparer,
        ILocalStoreAdapter store,
        Func<PreparedUploadReconciliation, CancellationToken, ValueTask>? reconcile = null) =>
        new(lease, preparer, store, reconcile ?? DefaultReconcileAsync, CreateOptions());

    /// <summary>Creates a default coordinator request with a single-argument reconciliation callback.</summary>
    /// <param name="lease">The leased batch.</param>
    /// <param name="preparer">The prepared transport.</param>
    /// <param name="store">The local store.</param>
    /// <param name="reconcile">The reconciliation callback.</param>
    /// <returns>The prepared upload attempt request.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PreparedUploadAttemptRequest CreateRequest(
        LeasedOperationBatch lease,
        IRemoteTransportBatchPreparer preparer,
        ILocalStoreAdapter store,
        Func<PreparedUploadReconciliation, ValueTask> reconcile) =>
        CreateRequest(lease, preparer, store, (payload, _) => reconcile(payload));

    /// <summary>Default reconciliation callback.</summary>
    /// <param name="reconciliation">The reconciliation payload.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The completed operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask DefaultReconcileAsync(PreparedUploadReconciliation reconciliation, CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;

    /// <summary>Creates bounded coordinator options.</summary>
    /// <returns>The bounded options.</returns>
    private static PreparedUploadAttemptOptions CreateOptions() =>
        new() { MaximumOperations = LeaseOperationLimit, MaximumEncodedSizeBytes = DefaultEncodedSizeBytes };

    /// <summary>Creates an initialized in-memory adapter.</summary>
    /// <param name="timeProvider">The time provider.</param>
    /// <returns>The initialized store.</returns>
    private static async Task<InMemoryLocalStoreAdapter> CreateInitializedInMemoryStoreAsync(TimeProvider timeProvider)
    {
        var store = new InMemoryLocalStoreAdapter(timeProvider, LeaseOperationLimit, DefaultEncodedSizeBytes, new RetentionOptions());
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        return store;
    }

    /// <summary>Creates invalid coordinator options.</summary>
    /// <param name="scenario">The invalid option scenario.</param>
    /// <returns>The invalid options.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The scenario is unknown.</exception>
    private static PreparedUploadAttemptOptions CreateInvalidOptions(string scenario) =>
        scenario switch
        {
            "operation-limit" => CreateOptions() with { MaximumOperations = 0 },
            "size-limit" => CreateOptions() with { MaximumEncodedSizeBytes = 0 },
            "interval-zero" => CreateOptions() with { LeaseRenewalInterval = TimeSpan.Zero },
            "interval-infinite" => CreateOptions() with { LeaseRenewalInterval = Timeout.InfiniteTimeSpan },
            "duration-zero" => CreateOptions() with { LeaseRenewalDuration = TimeSpan.Zero },
            "duration-infinite" => CreateOptions() with { LeaseRenewalDuration = Timeout.InfiniteTimeSpan },
            "duration-max" => CreateOptions() with { LeaseRenewalDuration = TimeSpan.MaxValue },
            "interval-equals-duration" => CreateOptions() with { LeaseRenewalInterval = TimeSpan.FromSeconds(SecondSequence), LeaseRenewalDuration = TimeSpan.FromSeconds(SecondSequence) },
            _ => throw new ArgumentOutOfRangeException(nameof(scenario), scenario, "Unknown invalid option scenario."),
        };

    /// <summary>Creates a leased batch wrapper.</summary>
    /// <param name="operations">The leased operations.</param>
    /// <returns>The leased batch.</returns>
    private static LeasedOperationBatch CreateLease(params SyncOperation[] operations) =>
        new(Guid.NewGuid(), TimeProvider.System.GetUtcNow().AddMinutes(FirstSequence), operations);

    /// <summary>Creates a leased batch wrapper with a custom expiry.</summary>
    /// <param name="expiresAtUtc">The lease expiry.</param>
    /// <param name="operations">The leased operations.</param>
    /// <returns>The leased batch.</returns>
    private static LeasedOperationBatch CreateLease(DateTimeOffset expiresAtUtc, params SyncOperation[] operations) =>
        new(Guid.NewGuid(), expiresAtUtc, operations);

    /// <summary>Creates a malformed leased batch.</summary>
    /// <param name="scenario">The malformed scenario.</param>
    /// <returns>The malformed lease.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The scenario is unknown.</exception>
    private static LeasedOperationBatch CreateMalformedLease(string scenario)
    {
        var expiry = TimeProvider.System.GetUtcNow().AddMinutes(FirstSequence);
        return scenario switch
        {
            "empty-lease-id" => new(Guid.Empty, expiry, [CreateOperation(FirstSequence)]),
            "empty-operations" => new(Guid.NewGuid(), expiry, []),
            "too-many-operations" => new(Guid.NewGuid(), expiry, Enumerable.Range(FirstSequence, LeaseOperationLimit + FirstSequence).Select(static index => CreateOperation(index)).ToArray()),
            "null-first-operation" => new(Guid.NewGuid(), expiry, new SyncOperation[FirstSequence]),
            "null-later-operation" => CreateLeaseWithNullLaterOperation(expiry),
            "empty-operation-id" => new(Guid.NewGuid(), expiry, [CreateOperation(FirstSequence) with { OperationId = new(Guid.Empty) }]),
            "wrong-stream" => new(Guid.NewGuid(), expiry, [CreateOperation(FirstSequence), CreateOperation(SecondSequence) with { StreamId = new("prepared/other") }]),
            "non-increasing-sequence" => new(Guid.NewGuid(), expiry, [CreateOperation(FirstSequence), CreateOperation(FirstSequence)]),
            _ => throw new ArgumentOutOfRangeException(nameof(scenario), scenario, "Unknown malformed lease scenario."),
        };
    }

    /// <summary>Creates a lease whose later operation slot is null.</summary>
    /// <param name="expiry">The lease expiry.</param>
    /// <returns>The malformed lease.</returns>
    private static LeasedOperationBatch CreateLeaseWithNullLaterOperation(DateTimeOffset expiry)
    {
        var operations = new SyncOperation[SecondSequence];
        operations[0] = CreateOperation(FirstSequence);
        return new(Guid.NewGuid(), expiry, operations);
    }

    /// <summary>Creates a fake store with durable status for every leased operation.</summary>
    /// <param name="lease">The leased batch.</param>
    /// <returns>The fake store.</returns>
    private static RecordingStore CreateStore(LeasedOperationBatch lease)
    {
        var store = new RecordingStore { LeaseExpiresAtUtc = lease.ExpiresAtUtc };
        foreach (var operation in lease.Operations)
        {
            store.Statuses[operation.OperationId] = CreateStatus(operation, 0);
        }

        return store;
    }

    /// <summary>Creates a representative operation.</summary>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="policy">The operation policy.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(long clientSequence, OperationPolicy? policy = null) =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = Stream,
            ClientSequence = clientSequence,
            TimestampUtc = TimestampUtc,
            BaseVersion = "server-a",
            Type = SyncOperationType.Update,
            Payload = CreatePayload($"payload-{clientSequence}"),
            Policy = policy ?? OperationPolicy.Default,
            Metadata = new Dictionary<string, string> { ["origin"] = "prepared-upload" },
        };

    /// <summary>Creates a representative payload.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope CreatePayload(string text) =>
        new("reading", FirstSequence, "application/json", Encoding.UTF8.GetBytes(text), $"hash-{text}");

    /// <summary>Creates an accepted result for a batch.</summary>
    /// <param name="batch">The synchronization batch.</param>
    /// <returns>The accepted result.</returns>
    private static RemoteSyncResult CreateResult(SyncBatch batch) =>
        new(
            batch.BatchId,
            batch.Operations
                .Select(static operation => new OperationSyncResult(operation.OperationId, OperationResultKind.Accepted, null, "server-v"))
                .ToArray(),
            "cursor-a",
            null);

    /// <summary>Creates a status receipt for an operation.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="attempt">The attempt count.</param>
    /// <returns>The operation status.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncOperationStatus CreateStatus(SyncOperation operation, int attempt) =>
        CreateStatus(operation, attempt, SyncOperationState.Uploading);

    /// <summary>Creates a status receipt for an operation and state.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="attempt">The attempt count.</param>
    /// <param name="state">The durable state.</param>
    /// <returns>The operation status.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncOperationStatus CreateStatus(SyncOperation operation, int attempt, SyncOperationState state) =>
        new(operation.OperationId, operation.StreamId, state, attempt, TimestampUtc, null);

    /// <summary>Applies a malformed status fixture to a fake store.</summary>
    /// <param name="store">The fake store.</param>
    /// <param name="operation">The leased operation.</param>
    /// <param name="scenario">The malformed scenario.</param>
    /// <exception cref="ArgumentOutOfRangeException">The scenario is unknown.</exception>
    private static void ApplyMalformedStatus(RecordingStore store, SyncOperation operation, string scenario)
    {
        switch (scenario)
        {
            case "missing":
            {
                _ = store.Statuses.Remove(operation.OperationId);
                break;
            }

            case "operation":
            {
                store.Statuses[operation.OperationId] = CreateStatus(operation, 0) with { OperationId = OperationId.New() };
                break;
            }

            case "stream":
            {
                store.Statuses[operation.OperationId] = CreateStatus(operation, 0) with { StreamId = new("prepared/other") };
                break;
            }

            case "terminal":
            {
                store.Statuses[operation.OperationId] = CreateStatus(operation, 0, SyncOperationState.Synchronized);
                break;
            }

            case "negative-attempt":
            {
                store.Statuses[operation.OperationId] = CreateStatus(operation, -FirstSequence);
                break;
            }

            case "max-attempt":
            {
                store.Statuses[operation.OperationId] = CreateStatus(operation, int.MaxValue);
                break;
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(scenario), scenario, "Unknown malformed status scenario.");
        }
    }

    /// <summary>Creates an at-most-once operation policy.</summary>
    /// <returns>The operation policy.</returns>
    private static OperationPolicy CreateAtMostOncePolicy() =>
        new(DeliveryGuarantee.AtMostOnce, OperationDurability.Durable, 0, ConflictPolicy.Merge);

    /// <summary>Creates a loopback transport connection request.</summary>
    /// <returns>The connection request.</returns>
    private static TransportConnectRequest CreateConnectRequest() =>
        new(new(new(FirstSequence, 0), new(FirstSequence, 0)), new(ClientId, Tenant), [DeliveryGuarantee.AtLeastOnce, DeliveryGuarantee.AtMostOnce]);

    /// <summary>Creates loopback options.</summary>
    /// <param name="hub">The server hub.</param>
    /// <returns>The loopback options.</returns>
    private static LoopbackTransportAdapterOptions CreateLoopbackOptions(RecordingHub hub) =>
        new()
        {
            Hub = hub,
            AuthenticatedClient = new(Tenant, ClientId),
            PeerCapabilities = new(
                new(FirstSequence, 0),
                RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.ServerIdempotency,
                LeaseOperationLimit,
                DefaultEncodedSizeBytes,
                null,
                null),
        };

    /// <summary>Seeds one durable SQLite operation and returns it.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="policy">The operation policy.</param>
    /// <returns>The committed operation.</returns>
    private static async Task<SyncOperation> SeedSqliteOperationAsync(string databasePath, OperationPolicy policy)
    {
        await using var store = await CreateInitializedSqliteStoreAsync(databasePath);
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstSequence, policy);
        _ = await store.CommitLocalOperationAsync(operation, new(Stream, CreatePayload(SnapshotPayloadText), FirstSequence), CancellationToken.None);
        return operation;
    }

    /// <summary>Creates an initialized SQLite adapter.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <returns>The initialized store.</returns>
    private static async Task<SqliteLocalStoreAdapter> CreateInitializedSqliteStoreAsync(string databasePath)
    {
        var store = new SqliteLocalStoreAdapter(databasePath);
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        return store;
    }

    /// <summary>Leases a SQLite batch and keeps its store with it.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <returns>The lease context.</returns>
    private static async Task<SqliteLeaseContext> LeaseSqliteBatchAsync(string databasePath)
    {
        var store = await CreateInitializedSqliteStoreAsync(databasePath);
        return new(await LeaseOnlyBatchAsync(store), store);
    }

    /// <summary>Leases one pending batch from the store.</summary>
    /// <param name="store">The store.</param>
    /// <returns>The leased operation batch.</returns>
    /// <exception cref="InvalidOperationException">No pending operation is available.</exception>
    private static async Task<LeasedOperationBatch> LeaseOnlyBatchAsync(SqliteLocalStoreAdapter store)
    {
        var leased = await TryLeaseOnlyBatchAsync(store);
        return leased ?? throw new InvalidOperationException("Expected one leased operation batch.");
    }

    /// <summary>Leases one pending batch from an in-memory store.</summary>
    /// <param name="store">The store.</param>
    /// <param name="leaseDuration">The lease duration.</param>
    /// <returns>The leased operation batch.</returns>
    /// <exception cref="InvalidOperationException">No pending operation is available.</exception>
    private static async Task<LeasedOperationBatch> LeaseOnlyBatchAsync(InMemoryLocalStoreAdapter store, TimeSpan leaseDuration)
    {
        var leased = await TryLeaseOnlyBatchAsync(store, leaseDuration);
        return leased ?? throw new InvalidOperationException("Expected one leased operation batch.");
    }

    /// <summary>Attempts to lease one pending batch from the store.</summary>
    /// <param name="store">The store.</param>
    /// <returns>The leased operation batch, if one is available.</returns>
    private static async Task<LeasedOperationBatch?> TryLeaseOnlyBatchAsync(SqliteLocalStoreAdapter store)
    {
        LeasedOperationBatch? leased = null;
        await foreach (var batch in store.LeasePendingOperationsAsync(
            new(Stream, LeaseOperationLimit, DefaultEncodedSizeBytes, TimeSpan.FromMinutes(FirstSequence)),
            CancellationToken.None))
        {
            leased = batch;
        }

        return leased;
    }

    /// <summary>Attempts to lease one pending batch from an in-memory store.</summary>
    /// <param name="store">The store.</param>
    /// <param name="leaseDuration">The lease duration.</param>
    /// <returns>The leased operation batch, if one is available.</returns>
    private static async Task<LeasedOperationBatch?> TryLeaseOnlyBatchAsync(InMemoryLocalStoreAdapter store, TimeSpan leaseDuration)
    {
        LeasedOperationBatch? leased = null;
        await foreach (var batch in store.LeasePendingOperationsAsync(
            new(Stream, LeaseOperationLimit, DefaultEncodedSizeBytes, leaseDuration),
            CancellationToken.None))
        {
            leased = batch;
        }

        return leased;
    }
}
