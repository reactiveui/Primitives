// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ILocalStoreAdapterExtensions"/>.</summary>
public sealed class ILocalStoreAdapterExtensionsTests
{
    /// <summary>The number of adapter calls expected by the forwarding test.</summary>
    private const int AdapterCallCount = 14;

    /// <summary>The attempted send count.</summary>
    private const int AttemptNumber = 3;

    /// <summary>The expected snapshot revision.</summary>
    private const int ExpectedRevision = 1;

    /// <summary>The initial lease duration in minutes.</summary>
    private const int LeaseMinutes = 1;

    /// <summary>The maximum operation count.</summary>
    private const int MaximumBatchSize = 2;

    /// <summary>The maximum payload byte count.</summary>
    private const int MaximumBytes = 512;

    /// <summary>The renewal duration in minutes.</summary>
    private const int RenewalMinutes = 2;

    /// <summary>The committed snapshot revision.</summary>
    private const int SnapshotRevision = 0;

    /// <summary>Verifies the convenience overloads forward their arguments and cancellation token exactly once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConvenienceOverloadsForwardEveryArgumentExactlyOnce()
    {
        var adapter = new RecordingAdapter();
        var operation = CreateOperation();
        var mutation = CreateMutation(operation);
        var initialization = CreateInitialization();
        var subscription = SubscriptionId.New();
        var request = CreateLeaseRequest(operation);
        var leaseId = Guid.NewGuid();
        var result = new RemoteSyncResult(Guid.NewGuid(), [], null, null);
        var ids = new[] { Guid.NewGuid() };
        var batch = new RemoteEventBatch(Guid.NewGuid(), operation.StreamId, null, "cursor", []);
        var retry = RetryState.Start(DateTimeOffset.UnixEpoch);
        var compact = new CompactionRequest(operation.StreamId, DateTimeOffset.UnixEpoch, MaximumBytes);

        await adapter.InitializeAsync(initialization);
        var recovered = await adapter.RecoverStreamAsync(operation.StreamId, subscription);
        var committed = await adapter.CommitLocalOperationAsync(operation, mutation);
        var leases = new List<LeasedOperationBatch>();
        await foreach (var leased in adapter.LeasePendingOperationsAsync(request))
        {
            leases.Add(leased);
        }

        await adapter.ApplySyncResultAsync(leaseId, result);
        var unapplied = await adapter.GetUnappliedEventIdsAsync(operation.StreamId, ids);
        var remoteApplied = await adapter.ApplyRemoteBatchAsync(batch, mutation);
        var status = await adapter.GetOperationStatusAsync(operation.OperationId);
        var savedRetry = await adapter.GetRetryStateAsync(operation.OperationId);
        var barrier = await adapter.TryBeginRemoteAttemptAsync(leaseId, operation.OperationId, AttemptNumber);
        await adapter.SaveRetryStateAsync(operation.OperationId, retry);
        await adapter.RenewLeaseAsync(leaseId, TimeSpan.FromMinutes(RenewalMinutes));
        await adapter.ReleaseLeaseAsync(leaseId);
        var compaction = await adapter.CompactAsync(compact);

        await Assert.That(recovered.SubscriptionId).IsEqualTo(subscription);
        await Assert.That(committed.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(leases).Count().IsEqualTo(1);
        await Assert.That(unapplied).IsSameReferenceAs(ids);
        await Assert.That(remoteApplied.NextCursor).IsEqualTo(batch.NextCursor);
        await Assert.That(status).IsNull();
        await Assert.That(savedRetry).IsEqualTo(retry);
        await Assert.That(barrier.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(compaction).IsEqualTo(new(0, 0));
        await Assert.That(adapter.Calls).Count().IsEqualTo(AdapterCallCount);
        await AssertSetupCalls(adapter, initialization, operation, mutation, subscription, request);
        await AssertResultCalls(adapter, operation, leaseId, result, ids, batch, mutation);
        await AssertLeaseCalls(adapter, operation, leaseId, retry, compact);
    }

    /// <summary>Verifies the initialize overload propagates adapter failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task InitializeAsyncPropagatesAdapterFailure()
    {
        var error = new InvalidOperationException("failure");
        var adapter = new RecordingAdapter { Error = error };
        Func<Task> action = async () => await adapter.InitializeAsync(new("store", ExpectedRevision, false));
        var thrown = await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        await Assert.That(thrown).IsSameReferenceAs(error);
        await Assert.That(adapter.Calls).Count().IsEqualTo(1);
    }

    /// <summary>Asserts the setup-related recorded adapter calls.</summary>
    /// <param name="adapter">The recording adapter.</param>
    /// <param name="initialization">The initialization request.</param>
    /// <param name="operation">The synchronization operation.</param>
    /// <param name="mutation">The snapshot mutation.</param>
    /// <param name="subscription">The subscription identifier.</param>
    /// <param name="request">The lease request.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertSetupCalls(
        RecordingAdapter adapter,
        LocalStoreInitialization initialization,
        SyncOperation operation,
        SnapshotMutation mutation,
        SubscriptionId subscription,
        OutboxLeaseRequest request)
    {
        await AssertCall(adapter.Calls[0], initialization);
        await AssertCall(adapter.Calls[1], operation.StreamId, subscription);
        await AssertCall(adapter.Calls[2], operation, mutation);
        await AssertCall(adapter.Calls[3], request);
    }

    /// <summary>Asserts the result-related recorded adapter calls.</summary>
    /// <param name="adapter">The recording adapter.</param>
    /// <param name="operation">The synchronization operation.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="result">The remote synchronization result.</param>
    /// <param name="ids">The event identifiers.</param>
    /// <param name="batch">The remote event batch.</param>
    /// <param name="mutation">The snapshot mutation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertResultCalls(
        RecordingAdapter adapter,
        SyncOperation operation,
        Guid leaseId,
        RemoteSyncResult result,
        Guid[] ids,
        RemoteEventBatch batch,
        SnapshotMutation mutation)
    {
        await AssertCall(adapter.Calls[4], leaseId, result);
        await AssertCall(adapter.Calls[5], operation.StreamId, ids);
        await AssertCall(adapter.Calls[6], batch, mutation);
        await AssertCall(adapter.Calls[7], operation.OperationId);
        await AssertCall(adapter.Calls[8], operation.OperationId);
    }

    /// <summary>Asserts the lease-related recorded adapter calls.</summary>
    /// <param name="adapter">The recording adapter.</param>
    /// <param name="operation">The synchronization operation.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="retry">The retry state.</param>
    /// <param name="compact">The compaction request.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertLeaseCalls(
        RecordingAdapter adapter,
        SyncOperation operation,
        Guid leaseId,
        RetryState retry,
        CompactionRequest compact)
    {
        await AssertCall(adapter.Calls[9], leaseId, operation.OperationId, AttemptNumber);
        await AssertCall(adapter.Calls[10], operation.OperationId, retry);
        await AssertCall(adapter.Calls[11], leaseId, TimeSpan.FromMinutes(RenewalMinutes));
        await AssertCall(adapter.Calls[12], leaseId);
        await AssertCall(adapter.Calls[13], compact);
    }

    /// <summary>Asserts that one recorded call matches the expected arguments and default token.</summary>
    /// <param name="call">The recorded call arguments.</param>
    /// <param name="expected">The expected arguments before the cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertCall(object?[] call, params object?[] expected)
    {
        await Assert.That(call).Count().IsEqualTo(expected.Length + 1);

        for (var index = 0; index < expected.Length; index++)
        {
            await Assert.That(call[index]).IsEqualTo(expected[index]);
        }

        await Assert.That(call[^1]).IsEqualTo(CancellationToken.None);
    }

    /// <summary>Creates an initialization request for forwarding assertions.</summary>
    /// <returns>A local store initialization request.</returns>
    private static LocalStoreInitialization CreateInitialization() => new("store", ExpectedRevision, false);

    /// <summary>Creates a lease request for forwarding assertions.</summary>
    /// <param name="operation">The synchronization operation.</param>
    /// <returns>An outbox lease request.</returns>
    private static OutboxLeaseRequest CreateLeaseRequest(SyncOperation operation) =>
        new(operation.StreamId, MaximumBatchSize, MaximumBytes, TimeSpan.FromMinutes(LeaseMinutes));

    /// <summary>Creates a snapshot mutation for forwarding assertions.</summary>
    /// <param name="operation">The synchronization operation.</param>
    /// <returns>A snapshot mutation.</returns>
    private static SnapshotMutation CreateMutation(SyncOperation operation) =>
        new(operation.StreamId, operation.Payload, ExpectedRevision, SnapshotRevision);

    /// <summary>Creates a synchronization operation for forwarding assertions.</summary>
    /// <returns>A synchronization operation.</returns>
    private static SyncOperation CreateOperation() =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = new("sensor/temperature"),
            ClientSequence = ExpectedRevision,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Append,
            Payload = new("reading", ExpectedRevision, "application/json", ReadOnlyMemory<byte>.Empty, "hash"),
            Policy = OperationPolicy.Default,
            Metadata = new Dictionary<string, string>(),
        };

    /// <summary>Records local store adapter calls.</summary>
    private sealed class RecordingAdapter : ILocalStoreAdapter
    {
        /// <summary>Gets the calls recorded by the adapter.</summary>
        public List<object?[]> Calls { get; } = [];

        /// <summary>Gets the exception to throw from failing members.</summary>
        public Exception? Error { get; init; }

        /// <summary>Gets the local store capabilities.</summary>
        public LocalStoreCapabilities Capabilities => LocalStoreCapabilities.None;

        /// <inheritdoc />
        public ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken)
        {
            Calls.Add([initialization, cancellationToken]);
            return Error is null ? ValueTask.CompletedTask : ValueTask.FromException(Error);
        }

        /// <inheritdoc />
        public ValueTask<RecoveredStream> RecoverStreamAsync(
            StreamId streamId,
            SubscriptionId subscriptionId,
            CancellationToken cancellationToken)
        {
            Calls.Add([streamId, subscriptionId, cancellationToken]);
            return new(
                new RecoveredStream(subscriptionId, null, null, [], [], SnapshotRevision));
        }

        /// <inheritdoc />
        public ValueTask<LocalCommitResult> CommitLocalOperationAsync(
            SyncOperation operation,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken)
        {
            Calls.Add([operation, snapshotMutation, cancellationToken]);
            return new(
                new LocalCommitResult(
                    operation.OperationId,
                    operation.ClientSequence,
                    SnapshotRevision,
                    DateTimeOffset.UnixEpoch));
        }

        /// <inheritdoc />
        public IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
            OutboxLeaseRequest request,
            CancellationToken cancellationToken)
        {
            Calls.Add([request, cancellationToken]);
            return Leases();
        }

        /// <inheritdoc />
        public ValueTask ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            CancellationToken cancellationToken)
        {
            Calls.Add([leaseId, result, cancellationToken]);
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc />
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
            StreamId streamId,
            IReadOnlyList<Guid> eventIds,
            CancellationToken cancellationToken)
        {
            Calls.Add([streamId, eventIds, cancellationToken]);
            return new(eventIds);
        }

        /// <inheritdoc />
        public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
            RemoteEventBatch batch,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken)
        {
            Calls.Add([batch, snapshotMutation, cancellationToken]);
            return new(
                new RemoteApplyResult(batch.NextCursor, SnapshotRevision, SnapshotRevision, SnapshotRevision));
        }

        /// <inheritdoc />
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(
            OperationId operationId,
            CancellationToken cancellationToken)
        {
            Calls.Add([operationId, cancellationToken]);
            return new((SyncOperationStatus?)null);
        }

        /// <inheritdoc />
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken)
        {
            Calls.Add([operationId, cancellationToken]);
            return new((RetryState?)RetryState.Start(DateTimeOffset.UnixEpoch));
        }

        /// <inheritdoc />
        public ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
            Guid leaseId,
            OperationId operationId,
            int nextAttempt,
            CancellationToken cancellationToken)
        {
            Calls.Add([leaseId, operationId, nextAttempt, cancellationToken]);
            return new(
                new AttemptBarrierResult(operationId, nextAttempt, true, null));
        }

        /// <inheritdoc />
        public ValueTask SaveRetryStateAsync(
            OperationId operationId,
            RetryState retryState,
            CancellationToken cancellationToken)
        {
            Calls.Add([operationId, retryState, cancellationToken]);
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc />
        public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken)
        {
            Calls.Add([leaseId, extension, cancellationToken]);
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc />
        public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken)
        {
            Calls.Add([leaseId, cancellationToken]);
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc />
        public ValueTask<CompactionResult> CompactAsync(
            CompactionRequest request,
            CancellationToken cancellationToken)
        {
            Calls.Add([request, cancellationToken]);
            return new(new CompactionResult(0, 0));
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        /// <summary>Creates the leased batches.</summary>
        /// <returns>The leased batches.</returns>
        private static async IAsyncEnumerable<LeasedOperationBatch> Leases()
        {
            await Task.CompletedTask;
            yield return new(Guid.NewGuid(), DateTimeOffset.UnixEpoch, []);
        }
    }
}
