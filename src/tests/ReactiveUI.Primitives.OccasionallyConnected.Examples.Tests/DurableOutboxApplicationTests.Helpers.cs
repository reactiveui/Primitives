// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Extensions.Time.Testing;
using OccasionallyConnected.DurableOutbox;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Examples.Tests;

/// <summary>Tests for <see cref="DurableOutboxApplication"/>.</summary>
/// <content>Contains helper methods and composed store adapters.</content>
public sealed partial class DurableOutboxApplicationTests
{
    /// <summary>Runs an in-memory command through the application.</summary>
    /// <param name="command">The command to run.</param>
    /// <returns>The command result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<OutboxCommandResult> RunAsync(IOutboxCommand command) =>
        DurableOutboxApplication.RunAsync(command, CancellationToken.None);

    /// <summary>Runs an in-memory command through the provided application instance.</summary>
    /// <param name="application">The application instance.</param>
    /// <param name="command">The command to run.</param>
    /// <returns>The command result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<OutboxCommandResult> RunAsync(DurableOutboxApplication application, IOutboxCommand command) =>
        application.RunCommandAsync(command, CancellationToken.None).AsTask();

    /// <summary>Creates a synchronous disposal failure for failed-open cleanup tests.</summary>
    /// <returns>A failed disposal operation.</returns>
    /// <exception cref="InvalidOperationException">Always thrown to model a synchronous dispose invocation failure.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask CreateSynchronousDisposeFailure() =>
        throw new InvalidOperationException(DisposeFailureMessage);

    /// <summary>Forces dispose completion through an asynchronously queued continuation.</summary>
    /// <returns>A task that represents the asynchronous gate.</returns>
    private static async ValueTask AwaitDisposeGateAsync()
    {
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!ThreadPool.QueueUserWorkItem(static state => state.SetResult(true), gate, preferLocal: false))
        {
            gate.SetException(new InvalidOperationException("The asynchronous dispose gate could not be queued."));
        }

        await gate.Task.ConfigureAwait(false);
    }

    /// <summary>Creates the configured failed dispose operation.</summary>
    /// <returns>The failed dispose operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask CreateDisposeFailureAsync() =>
        ValueTask.FromException(new InvalidOperationException(DisposeFailureMessage));

    /// <summary>Creates a deterministic fake clock for lease workflow tests.</summary>
    /// <returns>The fake clock.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static FakeTimeProvider CreateClock() =>
        new(new DateTimeOffset(ClockStartYear, ClockStartMonth, ClockStartDay, 0, 0, 0, TimeSpan.Zero));

    /// <summary>Creates a durable status whose next attempt cannot be represented by <see cref="int"/>.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The exhausted durable operation status.</returns>
    private static SyncOperationStatus CreateExhaustedAttemptStatus(OperationId operationId) =>
        new(
            operationId,
            DurableOutboxApplication.TemperatureStream,
            SyncOperationState.QueuedForUpload,
            int.MaxValue,
            DateTimeOffset.UnixEpoch,
            ReasonCode: null);

    /// <summary>Returns the real recovered stream without its durable snapshot.</summary>
    /// <param name="inner">The real local store adapter.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The recovered stream with no snapshot.</returns>
    private static async ValueTask<RecoveredStream> RecoverWithoutSnapshotAsync(
        ILocalStoreAdapter inner,
        StreamId streamId,
        SubscriptionId subscriptionId,
        CancellationToken cancellationToken)
    {
        var recovered = await inner.RecoverStreamAsync(streamId, subscriptionId, cancellationToken).ConfigureAwait(false);
        return new(
            recovered.SubscriptionId,
            recovered.ServerCursor,
            snapshot: null,
            recovered.PendingOperations,
            recovered.DeadLetters,
            recovered.NextClientSequence) { ReplayOperations = recovered.ReplayOperations };
    }

    /// <summary>Creates the application serializer for direct durable-store seed data.</summary>
    /// <returns>The serializer configured with the sample contracts.</returns>
    private static JsonPayloadSerializer CreateAppSerializer()
    {
        var schemaRegistry = new SchemaRegistry()
            .Register(ReadingContract, PayloadSchemaVersion, DurableOutboxJsonContext.Default.TemperatureReading)
            .Register(SnapshotContract, PayloadSchemaVersion, DurableOutboxJsonContext.Default.TemperatureSnapshot);
        return new(schemaRegistry);
    }

    /// <summary>Creates a direct durable-store seed operation with the same payload contract as the sample app.</summary>
    /// <param name="serializer">The payload serializer.</param>
    /// <param name="clientSequence">The client sequence to persist.</param>
    /// <param name="reading">The temperature reading to persist.</param>
    /// <returns>The seed operation.</returns>
    private static async Task<SyncOperation> CreateDirectStoreOperationAsync(
        JsonPayloadSerializer serializer,
        long clientSequence,
        double reading)
    {
        var observedAt = DateTimeOffset.UnixEpoch.AddMinutes(clientSequence);
        var payload = await serializer.SerializeAsync(
            ReadingContract,
            PayloadSchemaVersion,
            new TemperatureReading(DeviceId, reading, Unit, observedAt),
            CancellationToken.None);
        return new()
        {
            OperationId = OperationId.New(),
            StreamId = DurableOutboxApplication.TemperatureStream,
            ClientSequence = clientSequence,
            TimestampUtc = observedAt,
            Type = SyncOperationType.Append,
            Payload = payload,
            Policy = new(DeliveryGuarantee.AtLeastOnce, OperationDurability.Durable, DefaultPriority, ConflictPolicy.Merge),
            Metadata = new Dictionary<string, string>(),
        };
    }

    /// <summary>Creates a direct durable-store seed snapshot without authoritative state.</summary>
    /// <param name="serializer">The payload serializer.</param>
    /// <param name="readingCount">The snapshot reading count.</param>
    /// <param name="lastReading">The last reading in the snapshot.</param>
    /// <param name="totalReading">The total reading in the snapshot.</param>
    /// <param name="expectedRevision">The expected snapshot revision.</param>
    /// <returns>The seed snapshot mutation.</returns>
    private static async Task<SnapshotMutation> CreateDirectStoreSnapshotAsync(
        JsonPayloadSerializer serializer,
        int readingCount,
        double lastReading,
        double totalReading,
        long expectedRevision)
    {
        var snapshot = new TemperatureSnapshot(
            readingCount,
            DeviceId,
            lastReading,
            DateTimeOffset.UnixEpoch.AddMinutes(readingCount),
            totalReading);
        var payload = await serializer.SerializeAsync(SnapshotContract, PayloadSchemaVersion, snapshot, CancellationToken.None);
        return new(DurableOutboxApplication.TemperatureStream, payload, SnapshotFormatVersion, expectedRevision);
    }

    /// <summary>Leases the first pending operation using the public SQLite store adapter.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="timeProvider">The time provider used by the store.</param>
    /// <returns>A task that represents the asynchronous lease operation.</returns>
    private static async Task LeaseFirstPendingOperationAsync(string databasePath, TimeProvider timeProvider)
    {
        SqliteLocalStoreAdapterOptions options = new() { TimeProvider = timeProvider };
        await using SqliteLocalStoreAdapter store = new(databasePath, options);
        await store.InitializeAsync(
            new(StoreIdentity, StoreSchemaVersion, RequireAuthenticatedEncryptionAtRest: false) { ClientId = StoreClientId },
            CancellationToken.None);
        var request = new OutboxLeaseRequest(
            DurableOutboxApplication.TemperatureStream,
            DurableOutboxApplication.MaximumLeaseOperations,
            LeaseCapacityBytes,
            TimeSpan.FromMinutes(LeaseExpiryAdvanceMinutes));
        var batches = store.LeasePendingOperationsAsync(request, CancellationToken.None);
        await using var enumerator = batches.GetAsyncEnumerator(CancellationToken.None);
        var hasLease = await enumerator.MoveNextAsync();
        await Assert.That(hasLease).IsTrue();
    }

    /// <summary>A command that throws an expected command exception.</summary>
    /// <param name="exception">The exception to throw.</param>
    private sealed class ThrowingCommand(Exception exception) : IOutboxCommand
    {
        /// <inheritdoc/>
        public ValueTask<OutboxCommandResult> ExecuteAsync(
            DurableOutboxApplication application,
            CancellationToken cancellationToken) =>
            throw exception;
    }

    /// <summary>Shares a single simulated competing attempt across reopened store wrappers.</summary>
    private sealed class AttemptPreemption
    {
        /// <summary>A value indicating whether the competing barrier was already recorded.</summary>
        private int _preempted;

        /// <summary>Tries to claim the one competing attempt.</summary>
        /// <returns><see langword="true"/> when the caller should preempt the attempt.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryClaim() => Interlocked.Exchange(ref _preempted, 1) == 0;
    }

    /// <summary>Observes durable store operations performed through a composed test adapter.</summary>
    private sealed class StoreAdapterObserver
    {
        /// <summary>The number of observed barrier calls.</summary>
        private int _beginAttemptCalls;

        /// <summary>The number of observed sync result apply calls.</summary>
        private int _applyResultCalls;

        /// <summary>Gets the number of observed barrier calls.</summary>
        internal int BeginAttemptCalls => Volatile.Read(ref _beginAttemptCalls);

        /// <summary>Gets the number of observed sync result apply calls.</summary>
        internal int ApplyResultCalls => Volatile.Read(ref _applyResultCalls);

        /// <summary>Records one durable send barrier call.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void RecordBeginAttempt() => Interlocked.Increment(ref _beginAttemptCalls);

        /// <summary>Records one durable sync result apply call.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void RecordApplyResult() => Interlocked.Increment(ref _applyResultCalls);
    }

    /// <summary>Wraps a real local store and lets one competing sender win the first attempt barrier.</summary>
    /// <param name="inner">The real local store adapter.</param>
    /// <param name="preemption">The shared preemption state.</param>
    private sealed class PreemptingAttemptStoreAdapter(ILocalStoreAdapter inner, AttemptPreemption preemption) : ILocalStoreAdapter
    {
        /// <inheritdoc/>
        public LocalStoreCapabilities Capabilities => inner.Capabilities;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken) =>
            inner.InitializeAsync(initialization, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken) =>
            inner.GetOrCreateSubscriptionIdAsync(streamId, preferredId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RecoveredStream> RecoverStreamAsync(
            StreamId streamId,
            SubscriptionId subscriptionId,
            CancellationToken cancellationToken) =>
            inner.RecoverStreamAsync(streamId, subscriptionId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalCommitResult> CommitLocalOperationAsync(
            SyncOperation operation,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            inner.CommitLocalOperationAsync(operation, snapshotMutation, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
            OutboxLeaseRequest request,
            CancellationToken cancellationToken) =>
            inner.LeasePendingOperationsAsync(request, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken) =>
            inner.ApplySyncResultAsync(leaseId, result, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            IReadOnlyList<SnapshotMutation> snapshotMutations,
            CancellationToken cancellationToken) =>
            inner.ApplySyncResultAsync(leaseId, result, snapshotMutations, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalSnapshot> DeadLetterOperationAsync(
            Guid leaseId,
            OperationId operationId,
            string reasonCode,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            inner.DeadLetterOperationAsync(leaseId, operationId, reasonCode, snapshotMutation, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
            StreamId streamId,
            IReadOnlyList<Guid> eventIds,
            CancellationToken cancellationToken) =>
            inner.GetUnappliedEventIdsAsync(streamId, eventIds, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
            RemoteEventBatch batch,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            inner.ApplyRemoteBatchAsync(batch, snapshotMutation, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken) =>
            inner.GetOperationStatusAsync(operationId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken) =>
            inner.GetRetryStateAsync(operationId, cancellationToken);

        /// <inheritdoc/>
        public async ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
            Guid leaseId,
            OperationId operationId,
            int nextAttempt,
            CancellationToken cancellationToken)
        {
            if (preemption.TryClaim())
            {
                _ = await inner.TryBeginRemoteAttemptAsync(leaseId, operationId, nextAttempt, cancellationToken)
                    .ConfigureAwait(false);
            }

            return await inner.TryBeginRemoteAttemptAsync(leaseId, operationId, nextAttempt, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken) =>
            inner.SaveRetryStateAsync(operationId, retryState, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken) =>
            inner.RenewLeaseAsync(leaseId, extension, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken) =>
            inner.ReleaseLeaseAsync(leaseId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken) =>
            inner.CompactAsync(request, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() =>
            inner.DisposeAsync();
    }

    /// <summary>Wraps a real local store and throws from initialization with asynchronous cleanup.</summary>
    /// <param name="inner">The real local store adapter.</param>
    /// <param name="failDispose">A value indicating whether asynchronous cleanup should fail.</param>
    private sealed class ThrowingDisposeOnInitializeFailureStoreAdapter(
        ILocalStoreAdapter inner,
        bool failDispose = true) : ILocalStoreAdapter
    {
        /// <inheritdoc/>
        public LocalStoreCapabilities Capabilities => inner.Capabilities;

        /// <inheritdoc/>
        public async ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken)
        {
            await inner.InitializeAsync(initialization, cancellationToken).ConfigureAwait(false);
            throw new InvalidOperationException(OpenFailureMessage);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken) =>
            inner.GetOrCreateSubscriptionIdAsync(streamId, preferredId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RecoveredStream> RecoverStreamAsync(
            StreamId streamId,
            SubscriptionId subscriptionId,
            CancellationToken cancellationToken) =>
            inner.RecoverStreamAsync(streamId, subscriptionId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalCommitResult> CommitLocalOperationAsync(
            SyncOperation operation,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            inner.CommitLocalOperationAsync(operation, snapshotMutation, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
            OutboxLeaseRequest request,
            CancellationToken cancellationToken) =>
            inner.LeasePendingOperationsAsync(request, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken) =>
            inner.ApplySyncResultAsync(leaseId, result, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            IReadOnlyList<SnapshotMutation> snapshotMutations,
            CancellationToken cancellationToken) =>
            inner.ApplySyncResultAsync(leaseId, result, snapshotMutations, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalSnapshot> DeadLetterOperationAsync(
            Guid leaseId,
            OperationId operationId,
            string reasonCode,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            inner.DeadLetterOperationAsync(leaseId, operationId, reasonCode, snapshotMutation, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
            StreamId streamId,
            IReadOnlyList<Guid> eventIds,
            CancellationToken cancellationToken) =>
            inner.GetUnappliedEventIdsAsync(streamId, eventIds, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
            RemoteEventBatch batch,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            inner.ApplyRemoteBatchAsync(batch, snapshotMutation, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken) =>
            inner.GetOperationStatusAsync(operationId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken) =>
            inner.GetRetryStateAsync(operationId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
            Guid leaseId,
            OperationId operationId,
            int nextAttempt,
            CancellationToken cancellationToken) =>
            inner.TryBeginRemoteAttemptAsync(leaseId, operationId, nextAttempt, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken) =>
            inner.SaveRetryStateAsync(operationId, retryState, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken) =>
            inner.RenewLeaseAsync(leaseId, extension, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken) =>
            inner.ReleaseLeaseAsync(leaseId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken) =>
            inner.CompactAsync(request, cancellationToken);

        /// <inheritdoc/>
        public ValueTask DisposeAsync() =>
            new(DisposeCoreAsync());

        /// <summary>Disposes the inner SQLite adapter after an asynchronous completion gate.</summary>
        /// <returns>A task that represents the asynchronous dispose operation.</returns>
        private async Task DisposeCoreAsync()
        {
            await inner.DisposeAsync().ConfigureAwait(false);
            await AwaitDisposeGateAsync().ConfigureAwait(false);
            if (!failDispose)
            {
                return;
            }

            await CreateDisposeFailureAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Wraps a real local store while observing or overriding selected adapter calls for resilience tests.</summary>
    /// <param name="inner">The real local store adapter.</param>
    /// <param name="observer">The store call observer.</param>
    /// <param name="statusFactory">The optional replacement status factory.</param>
    /// <param name="recoveryFactory">The optional replacement recovery factory.</param>
    /// <param name="attemptFactory">The optional replacement attempt barrier factory.</param>
    /// <param name="disposeFactory">The optional replacement dispose operation.</param>
    private sealed class ObservingStoreAdapter(
        ILocalStoreAdapter inner,
        StoreAdapterObserver observer,
        Func<ILocalStoreAdapter, OperationId, CancellationToken, ValueTask<SyncOperationStatus?>>? statusFactory = null,
        Func<ILocalStoreAdapter, StreamId, SubscriptionId, CancellationToken, ValueTask<RecoveredStream>>? recoveryFactory = null,
        Func<ILocalStoreAdapter, Guid, OperationId, int, CancellationToken, ValueTask<AttemptBarrierResult>>?
            attemptFactory = null,
        Func<ValueTask>? disposeFactory = null) : ILocalStoreAdapter
    {
        /// <inheritdoc/>
        public LocalStoreCapabilities Capabilities => inner.Capabilities;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken) =>
            inner.InitializeAsync(initialization, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken) =>
            inner.GetOrCreateSubscriptionIdAsync(streamId, preferredId, cancellationToken);

        /// <inheritdoc/>
        public async ValueTask<RecoveredStream> RecoverStreamAsync(
            StreamId streamId,
            SubscriptionId subscriptionId,
            CancellationToken cancellationToken) =>
            recoveryFactory is null
                ? await inner.RecoverStreamAsync(streamId, subscriptionId, cancellationToken).ConfigureAwait(false)
                : await recoveryFactory(inner, streamId, subscriptionId, cancellationToken).ConfigureAwait(false);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalCommitResult> CommitLocalOperationAsync(
            SyncOperation operation,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            inner.CommitLocalOperationAsync(operation, snapshotMutation, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
            OutboxLeaseRequest request,
            CancellationToken cancellationToken) =>
            inner.LeasePendingOperationsAsync(request, cancellationToken);

        /// <inheritdoc/>
        public async ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken)
        {
            observer.RecordApplyResult();
            await inner.ApplySyncResultAsync(leaseId, result, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            IReadOnlyList<SnapshotMutation> snapshotMutations,
            CancellationToken cancellationToken)
        {
            observer.RecordApplyResult();
            return await inner.ApplySyncResultAsync(leaseId, result, snapshotMutations, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalSnapshot> DeadLetterOperationAsync(
            Guid leaseId,
            OperationId operationId,
            string reasonCode,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            inner.DeadLetterOperationAsync(leaseId, operationId, reasonCode, snapshotMutation, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
            StreamId streamId,
            IReadOnlyList<Guid> eventIds,
            CancellationToken cancellationToken) =>
            inner.GetUnappliedEventIdsAsync(streamId, eventIds, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
            RemoteEventBatch batch,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            inner.ApplyRemoteBatchAsync(batch, snapshotMutation, cancellationToken);

        /// <inheritdoc/>
        public async ValueTask<SyncOperationStatus?> GetOperationStatusAsync(
            OperationId operationId,
            CancellationToken cancellationToken) =>
            statusFactory is null
                ? await inner.GetOperationStatusAsync(operationId, cancellationToken).ConfigureAwait(false)
                : await statusFactory(inner, operationId, cancellationToken).ConfigureAwait(false);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken) =>
            inner.GetRetryStateAsync(operationId, cancellationToken);

        /// <inheritdoc/>
        public async ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
            Guid leaseId,
            OperationId operationId,
            int nextAttempt,
            CancellationToken cancellationToken)
        {
            observer.RecordBeginAttempt();
            return attemptFactory is null
                ? await inner.TryBeginRemoteAttemptAsync(leaseId, operationId, nextAttempt, cancellationToken)
                    .ConfigureAwait(false)
                : await attemptFactory(inner, leaseId, operationId, nextAttempt, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken) =>
            inner.SaveRetryStateAsync(operationId, retryState, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken) =>
            inner.RenewLeaseAsync(leaseId, extension, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken) =>
            inner.ReleaseLeaseAsync(leaseId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken) =>
            inner.CompactAsync(request, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() =>
            disposeFactory?.Invoke() ?? inner.DisposeAsync();
    }
}
