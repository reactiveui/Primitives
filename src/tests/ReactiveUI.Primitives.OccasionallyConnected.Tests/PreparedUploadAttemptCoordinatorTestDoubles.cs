// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Test doubles for PreparedUploadAttemptCoordinator tests.</summary>
public sealed partial class PreparedUploadAttemptCoordinatorTests
{
    /// <summary>Records prepared transport activity.</summary>
    private sealed class RecordingPreparer : IRemoteTransportBatchPreparer
    {
        /// <summary>Gets or sets the prepared handle returned by this preparer.</summary>
        public RecordingPrepared? Prepared { get; set; }

        /// <summary>Gets or sets the result returned by the default prepared handle.</summary>
        public RemoteSyncResult? ResultOverride { get; set; }

        /// <summary>Gets or sets the prepare exception.</summary>
        public Exception? PrepareException { get; set; }

        /// <summary>Gets or sets the callback invoked before prepare returns.</summary>
        public Action? BeforePrepareReturns { get; set; }

        /// <summary>Gets or sets the asynchronous callback awaited before prepare returns.</summary>
        public Func<CancellationToken, ValueTask>? BeforePrepareCompletes { get; set; }

        /// <summary>Gets or sets the send exception.</summary>
        public Exception? SendException { get; set; }

        /// <summary>Gets or sets the dispose callback.</summary>
        public Func<ValueTask>? DisposeAsyncOverride { get; set; }

        /// <summary>Gets or sets the callback invoked before send returns.</summary>
        public Action? BeforeSendReturns { get; set; }

        /// <summary>Gets or sets the asynchronous callback awaited before send returns.</summary>
        public Func<CancellationToken, ValueTask>? BeforeSendCompletes { get; set; }

        /// <summary>Gets or sets the prepared encoded size.</summary>
        public long EncodedSizeBytes { get; set; } = DefaultEncodedSizeBytes / SecondSequence;

        /// <summary>Gets the prepare call count.</summary>
        public int PrepareCount { get; private set; }

        /// <inheritdoc/>
        public async ValueTask<IPreparedRemotePush> PreparePushAsync(SyncBatch batch, CancellationToken cancellationToken)
        {
            PrepareCount++;
            cancellationToken.ThrowIfCancellationRequested();
            if (PrepareException is not null)
            {
                throw PrepareException;
            }

            BeforePrepareReturns?.Invoke();
            if (BeforePrepareCompletes is not null)
            {
                await BeforePrepareCompletes(cancellationToken).ConfigureAwait(false);
            }

            Prepared ??= CreatePrepared(batch);
            return Prepared;
        }

        /// <summary>Creates a prepared handle.</summary>
        /// <param name="batch">The prepared batch.</param>
        /// <returns>The prepared handle.</returns>
        private RecordingPrepared CreatePrepared(SyncBatch batch) =>
            new(batch, ResultOverride ?? CreateResult(batch))
            {
                DisposeAsyncOverride = DisposeAsyncOverride,
                EncodedSizeBytes = EncodedSizeBytes,
                SendException = SendException,
                BeforeSendReturns = BeforeSendReturns,
                BeforeSendCompletes = BeforeSendCompletes,
            };
    }

    /// <summary>Records prepared handle activity.</summary>
    private sealed class RecordingPrepared : IPreparedRemotePush
    {
        /// <summary>Initializes a new instance of the <see cref="RecordingPrepared"/> class.</summary>
        /// <param name="batch">The prepared batch.</param>
        /// <param name="result">The remote result.</param>
        public RecordingPrepared(SyncBatch batch, RemoteSyncResult result)
        {
            Batch = batch;
            Result = result;
        }

        /// <summary>Gets or sets the send exception.</summary>
        public Exception? SendException { get; init; }

        /// <summary>Gets or sets the dispose callback.</summary>
        public Func<ValueTask>? DisposeAsyncOverride { get; init; }

        /// <summary>Gets or sets the callback invoked before send returns.</summary>
        public Action? BeforeSendReturns { get; init; }

        /// <summary>Gets or sets the asynchronous callback awaited before send returns.</summary>
        public Func<CancellationToken, ValueTask>? BeforeSendCompletes { get; init; }

        /// <summary>Gets the send call count.</summary>
        public int SendCount { get; private set; }

        /// <summary>Gets the dispose call count.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public SyncBatch Batch { get; }

        /// <inheritdoc/>
        public long EncodedSizeBytes { get; init; } = DefaultEncodedSizeBytes / SecondSequence;

        /// <summary>Gets the remote result returned by send.</summary>
        private RemoteSyncResult Result { get; }

        /// <inheritdoc/>
        public async ValueTask<RemoteSyncResult> SendAsync(CancellationToken cancellationToken)
        {
            SendCount++;
            cancellationToken.ThrowIfCancellationRequested();
            if (SendException is not null)
            {
                throw SendException;
            }

            BeforeSendReturns?.Invoke();
            if (BeforeSendCompletes is not null)
            {
                await BeforeSendCompletes(cancellationToken).ConfigureAwait(false);
            }

            return Result;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            return DisposeAsyncOverride?.Invoke() ?? ValueTask.CompletedTask;
        }
    }

    /// <summary>Records local store calls used by coordinator tests.</summary>
    private sealed class RecordingStore : ILocalStoreAdapter
    {
        /// <summary>The renewal wait gate.</summary>
#if NET9_0_OR_GREATER
        private readonly System.Threading.Lock _renewalGate = new();
#else
        private readonly object _renewalGate = new();
#endif

        /// <summary>The current renewal change signal.</summary>
        private TaskCompletionSource _renewalChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets durable statuses by operation id.</summary>
        public Dictionary<OperationId, SyncOperationStatus?> Statuses { get; } = [];

        /// <summary>Gets recorded call names.</summary>
        public List<string> Calls { get; } = [];

        /// <summary>Gets recorded barrier results.</summary>
        public List<AttemptBarrierResult> Barriers { get; } = [];

        /// <summary>Gets a signal set when a lease renews.</summary>
        public TaskCompletionSource Renewed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets renewal extensions requested by the coordinator.</summary>
        public List<TimeSpan> RenewalExtensions { get; } = [];

        /// <summary>Gets or sets the tracked fake lease expiry.</summary>
        public DateTimeOffset LeaseExpiresAtUtc { get; set; }

        /// <summary>Gets or sets the operation denied by the barrier.</summary>
        public OperationId? DeniedOperation { get; set; }

        /// <summary>Gets or sets the barrier index that throws.</summary>
        public int ThrowOnBarrierIndex { get; set; }

        /// <summary>Gets or sets the release exception.</summary>
        public Exception? ReleaseException { get; set; }

        /// <summary>Gets or sets the renewal exception.</summary>
        public Exception? RenewException { get; set; }

        /// <summary>Gets or sets a barrier receipt override.</summary>
        public Func<OperationId, int, AttemptBarrierResult>? BarrierOverride { get; set; }

        /// <summary>Gets or sets the asynchronous callback awaited before renewal completes.</summary>
        public Func<CancellationToken, ValueTask>? BeforeRenewCompletes { get; set; }

        /// <summary>Gets or sets a callback invoked after fake expiry is extended.</summary>
        public Action? AfterRenewAddsExtension { get; set; }

        /// <summary>Gets a signal set when a renewal call starts.</summary>
        public TaskCompletionSource RenewStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the lease renewal count.</summary>
        public int RenewCount { get; private set; }

        /// <summary>Gets the release count.</summary>
        public int ReleaseCount { get; private set; }

        /// <inheritdoc/>
        public LocalStoreCapabilities Capabilities => LocalStoreCapabilities.LeasedOutbox;

        /// <inheritdoc/>
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Calls.Add($"status-{operationId.Value:N}");
            return new(Statuses.TryGetValue(operationId, out var status) ? status : null);
        }

        /// <inheritdoc/>
        public ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
            Guid leaseId,
            OperationId operationId,
            int nextAttempt,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (ThrowOnBarrierIndex == Barriers.Count + FirstSequence)
            {
                throw new InvalidOperationException("barrier failed");
            }

            Calls.Add($"barrier-{Barriers.Count + FirstSequence}");
            var denied = DeniedOperation == operationId;
            var result = BarrierOverride?.Invoke(operationId, nextAttempt)
                ?? new AttemptBarrierResult(operationId, nextAttempt, !denied, denied ? "denied" : null);
            Barriers.Add(result);
            return new(result);
        }

        /// <inheritdoc/>
        public async ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _ = RenewStarted.TrySetResult();
            if (RenewException is not null)
            {
                throw RenewException;
            }

            if (BeforeRenewCompletes is not null)
            {
                await BeforeRenewCompletes(cancellationToken).ConfigureAwait(false);
            }

            Calls.Add("renew");
            TaskCompletionSource changed;
            lock (_renewalGate)
            {
                RenewalExtensions.Add(extension);
                LeaseExpiresAtUtc = LeaseExpiresAtUtc.Add(extension);
                RenewCount++;
                changed = _renewalChanged;
                _renewalChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            _ = changed.TrySetResult();
            _ = Renewed.TrySetResult();
            AfterRenewAddsExtension?.Invoke();
        }

        /// <summary>Waits until a renewal count is reached.</summary>
        /// <param name="expectedCount">The expected renewal count.</param>
        /// <returns>The wait task.</returns>
        public async Task WaitForRenewCountAsync(int expectedCount)
        {
            while (true)
            {
                Task changed;
                lock (_renewalGate)
                {
                    if (RenewCount >= expectedCount)
                    {
                        return;
                    }

                    changed = _renewalChanged.Task;
                }

                await changed.ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken)
        {
            ReleaseCount++;
            if (ReleaseException is not null)
            {
                throw ReleaseException;
            }

            return ValueTask.CompletedTask;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        /// <inheritdoc/>
        public ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<RecoveredStream> RecoverStreamAsync(
            StreamId streamId,
            SubscriptionId subscriptionId,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<LocalCommitResult> CommitLocalOperationAsync(
            SyncOperation operation,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public async IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
            OutboxLeaseRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask.ConfigureAwait(false);
            yield break;
        }

        /// <inheritdoc/>
        public ValueTask<LocalSnapshot> DeadLetterOperationAsync(Guid leaseId, OperationId operationId, string reasonCode, SnapshotMutation snapshotMutation, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            IReadOnlyList<SnapshotMutation> snapshotMutations,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
            StreamId streamId,
            IReadOnlyList<Guid> eventIds,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
            RemoteEventBatch batch,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }

    /// <summary>Delegates to a real store while exposing renewal observations.</summary>
    /// <param name="inner">The wrapped store.</param>
    private sealed class ObservedStore(ILocalStoreAdapter inner) : ILocalStoreAdapter
    {
        /// <summary>Gets a signal set when renewal starts.</summary>
        public TaskCompletionSource RenewStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets a signal set when renewal fails.</summary>
        public TaskCompletionSource RenewFailed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

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
        public async IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
            OutboxLeaseRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var batch in inner.LeasePendingOperationsAsync(request, cancellationToken).ConfigureAwait(false))
            {
                yield return batch;
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken) =>
            inner.GetOperationStatusAsync(operationId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
            Guid leaseId,
            OperationId operationId,
            int nextAttempt,
            CancellationToken cancellationToken) =>
            inner.TryBeginRemoteAttemptAsync(leaseId, operationId, nextAttempt, cancellationToken);

        /// <inheritdoc/>
        public async ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken)
        {
            _ = RenewStarted.TrySetResult();
            try
            {
                await inner.RenewLeaseAsync(leaseId, extension, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                _ = RenewFailed.TrySetResult();
                throw;
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken) =>
            inner.ReleaseLeaseAsync(leaseId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalSnapshot> DeadLetterOperationAsync(Guid leaseId, OperationId operationId, string reasonCode, SnapshotMutation snapshotMutation, CancellationToken cancellationToken) =>
            inner.DeadLetterOperationAsync(leaseId, operationId, reasonCode, snapshotMutation, cancellationToken);

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
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken) =>
            inner.GetRetryStateAsync(operationId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken) =>
            inner.SaveRetryStateAsync(operationId, retryState, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken) =>
            inner.CompactAsync(request, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    /// <summary>Creates timers that invoke their callback with invalid state.</summary>
    private sealed class InvalidTimerStateTimeProvider : TimeProvider
    {
        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            new InvalidTimerState(callback);

        /// <summary>A timer that replaces callback state.</summary>
        /// <param name="callback">The callback.</param>
        private sealed class InvalidTimerState(TimerCallback callback) : ITimer
        {
            /// <inheritdoc/>
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                callback(new());
                return true;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }

    /// <summary>Fails timer creation.</summary>
    private sealed class ThrowingTimerTimeProvider : TimeProvider
    {
        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            throw new InvalidOperationException("timer unavailable");
    }

    /// <summary>Creates timers that fail when disposed.</summary>
    private sealed class ThrowingTimerDisposeTimeProvider : TimeProvider
    {
        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            new ThrowingDisposeTimer();

        /// <summary>A timer whose synchronous disposal fails.</summary>
        private sealed class ThrowingDisposeTimer : ITimer
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Change(TimeSpan dueTime, TimeSpan period) => true;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose() => ThrowDisposeFailure();

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;

            /// <summary>Throws the timer dispose failure.</summary>
            /// <exception cref="InvalidOperationException">Timer disposal failed.</exception>
            private static void ThrowDisposeFailure() => throw new InvalidOperationException("timer dispose failed");
        }
    }

    /// <summary>Records loopback server calls.</summary>
    private sealed class RecordingHub : IServerStreamHub
    {
        /// <summary>Gets the number of apply calls.</summary>
        public int ApplyCalls { get; private set; }

        /// <inheritdoc/>
        public ValueTask<ServerSyncResult> ApplyOperationsAsync(
            SyncBatch batch,
            ClientIdentity client,
            CancellationToken cancellationToken)
        {
            ApplyCalls++;
            return new(new ServerSyncResult(CreateResult(batch), []));
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AcknowledgeAsync(
            ReceiveAcknowledgement acknowledgement,
            ClientIdentity client,
            CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc/>
        public async IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(
            RemoteSubscribeRequest request,
            ClientIdentity client,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask.ConfigureAwait(false);
            yield break;
        }
    }

    /// <summary>Owns a leased SQLite batch and its store.</summary>
    private sealed class SqliteLeaseContext : IAsyncDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="SqliteLeaseContext"/> class.</summary>
        /// <param name="lease">The leased batch.</param>
        /// <param name="store">The owned store.</param>
        public SqliteLeaseContext(LeasedOperationBatch lease, SqliteLocalStoreAdapter store)
        {
            Lease = lease;
            Store = store;
        }

        /// <summary>Gets the leased batch.</summary>
        public LeasedOperationBatch Lease { get; }

        /// <summary>Gets the owned SQLite store.</summary>
        public SqliteLocalStoreAdapter Store { get; }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync() => await Store.DisposeAsync().ConfigureAwait(false);
    }
}
