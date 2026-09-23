// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Store test doubles for <see cref="SyncEngineTests"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Records store initialization.</summary>
    private sealed class RecordingStore : ILocalStoreAdapter
    {
        /// <summary>Stores the currently leased batch for optional release replay.</summary>
        private LeasedOperationBatch? _activeLease;

        /// <summary>Gets the number of initialization calls.</summary>
        public int InitializeCalls { get; private set; }

        /// <summary>Gets the number of store disposal calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <summary>Gets the number of lease requests.</summary>
        public List<OutboxLeaseRequest> LeaseRequests { get; } = [];

        /// <summary>Gets queued lease batches.</summary>
        public Queue<LeasedOperationBatch> Leases { get; } = [];

        /// <summary>Gets configured operation statuses by identity.</summary>
        public Dictionary<OperationId, SyncOperationStatus> Statuses { get; } = [];

        /// <summary>Gets configured retry states by identity.</summary>
        public Dictionary<OperationId, RetryState> RetryStates { get; } = [];

        /// <summary>Gets or sets recovered stream state returned by the store.</summary>
        public RecoveredStream Recovery { get; set; } = new(Subscription, null, null, [], [], FirstSequence);

        /// <summary>Gets the number of recover stream calls.</summary>
        public int RecoverStreamCalls { get; private set; }

        /// <summary>Gets or sets the optional initialization callback.</summary>
        public Action? OnInitialize { get; init; }

        /// <summary>Gets or sets a value indicating whether dispose throws synchronously.</summary>
        public bool ThrowOnDispose { get; init; }

        /// <summary>Gets or sets the optional status query exception.</summary>
        public Exception? StatusQueryException { get; set; }

        /// <summary>Gets or sets the optional retry-state query exception.</summary>
        public Exception? RetryStateQueryException { get; set; }

        /// <summary>Gets or sets the optional lease enumeration exception.</summary>
        public Exception? LeasePendingOperationsException { get; set; }

        /// <summary>Gets or sets the optional retry-state save exception.</summary>
        public Exception? RetryStateSaveException { get; set; }

        /// <summary>Gets or sets the optional lease release exception.</summary>
        public Exception? ReleaseLeaseException { get; set; }

        /// <summary>Gets or sets a value indicating whether remote attempt barriers deny sending.</summary>
        public bool DenyRemoteAttempt { get; set; }

        /// <summary>Gets or sets the one-based barrier call to deny.</summary>
        public int DenyRemoteAttemptOnCall { get; set; }

        /// <summary>Gets or sets a value indicating whether released leases are made available for the next request.</summary>
        public bool RequeueReleasedLeases { get; set; }

        /// <summary>Gets the number of release lease calls.</summary>
        public int ReleaseLeaseCalls { get; private set; }

        /// <summary>Gets the number of barrier calls.</summary>
        public int BarrierCalls { get; private set; }

        /// <summary>Gets or sets the optional signal set when a remote attempt barrier begins.</summary>
        public TaskCompletionSource? BarrierEntered { get; set; }

        /// <summary>Gets or sets the optional signal that releases the remote attempt barrier.</summary>
        public TaskCompletionSource? ReleaseBarrier { get; set; }

        /// <summary>Gets the number of status query calls.</summary>
        public int StatusQueryCalls { get; private set; }

        /// <summary>Gets the optional signal set when initialization begins.</summary>
        public TaskCompletionSource? InitializeEntered { get; init; }

        /// <summary>Gets the optional signal that releases initialization.</summary>
        public TaskCompletionSource? ReleaseInitialize { get; init; }

        /// <inheritdoc/>
        public LocalStoreCapabilities Capabilities { get; init; } = RecordingStoreUploadCapabilities;

        /// <summary>Gets the last preferred subscription identity.</summary>
        public SubscriptionId? LastPreferredSubscriptionId { get; private set; }

        /// <inheritdoc/>
        public async ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken)
        {
            _ = initialization;
            cancellationToken.ThrowIfCancellationRequested();
            InitializeCalls++;
            OnInitialize?.Invoke();
            _ = InitializeEntered?.TrySetResult();
            if (ReleaseInitialize is not null)
            {
                await ReleaseInitialize.Task.ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            return ThrowOnDispose ? ThrowStoreDisposeFailure() : default;
        }

        /// <inheritdoc/>
        public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(StreamId streamId, SubscriptionId? preferredId, CancellationToken cancellationToken)
        {
            _ = streamId;
            cancellationToken.ThrowIfCancellationRequested();
            LastPreferredSubscriptionId = preferredId;
            return new(preferredId ?? Subscription);
        }

        /// <inheritdoc/>
        public ValueTask<RecoveredStream> RecoverStreamAsync(StreamId streamId, SubscriptionId subscriptionId, CancellationToken cancellationToken)
        {
            _ = streamId;
            _ = subscriptionId;
            cancellationToken.ThrowIfCancellationRequested();
            RecoverStreamCalls++;
            return new(Recovery);
        }

        /// <inheritdoc/>
        public ValueTask<LocalCommitResult> CommitLocalOperationAsync(SyncOperation operation, SnapshotMutation snapshotMutation, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public async IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
            OutboxLeaseRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            LeaseRequests.Add(request);
            await Task.CompletedTask.ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            if (LeasePendingOperationsException is not null)
            {
                throw LeasePendingOperationsException;
            }

            if (Leases.TryDequeue(out var lease))
            {
                _activeLease = lease;
                yield return lease;
            }
        }

        /// <inheritdoc/>
        public ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken)
        {
            _ = leaseId;
            _ = result;
            cancellationToken.ThrowIfCancellationRequested();
            return default;
        }

        /// <inheritdoc/>
        public ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            IReadOnlyList<SnapshotMutation> snapshotMutations,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<LocalSnapshot> DeadLetterOperationAsync(
            Guid leaseId,
            OperationId operationId,
            string reasonCode,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
            StreamId streamId,
            IReadOnlyList<Guid> eventIds,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(RemoteEventBatch batch, SnapshotMutation snapshotMutation, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            StatusQueryCalls++;
            if (StatusQueryException is not null)
            {
                throw StatusQueryException;
            }

            _ = Statuses.TryGetValue(operationId, out var status);
            return new(status);
        }

        /// <inheritdoc/>
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (RetryStateQueryException is not null)
            {
                throw RetryStateQueryException;
            }

            _ = RetryStates.TryGetValue(operationId, out var retryState);
            return new(retryState);
        }

        /// <inheritdoc/>
        public async ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
            Guid leaseId,
            OperationId operationId,
            int nextAttempt,
            CancellationToken cancellationToken)
        {
            _ = leaseId;
            cancellationToken.ThrowIfCancellationRequested();
            BarrierCalls++;
            _ = BarrierEntered?.TrySetResult();
            if (ReleaseBarrier is not null)
            {
                await ReleaseBarrier.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            var denied = DenyRemoteAttempt || DenyRemoteAttemptOnCall == BarrierCalls;
            return new(operationId, nextAttempt, MaySend: !denied, ReasonCode: denied ? "denied" : null);
        }

        /// <inheritdoc/>
        public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (RetryStateSaveException is not null)
            {
                throw RetryStateSaveException;
            }

            RetryStates[operationId] = retryState;
            return default;
        }

        /// <inheritdoc/>
        public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken)
        {
            _ = leaseId;
            _ = extension;
            cancellationToken.ThrowIfCancellationRequested();
            return default;
        }

        /// <inheritdoc/>
        public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken)
        {
            _ = leaseId;
            cancellationToken.ThrowIfCancellationRequested();
            ReleaseLeaseCalls++;
            if (ReleaseLeaseException is not null)
            {
                throw ReleaseLeaseException;
            }

            if (RequeueReleasedLeases && _activeLease is { } activeLease && activeLease.LeaseId == leaseId)
            {
                Leases.Enqueue(activeLease);
            }

            _activeLease = null;
            return default;
        }

        /// <inheritdoc/>
        public ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <summary>Throws the configured synchronous store disposal failure.</summary>
        /// <returns>This method does not return.</returns>
        /// <exception cref="InvalidOperationException">The configured synchronous disposal failure.</exception>
        private static ValueTask ThrowStoreDisposeFailure() =>
            throw new InvalidOperationException("store dispose failed");
    }
}
