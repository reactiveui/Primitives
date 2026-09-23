// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Snapshot recovery store test helpers for <see cref="SyncEngineTests"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Observes real-store snapshot recovery calls without replacing store behavior.</summary>
    /// <param name="inner">The real local store adapter.</param>
    private sealed class InstrumentedSnapshotRecoveryStore(InMemoryLocalStoreAdapter inner) :
        ILocalStoreAdapter,
        ILocalSnapshotRecoveryCaptureStore,
        ILocalSnapshotRecoveryStore
    {
        /// <summary>Protects captured request details.</summary>
        private readonly Lock _gate = new();

        /// <summary>Gets capture requests observed by the wrapper.</summary>
        private readonly List<LocalSnapshotRecoveryCaptureRequest> _captureRequests = [];

        /// <summary>Tracks stream recovery calls by stream.</summary>
        private readonly Dictionary<StreamId, int> _recoverStreamCallsByStream = [];

        /// <summary>Tracks stream recovery calls observed by the wrapper.</summary>
        private int _recoverStreamCalls;

        /// <summary>Tracks store initialization calls observed by the wrapper.</summary>
        private int _initializeCalls;

        /// <summary>Tracks capture requests observed by the wrapper.</summary>
        private int _captureRequestCount;

        /// <summary>Stores the operation expected to be synchronized before first capture.</summary>
        private OperationId? _existingUploadOperationId;

        /// <summary>Stores the tracked operation status observed at first capture.</summary>
        private SyncOperationStatus? _existingUploadStatusAtFirstCapture;

        /// <summary>Tracks whether the upload result completed for the tracked operation.</summary>
        private bool _trackedUploadSyncResultCompleted;

        /// <summary>Stores whether the tracked upload result completed by first capture.</summary>
        private bool _trackedUploadSyncResultCompletedAtFirstCapture;

        /// <summary>Stores the tracked upload lease observed at first capture.</summary>
        private Guid? _trackedUploadSyncResultLeaseAtFirstCapture;

        /// <summary>Stores the last tracked upload result lease.</summary>
        private Guid? _trackedUploadSyncResultLease;

        /// <summary>Stores the last snapshot recovery mutation observed by the wrapper.</summary>
        private LocalSnapshotRecoveryMutation? _lastSnapshotRecoveryMutation;

        /// <summary>Stores the last successful snapshot recovery commit observed by the wrapper.</summary>
        private LocalSnapshotRecoveryResult? _lastSnapshotRecoveryCommit;

        /// <summary>Stores the last snapshot recovery store exception observed by the wrapper.</summary>
        private Exception? _lastSnapshotRecoveryException;

        /// <summary>Gets or sets the stream whose snapshot recovery commit should be gated.</summary>
        public StreamId? SnapshotRecoveryCommitGateStream { get; set; }

        /// <summary>Gets or sets the optional signal raised before applying snapshot recovery.</summary>
        public TaskCompletionSource? SnapshotRecoveryCommitEntered { get; set; }

        /// <summary>Gets or sets the optional gate that releases snapshot recovery commit.</summary>
        public TaskCompletionSource? ReleaseSnapshotRecoveryCommit { get; set; }

        /// <summary>Gets or sets the optional capture transform used by malformed-capture tests.</summary>
        public Func<LocalSnapshotRecoveryCapture, int, LocalSnapshotRecoveryCapture>? SnapshotRecoveryCaptureTransform
        {
            get;
            set;
        }

        /// <summary>Gets or sets the optional recovery commit transform used by malformed-store tests.</summary>
        public Func<LocalSnapshotRecoveryResult, LocalSnapshotRecoveryResult>? SnapshotRecoveryCommitTransform
        {
            get;
            set;
        }

        /// <summary>Gets or sets work run after the durable recovery commit and before facade notifications.</summary>
        public Action? AfterSnapshotRecoveryCommit { get; set; }

        /// <summary>Gets or sets a postcommit status read override for notification fault controls.</summary>
        public Func<OperationId, ValueTask<SyncOperationStatus?>>? PostRecoveryStatusReadOverride { get; set; }

        /// <summary>Gets the last snapshot recovery mutation observed by the wrapper.</summary>
        public LocalSnapshotRecoveryMutation? LastSnapshotRecoveryMutation
        {
            get
            {
                lock (_gate)
                {
                    return _lastSnapshotRecoveryMutation;
                }
            }
        }

        /// <summary>Gets the last successful snapshot recovery commit observed by the wrapper.</summary>
        public LocalSnapshotRecoveryResult? LastSnapshotRecoveryCommit
        {
            get
            {
                lock (_gate)
                {
                    return _lastSnapshotRecoveryCommit;
                }
            }
        }

        /// <summary>Gets the last snapshot recovery store exception observed by the wrapper.</summary>
        public Exception? LastSnapshotRecoveryException
        {
            get
            {
                lock (_gate)
                {
                    return _lastSnapshotRecoveryException;
                }
            }
        }

        /// <summary>Gets the number of store initialization calls.</summary>
        public int InitializeCalls => Volatile.Read(ref _initializeCalls);

        /// <summary>Gets the number of stream recovery calls observed by the wrapper.</summary>
        public int RecoverStreamCalls => Volatile.Read(ref _recoverStreamCalls);

        /// <summary>Gets the number of capture requests observed by the wrapper.</summary>
        public int CaptureRequestCount => Volatile.Read(ref _captureRequestCount);

        /// <summary>Gets whether the tracked upload result completed before first capture.</summary>
        public bool TrackedUploadSyncResultCompletedAtFirstCapture
        {
            get
            {
                lock (_gate)
                {
                    return _trackedUploadSyncResultCompletedAtFirstCapture;
                }
            }
        }

        /// <summary>Gets the tracked upload lease observed at first capture.</summary>
        public Guid? TrackedUploadSyncResultLeaseAtFirstCapture
        {
            get
            {
                lock (_gate)
                {
                    return _trackedUploadSyncResultLeaseAtFirstCapture;
                }
            }
        }

        /// <summary>Gets the tracked operation status observed at first capture.</summary>
        public SyncOperationStatus? ExistingUploadStatusAtFirstCapture
        {
            get
            {
                lock (_gate)
                {
                    return _existingUploadStatusAtFirstCapture;
                }
            }
        }

        /// <inheritdoc/>
        public LocalStoreCapabilities Capabilities => inner.Capabilities;

        /// <summary>Initializes the store with the same identity used by the engine fixture.</summary>
        /// <returns>The initialization task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask InitializeForEngineAsync() =>
            InitializeAsync(
                new("sync-engine-tests", RequiredSchemaVersion: 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = EngineClientId },
                CancellationToken.None);

        /// <summary>Tracks an existing upload operation for first-capture assertions.</summary>
        /// <param name="operationId">The operation identity.</param>
        public void TrackExistingUpload(OperationId operationId) => _existingUploadOperationId = operationId;

        /// <summary>Gets stream recovery calls for one stream.</summary>
        /// <param name="streamId">The stream identity.</param>
        /// <returns>The observed recovery calls.</returns>
        public int GetRecoverStreamCalls(StreamId streamId)
        {
            lock (_gate)
            {
                return _recoverStreamCallsByStream.GetValueOrDefault(streamId);
            }
        }

        /// <inheritdoc/>
        public async ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken)
        {
            await inner.InitializeAsync(initialization, cancellationToken).ConfigureAwait(false);
            _ = Interlocked.Increment(ref _initializeCalls);
        }

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
            CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _recoverStreamCalls);
            lock (_gate)
            {
                _recoverStreamCallsByStream[streamId] = _recoverStreamCallsByStream.GetValueOrDefault(streamId) + 1;
            }

            return await inner.RecoverStreamAsync(streamId, subscriptionId, cancellationToken).ConfigureAwait(false);
        }

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
        public async ValueTask ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            CancellationToken cancellationToken)
        {
            await inner.ApplySyncResultAsync(leaseId, result, cancellationToken).ConfigureAwait(false);
            RecordTrackedUploadResult(leaseId, result);
        }

        /// <inheritdoc/>
        public async ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            IReadOnlyList<SnapshotMutation> snapshotMutations,
            CancellationToken cancellationToken)
        {
            var snapshots = await inner.ApplySyncResultAsync(leaseId, result, snapshotMutations, cancellationToken).ConfigureAwait(false);
            RecordTrackedUploadResult(leaseId, result);
            return snapshots;
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken) =>
            LastSnapshotRecoveryCommit is not null && PostRecoveryStatusReadOverride is { } replacement
                ? replacement(operationId)
                : inner.GetOperationStatusAsync(operationId, cancellationToken);

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
        public async ValueTask<LocalSnapshotRecoveryCapture> CaptureSnapshotRecoveryAsync(
            LocalSnapshotRecoveryCaptureRequest request,
            CancellationToken cancellationToken)
        {
            var tracked = _existingUploadOperationId;
            var status = tracked is null
                ? null
                : await inner.GetOperationStatusAsync(tracked.Value, CancellationToken.None).ConfigureAwait(false);
            lock (_gate)
            {
                _captureRequests.Add(request);
                if (_captureRequests.Count == ExpectedSingleOperation)
                {
                    _existingUploadStatusAtFirstCapture = status;
                    _trackedUploadSyncResultCompletedAtFirstCapture = _trackedUploadSyncResultCompleted;
                    _trackedUploadSyncResultLeaseAtFirstCapture = _trackedUploadSyncResultLease;
                }
            }

            var captureNumber = Interlocked.Increment(ref _captureRequestCount);
            var capture = await inner.CaptureSnapshotRecoveryAsync(request, cancellationToken).ConfigureAwait(false);
            var transform = SnapshotRecoveryCaptureTransform;
            return transform is null ? capture : transform(capture, captureNumber);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalSnapshotRecoveryResult> ApplySnapshotRecoveryAsync(
            LocalSnapshotRecoveryMutation mutation,
            CancellationToken cancellationToken)
        {
            if (SnapshotRecoveryCommitGateStream != mutation.StreamId)
            {
                return ApplySnapshotRecoveryObservedAsync(mutation, cancellationToken);
            }

            _ = SnapshotRecoveryCommitEntered?.TrySetResult();
            var release = ReleaseSnapshotRecoveryCommit;
            return release is null
                ? ApplySnapshotRecoveryObservedAsync(mutation, cancellationToken)
                : ApplySnapshotRecoveryWhenReleasedAsync(mutation, release, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => inner.DisposeAsync();

        /// <summary>Applies snapshot recovery after the test releases the commit gate.</summary>
        /// <param name="mutation">The snapshot recovery mutation.</param>
        /// <param name="release">The gate that releases the delegated commit.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The recovery result.</returns>
        private async ValueTask<LocalSnapshotRecoveryResult> ApplySnapshotRecoveryWhenReleasedAsync(
            LocalSnapshotRecoveryMutation mutation,
            TaskCompletionSource release,
            CancellationToken cancellationToken)
        {
            await release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return await ApplySnapshotRecoveryObservedAsync(mutation, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Applies snapshot recovery while recording the store boundary result.</summary>
        /// <param name="mutation">The snapshot recovery mutation.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The recovery result.</returns>
        private async ValueTask<LocalSnapshotRecoveryResult> ApplySnapshotRecoveryObservedAsync(
            LocalSnapshotRecoveryMutation mutation,
            CancellationToken cancellationToken)
        {
            RecordSnapshotRecoveryMutation(mutation);
            try
            {
                var commit = await inner.ApplySnapshotRecoveryAsync(mutation, cancellationToken).ConfigureAwait(false);
                var transform = SnapshotRecoveryCommitTransform;
                var observed = transform is null ? commit : transform(commit);
                RecordSnapshotRecoveryCommit(observed);
                AfterSnapshotRecoveryCommit?.Invoke();
                return observed;
            }
            catch (Exception exception)
            {
                RecordSnapshotRecoveryException(exception);
                throw;
            }
        }

        /// <summary>Records a snapshot recovery mutation attempt.</summary>
        /// <param name="mutation">The snapshot recovery mutation.</param>
        private void RecordSnapshotRecoveryMutation(LocalSnapshotRecoveryMutation mutation)
        {
            lock (_gate)
            {
                _lastSnapshotRecoveryMutation = mutation;
                _lastSnapshotRecoveryCommit = null;
                _lastSnapshotRecoveryException = null;
            }
        }

        /// <summary>Records a successful snapshot recovery commit.</summary>
        /// <param name="commit">The successful recovery commit.</param>
        private void RecordSnapshotRecoveryCommit(LocalSnapshotRecoveryResult commit)
        {
            lock (_gate)
            {
                _lastSnapshotRecoveryCommit = commit;
            }
        }

        /// <summary>Records a snapshot recovery store exception.</summary>
        /// <param name="exception">The snapshot recovery store exception.</param>
        private void RecordSnapshotRecoveryException(Exception exception)
        {
            lock (_gate)
            {
                _lastSnapshotRecoveryException = exception;
            }
        }

        /// <summary>Records successful tracked upload result completion.</summary>
        /// <param name="leaseId">The lease whose result was applied.</param>
        /// <param name="result">The applied sync result.</param>
        private void RecordTrackedUploadResult(Guid leaseId, RemoteSyncResult result)
        {
            var tracked = _existingUploadOperationId;
            if (tracked is null)
            {
                return;
            }

            var operations = result.Operations;
            for (var index = 0; index < operations.Count; index++)
            {
                if (operations[index].OperationId != tracked.Value)
                {
                    continue;
                }

                lock (_gate)
                {
                    _trackedUploadSyncResultCompleted = true;
                    _trackedUploadSyncResultLease = leaseId;
                }

                return;
            }
        }
    }

    /// <summary>Delegates local store calls while hiding optional snapshot recovery facets.</summary>
    /// <param name="inner">The wrapped store.</param>
    private class DelegatingSnapshotRecoveryStore(ILocalStoreAdapter inner) : ILocalStoreAdapter
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
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(
            OperationId operationId,
            CancellationToken cancellationToken) =>
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
        public ValueTask SaveRetryStateAsync(
            OperationId operationId,
            RetryState retryState,
            CancellationToken cancellationToken) =>
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
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    /// <summary>Delegates local store calls while exposing only the capture facet.</summary>
    /// <param name="inner">The wrapped capture-capable store.</param>
    private sealed class CaptureOnlySnapshotRecoveryStore(InstrumentedSnapshotRecoveryStore inner) :
        DelegatingSnapshotRecoveryStore(inner),
        ILocalSnapshotRecoveryCaptureStore
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalSnapshotRecoveryCapture> CaptureSnapshotRecoveryAsync(
            LocalSnapshotRecoveryCaptureRequest request,
            CancellationToken cancellationToken) =>
            inner.CaptureSnapshotRecoveryAsync(request, cancellationToken);
    }
}
