// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Recovered outbox scheduling tests for <see cref="OccasionallyConnectedBuilder"/>.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>Observes real SQLite lease attempts while delegating every store operation.</summary>
    /// <param name="inner">The wrapped SQLite store.</param>
    /// <param name="clock">The shared fake clock.</param>
    /// <param name="retryDueUtc">The persisted retry due time.</param>
    private sealed class RecoveredUploadObservedStore(
        SqliteLocalStoreAdapter inner,
        RecoveredUploadTimeProvider clock,
        DateTimeOffset retryDueUtc) : ILocalStoreAdapter
    {
        /// <summary>Gets a signal set when a lease attempt returns no batch before the persisted retry due time.</summary>
        public TaskCompletionSource EmptyLeaseBeforeDue { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc />
        public LocalStoreCapabilities Capabilities => inner.Capabilities;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken) =>
            inner.InitializeAsync(initialization, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken) =>
            inner.GetOrCreateSubscriptionIdAsync(streamId, preferredId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RecoveredStream> RecoverStreamAsync(
            StreamId streamId,
            SubscriptionId subscriptionId,
            CancellationToken cancellationToken) =>
            inner.RecoverStreamAsync(streamId, subscriptionId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalCommitResult> CommitLocalOperationAsync(
            SyncOperation operation,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            inner.CommitLocalOperationAsync(operation, snapshotMutation, cancellationToken);

        /// <inheritdoc />
        public async IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
            OutboxLeaseRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var yielded = false;
            await foreach (var batch in inner.LeasePendingOperationsAsync(request, cancellationToken).ConfigureAwait(false))
            {
                yielded = true;
                yield return batch;
            }

            if (!yielded && clock.GetUtcNow() < retryDueUtc)
            {
                _ = EmptyLeaseBeforeDue.TrySetResult();
            }
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken) =>
            inner.ApplySyncResultAsync(leaseId, result, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            IReadOnlyList<SnapshotMutation> snapshotMutations,
            CancellationToken cancellationToken) =>
            inner.ApplySyncResultAsync(leaseId, result, snapshotMutations, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalSnapshot> DeadLetterOperationAsync(
            Guid leaseId,
            OperationId operationId,
            string reasonCode,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            inner.DeadLetterOperationAsync(leaseId, operationId, reasonCode, snapshotMutation, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
            StreamId streamId,
            IReadOnlyList<Guid> eventIds,
            CancellationToken cancellationToken) =>
            inner.GetUnappliedEventIdsAsync(streamId, eventIds, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
            RemoteEventBatch batch,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            inner.ApplyRemoteBatchAsync(batch, snapshotMutation, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken) =>
            inner.GetOperationStatusAsync(operationId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken) =>
            inner.GetRetryStateAsync(operationId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
            Guid leaseId,
            OperationId operationId,
            int nextAttempt,
            CancellationToken cancellationToken) =>
            inner.TryBeginRemoteAttemptAsync(leaseId, operationId, nextAttempt, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken) =>
            inner.SaveRetryStateAsync(operationId, retryState, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken) =>
            inner.RenewLeaseAsync(leaseId, extension, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken) =>
            inner.ReleaseLeaseAsync(leaseId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken) =>
            inner.CompactAsync(request, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Records recovered upload scheduling through a remote transport adapter.</summary>
    private sealed class RecoveredUploadTransportAdapter : IRemoteTransportAdapter
    {
        /// <summary>Stores the optional prepared send failure.</summary>
        private readonly Exception? _sendException;

        /// <summary>Tracks the number of prepared upload calls.</summary>
        private int _prepareCalls;

        /// <summary>Tracks the number of prepared send calls.</summary>
        private int _sendCalls;

        /// <summary>Initializes a new instance of the <see cref="RecoveredUploadTransportAdapter"/> class.</summary>
        public RecoveredUploadTransportAdapter()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="RecoveredUploadTransportAdapter"/> class.</summary>
        /// <param name="sendException">The exception returned by prepared send.</param>
        public RecoveredUploadTransportAdapter(Exception sendException)
        {
            ArgumentNullException.ThrowIfNull(sendException);
            _sendException = sendException;
        }

        /// <summary>Gets the first pushed batch.</summary>
        public TaskCompletionSource<SyncBatch> Pushed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets failures thrown by successive connection attempts before sessions are created.</summary>
        public System.Collections.Concurrent.ConcurrentQueue<Exception> ConnectFailures { get; } = new();

        /// <summary>Gets the number of prepared upload calls.</summary>
        public int PrepareCalls => Volatile.Read(ref _prepareCalls);

        /// <summary>Gets the number of prepared send calls.</summary>
        public int SendCalls => Volatile.Read(ref _sendCalls);

        /// <inheritdoc />
        public RemoteTransportCapabilities Capabilities => RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.ServerIdempotency;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken)
        {
            _ = request;
            cancellationToken.ThrowIfCancellationRequested();
            return ConnectFailures.TryDequeue(out var failure)
                ? ValueTask.FromException<IRemoteTransportSession>(failure)
                : new(new RecoveredUploadTransportSession(this));
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        /// <summary>Records a pushed batch.</summary>
        /// <param name="batch">The pushed batch.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void RecordPush(SyncBatch batch) => _ = Pushed.TrySetResult(batch);

        /// <summary>Records a prepared upload call.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void RecordPrepare() => _ = Interlocked.Increment(ref _prepareCalls);

        /// <summary>Records a prepared send call.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void RecordSend() => _ = Interlocked.Increment(ref _sendCalls);

        /// <summary>Returns accepted prepared upload results while recording pushed batches.</summary>
        /// <param name="adapter">The owning transport adapter.</param>
        private sealed class RecoveredUploadTransportSession(RecoveredUploadTransportAdapter adapter) : IRemoteTransportSession, IRemoteTransportBatchPreparer
        {
            /// <summary>The maximum operation count returned during capability negotiation.</summary>
            private const int MaximumOperations = 100;

            /// <summary>The maximum batch payload byte count returned during capability negotiation.</summary>
            private const int MaximumBatchBytes = 1_048_576;

            /// <summary>The finite server idempotency retention advertised by the recovered upload transport.</summary>
            private static readonly TimeSpan ServerIdempotencyRetention = TimeSpan.FromMinutes(5);

            /// <summary>Stores the owning transport adapter.</summary>
            private readonly RecoveredUploadTransportAdapter _adapter = adapter;

            /// <inheritdoc />
            public NegotiatedCapabilities NegotiatedCapabilities { get; } = new(
                new(1, 0),
                RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.ServerIdempotency,
                MaximumOperations,
                MaximumBatchBytes,
                ServerIdempotencyRetention,
                null);

            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask<IPreparedRemotePush> PreparePushAsync(SyncBatch batch, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _adapter.RecordPrepare();
                return new(new RecoveredPreparedPush(_adapter, batch));
            }

            /// <inheritdoc />
            public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken)
            {
                _ = batch;
                _ = cancellationToken;
                throw new NotSupportedException();
            }

            /// <inheritdoc />
            public async IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(
                RemoteSubscribeRequest request,
                [EnumeratorCancellation] CancellationToken cancellationToken)
            {
                _ = request;
                cancellationToken.ThrowIfCancellationRequested();
                await Task.CompletedTask.ConfigureAwait(false);
                yield break;
            }

            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken)
            {
                _ = acknowledgement;
                cancellationToken.ThrowIfCancellationRequested();
                return ValueTask.CompletedTask;
            }

            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;

            /// <summary>Prepared push handle for recovered upload scheduling.</summary>
            /// <param name="adapter">The owning transport adapter.</param>
            /// <param name="batch">The prepared batch.</param>
            private sealed class RecoveredPreparedPush(RecoveredUploadTransportAdapter adapter, SyncBatch batch) : IPreparedRemotePush
            {
                /// <summary>Stores the owning transport adapter.</summary>
                private readonly RecoveredUploadTransportAdapter _adapter = adapter;

                /// <inheritdoc />
                public SyncBatch Batch { get; } = batch;

                /// <inheritdoc />
                public long EncodedSizeBytes => MaximumBatchBytes;

                /// <inheritdoc />
                public ValueTask<RemoteSyncResult> SendAsync(CancellationToken cancellationToken)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    _adapter.RecordSend();
                    if (_adapter._sendException is { } exception)
                    {
                        return new(Task.FromException<RemoteSyncResult>(exception));
                    }

                    _adapter.RecordPush(Batch);
                    var results = new OperationSyncResult[Batch.Operations.Count];
                    for (var i = 0; i < Batch.Operations.Count; i++)
                    {
                        results[i] = new(Batch.Operations[i].OperationId, OperationResultKind.Accepted, null, RecoveredUploadServerVersion);
                    }

                    return new(new RemoteSyncResult(Batch.BatchId, results, null, null));
                }

                /// <inheritdoc />
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public ValueTask DisposeAsync() => ValueTask.CompletedTask;
            }
        }
    }

    /// <summary>Records recovered upload diagnostics from public observable streams.</summary>
    /// <typeparam name="T">The diagnostic item type.</typeparam>
    private sealed class RecoveredUploadDiagnosticObserver<T> : IObserver<T>
    {
        /// <summary>Protects values from asynchronous observer callbacks.</summary>
        private readonly Lock _gate = new();

        /// <summary>Stores the observed values.</summary>
        private readonly List<T> _values = [];

        /// <summary>Gets a stable snapshot of the observed values.</summary>
        public List<T> Values
        {
            get
            {
                lock (_gate)
                {
                    return [.. _values];
                }
            }
        }

        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => ArgumentNullException.ThrowIfNull(error);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value)
        {
            lock (_gate)
            {
                _values.Add(value);
            }
        }
    }

    /// <summary>Stores recovered upload merge assertion fixtures.</summary>
    private sealed record RecoveredUploadMergeFixture
    {
        /// <summary>Gets the reopened context.</summary>
        public required OccasionallyConnectedContext ReopenedContext { get; init; }

        /// <summary>Gets the reopened stream.</summary>
        public required IOccasionallyConnectedStream<CounterState, CounterInput> ReopenedStream { get; init; }

        /// <summary>Gets the reopened SQLite store.</summary>
        public required SqliteLocalStoreAdapter ReopenedStore { get; init; }

        /// <summary>Gets the observed store wrapper.</summary>
        public required RecoveredUploadObservedStore ObservedStore { get; init; }

        /// <summary>Gets the reopened transport.</summary>
        public required RecoveredUploadTransportAdapter ReopenedTransport { get; init; }

        /// <summary>Gets the recovered retry fixture.</summary>
        public required RecoveredUploadRetryFixture RecoveredRetry { get; init; }

        /// <summary>Gets the reopened clock.</summary>
        public required RecoveredUploadTimeProvider ReopenedClock { get; init; }

        /// <summary>Gets the maximum batching dwell time.</summary>
        public TimeSpan MaximumDwellTime { get; init; }

        /// <summary>Gets the observed faults.</summary>
        public required RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault> Faults { get; init; }

        /// <summary>Gets the observed operation states.</summary>
        public required RecoveredUploadDiagnosticObserver<SyncOperationStatus> OperationStates { get; init; }

        /// <summary>Gets the observed wake count.</summary>
        public int ObservedWakeCount { get; init; }
    }
}
