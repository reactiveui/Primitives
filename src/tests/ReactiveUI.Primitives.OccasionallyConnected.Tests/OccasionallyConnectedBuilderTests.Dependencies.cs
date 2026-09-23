// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedBuilder"/>.</summary>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>Records store initialization and disposal while delegating behavior to the in-memory store.</summary>
    private sealed class RecordingStoreAdapter : ILocalStoreAdapter
    {
        /// <summary>Stores the inner in-memory adapter.</summary>
        private readonly InMemoryLocalStoreAdapter _inner = new();

        /// <summary>Gets the last initialization request.</summary>
        public LocalStoreInitialization? Initialization { get; private set; }

        /// <summary>Gets the last committed local operation.</summary>
        public SyncOperation? LastCommittedOperation { get; private set; }

        /// <summary>Gets the number of disposal calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <summary>Gets or sets a callback invoked before initialization delegates to the store.</summary>
        public Action BeforeInitialize { get; set; } = static () => { };

        /// <summary>Gets or sets a callback invoked before stream recovery delegates to the store.</summary>
        public Action BeforeRecover { get; set; } = static () => { };

        /// <summary>Gets or sets a token-aware callback invoked before stream recovery delegates to the store.</summary>
        public Action<CancellationToken> BeforeRecoverWithToken { get; set; } = static _ => { };

        /// <summary>Gets or sets a callback invoked before a durable operation status is read.</summary>
        public Action<SyncOperationStatus?> AfterGetOperationStatus { get; set; } = static _ => { };

        /// <inheritdoc />
        public LocalStoreCapabilities Capabilities => _inner.Capabilities;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken)
        {
            Initialization = initialization;
            BeforeInitialize();
            return _inner.InitializeAsync(initialization, cancellationToken);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken) =>
            _inner.GetOrCreateSubscriptionIdAsync(streamId, preferredId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RecoveredStream> RecoverStreamAsync(
            StreamId streamId,
            SubscriptionId subscriptionId,
            CancellationToken cancellationToken)
        {
            BeforeRecover();
            BeforeRecoverWithToken(cancellationToken);
            return _inner.RecoverStreamAsync(streamId, subscriptionId, cancellationToken);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalCommitResult> CommitLocalOperationAsync(
            SyncOperation operation,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken)
        {
            LastCommittedOperation = operation;
            return _inner.CommitLocalOperationAsync(operation, snapshotMutation, cancellationToken);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
            OutboxLeaseRequest request,
            CancellationToken cancellationToken) =>
            _inner.LeasePendingOperationsAsync(request, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken) =>
            _inner.ApplySyncResultAsync(leaseId, result, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            IReadOnlyList<SnapshotMutation> snapshotMutations,
            CancellationToken cancellationToken) =>
            _inner.ApplySyncResultAsync(leaseId, result, snapshotMutations, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalSnapshot> DeadLetterOperationAsync(
            Guid leaseId,
            OperationId operationId,
            string reasonCode,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            _inner.DeadLetterOperationAsync(leaseId, operationId, reasonCode, snapshotMutation, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
            StreamId streamId,
            IReadOnlyList<Guid> eventIds,
            CancellationToken cancellationToken) =>
            _inner.GetUnappliedEventIdsAsync(streamId, eventIds, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
            RemoteEventBatch batch,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            _inner.ApplyRemoteBatchAsync(batch, snapshotMutation, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken)
        {
            var status = await _inner.GetOperationStatusAsync(operationId, cancellationToken).ConfigureAwait(false);
            AfterGetOperationStatus(status);
            return status;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken) =>
            _inner.GetRetryStateAsync(operationId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
            Guid leaseId,
            OperationId operationId,
            int nextAttempt,
            CancellationToken cancellationToken) =>
            _inner.TryBeginRemoteAttemptAsync(leaseId, operationId, nextAttempt, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken) =>
            _inner.SaveRetryStateAsync(operationId, retryState, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken) =>
            _inner.RenewLeaseAsync(leaseId, extension, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken) =>
            _inner.ReleaseLeaseAsync(leaseId, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken) =>
            _inner.CompactAsync(request, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask DisposeAsync()
        {
            DisposeCalls++;
            await _inner.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Records remote transport connection and disposal.</summary>
    private sealed class RecordingTransportAdapter : IRemoteTransportAdapter
    {
        /// <summary>Gets or sets the optional connect signal.</summary>
        public TaskCompletionSource? ConnectEntered { get; init; }

        /// <summary>Gets or sets the optional connect release signal.</summary>
        public TaskCompletionSource? ReleaseConnect { get; init; }

        /// <summary>Gets or sets the exception thrown during connect.</summary>
        public Exception? ConnectException { get; init; }

        /// <summary>Gets or sets a callback invoked before connection completes.</summary>
        public Action BeforeConnect { get; set; } = static () => { };

        /// <summary>Gets the number of connect calls.</summary>
        public int ConnectCalls { get; private set; }

        /// <summary>Gets the number of disposal calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <summary>Gets or sets the exception thrown when a connected session is disposed.</summary>
        public Exception? SessionDisposeException { get; set; }

        /// <summary>Gets the last connected transport session.</summary>
        public RecordingTransportSession? LastSession { get; private set; }

        /// <inheritdoc />
        public RemoteTransportCapabilities Capabilities => RemoteTransportCapabilities.BatchPush;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<IRemoteTransportSession> ConnectAsync(
            TransportConnectRequest request,
            CancellationToken cancellationToken)
        {
            ConnectCalls++;
            _ = ConnectEntered?.TrySetResult();
            BeforeConnect();
            if (ReleaseConnect is not null)
            {
                await ReleaseConnect.Task.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            }

            if (ConnectException is not null)
            {
                throw ConnectException;
            }

            LastSession = new(SessionDisposeException);
            return LastSession;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>Provides an inert remote transport session.</summary>
    /// <param name="disposeException">The optional disposal failure.</param>
    private sealed class RecordingTransportSession(Exception? disposeException = null) : IRemoteTransportSession
    {
        /// <summary>The negotiated maximum batch operation count used by tests.</summary>
        private const int MaxBatchOperations = 100;

        /// <summary>The negotiated maximum batch payload size used by tests.</summary>
        private const int MaxBatchPayloadBytes = 1_048_576;

        /// <summary>Stores the optional disposal failure.</summary>
        private readonly Exception? _disposeException = disposeException;

        /// <summary>Gets the number of disposal calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <inheritdoc />
        public NegotiatedCapabilities NegotiatedCapabilities { get; } = new(
            new(1, 0),
            RemoteTransportCapabilities.BatchPush,
            MaxBatchOperations,
            MaxBatchPayloadBytes,
            null,
            null);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            new(new RemoteSyncResult(batch.BatchId, [], null, null));

        /// <inheritdoc />
        public async IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(
            RemoteSubscribeRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask.ConfigureAwait(false);
            yield break;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            return _disposeException is null ? ValueTask.CompletedTask : ValueTask.FromException(_disposeException);
        }
    }

    /// <summary>Serializes counter values as invariant text payloads.</summary>
    private sealed class TextPayloadSerializer : IPayloadSerializer
    {
        /// <inheritdoc />
        public string ContentType => "text/plain";

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken)
        {
            var text = value switch
            {
                CounterInput input => input.Delta.ToString(System.Globalization.CultureInfo.InvariantCulture),
                CounterState state => state.Sum.ToString(System.Globalization.CultureInfo.InvariantCulture),
                _ => throw new InvalidOperationException("Unexpected payload type."),
            };
            var bytes = Encoding.UTF8.GetBytes(text);
            return ValueTask.FromResult(new PayloadEnvelope(contractId, schemaVersion, ContentType, bytes, $"hash-{text}"));
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken)
        {
            var text = Encoding.UTF8.GetString(envelope.Payload.Span);
            var value = int.Parse(text, System.Globalization.CultureInfo.InvariantCulture);
            if (targetType == typeof(CounterInput))
            {
                return new(new CounterInput(value));
            }

            if (targetType == typeof(CounterState))
            {
                return new(new CounterState(value));
            }

            throw new InvalidOperationException("Unexpected target type.");
        }
    }

    /// <summary>Returns a fixed operation identifier for deterministic local commit tests.</summary>
    /// <param name="operationId">The fixed operation identifier.</param>
    private sealed class FixedOperationIdSource(OperationId operationId) : IOperationIdSource
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OperationId New() => operationId;
    }

    /// <summary>Returns a fixed retry jitter value for deterministic retry configuration tests.</summary>
    /// <param name="value">The fixed retry jitter value.</param>
    private sealed class RecordingRetryRandomSource(double value) : IRetryRandomSource
    {
        /// <summary>Gets the fixed retry jitter value.</summary>
        public double LastValue { get; } = value;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double NextDouble() => LastValue;
    }

    /// <summary>Records scheduled observer work items.</summary>
    private sealed class RecordingSequencer : ISequencer
    {
        /// <summary>Tracks an optional single scheduling rejection.</summary>
        private int _rejectNextSchedule;

        /// <summary>Gets the number of schedule calls.</summary>
        public int ScheduleCalls { get; private set; }

        /// <inheritdoc />
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

        /// <inheritdoc />
        public long Timestamp => 0;

        /// <summary>Rejects the next scheduled work item.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RejectNextSchedule() => Interlocked.Exchange(ref _rejectNextSchedule, 1);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item)
        {
            ScheduleCalls++;
            if (Interlocked.Exchange(ref _rejectNextSchedule, 0) != 0)
            {
                throw new InvalidOperationException("The sequencer rejected observer notification work.");
            }

            item.Execute();
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item, long dueTimestamp) => Schedule(item);
    }

    /// <summary>Records observed values.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class RecordingObserver<T> : IObserver<T>
    {
        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => ArgumentNullException.ThrowIfNull(error);

        /// <inheritdoc />
        public void OnNext(T value)
        {
        }
    }

    /// <summary>Projects counter state.</summary>
    private sealed class CounterProjection : ILocalProjection<CounterState, CounterInput>
    {
        /// <inheritdoc />
        public CounterState InitialState { get; } = new(0);

        /// <inheritdoc />
        public CounterState ApplyLocal(CounterState state, CounterInput input, SyncOperation operation) =>
            new(state.Sum + input.Delta);

        /// <inheritdoc />
        public CounterState ApplyRemote(CounterState state, CounterInput input, RemoteEvent remoteEvent) =>
            new(state.Sum + input.Delta);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CounterState Reconcile(CounterState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Stores explicit state for dedicated-thread build execution.</summary>
    /// <param name="Store">The store dependency.</param>
    /// <param name="Transport">The transport dependency.</param>
    private sealed record BuildTaskState(RecordingStoreAdapter Store, RecordingTransportAdapter Transport);

    /// <summary>Test input value.</summary>
    /// <param name="Delta">The delta.</param>
    private sealed record CounterInput(int Delta);

    /// <summary>Test state value.</summary>
    /// <param name="Sum">The sum.</param>
    private sealed record CounterState(int Sum);
}
