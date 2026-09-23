// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Receive and recovery transport double for <see cref="SyncEngineTests"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Records receive subscriptions and acknowledgements.</summary>
    private sealed class ReceiveSession : IRemoteTransportSession, IRemoteTransportBatchPreparer, IRemoteSnapshotRecoverySession
    {
        /// <summary>Protects per-subscription batch queues.</summary>
        private readonly Lock _subscribeGate = new();

        /// <summary>Protects prepared, sent, and snapshot recovery request observations.</summary>
        private readonly Lock _observationGate = new();

        /// <summary>Stores cancellation callback registrations owned by this session.</summary>
        private readonly List<CancellationTokenRegistration> _subscribeCancellationRegistrations = [];

        /// <summary>Stores the signal that releases a paused receive-session send attempt.</summary>
        private readonly TaskCompletionSource _releasePausedSend = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Stores snapshot recovery requests canceled while waiting on the remote response.</summary>
        private readonly List<RemoteSnapshotRecoveryRequest> _canceledSnapshotRecoveryRequests = [];

        /// <summary>Tracks emitted subscription gaps.</summary>
        private int _subscriptionGapEmissions;

        /// <summary>Tracks acknowledgement attempts.</summary>
        private int _acknowledgeAttempts;

        /// <summary>Tracks completed subscription iterators.</summary>
        private int _subscribeCompletedCount;

        /// <summary>Tracks prepared send attempts.</summary>
        private int _sendAttempts;

        /// <summary>Gets prepared upload batches received by the shared receive session.</summary>
        public List<SyncBatch> PreparedBatches { get; } = [];

        /// <summary>Gets sent upload batches received by the shared receive session.</summary>
        public List<SyncBatch> SentBatches { get; } = [];

        /// <summary>Gets the remote batches yielded to the receive pump.</summary>
        public List<RemoteEventBatch> Batches { get; } = [];

        /// <summary>Gets subscribe requests received by the session.</summary>
        public List<RemoteSubscribeRequest> SubscribeRequests { get; } = [];

        /// <summary>Gets per-subscription batches when a test needs different batches for each subscribe call.</summary>
        public Queue<IReadOnlyList<RemoteEventBatch>> SubscriptionBatches { get; } = [];

        /// <summary>Gets acknowledgements received by the session.</summary>
        public List<ReceiveAcknowledgement> Acknowledgements { get; } = [];

        /// <summary>Gets snapshot recovery requests received by the session.</summary>
        public List<RemoteSnapshotRecoveryRequest> SnapshotRecoveryRequests { get; } = [];

        /// <summary>Gets or sets the optional acknowledgement failure.</summary>
        public Exception? AcknowledgeException { get; init; }

        /// <summary>Gets or sets how many acknowledgement attempts fail when <see cref="AcknowledgeException"/> is configured.</summary>
        public int AcknowledgeExceptionLimit { get; init; } = int.MaxValue;

        /// <summary>Gets or sets the optional retained-history gap raised by subscription.</summary>
        public RemoteSubscriptionRetentionGapException? SubscriptionGap { get; init; }

        /// <summary>Gets or sets the optional retained-history gap factory raised by subscription.</summary>
        public Func<RemoteSubscribeRequest, RemoteSubscriptionRetentionGapException?>? SubscriptionGapFactory { get; init; }

        /// <summary>Gets or sets how many subscriptions emit <see cref="SubscriptionGap"/> when configured.</summary>
        public int SubscriptionGapEmissionLimit { get; init; } = int.MaxValue;

        /// <summary>Gets or sets the optional gate that releases retained-history gap emission.</summary>
        public TaskCompletionSource? ReleaseSubscriptionGap { get; init; }

        /// <summary>Gets or sets whether a gated gap ignores cancellation until the remote gate opens.</summary>
        public bool IgnoreSubscriptionGapCancellation { get; init; }

        /// <summary>Gets the signal set when a retained-history gap is ready to be emitted.</summary>
        public TaskCompletionSource SubscriptionGapReady { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets or sets the result returned after a snapshot recovery request is released.</summary>
        public RemoteSnapshotRecoveryResult SnapshotRecoveryResult { get; set; } = new() { Status = RemoteSnapshotRecoveryStatus.RetryableConcurrentChange };

        /// <summary>Gets or sets the optional snapshot recovery result factory.</summary>
        public Func<RemoteSnapshotRecoveryRequest, RemoteSnapshotRecoveryResult>? SnapshotRecoveryResultFactory { get; init; }

        /// <summary>Gets or sets the optional disposal failure.</summary>
        public Exception? DisposeException { get; init; }

        /// <summary>Gets or sets the optional cancellation callback failure.</summary>
        public Exception? SubscribeCancellationException { get; init; }

        /// <summary>Gets observed subscription cancellation failures before product fault sanitization.</summary>
        public List<Exception> SubscribeCancellationFailures { get; } = [];

        /// <summary>Gets the signal set when subscription cancellation reaches the transport.</summary>
        public TaskCompletionSource SubscribeCanceled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal set when a subscription iterator completes.</summary>
        public TaskCompletionSource SubscribeCompleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the number of completed subscription iterators.</summary>
        public int SubscribeCompletedCount => Volatile.Read(ref _subscribeCompletedCount);

        /// <summary>Gets or sets the optional gate that delays non-empty subscription batches.</summary>
        public TaskCompletionSource? ReleaseBatches { get; init; }

        /// <summary>Gets or sets the optional gate that delays canceled subscription completion.</summary>
        public TaskCompletionSource? ReleaseSubscribeCancellation { get; init; }

        /// <summary>Gets failed and successful acknowledgement attempts.</summary>
        public ConcurrentQueue<ReceiveAcknowledgement> AcknowledgementAttempts { get; } = new();

        /// <summary>Gets the number of acknowledgement attempts.</summary>
        public int AcknowledgeCalls => AcknowledgementAttempts.Count;

        /// <summary>Gets or sets the optional signal raised from inside a cancellation callback.</summary>
        public TaskCompletionSource? SubscribeCancellationCallbackEntered { get; init; }

        /// <summary>Gets or sets the optional gate that blocks a cancellation callback.</summary>
        public ManualResetEventSlim? ReleaseSubscribeCancellationCallback { get; init; }

        /// <summary>Gets the number of observed subscription cancellations.</summary>
        public int SubscribeCancellationCount { get; private set; }

        /// <summary>Gets the signal set when subscription begins.</summary>
        public TaskCompletionSource SubscribeEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the token held by the most recently entered subscription.</summary>
        public CancellationToken LastSubscriptionCancellationToken { get; private set; }

        /// <summary>Gets the signal set after the consumer has processed every configured batch.</summary>
        public TaskCompletionSource ConfiguredBatchesConsumed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal set when acknowledgement is received.</summary>
        public TaskCompletionSource AcknowledgementEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal set when snapshot recovery enters remote transport.</summary>
        public TaskCompletionSource SnapshotRecoveryEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets or sets the optional gate that delays snapshot recovery response.</summary>
        public TaskCompletionSource? ReleaseSnapshotRecovery { get; init; }

        /// <summary>Gets the signal set when a prepared push send is recorded.</summary>
        public TaskCompletionSource SentBatchEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal that keeps the subscription open after configured batches are yielded.</summary>
        public TaskCompletionSource HoldOpen { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal for a subscription MoveNext pause before yielding a batch.</summary>
        public TaskCompletionSource? BatchMoveNextEntered { get; init; }

        /// <summary>Gets the signal that releases a paused subscription MoveNext.</summary>
        public TaskCompletionSource? ReleaseBatchMoveNext { get; init; }

        /// <summary>Gets the signal set when session disposal begins.</summary>
        public TaskCompletionSource? DisposeEntered { get; init; }

        /// <summary>Gets the optional gate that delays session disposal completion.</summary>
        public TaskCompletionSource? ReleaseDispose { get; init; }

        /// <summary>Gets the number of disposal calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <summary>Gets the number of prepare calls.</summary>
        public int PrepareCalls { get; private set; }

        /// <summary>Gets or sets the one-based send attempt to pause before completing network I/O.</summary>
        public int PauseBeforeSendNumber { get; init; }

        /// <summary>Gets or sets an optional failure thrown after a prepared send is observed.</summary>
        public Exception? PreparedSendException { get; init; }

        /// <summary>Gets the signal set when the configured send attempt pauses.</summary>
        public TaskCompletionSource PausedSendEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public NegotiatedCapabilities NegotiatedCapabilities { get; init; } = new(
            new(1, 0),
            RemoteTransportCapabilities.ServerIdempotency
            | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
            | RemoteTransportCapabilities.ReceiveAcknowledgements,
            MaximumBatchOperations: 32,
            MaximumBatchBytes: 4096,
            ServerIdempotencyRetention: TimeSpan.FromMinutes(DefaultServerIdempotencyRetentionMinutes),
            ClientInboxRetentionRequired: null);

        /// <summary>Gets the number of snapshot recovery requests observed by the session.</summary>
        public int SnapshotRecoveryRequestCount
        {
            get
            {
                lock (_observationGate)
                {
                    return SnapshotRecoveryRequests.Count;
                }
            }
        }

        /// <summary>Gets the number of snapshot recovery requests canceled while waiting on the remote response.</summary>
        public int SnapshotRecoveryCancellationCount
        {
            get
            {
                lock (_observationGate)
                {
                    return _canceledSnapshotRecoveryRequests.Count;
                }
            }
        }

        /// <inheritdoc/>
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<IPreparedRemotePush> PreparePushAsync(SyncBatch batch, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PrepareCalls++;
            lock (_observationGate)
            {
                PreparedBatches.Add(batch);
            }

            return new(new ReceivePreparedPush(this, batch));
        }

        /// <inheritdoc/>
        public async IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(
            RemoteSubscribeRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            try
            {
                IReadOnlyList<RemoteEventBatch> batches;
                lock (_subscribeGate)
                {
                    SubscribeRequests.Add(request);
                    batches = SubscriptionBatches.Count == 0 ? Batches : SubscriptionBatches.Dequeue();
                }

                RegisterSubscribeCancellationCallback(cancellationToken);
                LastSubscriptionCancellationToken = cancellationToken;
                _ = SubscribeEntered.TrySetResult();
                await ThrowSubscriptionGapIfConfiguredAsync(request, cancellationToken).ConfigureAwait(false);

                if (batches.Count != 0 && ReleaseBatches is not null)
                {
                    await ReleaseBatches.Task.ConfigureAwait(false);
                }

                for (var i = 0; i < batches.Count; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await WaitForBatchMoveNextReleaseAsync().ConfigureAwait(false);
                    yield return batches[i];
                }

                _ = ConfiguredBatchesConsumed.TrySetResult();
                try
                {
                    await HoldOpen.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    await HandleSubscribeCancellationAsync().ConfigureAwait(false);
                    throw;
                }
            }
            finally
            {
                _ = Interlocked.Increment(ref _subscribeCompletedCount);
                _ = SubscribeCompleted.TrySetResult();
            }
        }

        /// <inheritdoc/>
        public async ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
            RemoteSnapshotRecoveryRequest request,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            lock (_observationGate)
            {
                SnapshotRecoveryRequests.Add(request);
            }

            _ = SnapshotRecoveryEntered.TrySetResult();
            try
            {
                if (ReleaseSnapshotRecovery is not null)
                {
                    await ReleaseSnapshotRecovery.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                }

                return SnapshotRecoveryResultFactory?.Invoke(request) ?? SnapshotRecoveryResult;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                lock (_observationGate)
                {
                    _canceledSnapshotRecoveryRequests.Add(request);
                }

                throw;
            }
        }

        /// <inheritdoc/>
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            AcknowledgementAttempts.Enqueue(acknowledgement);
            if (AcknowledgeException is not null
                && Interlocked.Increment(ref _acknowledgeAttempts) <= AcknowledgeExceptionLimit)
            {
                throw AcknowledgeException;
            }

            Acknowledgements.Add(acknowledgement);
            _ = AcknowledgementEntered.TrySetResult();
            return default;
        }

        /// <summary>Disposes subscription cancellation callback registrations owned by this session.</summary>
        public void DisposeSubscribeCancellationCallbacks()
        {
            List<CancellationTokenRegistration> registrations;
            lock (_subscribeGate)
            {
                registrations = [.. _subscribeCancellationRegistrations];
                _subscribeCancellationRegistrations.Clear();
            }

            for (var i = 0; i < registrations.Count; i++)
            {
                registrations[i].Dispose();
            }
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            DisposeSubscribeCancellationCallbacks();
            DisposeCalls++;
            _ = DisposeEntered?.TrySetResult();
            if (ReleaseDispose is not null)
            {
                return new(WaitForDisposeReleaseAsync(ReleaseDispose, DisposeException));
            }

            return DisposeException is null ? default : ValueTask.FromException(DisposeException);
        }

        /// <summary>Gets a snapshot recovery request by index.</summary>
        /// <param name="index">The request index.</param>
        /// <returns>The captured request.</returns>
        public RemoteSnapshotRecoveryRequest GetSnapshotRecoveryRequest(int index)
        {
            lock (_observationGate)
            {
                return SnapshotRecoveryRequests[index];
            }
        }

        /// <summary>Gets the snapshot recovery request stream trace.</summary>
        /// <returns>The observed request stream identifiers in request order.</returns>
        public StreamId[] GetSnapshotRecoveryRequestStreams()
        {
            lock (_observationGate)
            {
                var streams = new StreamId[SnapshotRecoveryRequests.Count];
                for (var index = 0; index < streams.Length; index++)
                {
                    streams[index] = SnapshotRecoveryRequests[index].StreamId;
                }

                return streams;
            }
        }

        /// <summary>Gets the canceled snapshot recovery request stream trace.</summary>
        /// <returns>The canceled request stream identifiers in request order.</returns>
        public StreamId[] GetCanceledSnapshotRecoveryRequestStreams()
        {
            lock (_observationGate)
            {
                var streams = new StreamId[_canceledSnapshotRecoveryRequests.Count];
                for (var index = 0; index < streams.Length; index++)
                {
                    streams[index] = _canceledSnapshotRecoveryRequests[index].StreamId;
                }

                return streams;
            }
        }

        /// <summary>Checks whether a prepared batch contains work for a stream.</summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <returns>Whether the stream has prepared work.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasPreparedBatchForStream(StreamId streamId) => HasBatchForStream(PreparedBatches, streamId);

        /// <summary>Checks whether a sent batch contains work for a stream.</summary>
        /// <param name="streamId">The stream identifier.</param>
        /// <returns>Whether the stream has sent work.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasSentBatchForStream(StreamId streamId) => HasBatchForStream(SentBatches, streamId);

        /// <summary>Releases a paused receive-session send attempt.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleasePausedSendAttempt() => _releasePausedSend.TrySetResult();

        /// <summary>Waits for a test-controlled session disposal gate before completing disposal.</summary>
        /// <param name="releaseDispose">The disposal release signal.</param>
        /// <param name="disposeException">The optional disposal exception.</param>
        /// <returns>The disposal wait task.</returns>
        private static async Task WaitForDisposeReleaseAsync(TaskCompletionSource releaseDispose, Exception? disposeException)
        {
            await releaseDispose.Task.ConfigureAwait(false);
            if (disposeException is not null)
            {
                throw disposeException;
            }
        }

        /// <summary>Invokes the configured subscription cancellation callback behavior.</summary>
        /// <param name="state">The callback state.</param>
        private static void InvokeSubscribeCancellationCallback(object? state)
        {
            var callbackState = state as SubscribeCancellationCallbackState
                ?? new SubscribeCancellationCallbackState(new InvalidOperationException("A subscription cancellation callback failed."), null, null);
            _ = callbackState.Entered?.TrySetResult();
            callbackState.Release?.Wait();
            if (callbackState.Exception is not null)
            {
                throw callbackState.Exception;
            }
        }

        /// <summary>Emits a configured retained-history gap for one subscription.</summary>
        /// <param name="request">The subscription request.</param>
        /// <param name="cancellationToken">The receive cancellation token.</param>
        /// <returns>The gap emission task.</returns>
        /// <exception cref="RemoteSubscriptionRetentionGapException">The configured gap is emitted.</exception>
        private async ValueTask ThrowSubscriptionGapIfConfiguredAsync(
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken)
        {
            var gap = SubscriptionGapFactory?.Invoke(request) ?? SubscriptionGap;
            if (gap is null || Interlocked.Increment(ref _subscriptionGapEmissions) > SubscriptionGapEmissionLimit)
            {
                return;
            }

            _ = SubscriptionGapReady.TrySetResult();
            if (ReleaseSubscriptionGap is not null)
            {
                if (IgnoreSubscriptionGapCancellation)
                {
                    await ReleaseSubscriptionGap.Task.ConfigureAwait(false);
                }
                else
                {
                    await ReleaseSubscriptionGap.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            throw gap;
        }

        /// <summary>Signals and optionally pauses before yielding a receive batch.</summary>
        /// <returns>The optional pause task.</returns>
        private async ValueTask WaitForBatchMoveNextReleaseAsync()
        {
            _ = BatchMoveNextEntered?.TrySetResult();
            if (ReleaseBatchMoveNext is not null)
            {
                await ReleaseBatchMoveNext.Task.ConfigureAwait(false);
            }
        }

        /// <summary>Records and optionally delays a subscribe cancellation observation.</summary>
        /// <returns>The cancellation handling task.</returns>
        /// <exception cref="Exception">The configured subscribe cancellation exception.</exception>
        private async ValueTask HandleSubscribeCancellationAsync()
        {
            SubscribeCancellationCount++;
            _ = SubscribeCanceled.TrySetResult();
            if (ReleaseSubscribeCancellation is not null)
            {
                await ReleaseSubscribeCancellation.Task.ConfigureAwait(false);
            }

            if (SubscribeCancellationException is null)
            {
                return;
            }

            SubscribeCancellationFailures.Add(SubscribeCancellationException);
            throw SubscribeCancellationException;
        }

        /// <summary>Checks whether one batch list contains work for a stream.</summary>
        /// <param name="batches">The batches to inspect.</param>
        /// <param name="streamId">The stream identifier.</param>
        /// <returns>Whether the stream is present.</returns>
        private bool HasBatchForStream(List<SyncBatch> batches, StreamId streamId)
        {
            lock (_observationGate)
            {
                for (var batchIndex = 0; batchIndex < batches.Count; batchIndex++)
                {
                    var operations = batches[batchIndex].Operations;
                    for (var operationIndex = 0; operationIndex < operations.Count; operationIndex++)
                    {
                        if (operations[operationIndex].StreamId == streamId)
                        {
                            return true;
                        }
                    }
                }
            }

            return false;
        }

        /// <summary>Waits if the configured receive-session send attempt is paused.</summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The send wait task.</returns>
        private async ValueTask WaitForPausedSendAsync(CancellationToken cancellationToken)
        {
            var attempt = Interlocked.Increment(ref _sendAttempts);
            if (PauseBeforeSendNumber != attempt)
            {
                return;
            }

            _ = PausedSendEntered.TrySetResult();
            await _releasePausedSend.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Registers an optional cancellation callback gate or failure.</summary>
        /// <param name="cancellationToken">The subscription cancellation token.</param>
        private void RegisterSubscribeCancellationCallback(CancellationToken cancellationToken)
        {
            if (SubscribeCancellationException is null && SubscribeCancellationCallbackEntered is null && ReleaseSubscribeCancellationCallback is null)
            {
                return;
            }

            var registration = cancellationToken.UnsafeRegister(
                InvokeSubscribeCancellationCallback,
                new SubscribeCancellationCallbackState(
                    SubscribeCancellationException,
                    SubscribeCancellationCallbackEntered,
                    ReleaseSubscribeCancellationCallback));
            lock (_subscribeGate)
            {
                _subscribeCancellationRegistrations.Add(registration);
            }
        }

        /// <summary>Prepared upload handle for receive-session shared transport regression tests.</summary>
        /// <param name="owner">The owning session.</param>
        /// <param name="batch">The prepared batch.</param>
        private sealed class ReceivePreparedPush(ReceiveSession owner, SyncBatch batch) : IPreparedRemotePush
        {
            /// <inheritdoc/>
            public SyncBatch Batch { get; } = batch;

            /// <inheritdoc/>
            public long EncodedSizeBytes { get; } = PreparedUploadBytes;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => default;

            /// <inheritdoc/>
            public async ValueTask<RemoteSyncResult> SendAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await owner.WaitForPausedSendAsync(cancellationToken).ConfigureAwait(false);
                lock (owner._observationGate)
                {
                    owner.SentBatches.Add(Batch);
                }

                _ = owner.SentBatchEntered.TrySetResult();
                if (owner.PreparedSendException is { } exception)
                {
                    throw exception;
                }

                var results = new OperationSyncResult[Batch.Operations.Count];
                for (var i = 0; i < results.Length; i++)
                {
                    results[i] = new(Batch.Operations[i].OperationId, OperationResultKind.Accepted, ReasonCode: null, ServerVersion: "v1");
                }

                return new(Batch.BatchId, results, serverCursor: null, retryAfter: null);
            }
        }

        /// <summary>Stores configured subscription cancellation callback behavior.</summary>
        /// <param name="Exception">The optional exception to throw.</param>
        /// <param name="Entered">The optional signal raised from inside the callback.</param>
        /// <param name="Release">The optional gate that releases the callback.</param>
        private sealed record SubscribeCancellationCallbackState(Exception? Exception, TaskCompletionSource? Entered, ManualResetEventSlim? Release);
    }
}
