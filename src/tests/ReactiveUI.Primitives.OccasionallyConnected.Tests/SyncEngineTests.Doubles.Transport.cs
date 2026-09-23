// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Transport test doubles for <see cref="SyncEngineTests"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Records transport connections.</summary>
    private sealed class RecordingTransport : IRemoteTransportAdapter
    {
        /// <summary>Gets the number of connection calls.</summary>
        public int ConnectCalls { get; private set; }

        /// <summary>Gets the optional signal set when connection begins.</summary>
        public TaskCompletionSource? ConnectEntered { get; init; }

        /// <summary>Gets the optional signal that releases connection.</summary>
        public TaskCompletionSource? ReleaseConnect { get; init; }

        /// <summary>Gets the optional signal set after the session is created.</summary>
        public TaskCompletionSource? SessionCreated { get; init; }

        /// <summary>Gets the optional signal that releases a created session.</summary>
        public TaskCompletionSource? ReleaseCreatedSession { get; init; }

        /// <summary>Gets the last connected session.</summary>
        public RecordingSession? Session { get; private set; }

        /// <summary>Gets connection requests received by the transport.</summary>
        public List<TransportConnectRequest> ConnectRequests { get; } = [];

        /// <summary>Gets queued connection failures. A null entry lets the matching connection continue.</summary>
        public Queue<Exception?> ConnectFailures { get; } = [];

        /// <summary>Gets queued sessions returned before the explicit session override or default session.</summary>
        public Queue<IRemoteTransportSession> Sessions { get; } = [];

        /// <summary>Gets or sets an explicit session returned from connect.</summary>
        public IRemoteTransportSession? SessionOverride { get; init; }

        /// <summary>Gets the number of transport disposal calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <summary>Gets or sets a value indicating whether dispose throws synchronously.</summary>
        public bool ThrowOnDispose { get; init; }

        /// <inheritdoc/>
        public RemoteTransportCapabilities Capabilities { get; init; } = RecordingTransportUploadCapabilities;

        /// <inheritdoc/>
        public async ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ConnectRequests.Add(request);
            ConnectCalls++;
            _ = ConnectEntered?.TrySetResult();
            await WaitForReleaseAsync(ReleaseConnect).ConfigureAwait(false);

            if (ConnectFailures.TryDequeue(out var failure) && failure is not null)
            {
                throw failure;
            }

            if (Sessions.TryDequeue(out var queuedSession))
            {
                _ = SessionCreated?.TrySetResult();
                return queuedSession;
            }

            if (SessionOverride is not null)
            {
                _ = SessionCreated?.TrySetResult();
                await WaitForReleaseAsync(ReleaseCreatedSession).ConfigureAwait(false);
                return SessionOverride;
            }

            Session = new();
            _ = SessionCreated?.TrySetResult();
            await WaitForReleaseAsync(ReleaseCreatedSession).ConfigureAwait(false);

            return Session;
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            return ThrowOnDispose ? ThrowTransportDisposeFailure() : default;
        }

        /// <summary>Throws the configured synchronous transport disposal failure.</summary>
        /// <returns>This method does not return.</returns>
        /// <exception cref="InvalidOperationException">The configured synchronous disposal failure.</exception>
        private static ValueTask ThrowTransportDisposeFailure() =>
            throw new InvalidOperationException("transport dispose failed");

        /// <summary>Waits for an optional release gate.</summary>
        /// <param name="release">The optional release signal.</param>
        /// <returns>The release task.</returns>
        private static Task WaitForReleaseAsync(TaskCompletionSource? release) =>
            release?.Task ?? Task.CompletedTask;
    }

    /// <summary>Delegates normal connections and blocks the retry reconnect until engine cancellation.</summary>
    /// <param name="inner">The transport that owns ordinary connections.</param>
    private sealed class CancelableReconnectTransport(RecordingTransport inner) : IRemoteTransportAdapter
    {
        /// <summary>Stores the observed connect call count.</summary>
        private int _connectCalls;

        /// <summary>Gets the signal set when the retry reconnect begins.</summary>
        public TaskCompletionSource ReconnectEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal set when engine cancellation reaches the retry reconnect.</summary>
        public TaskCompletionSource ReconnectCanceled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the number of observed connection calls.</summary>
        public int ConnectCalls => Volatile.Read(ref _connectCalls);

        /// <inheritdoc/>
        public RemoteTransportCapabilities Capabilities => inner.Capabilities;

        /// <inheritdoc/>
        public async ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken)
        {
            var call = Interlocked.Increment(ref _connectCalls);
            if (call == ExpectedTwoOperations)
            {
                _ = ReconnectEntered.TrySetResult();
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _ = ReconnectCanceled.TrySetResult();
                    throw;
                }
            }

            return await inner.ConnectAsync(request, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    /// <summary>Delegates normal connections and pauses the retry reconnect until released by the test.</summary>
    /// <param name="inner">The transport that owns ordinary connections.</param>
    private sealed class ControlledReconnectTransport(RecordingTransport inner) : IRemoteTransportAdapter
    {
        /// <summary>Stores the observed connect call count.</summary>
        private int _connectCalls;

        /// <summary>Gets the signal set when the retry reconnect begins.</summary>
        public TaskCompletionSource ReconnectEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal that releases the retry reconnect.</summary>
        public TaskCompletionSource ReleaseReconnect { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal set when cancellation reaches the retry reconnect.</summary>
        public TaskCompletionSource ReconnectCanceled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the number of observed connection calls.</summary>
        public int ConnectCalls => Volatile.Read(ref _connectCalls);

        /// <inheritdoc/>
        public RemoteTransportCapabilities Capabilities => inner.Capabilities;

        /// <inheritdoc/>
        public async ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken)
        {
            var call = Interlocked.Increment(ref _connectCalls);
            if (call == ExpectedTwoOperations)
            {
                _ = ReconnectEntered.TrySetResult();
                try
                {
                    await ReleaseReconnect.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _ = ReconnectCanceled.TrySetResult();
                    throw;
                }
            }

            return await inner.ConnectAsync(request, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    /// <summary>Delegates connections while exposing renewal token cancellation.</summary>
    /// <param name="inner">The transport that owns the sessions.</param>
    /// <param name="renewalCandidateReturned">The optional signal set after the renewal candidate connect returns.</param>
    private sealed class RenewalCancellationTrackingTransport(
        RecordingTransport inner,
        TaskCompletionSource? renewalCandidateReturned = null) : IRemoteTransportAdapter
    {
        /// <summary>Stores the second connect cancellation registration.</summary>
        private CancellationTokenRegistration _renewalCancellationRegistration;

        /// <summary>Stores the observed connect call count.</summary>
        private int _connectCalls;

        /// <summary>Gets the signal set when the renewal connect token is canceled.</summary>
        public TaskCompletionSource RenewalCancellationObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the number of observed connection calls.</summary>
        public int ConnectCalls => Volatile.Read(ref _connectCalls);

        /// <inheritdoc/>
        public RemoteTransportCapabilities Capabilities => inner.Capabilities;

        /// <inheritdoc/>
        public async ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken)
        {
            var call = Interlocked.Increment(ref _connectCalls);
            if (call == ExpectedCapacityCommitAttempts)
            {
                _renewalCancellationRegistration = cancellationToken.UnsafeRegister(
                    static state =>
                    {
                        if (state is TaskCompletionSource source)
                        {
                            _ = source.TrySetResult();
                        }
                    },
                    RenewalCancellationObserved);
            }

            var session = await inner.ConnectAsync(request, cancellationToken).ConfigureAwait(false);
            if (call == ExpectedCapacityCommitAttempts)
            {
                _ = renewalCandidateReturned?.TrySetResult();
            }

            return session;
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            _renewalCancellationRegistration.Dispose();
            await inner.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Prepared session wrapper that blocks and throws after a renewal candidate has connected.</summary>
    /// <param name="inner">The prepared session that handles normal upload calls.</param>
    /// <param name="renewalCandidateReturned">The signal set after the renewal candidate connect returns.</param>
    private sealed class CapabilityThrowingAfterRenewalCandidateSession(
        PreparedSession inner,
        Task renewalCandidateReturned) : IRemoteTransportSession, IRemoteTransportBatchPreparer
    {
        /// <summary>Gets the signal set when the throwing capability read is reached.</summary>
        public TaskCompletionSource ThrowingCapabilityReadEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal that releases the throwing capability read.</summary>
        public TaskCompletionSource ReleaseThrowingCapabilityRead { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public NegotiatedCapabilities NegotiatedCapabilities
        {
            get
            {
                if (!renewalCandidateReturned.IsCompleted)
                {
                    return inner.NegotiatedCapabilities;
                }

                _ = ThrowingCapabilityReadEntered.TrySetResult();
                ReleaseThrowingCapabilityRead.Task.GetAwaiter().GetResult();
                throw new OperationCanceledException("Capability validation was canceled by engine stop.");
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IPreparedRemotePush> PreparePushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            inner.PreparePushAsync(batch, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            inner.PushAsync(batch, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(RemoteSubscribeRequest request, CancellationToken cancellationToken) =>
            inner.SubscribeAsync(request, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken) =>
            inner.AcknowledgeAsync(acknowledgement, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    /// <summary>Records transport session disposal.</summary>
    private sealed class RecordingSession : IRemoteTransportSession
    {
        /// <summary>Gets the number of disposal calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <inheritdoc/>
        public NegotiatedCapabilities NegotiatedCapabilities { get; init; } = new(
            new(1, 0),
            RemoteTransportCapabilities.ServerIdempotency,
            MaximumBatchOperations: 1,
            MaximumBatchBytes: 1,
            ServerIdempotencyRetention: TimeSpan.FromMinutes(DefaultServerIdempotencyRetentionMinutes),
            ClientInboxRetentionRequired: null);

        /// <inheritdoc/>
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(RemoteSubscribeRequest request, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            return default;
        }
    }

    /// <summary>Prepared transport session used by upload pump tests.</summary>
    /// <param name="maximumBatchOperations">The negotiated maximum operation count.</param>
    /// <param name="maximumBatchBytes">The negotiated maximum encoded byte count.</param>
    private sealed class PreparedSession(int maximumBatchOperations, long maximumBatchBytes) : IRemoteTransportSession, IRemoteTransportBatchPreparer
    {
        /// <summary>Stores the signal that releases a paused send attempt.</summary>
        private readonly TaskCompletionSource _releasePausedSend = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Stores the next encoded size index.</summary>
        private int _encodedSizeIndex;

        /// <summary>Gets or sets the encoded sizes returned by successive prepared pushes.</summary>
        public long[] EncodedSizes { get; init; } = [];

        /// <summary>Gets prepared batches.</summary>
        public List<SyncBatch> PreparedBatches { get; } = [];

        /// <summary>Gets sent batches.</summary>
        public List<SyncBatch> SentBatches { get; } = [];

        /// <summary>Gets or sets the optional failure thrown by prepared sends.</summary>
        public Exception? SendException { get; init; }

        /// <summary>Gets queued send failures. A null entry lets the matching send continue.</summary>
        public Queue<Exception?> SendFailures { get; } = [];

        /// <summary>Gets or sets the optional callback invoked before a prepared send completes.</summary>
        public Action? OnSend { get; init; }

        /// <summary>Gets or sets the operation result kind returned for each sent operation.</summary>
        public OperationResultKind ResultKind { get; init; } = OperationResultKind.Accepted;

        /// <summary>Gets or sets the optional result factory for sent batches.</summary>
        public Func<SyncBatch, RemoteSyncResult>? ResultFactory { get; init; }

        /// <summary>Gets or sets the one-based send attempt to pause before completing network I/O.</summary>
        public int PauseBeforeSendNumber { get; init; }

        /// <summary>Gets the signal set when the configured send attempt is paused.</summary>
        public TaskCompletionSource PausedSendEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the number of prepare calls.</summary>
        public int PrepareCalls { get; private set; }

        /// <summary>Gets the number of disposal calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <summary>Gets or sets the optional disposal failure.</summary>
        public Exception? DisposeException { get; init; }

        /// <summary>Gets or sets the optional signal set when disposal begins.</summary>
        public TaskCompletionSource? DisposeEntered { get; init; }

        /// <summary>Gets or sets the optional gate that delays disposal completion.</summary>
        public TaskCompletionSource? ReleaseDispose { get; init; }

        /// <inheritdoc/>
        public NegotiatedCapabilities NegotiatedCapabilities { get; init; } = new(
            new(1, 0),
            RemoteTransportCapabilities.ServerIdempotency,
            maximumBatchOperations,
            maximumBatchBytes,
            ServerIdempotencyRetention: TimeSpan.FromMinutes(DefaultServerIdempotencyRetentionMinutes),
            ClientInboxRetentionRequired: null);

        /// <inheritdoc/>
        public ValueTask<IPreparedRemotePush> PreparePushAsync(SyncBatch batch, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PrepareCalls++;
            PreparedBatches.Add(batch);
            var encodedSize = maximumBatchBytes;
            if (_encodedSizeIndex < EncodedSizes.Length)
            {
                encodedSize = EncodedSizes[_encodedSizeIndex];
                _encodedSizeIndex++;
            }

            return new(new PreparedPush(this, batch, encodedSize));
        }

        /// <inheritdoc/>
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(RemoteSubscribeRequest request, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            _ = DisposeEntered?.TrySetResult();
            if (ReleaseDispose is not null)
            {
                return new(WaitForPreparedDisposeReleaseAsync(ReleaseDispose, DisposeException));
            }

            return DisposeException is null ? default : ValueTask.FromException(DisposeException);
        }

        /// <summary>Releases a send attempt paused by <see cref="PauseBeforeSendNumber"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReleasePausedSendAttempt() => _ = _releasePausedSend.TrySetResult();

        /// <summary>Waits for a test-controlled prepared-session disposal gate before completing disposal.</summary>
        /// <param name="releaseDispose">The disposal release signal.</param>
        /// <param name="disposeException">The optional disposal exception.</param>
        /// <returns>The disposal wait task.</returns>
        private static async Task WaitForPreparedDisposeReleaseAsync(
            TaskCompletionSource releaseDispose,
            Exception? disposeException)
        {
            await releaseDispose.Task.ConfigureAwait(false);
            if (disposeException is not null)
            {
                throw disposeException;
            }
        }

        /// <summary>Prepared push handle for upload tests.</summary>
        /// <param name="owner">The owning session.</param>
        /// <param name="batch">The prepared batch.</param>
        /// <param name="encodedSizeBytes">The encoded size.</param>
        private sealed class PreparedPush(PreparedSession owner, SyncBatch batch, long encodedSizeBytes) : IPreparedRemotePush
        {
            /// <inheritdoc/>
            public SyncBatch Batch { get; } = batch;

            /// <inheritdoc/>
            public long EncodedSizeBytes { get; } = encodedSizeBytes;

            /// <inheritdoc/>
            public async ValueTask<RemoteSyncResult> SendAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (owner.PauseBeforeSendNumber > 0 && owner.SentBatches.Count + 1 == owner.PauseBeforeSendNumber)
                {
                    _ = owner.PausedSendEntered.TrySetResult();
                    await owner._releasePausedSend.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                }

                owner.SentBatches.Add(Batch);
                owner.OnSend?.Invoke();
                if (owner.SendFailures.TryDequeue(out var queuedFailure) && queuedFailure is not null)
                {
                    throw queuedFailure;
                }

                if (owner.SendException is not null)
                {
                    throw owner.SendException;
                }

                return owner.ResultFactory?.Invoke(Batch) ?? CreateResult(Batch, owner.ResultKind);
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => default;

            /// <summary>Creates an accepted result for every operation in a batch.</summary>
            /// <param name="batch">The sent batch.</param>
            /// <param name="resultKind">The operation result kind.</param>
            /// <returns>The accepted result.</returns>
            private static RemoteSyncResult CreateResult(SyncBatch batch, OperationResultKind resultKind)
            {
                var results = new OperationSyncResult[batch.Operations.Count];
                for (var i = 0; i < results.Length; i++)
                {
                    results[i] = new(batch.Operations[i].OperationId, resultKind, ReasonCode: null, ServerVersion: "v1");
                }

                return new(batch.BatchId, results, serverCursor: null, retryAfter: null);
            }
        }
    }
}
