// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Demonstrates bounded retries through a public context and durable store.</summary>
internal static partial class RetryBackoffScenario
{
    /// <summary>Captures public faults for a useful failed-demo diagnostic.</summary>
    private sealed class FaultObserver : IObserver<OccasionallyConnectedFault>
    {
        /// <summary>Gets the latest fault.</summary>
        internal OccasionallyConnectedFault? Latest { get; private set; }

        /// <inheritdoc/>
        public void OnNext(OccasionallyConnectedFault value) => Latest = value;

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }

    /// <summary>Composes a loopback connection with application upload failures.</summary>
    /// <param name="inner">The loopback adapter.</param>
    private sealed class FailingUploadAdapter(IRemoteTransportAdapter inner) : IRemoteTransportAdapter
    {
        /// <summary>The observed upload count.</summary>
        private int _uploadCount;

        /// <inheritdoc/>
        public RemoteTransportCapabilities Capabilities => inner.Capabilities;

        /// <summary>Gets the number of upload calls.</summary>
        internal int UploadCount => Volatile.Read(ref _uploadCount);

        /// <summary>Gets the signal for the retry upload.</summary>
        internal TaskCompletionSource SecondUpload { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public async ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken) =>
            new FailingUploadSession(await inner.ConnectAsync(request, cancellationToken).ConfigureAwait(false), this);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => inner.DisposeAsync();

        /// <summary>Counts the upload and signals the second attempt.</summary>
        internal void RecordUpload()
        {
            if (Interlocked.Increment(ref _uploadCount) == ExpectedUploads)
            {
                _ = SecondUpload.TrySetResult();
            }
        }
    }

    /// <summary>Forwards receive work and supplies typed upload failures.</summary>
    /// <param name="inner">The loopback session.</param>
    /// <param name="owner">The upload counter.</param>
    private sealed class FailingUploadSession(IRemoteTransportSession inner, FailingUploadAdapter owner) : IRemoteTransportSession, IRemoteTransportBatchPreparer
    {
        /// <inheritdoc/>
        public NegotiatedCapabilities NegotiatedCapabilities => inner.NegotiatedCapabilities;

        /// <inheritdoc/>
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            owner.RecordUpload();
            throw new RetryHintException();
        }

        /// <inheritdoc/>
        /// <exception cref="InvalidOperationException">The inner transport does not support prepared pushes.</exception>
        public async ValueTask<IPreparedRemotePush> PreparePushAsync(SyncBatch batch, CancellationToken cancellationToken)
        {
            if (inner is not IRemoteTransportBatchPreparer preparer)
            {
                throw new InvalidOperationException("The retry demo requires a transport that supports prepared pushes.");
            }

            return new FailingPreparedPush(await preparer.PreparePushAsync(batch, cancellationToken).ConfigureAwait(false), owner);
        }

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

    /// <summary>Retains the inner capacity reservation while failing the actual send.</summary>
    /// <param name="inner">The real prepared push.</param>
    /// <param name="owner">The upload counter.</param>
    private sealed class FailingPreparedPush(IPreparedRemotePush inner, FailingUploadAdapter owner) : IPreparedRemotePush
    {
        /// <inheritdoc/>
        public SyncBatch Batch => inner.Batch;

        /// <inheritdoc/>
        public long EncodedSizeBytes => inner.EncodedSizeBytes;

        /// <inheritdoc/>
        public ValueTask<RemoteSyncResult> SendAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            owner.RecordUpload();
            throw new RetryHintException();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    /// <summary>Supplies a public failure classification and retry hint.</summary>
    private sealed class RetryHintException : IOException, IRemoteTransportFailure
    {
        /// <summary>Initializes a new instance of the <see cref="RetryHintException"/> class.</summary>
        public RetryHintException()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="RetryHintException"/> class.</summary>
        /// <param name="message">The failure message.</param>
        public RetryHintException(string message)
            : base(message)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="RetryHintException"/> class.</summary>
        /// <param name="message">The failure message.</param>
        /// <param name="innerException">The failure cause.</param>
        public RetryHintException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <inheritdoc/>
        public RetryFailure RetryFailure => RetryFailure.Transient(ServerHint);
    }
}
