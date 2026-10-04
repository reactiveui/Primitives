// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Represents an active bounded HTTP remote transport session.</summary>
/// <content>Owns exactly encoded uploads before callers record a durable send attempt.</content>
internal sealed partial class HttpRemoteTransportSession
{
    /// <inheritdoc/>
    public async ValueTask<IPreparedRemotePush> PreparePushAsync(SyncBatch batch, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(batch);
        cancellationToken.ThrowIfCancellationRequested();
        var prepared = new PreparedPush(this);
        try
        {
            await prepared.InitializeAsync(batch, cancellationToken).ConfigureAwait(false);
            return prepared;
        }
        catch
        {
            await prepared.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Enforces negotiated operation count and exact encoded request bytes alongside the local HTTP body limit.</summary>
    /// <param name="batch">The structurally validated batch.</param>
    /// <param name="encodedSizeBytes">The exact serialized request body size.</param>
    /// <exception cref="HttpRemoteTransportException">The batch exceeds negotiated limits.</exception>
    private void ValidateNegotiatedBatch(SyncBatch batch, long encodedSizeBytes) =>
        _ = batch.Operations.Count > _negotiatedCapabilities.MaximumBatchOperations
            || encodedSizeBytes > _negotiatedCapabilities.MaximumBatchBytes
            ? throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge)
            : false;

    /// <summary>Sends an already encoded body through the admitted request.</summary>
    /// <param name="batch">The original validated batch.</param>
    /// <param name="body">The exact encoded body.</param>
    /// <param name="cancellationToken">The send cancellation token.</param>
    /// <returns>The validated synchronization result.</returns>
    private async Task<RemoteSyncResult> SendPreparedAsync(SyncBatch batch, byte[] body, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var response = await SendAsync(HttpMethod.Post, _options.PushPath, body, cancellationToken).ConfigureAwait(false);
        var retryAfter = HttpTransportStatus.GetRetryAfter(response, _options.TimeProvider);
        var responseBytes = await HttpProtocolContent.ReadBoundedBytesAsync(response, _options, cancellationToken).ConfigureAwait(false);
        return _codec.DeserializePushResponse(batch, responseBytes, retryAfter);
    }

    /// <summary>Retains one bounded request body until its single send or disposal completes.</summary>
    /// <param name="owner">The owning session.</param>
    private sealed class PreparedPush(HttpRemoteTransportSession owner) : IPreparedRemotePush
    {
        /// <summary>Protects send admission and retained buffers.</summary>
        private readonly Lock _gate = new();

        /// <summary>The retained batch.</summary>
        private SyncBatch? _batch;

        /// <summary>The retained exact request bytes.</summary>
        private byte[]? _body;

        /// <summary>The reserved request capacity.</summary>
        private HttpRequestGate.Lease? _admission;

        /// <summary>The session lifetime reservation.</summary>
        private SessionOperation? _operation;

        /// <summary>The owner cancellation source.</summary>
        private CancellationTokenSource? _lifetime;

        /// <summary>The owner cancellation callback registration.</summary>
        private CancellationTokenRegistration _registration;

        /// <summary>The stable disposal completion.</summary>
        private Task? _disposeTask;

        /// <summary>The published active-send drain task.</summary>
        private Task? _activeSend;

        /// <summary>The exact encoded request body size.</summary>
        private long _encodedSizeBytes;

        /// <summary>Whether sending has already started.</summary>
        private bool _sent;

        /// <summary>Whether the handle is closed.</summary>
        private bool _disposed;

        /// <summary>Whether capacity reservations were released.</summary>
        private int _released;

        /// <inheritdoc/>
        public SyncBatch Batch => _batch ?? throw new ObjectDisposedException(nameof(PreparedPush));

        /// <inheritdoc/>
        public long EncodedSizeBytes => _encodedSizeBytes;

        /// <inheritdoc/>
        public ValueTask<RemoteSyncResult> SendAsync(CancellationToken cancellationToken)
        {
            var send = BeginSend();
            return new(SendCoreAsync(send, cancellationToken));
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            TaskCompletionSource<object?>? completion = null;
            Task? active = null;
            Task disposal;
            lock (_gate)
            {
                if (_disposeTask is null)
                {
                    _disposed = true;
                    active = _activeSend;
                    completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                    _disposeTask = completion.Task;
                }

                disposal = _disposeTask;
            }

            if (completion is not null)
            {
                _ = DisposeCoreAsync(active, completion);
            }

            return new(disposal);
        }

        /// <summary>Acquires, validates, and publishes the retained upload state.</summary>
        /// <param name="batch">The validated batch.</param>
        /// <param name="cancellationToken">The caller cancellation token.</param>
        /// <returns>The asynchronous initialization operation.</returns>
        /// <exception cref="HttpRemoteTransportException">The batch exceeds negotiated limits.</exception>
        /// <exception cref="ObjectDisposedException">The session is disposed.</exception>
        internal async ValueTask InitializeAsync(SyncBatch batch, CancellationToken cancellationToken)
        {
            var operation = owner.BeginOperation();
            _operation = operation;
            var lifetime = CancellationTokenSource.CreateLinkedTokenSource(owner._shutdown.Token, owner._adapterShutdownToken);
            _lifetime = lifetime;
            var admission = await owner._requestGate.EnterAsync(lifetime.Token).ConfigureAwait(false);
            _admission = admission;
            var body = owner._codec.SerializePushRequest(batch);
            owner.ValidateNegotiatedBatch(batch, body.LongLength);
            cancellationToken.ThrowIfCancellationRequested();
            lifetime.Token.ThrowIfCancellationRequested();
            _batch = batch;
            _body = body;
            _encodedSizeBytes = body.LongLength;
            _registration = lifetime.Token.Register(OwnerCanceled);
        }

        /// <summary>Publishes send ownership before any HTTP callback can reenter disposal.</summary>
        /// <returns>The owned send state.</returns>
        /// <exception cref="InvalidOperationException">The prepared push was already sent.</exception>
        /// <exception cref="ObjectDisposedException">The handle is closed.</exception>
        private PreparedSend BeginSend()
        {
            lock (_gate)
            {
                ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
                if (_sent)
                {
                    throw new InvalidOperationException("The prepared push has already been sent.");
                }

                var retainedBatch = _batch;
                var retainedBody = _body;
                var lifetime = _lifetime;
                ArgumentExceptionHelper.ThrowIfNull(retainedBatch);
                ArgumentExceptionHelper.ThrowIfNull(retainedBody);
                ArgumentExceptionHelper.ThrowIfNull(lifetime);
                var completion = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
                var send = new PreparedSend(retainedBatch, retainedBody, lifetime, completion);
                _sent = true;
                _activeSend = completion.Task;
                return send;
            }
        }

        /// <summary>Sends once and signals drain completion after releasing the owned request.</summary>
        /// <param name="send">The captured send state.</param>
        /// <param name="cancellationToken">The caller cancellation token.</param>
        /// <returns>The remote result.</returns>
        private async Task<RemoteSyncResult> SendCoreAsync(PreparedSend send, CancellationToken cancellationToken)
        {
            try
            {
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(send.Lifetime.Token, cancellationToken);
                return await owner.SendPreparedAsync(send.Batch, send.Body, linked.Token).ConfigureAwait(false);
            }
            finally
            {
                ClearBuffers();
                ReleaseReservations();
                _ = send.Completion.TrySetResult(null);
            }
        }

        /// <summary>Closes the handle when a session or adapter is canceled.</summary>
        private void OwnerCanceled()
        {
            bool idle;
            lock (_gate)
            {
                _disposed = true;
                idle = !_sent;
            }

            ClearBuffers();
            if (!idle)
            {
                return;
            }

            ReleaseReservations();
        }

        /// <summary>Cancels and drains any active send before releasing cancellation resources.</summary>
        /// <param name="active">The active send drain task.</param>
        /// <param name="completion">The shared disposal completion.</param>
        /// <returns>The asynchronous cleanup operation.</returns>
        private async Task DisposeCoreAsync(Task? active, TaskCompletionSource<object?> completion)
        {
            var lifetime = _lifetime;
            Exception? failure = null;
            try
            {
                if (lifetime is not null)
                {
                    await lifetime.CancelAsync().ConfigureAwait(false);
                }
            }
            catch (Exception exception)
            {
                failure = exception;
            }

            ClearBuffers();
            if (active is not null)
            {
                await active.ConfigureAwait(false);
            }

            ReleaseReservations();
#if NET5_0_OR_GREATER
            await _registration.DisposeAsync().ConfigureAwait(false);
#else
            _registration.Dispose();
#endif
            lifetime?.Dispose();
            if (failure is null)
            {
                _ = completion.TrySetResult(null);
            }
            else
            {
                _ = completion.TrySetException(failure);
            }
        }

        /// <summary>Clears retained payload and batch references.</summary>
        private void ClearBuffers()
        {
            lock (_gate)
            {
                _batch = null;
                _body = null;
            }
        }

        /// <summary>Releases each reservation exactly once.</summary>
        private void ReleaseReservations()
        {
            if (Interlocked.Exchange(ref _released, 1) != 0)
            {
                return;
            }

            _admission?.Dispose();
            if (_operation is { } operation)
            {
                operation.Dispose();
            }
        }

        /// <summary>Owns the exact state of one active send.</summary>
        /// <param name="Batch">The original batch.</param>
        /// <param name="Body">The encoded body.</param>
        /// <param name="Lifetime">The owner cancellation source captured for the send.</param>
        /// <param name="Completion">The published drain signal.</param>
        private sealed record PreparedSend(
            SyncBatch Batch,
            byte[] Body,
            CancellationTokenSource Lifetime,
            TaskCompletionSource<object?> Completion);
    }
}
