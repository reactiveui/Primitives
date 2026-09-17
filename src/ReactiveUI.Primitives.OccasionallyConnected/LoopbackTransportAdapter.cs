// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Connects a transport session directly to an in-process server stream hub supplied by the trusted host.</summary>
[DebuggerDisplay("Loopback; Capabilities={Capabilities,nq}")]
public sealed partial class LoopbackTransportAdapter : IRemoteTransportAdapter
{
    /// <summary>Synchronizes adapter session lifetime.</summary>
#if NET9_0_OR_GREATER
    private readonly Lock _gate = new();
#else
    private readonly object _gate = new();
#endif

    /// <summary>Stores the configured loopback options.</summary>
    private readonly LoopbackTransportAdapterOptions _options;

    /// <summary>The active session, when one is connected.</summary>
    private LoopbackTransportSession? _activeSession;

    /// <summary>Whether adapter disposal has closed new admission.</summary>
    private bool _disposed;

    /// <summary>The stable disposal task for repeated adapter disposal.</summary>
    private Task? _disposeTask;

    /// <summary>Initializes a new instance of the <see cref="LoopbackTransportAdapter"/> class.</summary>
    /// <param name="options">The trusted host supplied loopback options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The options are malformed or unbounded.</exception>
    public LoopbackTransportAdapter(LoopbackTransportAdapterOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        LoopbackTransportValidator.ValidateOptions(options);
        _options = options;
    }

    /// <inheritdoc/>
    public RemoteTransportCapabilities Capabilities => _options.PeerCapabilities.Features;

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <see langword="null"/>.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
    /// <exception cref="InvalidOperationException">The request is malformed, incompatible, or overlaps an active session.</exception>
    public ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        LoopbackTransportValidator.ValidateConnectRequest(request, _options);

        LoopbackTransportSession session;
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_activeSession is not null)
            {
                throw new InvalidOperationException("The loopback transport adapter already has an active session.");
            }

            session = new(this, _options);
            _activeSession = session;
        }

        return new(session);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync()
    {
        LoopbackTransportSession? session = null;
        TaskCompletionSource<object?>? completion = null;
        Task task;
        lock (_gate)
        {
            if (_disposeTask is null)
            {
                _disposed = true;
                session = _activeSession;
                completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                _disposeTask = completion.Task;
            }

            task = _disposeTask;
        }

        if (completion is not null)
        {
            _ = LoopbackTransportDisposal.DisposeSessionAsync(session, completion);
        }

        return new(task);
    }

    /// <summary>Releases a session only when it still owns the active slot.</summary>
    /// <param name="session">The session being released.</param>
    private void ReleaseSession(LoopbackTransportSession session)
    {
        lock (_gate)
        {
            if (ReferenceEquals(_activeSession, session))
            {
                _activeSession = null;
            }
        }
    }

    /// <summary>Throws when the adapter is disposed.</summary>
    /// <exception cref="ObjectDisposedException">The adapter is disposed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);

    /// <summary>Represents a bounded loopback session.</summary>
    /// <param name="owner">The owning adapter.</param>
    /// <param name="options">The trusted host supplied options.</param>
    private sealed partial class LoopbackTransportSession(LoopbackTransportAdapter owner, LoopbackTransportAdapterOptions options) : IRemoteTransportSession, IRemoteTransportBatchPreparer
    {
        /// <summary>The operation kind for push requests.</summary>
        private const int PushOperation = 0;

        /// <summary>The operation kind for acknowledgement requests.</summary>
        private const int AcknowledgeOperation = 1;

        /// <summary>The operation kind for subscription enumerators.</summary>
        private const int SubscribeOperation = 2;

        /// <summary>Synchronizes session admission and drain state.</summary>
#if NET9_0_OR_GREATER
        private readonly Lock _gate = new();
#else
        private readonly object _gate = new();
#endif

        /// <summary>Cancels in-flight work during disposal.</summary>
        private readonly CancellationTokenSource _disposeCts = new();

        /// <summary>The active subscription enumerators owned by this session.</summary>
        private readonly HashSet<LoopbackSubscriptionEnumerator> _activeSubscriptionEnumerators = [];

        /// <summary>The prepared push handles owned by this session.</summary>
        private readonly HashSet<LoopbackPreparedPush> _preparedPushes = [];

        /// <summary>The number of active push requests.</summary>
        private int _activePushRequests;

        /// <summary>The number of active acknowledgement requests.</summary>
        private int _activeAcknowledgements;

        /// <summary>The number of active subscription enumerators.</summary>
        private int _activeSubscriptions;

        /// <summary>Whether session disposal has closed admission.</summary>
        private bool _disposed;

        /// <summary>The drain signal used by disposal.</summary>
        private TaskCompletionSource<object?>? _drained;

        /// <summary>The stable disposal task for repeated disposal.</summary>
        private Task? _disposeTask;

        /// <inheritdoc/>
        public NegotiatedCapabilities NegotiatedCapabilities => options.PeerCapabilities;

        /// <summary>Gets the total active operation count.</summary>
        private int ActiveOperationCount => _activePushRequests + _activeAcknowledgements + _activeSubscriptions;

        /// <inheritdoc/>
        /// <exception cref="ArgumentNullException"><paramref name="batch"/> is <see langword="null"/>.</exception>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
        /// <exception cref="InvalidOperationException">The batch exceeds loopback bounds or the hub returns a malformed response.</exception>
        /// <exception cref="SyncBatchValidationException">The hub result does not exactly match the pushed batch.</exception>
        public async ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken)
        {
            await using var prepared = CreatePreparedPush(batch, cancellationToken);
            return await prepared.SendAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        /// <exception cref="ArgumentNullException"><paramref name="request"/> is <see langword="null"/>.</exception>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
        /// <exception cref="InvalidOperationException">The request or a received batch is malformed or exceeds loopback bounds.</exception>
        public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(RemoteSubscribeRequest request, CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(request);
            LoopbackTransportValidator.ValidateSubscribeRequest(request, options);
            return new LoopbackSubscriptionEnumerable(this, request, options, cancellationToken);
        }

        /// <inheritdoc/>
        /// <exception cref="ArgumentNullException"><paramref name="acknowledgement"/> is <see langword="null"/>.</exception>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
        /// <exception cref="InvalidOperationException">The acknowledgement is malformed or the session has reached its acknowledgement limit.</exception>
        public async ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(acknowledgement);
            LoopbackTransportValidator.ValidateAcknowledgement(acknowledgement, options);
            using var lease = Admit(AcknowledgeOperation, cancellationToken);
            await options.Hub.AcknowledgeAsync(acknowledgement, options.AuthenticatedClient, lease.Token).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync()
        {
            TaskCompletionSource<object?>? completion = null;
            var drainTask = Task.CompletedTask;
            LoopbackSubscriptionEnumerator[] subscriptions = [];
            LoopbackPreparedPush[] preparedPushes = [];
            Task task;
            lock (_gate)
            {
                if (_disposeTask is null)
                {
                    _disposed = true;
                    drainTask = GetDrainTask();
                    subscriptions = CopySubscriptions();
                    preparedPushes = CopyPreparedPushes();
                    completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                    _disposeTask = completion.Task;
                }

                task = _disposeTask;
            }

            if (completion is not null)
            {
                _ = DisposeCoreAsync(drainTask, subscriptions, preparedPushes, completion);
            }

            return new(task);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<IPreparedRemotePush> IRemoteTransportBatchPreparer.PreparePushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            new(CreatePreparedPush(batch, cancellationToken));

        /// <summary>Prepares a push batch and reserves loopback capacity until send or disposal.</summary>
        /// <param name="batch">The synchronization batch to prepare.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The prepared remote push handle.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="batch"/> is <see langword="null"/>.</exception>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
        /// <exception cref="InvalidOperationException">The batch exceeds loopback bounds.</exception>
        private LoopbackPreparedPush CreatePreparedPush(SyncBatch batch, CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(batch);
            var prepared = AdmitPreparedPush(batch, cancellationToken);
            try
            {
                prepared.SetEncodedSizeBytes(LoopbackTransportValidator.ValidateOutgoingBatch(batch, options));
                return prepared;
            }
            catch
            {
                prepared.ReleaseValidationFailureReservation();
                throw;
            }
        }

        /// <summary>Admits one operation when its bounded slot is available.</summary>
        /// <param name="operationKind">The operation kind.</param>
        /// <param name="cancellationToken">The caller cancellation token.</param>
        /// <returns>The operation lease.</returns>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
        /// <exception cref="ObjectDisposedException">The session is disposed.</exception>
        /// <exception cref="InvalidOperationException">No bounded slot is available.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private OperationLease Admit(int operationKind, CancellationToken cancellationToken) =>
            Admit(operationKind, cancellationToken, CancellationToken.None);

        /// <summary>Admits one operation when its bounded slot is available.</summary>
        /// <param name="operationKind">The operation kind.</param>
        /// <param name="cancellationToken">The primary caller cancellation token.</param>
        /// <param name="secondaryCancellationToken">The secondary caller cancellation token.</param>
        /// <returns>The operation lease.</returns>
        /// <exception cref="OperationCanceledException">A caller token is canceled.</exception>
        /// <exception cref="ObjectDisposedException">The session is disposed.</exception>
        /// <exception cref="InvalidOperationException">No bounded slot is available.</exception>
        private OperationLease Admit(int operationKind, CancellationToken cancellationToken, CancellationToken secondaryCancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            secondaryCancellationToken.ThrowIfCancellationRequested();
            CancellationToken disposeToken;
            lock (_gate)
            {
                ThrowIfDisposed();
                if (!TryIncrement(operationKind))
                {
                    throw new InvalidOperationException("The loopback session has reached its active operation limit.");
                }

                disposeToken = _disposeCts.Token;
            }

            var cancellation = secondaryCancellationToken.CanBeCanceled
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, secondaryCancellationToken, disposeToken)
                : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, disposeToken);
            return new(this, operationKind, cancellation);
        }

        /// <summary>Admits a prepared push without linking the later send to the prepare token.</summary>
        /// <param name="batch">The synchronization batch retained by the prepared push.</param>
        /// <param name="cancellationToken">The caller cancellation token.</param>
        /// <returns>The operation lease.</returns>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
        /// <exception cref="ObjectDisposedException">The session is disposed.</exception>
        /// <exception cref="InvalidOperationException">No bounded slot is available.</exception>
        private LoopbackPreparedPush AdmitPreparedPush(SyncBatch batch, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CancellationToken disposeToken;
            CancellationTokenSource cancellation;
            LoopbackPreparedPush prepared;
            lock (_gate)
            {
                ThrowIfDisposed();
                if (!TryIncrement(PushOperation))
                {
                    throw new InvalidOperationException("The loopback session has reached its active operation limit.");
                }

                disposeToken = _disposeCts.Token;
                cancellation = CancellationTokenSource.CreateLinkedTokenSource(disposeToken);
                prepared = new(this, options, batch, new(this, PushOperation, cancellation));
                _ = _preparedPushes.Add(prepared);
            }

            return prepared;
        }

        /// <summary>Gets the drain task for the current operation count.</summary>
        /// <returns>The drain task.</returns>
        private Task GetDrainTask()
        {
            if (ActiveOperationCount == 0)
            {
                return Task.CompletedTask;
            }

            _drained ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
            return _drained.Task;
        }

        /// <summary>Copies active subscription enumerators for disposal outside the session gate.</summary>
        /// <returns>The copied subscriptions.</returns>
        private LoopbackSubscriptionEnumerator[] CopySubscriptions()
        {
            if (_activeSubscriptionEnumerators.Count == 0)
            {
                return [];
            }

            LoopbackSubscriptionEnumerator[] subscriptions = [.. _activeSubscriptionEnumerators];
            return subscriptions;
        }

        /// <summary>Copies prepared push handles for disposal outside the session gate.</summary>
        /// <returns>The copied prepared pushes.</returns>
        private LoopbackPreparedPush[] CopyPreparedPushes()
        {
            if (_preparedPushes.Count == 0)
            {
                return [];
            }

            LoopbackPreparedPush[] preparedPushes = [.. _preparedPushes];
            return preparedPushes;
        }

        /// <summary>Unregisters a prepared push handle.</summary>
        /// <param name="prepared">The prepared push handle.</param>
        private void UnregisterPreparedPush(LoopbackPreparedPush prepared)
        {
            lock (_gate)
            {
                _ = _preparedPushes.Remove(prepared);
            }
        }

        /// <summary>Registers an active subscription enumerator.</summary>
        /// <param name="subscription">The subscription enumerator.</param>
        /// <returns>Whether the subscription must be closed because disposal has already started.</returns>
        private bool RegisterSubscription(LoopbackSubscriptionEnumerator subscription)
        {
            lock (_gate)
            {
                _ = _activeSubscriptionEnumerators.Add(subscription);
                return _disposed;
            }
        }

        /// <summary>Unregisters an active subscription enumerator.</summary>
        /// <param name="subscription">The subscription enumerator.</param>
        private void UnregisterSubscription(LoopbackSubscriptionEnumerator subscription)
        {
            lock (_gate)
            {
                _ = _activeSubscriptionEnumerators.Remove(subscription);
            }
        }

        /// <summary>Attempts to increment the counter for one operation kind.</summary>
        /// <param name="operationKind">The operation kind.</param>
        /// <returns>Whether the operation was admitted.</returns>
        private bool TryIncrement(int operationKind)
        {
            if (operationKind == PushOperation && _activePushRequests < options.MaximumConcurrentRequests)
            {
                _activePushRequests++;
                return true;
            }

            if (operationKind == AcknowledgeOperation && _activeAcknowledgements < options.MaximumConcurrentAcknowledgements)
            {
                _activeAcknowledgements++;
                return true;
            }

            if (operationKind != SubscribeOperation || _activeSubscriptions >= options.MaximumConcurrentSubscriptions)
            {
                return false;
            }

            _activeSubscriptions++;
            return true;
        }

        /// <summary>Releases an admitted operation.</summary>
        /// <param name="operationKind">The operation kind.</param>
        private void Release(int operationKind)
        {
            TaskCompletionSource<object?>? drained = null;
            lock (_gate)
            {
                Decrement(operationKind);
                if (_disposed && ActiveOperationCount == 0)
                {
                    drained = _drained;
                    _drained = null;
                }
            }

            drained?.TrySetResult(null);
        }

        /// <summary>Decrements the counter for one operation kind.</summary>
        /// <param name="operationKind">The operation kind.</param>
        private void Decrement(int operationKind)
        {
            if (operationKind == PushOperation)
            {
                _activePushRequests--;
                return;
            }

            if (operationKind == AcknowledgeOperation)
            {
                _activeAcknowledgements--;
                return;
            }

            _activeSubscriptions--;
        }

        /// <summary>Cancels new work and waits for active work to drain.</summary>
        /// <param name="drainTask">The operation drain task.</param>
        /// <param name="subscriptions">The active subscriptions captured for disposal.</param>
        /// <param name="preparedPushes">The active prepared pushes captured for disposal.</param>
        /// <param name="completion">The disposal completion signal.</param>
        /// <returns>The disposal task.</returns>
        private async Task DisposeCoreAsync(
            Task drainTask,
            LoopbackSubscriptionEnumerator[] subscriptions,
            LoopbackPreparedPush[] preparedPushes,
            TaskCompletionSource<object?> completion)
        {
            Exception? failure = null;
            try
            {
                await CancelDisposeTokenAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failure = exception;
            }

            for (var index = 0; index < subscriptions.Length; index++)
            {
                try
                {
                    await subscriptions[index].DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    failure ??= exception;
                }
            }

            for (var index = 0; index < preparedPushes.Length; index++)
            {
                preparedPushes[index].ReleaseIdleReservationForSessionDispose();
            }

            await drainTask.ConfigureAwait(false);
            _disposeCts.Dispose();
            owner.ReleaseSession(this);
            if (failure is null)
            {
                _ = completion.TrySetResult(null);
                return;
            }

            _ = completion.TrySetException(failure);
        }

        /// <summary>Cancels the session disposal token.</summary>
        /// <returns>The cancellation task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Task CancelDisposeTokenAsync() => _disposeCts.CancelAsync();

        /// <summary>Throws when the session is disposed.</summary>
        /// <exception cref="ObjectDisposedException">The session is disposed.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);

        /// <summary>Creates tracked subscription enumerators.</summary>
        /// <param name="session">The owning session.</param>
        /// <param name="request">The subscription request.</param>
        /// <param name="validatorOptions">The loopback validator options.</param>
        /// <param name="callerCancellationToken">The caller cancellation token.</param>
        private sealed class LoopbackSubscriptionEnumerable(
            LoopbackTransportSession session,
            RemoteSubscribeRequest request,
            LoopbackTransportAdapterOptions validatorOptions,
            CancellationToken callerCancellationToken) : IAsyncEnumerable<RemoteEventBatch>
        {
            /// <inheritdoc/>
            public IAsyncEnumerator<RemoteEventBatch> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            {
                var lease = session.Admit(SubscribeOperation, callerCancellationToken, cancellationToken);
                try
                {
                    var source = validatorOptions.Hub.SubscribeStreamAsync(request, validatorOptions.AuthenticatedClient, lease.Token)
                        ?? throw new InvalidOperationException("The loopback hub returned no subscription sequence.");
                    var upstream = source.GetAsyncEnumerator(lease.Token);
                    var subscription = new LoopbackSubscriptionEnumerator(session, lease, upstream, request, validatorOptions);
                    if (session.RegisterSubscription(subscription))
                    {
                        _ = subscription.DisposeAsync().AsTask();
                    }

                    return subscription;
                }
                catch
                {
                    lease.Dispose();
                    throw;
                }
            }
        }

        /// <summary>Owns one upstream subscription enumerator.</summary>
        /// <param name="session">The owning session.</param>
        /// <param name="lease">The admitted subscription lease.</param>
        /// <param name="upstream">The upstream enumerator.</param>
        /// <param name="request">The subscription request that defines stream and cursor continuity.</param>
        /// <param name="validatorOptions">The loopback validator options.</param>
        private sealed class LoopbackSubscriptionEnumerator(
            LoopbackTransportSession session,
            OperationLease lease,
            IAsyncEnumerator<RemoteEventBatch> upstream,
            RemoteSubscribeRequest request,
            LoopbackTransportAdapterOptions validatorOptions) : IAsyncEnumerator<RemoteEventBatch>
        {
            /// <summary>Synchronizes move and disposal state.</summary>
#if NET9_0_OR_GREATER
            private readonly Lock _gate = new();
#else
            private readonly object _gate = new();
#endif

            /// <summary>The current batch.</summary>
            private RemoteEventBatch? _current;

            /// <summary>Whether an upstream move is active.</summary>
            private bool _moveActive;

            /// <summary>The current move completion signal.</summary>
            private TaskCompletionSource<object?> _moveCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);

            /// <summary>The stable disposal task.</summary>
            private Task? _disposeTask;

            /// <summary>The cursor that must precede the next received batch.</summary>
            private string? _previousCursor = request.Cursor;

            /// <summary>Whether the next received batch must match the expected previous cursor.</summary>
            private bool _requiresPreviousCursor = request.Cursor is not null;

            /// <inheritdoc/>
            public RemoteEventBatch Current => _current ?? throw new InvalidOperationException("The loopback subscription has no current batch.");

            /// <inheritdoc/>
            public async ValueTask<bool> MoveNextAsync()
            {
                Exception? failure = null;
                TaskCompletionSource<object?>? disposeCompletion = null;
                var disposalAlreadyStarted = false;
                var shouldDispose = false;
                var result = false;
                if (!TryBeginMove(out var disposalTask))
                {
                    await disposalTask.ConfigureAwait(false);
                    return false;
                }

                try
                {
                    result = await MoveNextCoreAsync().ConfigureAwait(false);
                    shouldDispose = !result;
                }
                catch (Exception exception)
                {
                    failure = exception;
                    shouldDispose = true;
                }
                finally
                {
                    disposeCompletion = CompleteMove(shouldDispose, out disposalAlreadyStarted);
                }

                var moveDisposeTask = Task.CompletedTask;
                if (disposeCompletion is not null)
                {
                    _ = DisposeCoreAsync(null, disposeCompletion);
                    moveDisposeTask = disposeCompletion.Task;
                }

                try
                {
                    await moveDisposeTask.ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    if (failure is not null)
                    {
                        throw new AggregateException(failure, exception);
                    }

                    throw;
                }

                if (failure is not null)
                {
                    ExceptionDispatchInfo.Capture(failure).Throw();
                }

                return result;
            }

            /// <inheritdoc/>
            public ValueTask DisposeAsync()
            {
                Task? moveTask = null;
                TaskCompletionSource<object?>? completion = null;
                Task task;
                lock (_gate)
                {
                    if (_disposeTask is null)
                    {
                        moveTask = _moveActive ? _moveCompleted.Task : null;
                        completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                        _disposeTask = completion.Task;
                    }

                    task = _disposeTask;
                }

                if (completion is not null)
                {
                    _ = DisposeCoreAsync(moveTask, completion);
                }

                return new(task);
            }

            /// <summary>Begins a move or returns the completed disposal task through <paramref name="disposalTask"/>.</summary>
            /// <param name="disposalTask">The disposal task when the enumerator is already disposed.</param>
            /// <returns><see langword="true"/> when a move was started; otherwise, <see langword="false"/>.</returns>
            /// <exception cref="InvalidOperationException">The subscription already has an active move.</exception>
            private bool TryBeginMove(out Task disposalTask)
            {
                lock (_gate)
                {
                    if (_disposeTask is not null)
                    {
                        disposalTask = _disposeTask;
                        return false;
                    }

                    if (_moveActive)
                    {
                        throw new InvalidOperationException("The loopback subscription already has an active move.");
                    }

                    _moveActive = true;
                    _moveCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
                    disposalTask = Task.CompletedTask;
                    return true;
                }
            }

            /// <summary>Moves upstream once and validates the batch before exposing it.</summary>
            /// <returns>Whether a valid batch was received.</returns>
            private async ValueTask<bool> MoveNextCoreAsync()
            {
                if (!await upstream.MoveNextAsync().ConfigureAwait(false))
                {
                    _current = null;
                    return false;
                }

                var batch = upstream.Current;
                LoopbackTransportValidator.ValidateReceiveBatch(
                    batch,
                    validatorOptions,
                    request.StreamId,
                    _previousCursor,
                    _requiresPreviousCursor);
                _current = batch;
                _previousCursor = batch.NextCursor;
                _requiresPreviousCursor = true;
                return true;
            }

            /// <summary>Completes an active move signal and marks terminal moves as disposed before releasing the gate.</summary>
            /// <param name="shouldDispose">Whether the completed move reached a terminal state.</param>
            /// <param name="disposalAlreadyStarted">Whether another caller already owns disposal.</param>
            /// <returns>The disposal completion signal when this move owns cleanup; otherwise, <see langword="null"/>.</returns>
            private TaskCompletionSource<object?>? CompleteMove(bool shouldDispose, out bool disposalAlreadyStarted)
            {
                TaskCompletionSource<object?>? disposeCompletion = null;
                TaskCompletionSource<object?> moveCompleted;
                lock (_gate)
                {
                    disposalAlreadyStarted = _disposeTask is not null;
                    if (shouldDispose && !disposalAlreadyStarted)
                    {
                        disposeCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                        _disposeTask = disposeCompletion.Task;
                    }

                    _moveActive = false;
                    moveCompleted = _moveCompleted;
                }

                _ = moveCompleted.TrySetResult(null);
                return disposeCompletion;
            }

            /// <summary>Cancels the upstream enumerator and releases the session lease.</summary>
            /// <param name="moveTask">The active move task, when present.</param>
            /// <param name="completion">The disposal completion signal.</param>
            /// <returns>The disposal task.</returns>
            private async Task DisposeCoreAsync(Task? moveTask, TaskCompletionSource<object?> completion)
            {
                static List<Exception> AddFailure(List<Exception>? failures, Exception exception)
                {
                    failures ??= [];
                    failures.Add(exception);
                    return failures;
                }

                List<Exception>? failures = null;
                try
                {
                    await lease.CancelAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    failures = AddFailure(failures, exception);
                }

                if (moveTask is not null)
                {
                    await moveTask.ConfigureAwait(false);
                }

                try
                {
                    await upstream.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    failures = AddFailure(failures, exception);
                }
                finally
                {
                    session.UnregisterSubscription(this);
                    lease.Dispose();
                }

                if (failures is null)
                {
                    _ = completion.TrySetResult(null);
                    return;
                }

                _ = completion.TrySetException(failures.Count == 1 ? failures[0] : new AggregateException(failures));
            }
        }

        /// <summary>Releases a bounded operation admission.</summary>
        /// <param name="session">The owning session.</param>
        /// <param name="operationKind">The operation kind.</param>
        /// <param name="cancellation">The linked operation cancellation source.</param>
        private sealed class OperationLease(LoopbackTransportSession session, int operationKind, CancellationTokenSource cancellation) : IDisposable
        {
            /// <summary>Gets the linked operation cancellation token.</summary>
            public CancellationToken Token => cancellation.Token;

            /// <summary>Cancels the linked operation token.</summary>
            /// <returns>The cancellation task.</returns>
            public Task CancelAsync()
            {
#if NET8_0_OR_GREATER
                return cancellation.CancelAsync();
#else
                cancellation.Cancel();
                return Task.CompletedTask;
#endif
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                session.Release(operationKind);
                cancellation.Dispose();
            }
        }
    }
}
