// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Concrete authorized server stream hub facade over the internal server journal and processor.</summary>
public sealed partial class ServerStreamHub
{
    /// <summary>Creates subscription enumerators for a fixed request and call token.</summary>
    private sealed class SubscriptionEnumerable : IAsyncEnumerable<RemoteEventBatch>
    {
        /// <summary>The owning hub.</summary>
        private readonly ServerStreamHub _hub;

        /// <summary>The subscription request.</summary>
        private readonly RemoteSubscribeRequest _request;

        /// <summary>The authenticated client.</summary>
        private readonly ServerAuthenticatedClient _client;

        /// <summary>The call cancellation token.</summary>
        private readonly CancellationToken _callCancellationToken;

        /// <summary>Initializes a new instance of the <see cref="SubscriptionEnumerable"/> class.</summary>
        /// <param name="hub">The owning hub.</param>
        /// <param name="request">The subscription request.</param>
        /// <param name="client">The authenticated client.</param>
        /// <param name="cancellationToken">The call cancellation token.</param>
        internal SubscriptionEnumerable(
            ServerStreamHub hub,
            RemoteSubscribeRequest request,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            _hub = hub;
            _request = request;
            _client = client;
            _callCancellationToken = cancellationToken;
        }

        /// <inheritdoc/>
        public IAsyncEnumerator<RemoteEventBatch> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            var linked = CreateEnumeratorCancellation(_callCancellationToken, cancellationToken);
            var token = linked?.Token ?? SelectEnumeratorCancellation(_callCancellationToken, cancellationToken);
            return new SubscriptionEnumerator(_hub, _request, _client, linked, token);
        }

        /// <summary>Creates a linked cancellation source when both tokens are distinct and cancellable.</summary>
        /// <param name="callCancellationToken">The call cancellation token.</param>
        /// <param name="enumeratorCancellationToken">The enumerator cancellation token.</param>
        /// <returns>A linked cancellation source, or <see langword="null"/> when one token can be used directly.</returns>
        private static CancellationTokenSource? CreateEnumeratorCancellation(
            CancellationToken callCancellationToken,
            CancellationToken enumeratorCancellationToken) =>
            !callCancellationToken.CanBeCanceled || !enumeratorCancellationToken.CanBeCanceled || callCancellationToken == enumeratorCancellationToken
                ? null
                : CancellationTokenSource.CreateLinkedTokenSource(callCancellationToken, enumeratorCancellationToken);

        /// <summary>Selects a single cancellation token for enumeration.</summary>
        /// <param name="callCancellationToken">The call cancellation token.</param>
        /// <param name="enumeratorCancellationToken">The enumerator cancellation token.</param>
        /// <returns>The cancellation token used by the enumerator.</returns>
        private static CancellationToken SelectEnumeratorCancellation(
            CancellationToken callCancellationToken,
            CancellationToken enumeratorCancellationToken) =>
            callCancellationToken.CanBeCanceled ? callCancellationToken : enumeratorCancellationToken;
    }

    /// <summary>Reads subscription pages while owning one active subscription reservation.</summary>
    private sealed class SubscriptionEnumerator : IAsyncEnumerator<RemoteEventBatch>
    {
        /// <summary>The owning hub.</summary>
        private readonly ServerStreamHub _hub;

        /// <summary>The subscription request.</summary>
        private readonly RemoteSubscribeRequest _request;

        /// <summary>The authenticated client.</summary>
        private readonly ServerAuthenticatedClient _client;

        /// <summary>The combined enumeration cancellation token.</summary>
        private readonly CancellationToken _cancellationToken;

        /// <summary>The optional linked cancellation source owned by the enumerator.</summary>
        private readonly CancellationTokenSource? _enumeratorCancellation;

        /// <summary>The cancellation source signaled when the enumerator is disposed.</summary>
        private readonly CancellationTokenSource _disposeCancellation = new();

        /// <summary>The move token source combining caller cancellation with enumerator disposal.</summary>
        private readonly CancellationTokenSource _moveCancellation;

        /// <summary>The completion source signaled when active move operations drain.</summary>
        private readonly TaskCompletionSource<bool> _moveDrained = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The completion source signaled when owned resources have been released.</summary>
        private readonly TaskCompletionSource<bool> _resourcesReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The completion source signaled when disposal cancellation has finished running callbacks.</summary>
        private readonly TaskCompletionSource<bool> _disposeCancellationCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);

#if NET9_0_OR_GREATER
        /// <summary>Protects active move admission and drain state.</summary>
        private readonly Lock _moveGate = new();
#else
        /// <summary>Protects active move admission and drain state.</summary>
        private readonly object _moveGate = new();
#endif

        /// <summary>The next receive cursor.</summary>
        private string? _cursor;

        /// <summary>The number of active move operations.</summary>
        private int _activeMoves;

        /// <summary>Whether the active subscription has been reserved.</summary>
        private int _started;

        /// <summary>Whether the enumerator has finished.</summary>
        private int _finished;

        /// <summary>The shared disposal task returned to all disposal callers.</summary>
        private Task? _disposeTask;

        /// <summary>Whether enumerator disposal has started.</summary>
        private int _disposeStarted;

        /// <summary>Whether owned enumerator resource release has started.</summary>
        private int _resourceReleaseStarted;

        /// <summary>Initializes a new instance of the <see cref="SubscriptionEnumerator"/> class.</summary>
        /// <param name="hub">The owning hub.</param>
        /// <param name="request">The subscription request.</param>
        /// <param name="client">The authenticated client.</param>
        /// <param name="enumeratorCancellation">The optional linked cancellation source owned by the enumerator.</param>
        /// <param name="cancellationToken">The combined enumeration cancellation token.</param>
        internal SubscriptionEnumerator(
            ServerStreamHub hub,
            RemoteSubscribeRequest request,
            ServerAuthenticatedClient client,
            CancellationTokenSource? enumeratorCancellation,
            CancellationToken cancellationToken)
        {
            _hub = hub;
            _request = request;
            _client = client;
            _cancellationToken = cancellationToken;
            _enumeratorCancellation = enumeratorCancellation;
            _moveCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCancellation.Token);
        }

        /// <inheritdoc/>
        public RemoteEventBatch Current { get; private set; } = new(Guid.Empty, new("stream"), null, string.Empty, []);

        /// <inheritdoc/>
        public async ValueTask<bool> MoveNextAsync()
        {
            if (!TryEnterMove())
            {
                return false;
            }

            try
            {
                EnsureStarted();
                return await MoveNextCoreAsync().ConfigureAwait(false);
            }
            catch
            {
                MarkFinished();
                throw;
            }
            finally
            {
                ReleaseMove();
            }
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync() => await BeginDispose().ConfigureAwait(false);

        /// <summary>Reads until a page is available, cancellation is observed or disposal completes the stream.</summary>
        /// <returns><see langword="true"/> when <see cref="Current"/> has been updated; otherwise <see langword="false"/>.</returns>
        /// <exception cref="ServerReceiveRetentionGapException">The requested cursor is outside retained history.</exception>
        /// <exception cref="OperationCanceledException">Enumeration cancellation has been requested.</exception>
        private async ValueTask<bool> MoveNextCoreAsync()
        {
            var disposalObserved = false;
            var moveToken = _moveCancellation.Token;
            while (!disposalObserved && !moveToken.IsCancellationRequested)
            {
                var page = await TryReadSubscriptionPageAsync(moveToken).ConfigureAwait(false);
                if (page is null)
                {
                    await ObserveDisposeCancellationAsync().ConfigureAwait(false);
                    disposalObserved = !_cancellationToken.IsCancellationRequested;
                    break;
                }

                if (TryAcceptPage(page))
                {
                    return true;
                }

                disposalObserved = await WaitForNextPollOrCompletionAsync(moveToken).ConfigureAwait(false);
            }

            MarkFinished();
            _cancellationToken.ThrowIfCancellationRequested();
            return false;
        }

        /// <summary>Accepts a page result for the current move.</summary>
        /// <param name="page">The page result.</param>
        /// <returns><see langword="true"/> when a page was accepted.</returns>
        /// <exception cref="ServerReceiveRetentionGapException">The requested cursor is outside retained history.</exception>
        private bool TryAcceptPage(ServerReceivePageResult page)
        {
            if (page.Status == ServerReceivePageStatus.RetentionGap)
            {
                throw new ServerReceiveRetentionGapException(RetentionGapMessage);
            }

            if (page.Status != ServerReceivePageStatus.Page)
            {
                return false;
            }

            var batch = page.RequireBatch();
            Current = batch;
            _cursor = batch.NextCursor;
            return true;
        }

        /// <summary>Reads one page and converts disposal cancellation into move completion.</summary>
        /// <param name="moveToken">The move cancellation token.</param>
        /// <returns>The page result, or <see langword="null"/> when cancellation ends the move.</returns>
        private async ValueTask<ServerReceivePageResult?> TryReadSubscriptionPageAsync(CancellationToken moveToken)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(moveToken, _hub._disposeCancellation.Token);
            try
            {
                return await _hub.ReadSubscriptionPageAsync(_request, _client, _cursor, linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_cancellationToken.IsCancellationRequested)
            {
                return null;
            }
            catch (OperationCanceledException) when (_disposeCancellation.IsCancellationRequested)
            {
                return null;
            }
            catch (OperationCanceledException) when (Volatile.Read(ref _hub._disposed) != 0)
            {
                return null;
            }
        }

        /// <summary>Observes cancellation callback failures from enumerator disposal.</summary>
        /// <returns>The cancellation observation task.</returns>
        /// <exception cref="Exception">Enumerator disposal cancellation failed.</exception>
        private async ValueTask ObserveDisposeCancellationAsync()
        {
            if (!_disposeCancellation.IsCancellationRequested)
            {
                return;
            }

            await _disposeCancellationCompleted.Task.ConfigureAwait(false);
        }

        /// <summary>Waits for the next poll or observes cancellation that completes the move.</summary>
        /// <param name="moveToken">The move cancellation token.</param>
        /// <returns><see langword="true"/> when the current move should complete.</returns>
        private async ValueTask<bool> WaitForNextPollOrCompletionAsync(CancellationToken moveToken)
        {
            try
            {
                await _hub.WaitForNextPollAsync(moveToken).ConfigureAwait(false);
                return false;
            }
            catch (OperationCanceledException) when (_cancellationToken.IsCancellationRequested)
            {
                return true;
            }
            catch (OperationCanceledException) when (_disposeCancellation.IsCancellationRequested)
            {
                return true;
            }
            catch (OperationCanceledException) when (Volatile.Read(ref _hub._disposed) != 0)
            {
                return true;
            }
        }

        /// <summary>Reserves subscription capacity on the first move.</summary>
        private void EnsureStarted()
        {
            if (Volatile.Read(ref _started) != 0)
            {
                return;
            }

            ArgumentExceptionHelper.ThrowIfNull(_request);
            _cursor = _request.Cursor;
            _hub.EnterActiveSubscription();
            _ = Interlocked.Exchange(ref _started, 1);
        }

        /// <summary>Marks the enumerator as finished.</summary>
        private void MarkFinished() => _ = Interlocked.Exchange(ref _finished, 1);

        /// <summary>Attempts to admit one move operation.</summary>
        /// <returns><see langword="true"/> when a move operation was admitted; otherwise <see langword="false"/>.</returns>
        /// <exception cref="InvalidOperationException">Another move operation is already active.</exception>
        private bool TryEnterMove()
        {
            lock (_moveGate)
            {
                if (Volatile.Read(ref _finished) != 0)
                {
                    return false;
                }

                if (_activeMoves != 0)
                {
                    throw new InvalidOperationException("Concurrent subscription moves are not supported.");
                }

                _activeMoves++;
                return true;
            }
        }

        /// <summary>Releases one active move operation and completes the drain when appropriate.</summary>
        private void ReleaseMove()
        {
            TaskCompletionSource<bool>? drained = null;
            var releaseResources = false;
            lock (_moveGate)
            {
                _activeMoves--;
                if (_activeMoves == 0 && (Volatile.Read(ref _finished) != 0 || Volatile.Read(ref _disposeStarted) != 0))
                {
                    drained = _moveDrained;
                }

                releaseResources = _activeMoves == 0
                    && Volatile.Read(ref _finished) != 0
                    && Volatile.Read(ref _disposeStarted) == 0
                    && _resourceReleaseStarted == 0;
                if (releaseResources)
                {
                    _resourceReleaseStarted = 1;
                }
            }

            _ = drained?.TrySetResult(true);
            if (!releaseResources)
            {
                return;
            }

            ReleaseResourcesAfterCompletion();
        }

        /// <summary>Gets the active move drain task.</summary>
        /// <returns>The active move drain task, or a completed task when no move operations are active.</returns>
        private Task GetMoveDrainTask()
        {
            Task task;
            lock (_moveGate)
            {
                task = _activeMoves == 0 ? Task.CompletedTask : _moveDrained.Task;
            }

            return task;
        }

        /// <summary>Starts or joins enumerator disposal.</summary>
        /// <returns>The shared disposal task.</returns>
        private Task BeginDispose()
        {
            Task task;
            TaskCompletionSource<bool>? completion = null;
            lock (_moveGate)
            {
                if (_disposeTask is not null)
                {
                    task = _disposeTask;
                }
                else if (_resourceReleaseStarted != 0)
                {
                    task = _resourcesReleased.Task;
                }
                else
                {
                    _disposeStarted = 1;
                    _finished = 1;
                    completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                    _disposeTask = completion.Task;
                    task = _disposeTask;
                }
            }

            if (completion is null)
            {
                return task;
            }

            _ = CompleteDisposeAsync(completion);
            return task;
        }

        /// <summary>Runs enumerator disposal outside the move gate and publishes the shared result.</summary>
        /// <param name="completion">The shared disposal completion source.</param>
        /// <returns>The asynchronous completion operation.</returns>
        private async Task CompleteDisposeAsync(TaskCompletionSource<bool> completion)
        {
            Exception? firstException = null;
            firstException = await CancelDisposeAsync(firstException).ConfigureAwait(false);
            firstException = await CaptureDisposalExceptionAsync(firstException, GetMoveDrainTask).ConfigureAwait(false);
            firstException = await CaptureDisposalExceptionAsync(firstException, ReleaseResourcesForDisposeAsync).ConfigureAwait(false);
            if (firstException is null)
            {
                _ = completion.TrySetResult(true);
                return;
            }

            _ = completion.TrySetException(firstException);
        }

        /// <summary>Cancels the enumerator and publishes cancellation callback completion.</summary>
        /// <param name="firstException">The first exception already captured.</param>
        /// <returns>The first captured exception, when any stage has failed.</returns>
        private async ValueTask<Exception?> CancelDisposeAsync(Exception? firstException)
        {
            try
            {
                await _disposeCancellation.CancelAsync().ConfigureAwait(false);
                _ = _disposeCancellationCompleted.TrySetResult(true);
            }
            catch (Exception exception)
            {
                firstException ??= exception;
                _ = _disposeCancellationCompleted.TrySetException(exception);
            }

            return firstException;
        }

        /// <summary>Releases owned resources after normal move completion.</summary>
        private void ReleaseResourcesAfterCompletion()
        {
            Close();
            _ = _resourcesReleased.TrySetResult(true);
        }

        /// <summary>Releases owned resources as part of enumerator disposal.</summary>
        /// <returns>A task that completes when resources have been released.</returns>
        private Task ReleaseResourcesForDisposeAsync()
        {
            lock (_moveGate)
            {
                _resourceReleaseStarted = 1;
            }

            Close();
            _ = _resourcesReleased.TrySetResult(true);
            return Task.CompletedTask;
        }

        /// <summary>Closes enumerator-managed resources after asynchronous cancellation and drain complete.</summary>
        private void Close()
        {
            if (Volatile.Read(ref _started) != 0)
            {
                _hub.ReleaseActiveSubscription();
            }

            _enumeratorCancellation?.Dispose();
            _moveCancellation.Dispose();
            _disposeCancellation.Dispose();
        }
    }
}
