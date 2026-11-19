// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Shared state that coordinates pair-wise combination of elements from both source sequences.</summary>
/// <typeparam name="T1">The type of the elements in the first source sequence.</typeparam>
/// <typeparam name="T2">The type of the elements in the second source sequence.</typeparam>
/// <typeparam name="TResult">The type of the elements in the resulting sequence produced by the selector function.</typeparam>
[System.Diagnostics.DebuggerDisplay("ZipState<T1, T2, TResult>: Observer = {_observer}")]
public sealed class ZipState<T1, T2, TResult> : IAsyncDisposable
{
    /// <summary>The downstream observer to forward combined results to.</summary>
    private readonly IObserverAsync<TResult> _observer;

    /// <summary>The function that combines paired elements.</summary>
    private readonly Func<T1, T2, TResult> _resultSelector;

    /// <summary>Cancellation source for disposal.</summary>
    private readonly CancellationTokenSource _disposeCts = new();

    /// <summary>Token fired when the zip state is disposed.</summary>
    private readonly CancellationToken _disposeToken;

    /// <summary>The synchronization gate protecting shared state access.</summary>
    private readonly Lock _gate = new();

    /// <summary>Serializes downstream observer notifications across both source arms.</summary>
    private readonly ReactiveUI.Primitives.Async.Internal.AsyncFlowGate _downstreamGate = new();

    /// <summary>Queue of buffered elements from the first source awaiting a pair from the second source.</summary>
    private readonly Queue<T1> _queue1 = new();

    /// <summary>Queue of buffered elements from the second source awaiting a pair from the first source.</summary>
    private readonly Queue<T2> _queue2 = new();

    /// <summary>Upstream subscription to the first source.</summary>
    private IAsyncDisposable? _sub1;

    /// <summary>Upstream subscription to the second source.</summary>
    private IAsyncDisposable? _sub2;

    /// <summary>Registration that propagates the original subscribe-token cancellation into <see cref="_disposeCts"/>.</summary>
    private CancellationTokenRegistration _externalLinkRegistration;

    /// <summary>Whether this state has been disposed.</summary>
    private int _disposed;

    /// <summary>Indicates whether the first source has completed.</summary>
    private bool _completed1;

    /// <summary>Indicates whether the second source has completed.</summary>
    private bool _completed2;

    /// <summary>Indicates whether the zip operation has finished and no more elements will be emitted.</summary>
    private bool _done;

    /// <summary>Initializes a new instance of the <see cref="ZipState{T1, T2, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer to forward combined results to.</param>
    /// <param name="resultSelector">The function that combines paired elements.</param>
    public ZipState(
        IObserverAsync<TResult> observer,
        Func<T1, T2, TResult> resultSelector)
    {
        _observer = observer;
        _resultSelector = resultSelector;
        _disposeToken = _disposeCts.Token;
    }

    /// <summary>Gets a value indicating whether the zip sequence has already terminated.</summary>
    internal bool IsDone
    {
        get
        {
            lock (_gate)
            {
                return _done;
            }
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (DisposalHelper.TrySetDisposed(ref _disposed))
        {
            return;
        }

        lock (_gate)
        {
            _done = true;
            _queue1.Clear();
            _queue2.Clear();
        }

        await _disposeCts.CancelAsync().ConfigureAwait(false);
        await DisposeUpstreamAsync().ConfigureAwait(false);
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        await _externalLinkRegistration.DisposeAsync().ConfigureAwait(false);
#else
        _externalLinkRegistration.Dispose();
#endif
        _disposeCts.Dispose();
    }

    /// <summary>Attaches the first source subscription, disposing it immediately if already terminated.</summary>
    /// <param name="subscription">The first source subscription.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal ValueTask AttachFirstAsync(IAsyncDisposable subscription)
    {
        bool disposeNow;
        lock (_gate)
        {
            disposeNow = _done;
            if (!disposeNow)
            {
                _sub1 = subscription;
            }
        }

        return disposeNow ? subscription.DisposeAsync() : default;
    }

    /// <summary>Attaches the second source subscription, disposing it immediately if already terminated.</summary>
    /// <param name="subscription">The second source subscription.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal ValueTask AttachSecondAsync(IAsyncDisposable subscription)
    {
        bool disposeNow;
        lock (_gate)
        {
            disposeNow = _done;
            if (!disposeNow)
            {
                _sub2 = subscription;
            }
        }

        return disposeNow ? subscription.DisposeAsync() : default;
    }

    /// <summary>Handles a new element from the first source, pairing it with a buffered element from the second source if available.</summary>
    /// <param name="value">The element from the first source.</param>
    /// <param name="token">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal ValueTask OnNext1Async(T1 value, CancellationToken token)
    {
        _ = token;
        var outcome = ReactiveUI.Primitives.Async.Internal.ZipQueueCoordinatorHelper.DequeueOrEnqueue(
            _gate,
            ref _done,
            _queue1,
            value,
            _queue2,
            in _completed2);
        return outcome.HasPair
            ? EmitPairedAsync(_resultSelector(value, outcome.Counterpart), outcome.CompleteAfterPair)
            : default;
    }

    /// <summary>Handles a new element from the second source, pairing it with a buffered element from the first source if available.</summary>
    /// <param name="value">The element from the second source.</param>
    /// <param name="token">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal ValueTask OnNext2Async(T2 value, CancellationToken token)
    {
        _ = token;
        var outcome = ReactiveUI.Primitives.Async.Internal.ZipQueueCoordinatorHelper.DequeueOrEnqueue(
            _gate,
            ref _done,
            _queue2,
            value,
            _queue1,
            in _completed1);
        return outcome.HasPair
            ? EmitPairedAsync(_resultSelector(outcome.Counterpart, value), outcome.CompleteAfterPair)
            : default;
    }

    /// <summary>Handles the first source completing, propagating completion downstream when appropriate.</summary>
    /// <param name="result">The completion result from the first source.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal ValueTask OnCompleted1Async(Result result) =>
        ReactiveUI.Primitives.Async.Internal.ZipQueueCoordinatorHelper.MarkSideCompleted(
            _gate,
            ref _done,
            ref _completed1,
            in _completed2,
            _queue1,
            _queue2,
            result)
            ? DeliverCompletionAsync(result)
            : default;

    /// <summary>Handles the second source completing, propagating completion downstream when appropriate.</summary>
    /// <param name="result">The completion result from the second source.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal ValueTask OnCompleted2Async(Result result) =>
        ReactiveUI.Primitives.Async.Internal.ZipQueueCoordinatorHelper.MarkSideCompleted(
            _gate,
            ref _done,
            ref _completed2,
            in _completed1,
            _queue2,
            _queue1,
            result)
            ? DeliverCompletionAsync(result)
            : default;

    /// <summary>Forwards a non-fatal error from either source to the downstream observer.</summary>
    /// <param name="error">The error to forward.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    internal ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
        _downstreamGate.RunAsync(
            (Self: this, Error: error, Token: cancellationToken),
            static s => DisposalHelper.HasDisposed(s.Self._disposed)
                ? default
                : s.Self._observer.OnErrorResumeAsync(s.Error, s.Token),
            cancellationToken);

    /// <summary>Routes cancellation of the subscribe-time token into <see cref="_disposeCts"/>, so per-emission code needs no linked source.</summary>
    /// <param name="external">The subscribe-time token.</param>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    internal void LinkExternalCancellation(CancellationToken external) =>
        _externalLinkRegistration = ReactiveUI.Primitives.Async.Internal.AsyncStopTokenHelper.LinkExternal(
            _disposeCts,
            external);

    /// <summary>Forwards a paired result, then completion when this pair drained the zip, while holding the downstream gate.</summary>
    /// <param name="result">The projected pair value.</param>
    /// <param name="completeAfterPair">Whether draining this pair completed the zip sequence.</param>
    /// <returns>A task representing the asynchronous delivery.</returns>
    internal async ValueTask DeliverPairAsync(TResult result, bool completeAfterPair)
    {
        if (DisposalHelper.HasDisposed(_disposed))
        {
            return;
        }

        await _observer.OnNextAsync(result, _disposeToken).ConfigureAwait(false);
        if (completeAfterPair)
        {
            await _observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        }
    }

    /// <summary>Delivers terminal completion under the downstream gate and disposes upstream subscriptions.</summary>
    /// <param name="result">The terminal completion result.</param>
    /// <returns>A task representing the asynchronous completion and teardown.</returns>
    internal async ValueTask DeliverCompletionAsync(Result result)
    {
        await _downstreamGate.RunAsync(
            (Self: this, Result: result),
            static s => DisposalHelper.HasDisposed(s.Self._disposed)
                ? default
                : s.Self._observer.OnCompletedAsync(s.Result),
            CancellationToken.None).ConfigureAwait(false);

        await DisposeUpstreamAsync().ConfigureAwait(false);
    }

    /// <summary>Forwards a paired result under the downstream gate and optionally completes and disposes upstream.</summary>
    /// <param name="result">The projected pair value.</param>
    /// <param name="completeAfterPair">Whether draining this pair completed the zip sequence.</param>
    /// <returns>A task representing the asynchronous emission.</returns>
    private async ValueTask EmitPairedAsync(TResult result, bool completeAfterPair)
    {
        await _downstreamGate.RunAsync(
            (Self: this, Result: result, CompleteAfterPair: completeAfterPair),
            static s => s.Self.DeliverPairAsync(s.Result, s.CompleteAfterPair),
            _disposeToken).ConfigureAwait(false);

        if (completeAfterPair)
        {
            await DisposeUpstreamAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Disposes both upstream subscriptions once.</summary>
    /// <returns>A task representing the asynchronous teardown.</returns>
    private async ValueTask DisposeUpstreamAsync()
    {
        IAsyncDisposable? sub1;
        IAsyncDisposable? sub2;
        lock (_gate)
        {
            sub1 = _sub1;
            sub2 = _sub2;
            _sub1 = null;
            _sub2 = null;
        }

        if (sub1 is not null)
        {
            await sub1.DisposeAsync().ConfigureAwait(false);
        }

        if (sub2 is not null)
        {
            await sub2.DisposeAsync().ConfigureAwait(false);
        }
    }
}
