// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Manages subscriptions for merged observable sequences, forwarding items from all inner sources to a single observer.</summary>
/// <typeparam name="T">The type of the elements in the merged sequence.</typeparam>
/// <remarks>With a concurrency limit, a semaphore bounds the inner sources subscribed at once and each branch releases its slot once when it is disposed.</remarks>
[System.Diagnostics.DebuggerDisplay("BlendCoordinator: DisposeCts = {_disposeCts}")]
public sealed class BlendCoordinator<T> : IAsyncDisposable
{
    /// <summary>The cancellation token source backing <see cref="DisposedCancellationToken"/>.</summary>
    private readonly CancellationTokenSource _disposeCts = new();

    /// <summary>Holds the outer subscription so it can be disposed on teardown.</summary>
    private readonly SingleAssignmentDisposableAsync _outerDisposable = new();

    /// <summary>Tracks all inner subscriptions for disposal.</summary>
    private readonly MultipleDisposableAsync _innerDisposables = new();

    /// <summary>Serializes observer notifications to prevent concurrent calls.</summary>
    private readonly ReactiveUI.Primitives.Async.Internal.AsyncFlowGate _onSomethingGate = new();

    /// <summary>The downstream observer that receives merged items.</summary>
    private readonly IObserverAsync<T> _observer;

    /// <summary>Limits the number of concurrently subscribed inner observables, or <see langword="null"/> when unbounded.</summary>
    private readonly SemaphoreSlim? _semaphore;

    /// <summary>Registration that propagates the original subscribe-token cancellation into <see cref="_disposeCts"/>.</summary>
    private CancellationTokenRegistration _externalLinkRegistration;

    /// <summary>The number of currently active inner subscriptions.</summary>
    private int _innerActiveCount;

    /// <summary>Whether the outer source has completed.</summary>
    private bool _outerCompleted;

    /// <summary>Whether this subscription has been disposed.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="BlendCoordinator{T}"/> class with no concurrency limit.</summary>
    /// <param name="observer">The downstream observer to forward merged items to.</param>
    public BlendCoordinator(IObserverAsync<T> observer) => _observer = observer;

    /// <summary>Initializes a new instance of the <see cref="BlendCoordinator{T}"/> class that limits concurrent inner subscriptions.</summary>
    /// <param name="observer">The downstream observer to forward merged items to.</param>
    /// <param name="maxConcurrent">The maximum number of inner observable sequences to subscribe to concurrently.</param>
    public BlendCoordinator(IObserverAsync<T> observer, int maxConcurrent)
        : this(observer) => _semaphore = new(maxConcurrent, maxConcurrent);

    /// <summary>Gets a cancellation token that is canceled when this subscription is disposed.</summary>
    private CancellationToken DisposedCancellationToken => _disposeCts.Token;

    /// <summary>Asynchronously releases resources used by this subscription.</summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => FinishAsync(null);

    /// <summary>Subscribes to the outer observable and begins merging inner observable sequences.</summary>
    /// <param name="source">The outer observable whose inner sequences will be merged.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>A task representing the asynchronous subscribe operation.</returns>
    internal async ValueTask SubscribeSourcesAsync(
        IObservableAsync<IObservableAsync<T>> source,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        var outerSubscription = await source.SubscribeAsync(
            (x, _) => SubscribeBranchAsync(x),
            RelayErrorAsync,
            result =>
            {
                bool shouldComplete;
                lock (_disposeCts)
                {
                    _outerCompleted = true;
                    shouldComplete = _innerActiveCount == 0 || result.IsFailure;
                }

                return shouldComplete ? FinishAsync(result) : default;
            },
            DisposedCancellationToken).ConfigureAwait(false);

        await _outerDisposable.SetDisposableAsync(outerSubscription).ConfigureAwait(false);
    }

    /// <summary>Links the subscribe-time token into this subscription's dispose chain, surfacing it as <see cref="DisposedCancellationToken"/>.</summary>
    /// <param name="external">The subscribe-time token.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void LinkExternalCancellation(CancellationToken external) =>
        _externalLinkRegistration = ReactiveUI.Primitives.Async.Internal.AsyncStopTokenHelper.LinkExternal(
            _disposeCts,
            external);

    /// <summary>Re-checks the disposed flag inside the serialization gate and forwards the value downstream if the subscription is alive.</summary>
    /// <param name="value">The value to forward.</param>
    /// <returns>A task representing the asynchronous forward operation.</returns>
    internal ValueTask RelayNextIfActiveAsync(T value) => DisposalHelper.HasDisposed(_disposed)
        ? default
        : _observer.OnNextAsync(value, DisposedCancellationToken);

    /// <summary>Re-checks the disposed flag inside the serialization gate and forwards the error downstream if the subscription is alive.</summary>
    /// <param name="exception">The error to forward.</param>
    /// <returns>A task representing the asynchronous forward operation.</returns>
    internal ValueTask RelayErrorIfActiveAsync(Exception exception) => DisposalHelper.HasDisposed(_disposed)
        ? default
        : _observer.OnErrorResumeAsync(exception, DisposedCancellationToken);

    /// <summary>Forwards a value to the downstream observer under the serialization gate.</summary>
    /// <param name="value">The value to forward.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous forward operation.</returns>
    internal async ValueTask RelayNextAsync(T value, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        if (DisposalHelper.HasDisposed(_disposed))
        {
            return;
        }

        using (await _onSomethingGate.EnterAsync(DisposedCancellationToken).ConfigureAwait(false))
        {
            await RelayNextIfActiveAsync(value).ConfigureAwait(false);
        }
    }

    /// <summary>Forwards a non-terminal error to the downstream observer under the serialization gate.</summary>
    /// <param name="exception">The error to forward.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous forward operation.</returns>
    internal async ValueTask RelayErrorAsync(
        Exception exception,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        if (DisposalHelper.HasDisposed(_disposed))
        {
            return;
        }

        using (await _onSomethingGate.EnterAsync(DisposedCancellationToken).ConfigureAwait(false))
        {
            await RelayErrorIfActiveAsync(exception).ConfigureAwait(false);
        }
    }

    /// <summary>Subscribes to an inner observable sequence and begins forwarding its items, waiting for a slot first when concurrency is limited.</summary>
    /// <param name="inner">The inner observable to subscribe to.</param>
    /// <returns>A task representing the asynchronous subscribe operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask SubscribeBranchAsync(IObservableAsync<T> inner) =>
        _semaphore is null ? SubscribeUnboundedBranchAsync(inner) : SubscribeBoundedBranchAsync(inner, _semaphore);

    /// <summary>Creates a new inner witness for subscribing to an inner observable sequence.</summary>
    /// <returns>A new inner witness.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal BlendBranchWitness CreateBranchObserver() => new(this);

    /// <summary>Completes the merged sequence, disposes all subscriptions, and optionally signals the downstream observer.</summary>
    /// <param name="result">The completion result to forward, or null if disposing without signaling completion.</param>
    /// <returns>A task representing the asynchronous completion operation.</returns>
    internal async ValueTask FinishAsync(Result? result)
    {
        if (DisposalHelper.TrySetDisposed(ref _disposed))
        {
            return;
        }

        await _disposeCts.CancelAsync().ConfigureAwait(false);
        await _innerDisposables.DisposeAsync().ConfigureAwait(false);
        await _outerDisposable.DisposeAsync().ConfigureAwait(false);
        if (result is not null)
        {
            await _observer.OnCompletedAsync(result.Value).ConfigureAwait(false);
        }

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        await _externalLinkRegistration.DisposeAsync().ConfigureAwait(false);
#else
        _externalLinkRegistration.Dispose();
#endif
        _disposeCts.Dispose();
        _semaphore?.Dispose();
    }

    /// <summary>Subscribes an inner source with no concurrency limit, finishing the merge when subscription fails.</summary>
    /// <param name="inner">The inner observable to subscribe to.</param>
    /// <returns>A task representing the asynchronous subscribe operation.</returns>
    private async ValueTask SubscribeUnboundedBranchAsync(IObservableAsync<T> inner)
    {
        try
        {
            var innerObserver = CreateBranchObserver();
            await innerObserver.SubscribeSourcesAsync(inner, DisposedCancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            await FinishAsync(Result.Failure(e)).ConfigureAwait(false);
        }
    }

    /// <summary>Waits for a concurrency slot, then subscribes an inner source, releasing the slot when subscription fails.</summary>
    /// <param name="inner">The inner observable to subscribe to.</param>
    /// <param name="semaphore">The semaphore bounding concurrent inner subscriptions.</param>
    /// <returns>A task representing the asynchronous subscribe operation.</returns>
    private async ValueTask SubscribeBoundedBranchAsync(IObservableAsync<T> inner, SemaphoreSlim semaphore)
    {
        await semaphore.WaitAsync(DisposedCancellationToken).ConfigureAwait(false);
        var innerObserver = CreateBranchObserver();
        Exception failure;
        try
        {
            await innerObserver.SubscribeSourcesAsync(inner, DisposedCancellationToken).ConfigureAwait(false);
            return;
        }
        catch (Exception e)
        {
            failure = e;
        }

        await innerObserver.DisposeAsync().ConfigureAwait(false);
        await FinishAsync(Result.Failure(failure)).ConfigureAwait(false);
    }

    /// <summary>Witness that forwards items from an inner observable to the parent merge subscription.</summary>
    /// <param name="parent">The parent merge coordinator that receives forwarded notifications.</param>
    [System.Diagnostics.DebuggerDisplay("BlendBranchWitness: {_witness}")]
    internal sealed class BlendBranchWitness(BlendCoordinator<T> parent) : IWitnessAsync<T>
    {
        /// <summary>The notification gate, cancellation link and disposal state.</summary>
        private WitnessAsyncState _witness;

        /// <summary>Releases the parent's concurrency slot once for this witness.</summary>
        private int _released;

        /// <inheritdoc/>
        ref WitnessAsyncState IWitnessState.Witness => ref _witness;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
            WitnessAsync.OnNextAsync(this, value, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
            WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
            parent.RelayNextAsync(value, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            parent.RelayErrorAsync(error, cancellationToken);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
        {
            bool shouldComplete;
            lock (parent._disposeCts)
            {
                var count = --parent._innerActiveCount;
                shouldComplete = result.IsFailure || (count == 0 && parent._outerCompleted);
            }

            return shouldComplete ? parent.FinishAsync(result) : default;
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            ReleaseSlot();
            await parent._innerDisposables.Remove(this).ConfigureAwait(false);
            await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
        }

        /// <summary>Subscribes this witness to an inner observable sequence.</summary>
        /// <param name="inner">The inner observable to subscribe to.</param>
        /// <param name="cancellationToken">A token to cancel the subscription.</param>
        /// <returns>A task representing the asynchronous subscribe operation.</returns>
        internal async ValueTask SubscribeSourcesAsync(IObservableAsync<T> inner, CancellationToken cancellationToken)
        {
            lock (parent._disposeCts)
            {
                parent._innerActiveCount++;
            }

            await parent._innerDisposables.AddAsync(this).ConfigureAwait(false);
            var sub = await inner.SubscribeAsync(this, cancellationToken).ConfigureAwait(false);
            await this.AssignSourceSubscriptionAsync(sub).ConfigureAwait(false);
        }

        /// <summary>Releases the parent's concurrency slot once, when concurrency is limited and the parent is still active.</summary>
        private void ReleaseSlot()
        {
            if (parent._semaphore is null
                || Interlocked.Exchange(ref _released, 1) != 0
                || DisposalHelper.HasDisposed(parent._disposed))
            {
                return;
            }

            _ = parent._semaphore.Release();
        }
    }
}
