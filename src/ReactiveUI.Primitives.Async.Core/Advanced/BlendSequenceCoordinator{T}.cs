// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Manages subscriptions to all sources in the enumerable and forwards their items to a single observer.</summary>
/// <typeparam name="T">The type of the elements emitted by the observable sequences.</typeparam>
[System.Diagnostics.DebuggerDisplay("BlendSequenceCoordinator<T>: Sources = {_sources}")]
public sealed class BlendSequenceCoordinator<T> : IAsyncDisposable
{
    /// <summary>The collection of source observables to merge.</summary>
    private readonly IEnumerable<IObservableAsync<T>> _sources;

    /// <summary>Tracks all inner subscriptions for disposal.</summary>
    private readonly MultipleDisposableAsync _innerDisposables = new();

    /// <summary>Cancellation source for disposal.</summary>
    private readonly CancellationTokenSource _cts = new();

    /// <summary>Token signalled when this subscription is disposed.</summary>
    private readonly CancellationToken _disposedCancellationToken;

    /// <summary>Serializes observer notifications to prevent concurrent calls.</summary>
    private readonly ReactiveUI.Primitives.Async.Internal.AsyncFlowGate _onSomethingGate = new();

    /// <summary>Signals when the initial subscription loop has finished.</summary>
    private readonly TaskCompletionSource<bool> _subscriptionFinished =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Marks the subscribing loop's async flow so reentrant <see cref="FinishAsync"/> calls do not deadlock.</summary>
    private readonly AsyncLocal<bool> _reentrant = new();

    /// <summary>The downstream observer.</summary>
    private readonly IObserverAsync<T> _observer;

    /// <summary>Registration that propagates the original subscribe-token cancellation into <see cref="_cts"/>.</summary>
    private CancellationTokenRegistration _externalLinkRegistration;

    /// <summary>The number of currently active inner subscriptions.</summary>
    private int _active;

    /// <summary>Whether this subscription has been disposed.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="BlendSequenceCoordinator{T}"/> class.</summary>
    /// <param name="observer">The downstream observer to forward merged items to.</param>
    /// <param name="sources">The enumerable of observable sources to merge.</param>
    public BlendSequenceCoordinator(IObserverAsync<T> observer, IEnumerable<IObservableAsync<T>> sources)
    {
        _observer = observer;
        _sources = sources;
        _disposedCancellationToken = _cts.Token;
    }

    /// <summary>Asynchronously releases resources used by this subscription.</summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => FinishAsync(null);

    /// <summary>Routes the exception of a completion result that arrives after disposal to the unhandled exception handler.</summary>
    /// <param name="result">The completion result, or null if disposing without signaling.</param>
    internal static void RoutePostDisposalException(Result? result)
    {
        _ = typeof(T);
        if (result?.Exception is not { } ex)
        {
            return;
        }

        UnhandledExceptionHandler.ReportUnhandledException(ex);
    }

    /// <summary>Begins subscribing to all source observables asynchronously.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void BeginSubscribing() => FireAndForgetHelper.Run(async () =>
    {
        _reentrant.Value = true;
        try
        {
            _ = Interlocked.Increment(ref _active);

            foreach (var src in _sources)
            {
                _ = Interlocked.Increment(ref _active);

                BlendBranchWitness innerObserver = new(this);
                await _innerDisposables.AddAsync(innerObserver).ConfigureAwait(false);
                try
                {
                    var sub = await src.SubscribeAsync(innerObserver, _disposedCancellationToken).ConfigureAwait(false);
                    await innerObserver.AssignSourceSubscriptionAsync(sub).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    await FinishAsync(Result.Failure(ex)).ConfigureAwait(false);
                    return;
                }
            }

            if (Interlocked.Decrement(ref _active) == 0)
            {
                await FinishAsync(Result.Success).ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            await FinishAsync(Result.Failure(e)).ConfigureAwait(false);
        }
        finally
        {
            _subscriptionFinished.SetResult(true);
        }
    });

    /// <summary>Links the subscribe-time token into this subscription's dispose chain, surfacing it as <see cref="_disposedCancellationToken"/>.</summary>
    /// <param name="external">The subscribe-time token.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void LinkExternalCancellation(CancellationToken external) =>
        _externalLinkRegistration = ReactiveUI.Primitives.Async.Internal.AsyncStopTokenHelper.LinkExternal(
            _cts,
            external);

    /// <summary>Forwards a value to the downstream observer under the serialization gate.</summary>
    /// <param name="value">The value to forward.</param>
    /// <param name="token">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous forward operation.</returns>
    internal async ValueTask RelayNextAsync(T value, CancellationToken token)
    {
        _ = token;
        if (DisposalHelper.HasDisposed(_disposed))
        {
            return;
        }

        using (await _onSomethingGate.EnterAsync(_disposedCancellationToken).ConfigureAwait(false))
        {
            await RelayNextIfActiveAsync(value).ConfigureAwait(false);
        }
    }

    /// <summary>Re-checks the disposed flag inside the serialization gate and forwards the value downstream if the subscription is alive.</summary>
    /// <param name="value">The value to forward.</param>
    /// <returns>A task representing the asynchronous forward operation.</returns>
    internal ValueTask RelayNextIfActiveAsync(T value) => DisposalHelper.HasDisposed(_disposed)
        ? default
        : _observer.OnNextAsync(value, _disposedCancellationToken);

    /// <summary>Forwards a non-terminal error to the downstream observer under the serialization gate.</summary>
    /// <param name="ex">The error to forward.</param>
    /// <param name="token">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous forward operation.</returns>
    internal async ValueTask RelayErrorAsync(Exception ex, CancellationToken token)
    {
        _ = token;
        if (DisposalHelper.HasDisposed(_disposed))
        {
            return;
        }

        using (await _onSomethingGate.EnterAsync(_disposedCancellationToken).ConfigureAwait(false))
        {
            await RelayErrorIfActiveAsync(ex).ConfigureAwait(false);
        }
    }

    /// <summary>Re-checks the disposed flag inside the serialization gate and forwards the error downstream if the subscription is alive.</summary>
    /// <param name="ex">The error to forward.</param>
    /// <returns>A task representing the asynchronous forward operation.</returns>
    internal ValueTask RelayErrorIfActiveAsync(Exception ex) => DisposalHelper.HasDisposed(_disposed)
        ? default
        : _observer.OnErrorResumeAsync(ex, _disposedCancellationToken);

    /// <summary>Handles completion from an inner source, triggering final completion when all sources are done.</summary>
    /// <param name="result">The completion result from the inner source.</param>
    /// <returns>A task representing the asynchronous completion operation.</returns>
    internal ValueTask AcceptBranchCompletionAsync(Result result)
    {
        if (result.IsFailure)
        {
            return FinishAsync(result);
        }

        return Interlocked.Decrement(ref _active) != 0 ? default : FinishAsync(Result.Success);
    }

    /// <summary>Completes the merged sequence, disposes all subscriptions, and optionally signals the downstream observer.</summary>
    /// <param name="result">The completion result to forward, or <see langword="null"/> if disposing without signaling completion.</param>
    /// <returns>A task representing the asynchronous completion operation.</returns>
    internal async ValueTask FinishAsync(Result? result)
    {
        if (DisposalHelper.TrySetDisposed(ref _disposed))
        {
            RoutePostDisposalException(result);
            return;
        }

        await _cts.CancelAsync().ConfigureAwait(false);
        await _innerDisposables.DisposeAsync().ConfigureAwait(false);
        if (!_reentrant.Value)
        {
            await _subscriptionFinished.Task.ConfigureAwait(false);
        }

        if (result is not null)
        {
            await _observer.OnCompletedAsync(result.Value).ConfigureAwait(false);
        }

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        await _externalLinkRegistration.DisposeAsync().ConfigureAwait(false);
#else
        _externalLinkRegistration.Dispose();
#endif
        _cts.Dispose();
    }

    /// <summary>Witness that forwards items from an inner source to the parent enumerable merge subscription.</summary>
    /// <param name="parent">The parent enumerable merge coordinator that receives forwarded notifications.</param>
    [System.Diagnostics.DebuggerDisplay("BlendBranchWitness: {_witness}")]
    internal sealed class BlendBranchWitness(BlendSequenceCoordinator<T> parent) : IWitnessAsync<T>
    {
        /// <summary>The notification gate, cancellation link and disposal state.</summary>
        private WitnessAsyncState _witness;

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
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(
            Exception error,
            CancellationToken cancellationToken) =>
            parent.RelayErrorAsync(error, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
            parent.AcceptBranchCompletionAsync(result);

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            await parent._innerDisposables.Remove(this).ConfigureAwait(false);
            await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
        }
    }
}
