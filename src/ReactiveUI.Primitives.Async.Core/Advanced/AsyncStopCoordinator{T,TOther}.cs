// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Manages subscriptions to both the source and the signal observable, completing when the signal fires.</summary>
/// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
/// <typeparam name="TOther">The type of the elements in the signal sequence.</typeparam>
[System.Diagnostics.DebuggerDisplay("AsyncStopCoordinator<T, TOther>: Parent = {_parent}")]
public sealed class AsyncStopCoordinator<T, TOther> : IAsyncDisposable
{
    /// <summary>The parent observable that owns this subscription.</summary>
    private readonly TakeUntilAsyncSignal<T, TOther> _parent;

    /// <summary>Shared subscription lifecycle (gate / dispose CTS / external link / forwarders).</summary>
    private readonly TakeUntilLifecycle<T> _lifecycle;

    /// <summary>Holds the source subscription so it can be disposed on teardown.</summary>
    private readonly SingleAssignmentDisposableAsync _disposable = new();

    /// <summary>Holds the signal subscription so it can be disposed on teardown.</summary>
    private readonly SingleAssignmentDisposableAsync _otherDisposable = new();

    /// <summary>The registration that completes this sequence when the stop token fires.</summary>
    private CancellationTokenRegistration _stopTokenRegistration;

    /// <summary>Initializes a new instance of the <see cref="AsyncStopCoordinator{T, TOther}"/> class.</summary>
    /// <param name="parent">The parent observable that owns this subscription.</param>
    /// <param name="observer">The downstream observer to forward items to.</param>
    public AsyncStopCoordinator(TakeUntilAsyncSignal<T, TOther> parent, IObserverAsync<T> observer)
    {
        _parent = parent;
        _lifecycle = new(observer);
    }

    /// <summary>Asynchronously releases resources used by this subscription.</summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        await _stopTokenRegistration.DisposeAsync().ConfigureAwait(false);
#else
        _stopTokenRegistration.Dispose();
#endif
        await _otherDisposable.DisposeAsync().ConfigureAwait(false);
        await _disposable.DisposeAsync().ConfigureAwait(false);
        await _lifecycle.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Subscribes to both the source and signal observables.</summary>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>This subscription as an async disposable.</returns>
    internal async ValueTask<IAsyncDisposable> SubscribeSourcesAsync(CancellationToken cancellationToken)
    {
        _stopTokenRegistration = _lifecycle.CompleteWhenCancelled(
            async () =>
            {
                await _disposable.DisposeAsync().ConfigureAwait(false);
                await _otherDisposable.DisposeAsync().ConfigureAwait(false);
            },
            _parent.StopToken);

        var otherSubscription = await _parent.Other
            .SubscribeAsync(new StopSignalWitness(this), cancellationToken).ConfigureAwait(false);
        await _otherDisposable.SetDisposableAsync(otherSubscription).ConfigureAwait(false);

        var sourceSubscription =
            await _parent.Source.SubscribeAsync(new TakeUntilSourceWitness<T>(_lifecycle), cancellationToken)
                .ConfigureAwait(false);
        await _disposable.SetDisposableAsync(sourceSubscription).ConfigureAwait(false);
        if (_lifecycle.IsCompleted || _parent.StopToken.IsCancellationRequested)
        {
            await _disposable.DisposeAsync().ConfigureAwait(false);
            await _otherDisposable.DisposeAsync().ConfigureAwait(false);
        }

        return this;
    }

    /// <summary>Forwards the original subscribe-time token into the shared lifecycle's dispose chain.</summary>
    /// <param name="external">The subscribe-time token.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void LinkExternalCancellation(CancellationToken external) =>
        _lifecycle.LinkExternalCancellation(external);

    /// <summary>Observer for the signal observable that triggers completion of the source subscription.</summary>
    /// <param name="parent">The parent coordinator that owns this witness.</param>
    [System.Diagnostics.DebuggerDisplay("StopSignalWitness: {_witness}")]
    private sealed class StopSignalWitness(AsyncStopCoordinator<T, TOther> parent) : IWitnessAsync<TOther>
    {
        /// <summary>The notification gate, cancellation link and disposal state.</summary>
        private WitnessAsyncState _witness;

        /// <inheritdoc/>
        ref WitnessAsyncState IWitnessState.Witness => ref _witness;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(TOther value, CancellationToken cancellationToken) =>
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
        public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

        /// <inheritdoc/>
        async ValueTask IWitnessAsync<TOther>.OnNextAsyncCore(TOther value, CancellationToken cancellationToken)
        {
            _ = value;
            _ = cancellationToken;
            await parent._disposable.DisposeAsync().ConfigureAwait(false);
            await parent._otherDisposable.DisposeFromNotificationAsync().ConfigureAwait(false);
            await parent._lifecycle.RelayCompletionAsync(Result.Success).ConfigureAwait(false);
            await WitnessAsync.DisposeFromNotificationAsync(this).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        ValueTask IWitnessAsync<TOther>.OnErrorResumeAsyncCore(
            Exception error,
            CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return parent._lifecycle.RelayErrorAsync(error);
        }

        /// <inheritdoc/>
        async ValueTask IWitnessAsync<TOther>.OnCompletedAsyncCore(Result result)
        {
            if (!result.IsFailure)
            {
                return;
            }

            await parent._disposable.DisposeAsync().ConfigureAwait(false);
            await parent._otherDisposable.DisposeFromNotificationAsync().ConfigureAwait(false);
            await parent._lifecycle.RelayCompletionAsync(
                parent._parent.Options.SourceFailsWhenOtherFails
                    ? result
                    : Result.Success).ConfigureAwait(false);
        }
    }
}
