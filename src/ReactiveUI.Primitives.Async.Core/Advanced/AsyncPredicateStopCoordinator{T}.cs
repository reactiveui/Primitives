// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Internal;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Forwards source items until the async predicate returns true.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="parent">The owning signal.</param>
/// <param name="observer">The downstream observer.</param>
[System.Diagnostics.DebuggerDisplay("AsyncPredicateStopCoordinator<T>: {_witness}")]
public sealed class AsyncPredicateStopCoordinator<T>(
    AsyncPredicateStopSignal<T> parent,
    IObserverAsync<T> observer) : IWitnessAsync<T>
{
    /// <summary>The inner subscription handle.</summary>
    private IAsyncDisposable? _subscription;

    /// <summary>The registration that completes this sequence when the stop token fires.</summary>
    private CancellationTokenRegistration _stopRegistration;

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
    public async ValueTask DisposeAsync()
    {
        await AsyncStopTokenHelper.DisposeStopRegistrationAsync(_stopRegistration).ConfigureAwait(false);
        await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
        await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
    }

    /// <summary>Subscribes to the source observable.</summary>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>A task representing the asynchronous subscribe operation.</returns>
    internal async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
    {
        _stopRegistration = AsyncStopTokenHelper.WatchStopToken(
            this,
            static state => FireAndForgetHelper.Run(((AsyncPredicateStopCoordinator<T>)state!).CompleteAfterYieldAsync),
            parent.StopToken);
        var sub = await parent.Source.SubscribeAsync(this, cancellationToken).ConfigureAwait(false);
        await DisposableAsyncSlot.AssignAsync(ref _subscription, sub).ConfigureAwait(false);
        if (AsyncStopTokenHelper.ShouldReleaseAfterSubscribe(this, parent.StopToken))
        {
            await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    async ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        var matched = await parent.AsyncPredicate(value, cancellationToken).ConfigureAwait(false);
        await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);

        if (!matched)
        {
            return;
        }

        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
        await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
        await WitnessAsync.DisposeFromNotificationAsync(this).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        observer.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) => observer.OnCompletedAsync(result);

    /// <summary>Completes downstream and disposes the source subscription after yielding from the stop-token callback.</summary>
    /// <returns>A task representing the asynchronous completion.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private async ValueTask CompleteAfterYieldAsync()
    {
        await Task.Yield();
        await OnCompletedAsync(Result.Success).ConfigureAwait(false);
        await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
    }
}
