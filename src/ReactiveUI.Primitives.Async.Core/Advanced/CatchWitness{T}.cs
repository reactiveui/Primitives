// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Forwards values, routes error-resume to the callback or downstream, and swaps in the fallback on failure.</summary>
/// <typeparam name="T">The element type of the source sequence.</typeparam>
/// <param name="downstream">The downstream witness.</param>
/// <param name="handler">The fallback factory.</param>
/// <param name="onErrorResume">Optional async error-resume callback.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain and reused for the handler
/// subscription.</param>
[System.Diagnostics.DebuggerDisplay("CatchWitness<T>: {_witness}")]
public sealed class CatchWitness<T>(
    IObserverAsync<T> downstream,
    Func<Exception, IObservableAsync<T>> handler,
    Func<Exception, CancellationToken, ValueTask>? onErrorResume,
    CancellationToken subscribeToken) : IWitnessAsync<T>
{
    /// <summary>Holds the handler-produced subscription, assigned at most once, so it disposes with the sink.</summary>
    private readonly SingleAssignmentDisposableAsync _handlerDisposable = new();

    /// <summary>The subscribe-time token, reused when subscribing the fallback handler observable.</summary>
    private readonly CancellationToken _subscribeToken = subscribeToken;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness = new(subscribeToken);

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
        downstream.OnNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        onErrorResume is null
            ? downstream.OnErrorResumeAsync(error, cancellationToken)
            : onErrorResume(error, cancellationToken);

    /// <inheritdoc/>
    async ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
    {
        if (result.IsSuccess)
        {
            await downstream.OnCompletedAsync(result).ConfigureAwait(false);
            return;
        }

        try
        {
            var handlerObservable = handler(result.Exception);
            var handlerSubscription =
                await handlerObservable.SubscribeAsync(downstream.Wrap(), _subscribeToken)
                    .ConfigureAwait(false);
            await _handlerDisposable.SetDisposableAsync(handlerSubscription).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            await downstream.OnCompletedAsync(Result.Failure(e)).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _handlerDisposable.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }

        await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
    }
}
