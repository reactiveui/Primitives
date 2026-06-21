// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Static operations for completing async signals.</summary>
internal static class SignalAsyncStateHelper
{
    /// <summary>Gets an observable sequence that represents the values published by the signal.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="signal">The signal instance.</param>
    /// <returns>The signal as an observable sequence.</returns>
    public static IObservableAsync<T> Values<T>(ISignalAsync<T> signal) => signal;

    /// <summary>Asynchronously notifies subscribed observers of a new value.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <param name="kind">The broadcast mode for observer notifications.</param>
    /// <param name="value">The value to publish.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    /// <returns>A task that represents the asynchronous notification.</returns>
    public static ValueTask OnNextAsync<T>(
        SignalAsyncState<T> state,
        SignalBroadcastKind kind,
        T value,
        CancellationToken cancellationToken) =>
        !state.TryGetObservers(out var observers)
            ? default
            : BroadcastOnNextAsync(kind, observers, value, cancellationToken);

    /// <summary>Notifies subscribed observers of a recoverable error.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <param name="kind">The broadcast mode for observer notifications.</param>
    /// <param name="error">The recoverable error to publish.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    /// <returns>A task that represents the asynchronous notification.</returns>
    public static ValueTask OnErrorResumeAsync<T>(
        SignalAsyncState<T> state,
        SignalBroadcastKind kind,
        Exception error,
        CancellationToken cancellationToken) =>
        !state.TryGetObservers(out var observers) ? default : BroadcastOnErrorResumeAsync(kind, observers, error, cancellationToken);

    /// <summary>Completes the signal and notifies subscribed observers.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <param name="kind">The broadcast mode for observer notifications.</param>
    /// <param name="result">The completion result to publish.</param>
    /// <returns>A task that represents the asynchronous notification.</returns>
    public static ValueTask OnCompletedAsync<T>(
        SignalAsyncState<T> state,
        SignalBroadcastKind kind,
        Result result) =>
        !state.TryComplete(result, out var observers) ? default : BroadcastOnCompletedAsync(kind, observers, result);

    /// <summary>Releases resources used by the signal.</summary>
    /// <returns>A completed task.</returns>
    public static ValueTask DisposeAsync() => default;

    /// <summary>Subscribes an observer to a signal.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    /// <returns>The subscription handle for the observer.</returns>
    public static async ValueTask<IAsyncDisposable> SubscribeAsync<T>(
        SignalAsyncState<T> state,
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var result = state.Subscribe(observer);

        if (result is not null)
        {
            await observer.OnCompletedAsync(result.Value).ConfigureAwait(false);
            return DisposableAsync.Empty;
        }

        return new SignalAsyncStateObserverLease<T>(state, observer);
    }

    /// <summary>Forwards a value to observers according to <paramref name="kind"/>.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="kind">The broadcast mode for observer notifications.</param>
    /// <param name="observers">The observers to notify.</param>
    /// <param name="value">The value to publish.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    /// <returns>A task that represents the asynchronous notification.</returns>
    public static ValueTask BroadcastOnNextAsync<T>(
        SignalBroadcastKind kind,
        ImmutableArray<IObserverAsync<T>> observers,
        T value,
        CancellationToken cancellationToken) =>
        kind switch
        {
            SignalBroadcastKind.Serial => SerialBroadcastHelpers.BroadcastOnNextAsync(observers, value, cancellationToken),
            SignalBroadcastKind.SerialMulti => SerialBroadcastHelpers.BroadcastOnNextAsyncMulti(observers, value, cancellationToken),
            _ => Concurrent.ForwardOnNextConcurrently(observers, value, cancellationToken),
        };

    /// <summary>Forwards a recoverable error to observers according to <paramref name="kind"/>.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="kind">The broadcast mode for observer notifications.</param>
    /// <param name="observers">The observers to notify.</param>
    /// <param name="error">The recoverable error to publish.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    /// <returns>A task that represents the asynchronous notification.</returns>
    public static ValueTask BroadcastOnErrorResumeAsync<T>(
        SignalBroadcastKind kind,
        ImmutableArray<IObserverAsync<T>> observers,
        Exception error,
        CancellationToken cancellationToken) =>
        kind == SignalBroadcastKind.Concurrent
            ? Concurrent.ForwardOnErrorResumeConcurrently(observers, error, cancellationToken)
            : SerialBroadcastHelpers.BroadcastOnErrorResumeAsync(observers, error, cancellationToken);

    /// <summary>Forwards completion to observers according to <paramref name="kind"/>.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="kind">The broadcast mode for observer notifications.</param>
    /// <param name="observers">The observers to notify.</param>
    /// <param name="result">The completion result to publish.</param>
    /// <returns>A task that represents the asynchronous notification.</returns>
    public static ValueTask BroadcastOnCompletedAsync<T>(
        SignalBroadcastKind kind,
        ImmutableArray<IObserverAsync<T>> observers,
        Result result) =>
        kind == SignalBroadcastKind.Concurrent
            ? Concurrent.ForwardOnCompletedConcurrently(observers, result)
            : SerialBroadcastHelpers.BroadcastOnCompletedAsync(observers, result);
}
