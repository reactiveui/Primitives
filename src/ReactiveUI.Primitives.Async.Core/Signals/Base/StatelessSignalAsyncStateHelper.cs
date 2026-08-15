// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Static operations for stateless async signals.</summary>
internal static class StatelessSignalAsyncStateHelper
{
    /// <summary>Asynchronously notifies subscribed observers of a new value.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <param name="kind">The broadcast mode for observer notifications.</param>
    /// <param name="value">The value to publish.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    /// <returns>A task that represents the asynchronous notification.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ValueTask OnNextAsync<T>(
        StatelessSignalAsyncState<T> state,
        SignalBroadcastKind kind,
        T value,
        CancellationToken cancellationToken) =>
        SignalAsyncStateHelper.BroadcastOnNextAsync(kind, state.Snapshot(), value, cancellationToken);

    /// <summary>Notifies subscribed observers of a recoverable error.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <param name="kind">The broadcast mode for observer notifications.</param>
    /// <param name="error">The recoverable error to publish.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    /// <returns>A task that represents the asynchronous notification.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ValueTask OnErrorResumeAsync<T>(
        StatelessSignalAsyncState<T> state,
        SignalBroadcastKind kind,
        Exception error,
        CancellationToken cancellationToken) =>
        SignalAsyncStateHelper.BroadcastOnErrorResumeAsync(kind, state.Snapshot(), error, cancellationToken);

    /// <summary>Completes subscribed observers.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <param name="kind">The broadcast mode for observer notifications.</param>
    /// <param name="result">The completion result to publish.</param>
    /// <returns>A task that represents the asynchronous notification.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ValueTask OnCompletedAsync<T>(
        StatelessSignalAsyncState<T> state,
        SignalBroadcastKind kind,
        Result result) =>
        SignalAsyncStateHelper.BroadcastOnCompletedAsync(kind, state.Snapshot(), result);

    /// <summary>Clears observers from the signal.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <returns>A task that represents the asynchronous disposal operation.</returns>
    internal static ValueTask DisposeAsync<T>(StatelessSignalAsyncState<T> state)
    {
        state.Clear();
        return default;
    }

    /// <summary>Subscribes an observer to a signal.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    /// <returns>The subscription handle for the observer.</returns>
    internal static ValueTask<IAsyncDisposable> SubscribeAsync<T>(
        StatelessSignalAsyncState<T> state,
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var disposable = DisposableAsync.Create(
            (state, observer),
            static tuple =>
            {
                tuple.state.Remove(tuple.observer);
                return default;
            });

        state.Add(observer);

        return new(disposable);
    }
}
