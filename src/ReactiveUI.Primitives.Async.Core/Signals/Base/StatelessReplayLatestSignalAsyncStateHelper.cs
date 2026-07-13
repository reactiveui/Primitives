// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Static operations for stateless replay-latest async signals.</summary>
internal static class StatelessReplayLatestSignalAsyncStateHelper
{
    /// <summary>Asynchronously notifies subscribed observers of a new value.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <param name="kind">The broadcast mode for observer notifications.</param>
    /// <param name="value">The value to publish.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    /// <returns>A task that represents the asynchronous notification.</returns>
    public static async ValueTask OnNextAsync<T>(
        StatelessReplayLatestSignalAsyncState<T> state,
        SignalBroadcastKind kind,
        T value,
        CancellationToken cancellationToken)
    {
        var token = GetOperationCancellationToken(state, cancellationToken, out var linkedCts);
        try
        {
            ImmutableArray<IObserverAsync<T>> observers;
            using (await state.Gate.EnterAsync(token).ConfigureAwait(false))
            {
                state.Value = new(value);
                observers = state.Observers;
            }

            await SignalAsyncStateHelper.BroadcastOnNextAsync(kind, observers, value, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            linkedCts?.Dispose();
        }
    }

    /// <summary>Notifies subscribed observers of a recoverable error.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <param name="kind">The broadcast mode for observer notifications.</param>
    /// <param name="error">The recoverable error to publish.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    /// <returns>A task that represents the asynchronous notification.</returns>
    public static async ValueTask OnErrorResumeAsync<T>(
        StatelessReplayLatestSignalAsyncState<T> state,
        SignalBroadcastKind kind,
        Exception error,
        CancellationToken cancellationToken)
    {
        var token = GetOperationCancellationToken(state, cancellationToken, out var linkedCts);
        try
        {
            ImmutableArray<IObserverAsync<T>> observers;
            using (await state.Gate.EnterAsync(token).ConfigureAwait(false))
            {
                observers = state.Observers;
            }

            await SignalAsyncStateHelper.BroadcastOnErrorResumeAsync(kind, observers, error, token)
                .ConfigureAwait(false);
        }
        finally
        {
            linkedCts?.Dispose();
        }
    }

    /// <summary>Completes subscribed observers and resets state.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <param name="kind">The broadcast mode for observer notifications.</param>
    /// <param name="result">The completion result to publish.</param>
    /// <returns>A task that represents the asynchronous notification.</returns>
    public static async ValueTask OnCompletedAsync<T>(
        StatelessReplayLatestSignalAsyncState<T> state,
        SignalBroadcastKind kind,
        Result result)
    {
        ImmutableArray<IObserverAsync<T>> observers;
        using (await state.Gate.EnterAsync(state.DisposedCts.Token).ConfigureAwait(false))
        {
            observers = state.Observers;
            state.Observers = [];
            state.Value = state.InitialValue;
        }

        await SignalAsyncStateHelper.BroadcastOnCompletedAsync(kind, observers, result).ConfigureAwait(false);
    }

    /// <summary>Releases resources used by the signal.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <returns>A task that represents the asynchronous disposal operation.</returns>
    public static async ValueTask DisposeAsync<T>(StatelessReplayLatestSignalAsyncState<T> state)
    {
        if (state.IsDisposed)
        {
            return;
        }

        state.IsDisposed = true;
        await state.DisposedCts.CancelAsync().ConfigureAwait(false);
        state.Dispose();
    }

    /// <summary>Subscribes an observer to a signal.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    /// <returns>The subscription handle for the observer.</returns>
    public static async ValueTask<IAsyncDisposable> SubscribeAsync<T>(
        StatelessReplayLatestSignalAsyncState<T> state,
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var token = GetOperationCancellationToken(state, cancellationToken, out var linkedCts);
        try
        {
            token.ThrowIfCancellationRequested();

            using (await state.Gate.EnterAsync(token).ConfigureAwait(false))
            {
                state.Observers = state.Observers.Add(observer);
                if (state.Value.TryGetValue(out var value))
                {
                    await observer.OnNextAsync(value, token).ConfigureAwait(false);
                }
            }

            return new StatelessReplayLatestSignalAsyncStateObserverLease<T>(state, observer);
        }
        finally
        {
            linkedCts?.Dispose();
        }
    }

    /// <summary>Gets the cancellation token used for a gate-protected operation.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="state">The mutable signal state.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    /// <param name="linkedCts">Receives the linked token source created for the operation, if one is needed.</param>
    /// <returns>The token that should guard the operation.</returns>
    internal static CancellationToken GetOperationCancellationToken<T>(
        StatelessReplayLatestSignalAsyncState<T> state,
        CancellationToken cancellationToken,
        out CancellationTokenSource? linkedCts)
    {
        var disposedToken = state.DisposedCts.Token;
        if (cancellationToken == disposedToken || !cancellationToken.CanBeCanceled)
        {
            linkedCts = null;
            return disposedToken;
        }

        linkedCts = CancellationTokenSource.CreateLinkedTokenSource(disposedToken, cancellationToken);
        return linkedCts.Token;
    }
}
