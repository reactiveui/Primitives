// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Async.Advanced;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides connectable-signal operations over flat state records.</summary>
internal static class ConnectableSignalAsyncHelper
{
    /// <summary>Connects the state source once and returns a handle that can disconnect that connection.</summary>
    /// <typeparam name="T">The type of elements produced by the source sequence.</typeparam>
    /// <param name="state">The connectable signal state to operate on.</param>
    /// <param name="cancellationToken">A token that can cancel connection establishment.</param>
    /// <returns>The active connection handle.</returns>
    public static async ValueTask<IAsyncDisposable> ConnectAsync<T>(
        ConnectableSignalAsyncState<T> state,
        CancellationToken cancellationToken)
    {
        CancellationTokenSource? linkedCts = null;
        CancellationToken token;
        if (cancellationToken == state.DisposedCancellationToken || !cancellationToken.CanBeCanceled)
        {
            token = state.DisposedCancellationToken;
        }
        else
        {
            linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                state.DisposedCancellationToken,
                cancellationToken);
            token = linkedCts.Token;
        }

        try
        {
            using (await state.Gate.EnterAsync(token).ConfigureAwait(false))
            {
                if (state.Connection is not null)
                {
                    return state.Connection;
                }

                SingleAssignmentDisposableAsync? connection = new();
                state.Connection = connection;
                var subscription = await state.Source.SubscribeAsync(
                    state.Signal.AsObserverAsync(),
                    token).ConfigureAwait(false);
                await connection.SetDisposableAsync(subscription).ConfigureAwait(false);

                return DisposableAsync.Create(
                    (state, connection),
                    static async s =>
                    {
                        using (await s.state.Gate.EnterAsync(s.state.DisposedCancellationToken).ConfigureAwait(false))
                        {
                            if (!ReferenceEquals(s.state.Connection, s.connection))
                            {
                                return;
                            }

                            s.state.Connection = null;
                            await s.connection.DisposeAsync().ConfigureAwait(false);
                        }
                    });
            }
        }
        finally
        {
            linkedCts?.Dispose();
        }
    }

    /// <summary>Disposes the connection state and releases its gate resources.</summary>
    /// <typeparam name="T">The type of elements produced by the source sequence.</typeparam>
    /// <param name="state">The connectable signal state to dispose.</param>
    [SuppressMessage(
        "Major Bug",
        "S4462:Calls to async methods should not be blocking",
        Justification =
            "IDisposable.Dispose is intrinsically synchronous; this method must tear down async connection state on the sync dispose path.")]
    public static void Dispose<T>(ConnectableSignalAsyncState<T> state)
    {
        if (!state.TryMarkDisposed())
        {
            return;
        }

        state.DisposedCts.Cancel();
        state.Connection?.DisposeAsync().AsTask().Wait();
        state.Dispose();
    }

    /// <summary>Subscribes an observer to the current signal values without connecting the source.</summary>
    /// <typeparam name="T">The type of elements produced by the source sequence.</typeparam>
    /// <param name="state">The connectable signal state to observe.</param>
    /// <param name="observer">The observer that receives multicasted values.</param>
    /// <param name="cancellationToken">A token that can cancel subscription establishment.</param>
    /// <returns>The subscription to the signal values.</returns>
    public static ValueTask<IAsyncDisposable> SubscribeAsync<T>(
        ConnectableSignalAsyncState<T> state,
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        RelayWitnessAsync<T> wrap = new(observer);
        if (observer is WitnessAsync<T> downstream)
        {
            downstream.LinkUpstreamCancellation(wrap.InternalDisposedToken);
        }

        return state.Signal.Values.SubscribeAsync(wrap, cancellationToken);
    }
}
