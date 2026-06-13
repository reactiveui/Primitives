// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>A connectable observable that multicasts notifications from a source observable through a signal.</summary>
/// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
/// <param name="source">The source signal to multicast.</param>
/// <param name="signal">The signal used to broadcast notifications to multiple observers.</param>
internal sealed class MulticastSignalAsync<T>(IObservableAsync<T> source, ISignalAsync<T> signal)
    : ConnectableSignalAsync<T>, IDisposable
{
    /// <summary>The cancellation token source that is cancelled when this instance is disposed.</summary>
    private readonly CancellationTokenSource _disposedCts = new();

    /// <summary>The asynchronous gate used to synchronize connection and disconnection operations.</summary>
    private readonly AsyncSerialGate _gate = new();

    /// <summary>The current connection subscription, or <see langword="null"/> if not connected.</summary>
    private SingleAssignmentDisposableAsync? _connection;

    /// <summary>A value indicating whether this instance has been disposed.</summary>
    private int _isDisposed;

    /// <summary>Gets the cancellation token that is cancelled when this instance is disposed.</summary>
    private CancellationToken DisposedCancellationToken => _disposedCts.Token;

    /// <summary>
    /// Asynchronously establishes a connection to the underlying observable sequence and returns a disposable handle
    /// for managing the connection's lifetime.
    /// </summary>
    /// <remarks>If a connection is already established, the existing connection handle is returned. Disposing
    /// the returned handle will disconnect the subscription. This method is thread-safe.</remarks>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous connection operation.</param>
    /// <returns>A value task that represents the asynchronous operation. The result contains an <see cref="IAsyncDisposable"/>
    /// that disconnects the subscription when disposed.</returns>
    public override async ValueTask<IAsyncDisposable> ConnectAsync(CancellationToken cancellationToken)
    {
        // Fast path: when the caller passes our own dispose token (or no token at all), there is nothing to
        // combine — use DisposedCancellationToken directly and skip the per-call linked CTS allocation.
        // Slow path keeps the linked CTS scoped to this call.
        CancellationTokenSource? linkedCts = null;
        CancellationToken token;
        if (cancellationToken == DisposedCancellationToken || !cancellationToken.CanBeCanceled)
        {
            token = DisposedCancellationToken;
        }
        else
        {
            linkedCts = CancellationTokenSource.CreateLinkedTokenSource(DisposedCancellationToken, cancellationToken);
            token = linkedCts.Token;
        }

        try
        {
            using (await _gate.EnterAsync(token).ConfigureAwait(false))
            {
                if (_connection is not null)
                {
                    return _connection;
                }

                SingleAssignmentDisposableAsync? connection = new();
                _connection = connection;
                await connection.SetDisposableAsync(await source.SubscribeAsync(
                    signal.AsObserverAsync(),
                    token).ConfigureAwait(false)).ConfigureAwait(false);
                return DisposableAsync.Create(async () =>
                {
                    using (await _gate.EnterAsync(DisposedCancellationToken).ConfigureAwait(false))
                    {
                        if (connection is null)
                        {
                            return;
                        }

                        var localConn = connection;
                        connection = null;
                        _connection = null;
                        await localConn.DisposeAsync().ConfigureAwait(false);
                    }
                });
            }
        }
        finally
        {
            linkedCts?.Dispose();
        }
    }

    /// <summary>Releases all resources used by the current instance of the class.</summary>
    /// <remarks>Call this method when you are finished using the object to release unmanaged resources and
    /// perform other cleanup operations. After calling Dispose, the object should not be used.</remarks>
    [SuppressMessage(
        "Major Bug",
        "S4462:Calls to async methods should not be blocking",
        Justification = "IDisposable.Dispose is intrinsically synchronous; this method must tear down async connection state on the sync dispose path.")]
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
        {
            return;
        }

        _disposedCts.Cancel();
        _connection?.DisposeAsync().AsTask().Wait();
        _gate.Dispose();
        _disposedCts.Dispose();
    }

    /// <summary>Subscribes the specified asynchronous observer to receive notifications from the sequence.</summary>
    /// <param name="observer">The asynchronous observer that will receive notifications. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the subscription operation.</param>
    /// <returns>A task that represents the asynchronous subscription operation. The result contains an object that can be
    /// disposed to unsubscribe the observer.</returns>
    protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        // The downstream observer (when it itself is an ObserverAsync) receives the wrap's dispose
        // token through the broadcast chain — pre-linking it here eliminates the per-emission
        // linked-CTS allocation that the downstream's TryEnter would otherwise produce. The wrap
        // itself benefits from SignalAsyncWitness forwarding CancellationToken.None to the
        // signal (the wrap's TryEnter sees None and fast-paths), so no wrap-side link is needed.
        RelayWitnessAsync<T> wrap = new(observer);
        if (observer is WitnessAsync<T> downstream)
        {
            downstream.LinkUpstreamCancellation(wrap.InternalDisposedToken);
        }

        return signal.Values.SubscribeAsync(wrap, cancellationToken);
    }
}
