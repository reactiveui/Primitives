// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that automatically connects to the underlying connectable source when the first observer subscribes and disconnects when the last observer unsubscribes.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="source">The connectable observable to manage with reference counting.</param>
[System.Diagnostics.DebuggerDisplay("RefCountSignal: Gate = {_gate}")]
public sealed class RefCountSignal<T>(ConnectableSignalAsync<T> source) : IObservableAsync<T>, IDisposable
{
    /// <summary>The asynchronous gate that serializes subscribe and dispose operations.</summary>
    private readonly ReactiveUI.Primitives.Async.Internal.AsyncFlowGate _gate = new();

    /// <summary>The current number of active subscribers.</summary>
    private int _refCount;

    /// <summary>The single-assignment disposable holding the connection to the underlying connectable source.</summary>
    private SingleAssignmentDisposableAsync? _connection;

    /// <summary>Non-zero after this instance has been disposed.</summary>
    private int _disposedValue;

    /// <summary>Creates a reference-counted sequence that replays the last observed value to new subscribers, seeded with <paramref name="initialValue"/>.</summary>
    /// <param name="source">The upstream observable sequence.</param>
    /// <param name="initialValue">The initial replay value.</param>
    /// <returns>A configured <see cref="RefCountSignal{T}"/>.</returns>
    public static RefCountSignal<T> CreateReplayLast(IObservableAsync<T> source, T initialValue)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new(new(source, new SerialReplayLatestSignalAsync<T>(new(initialValue))));
    }

    /// <summary>Disposes the ref-count observable, releasing the connection gate and any active connection.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Dispose(true);

    /// <summary>Releases managed resources if <paramref name="disposing"/> is true.</summary>
    /// <param name="disposing">True to release managed resources; false when called from a finalizer.</param>
    [SuppressMessage(
        "Concurrency",
        "PSH1315:A blocking wait on an awaitable that may not be done",
        Justification = "IDisposable.Dispose is synchronous and must tear down the asynchronous connection.")]
    internal void Dispose(bool disposing)
    {
        if (Interlocked.Exchange(ref _disposedValue, 1) != 0)
        {
            return;
        }

        if (!disposing)
        {
            return;
        }

        _connection?.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    /// <summary>Decrements the active subscriber count and disconnects when the last subscriber leaves.</summary>
    /// <returns>A task representing the asynchronous release.</returns>
    internal async ValueTask ReleaseSubscriberAsync()
    {
        using (await _gate.EnterAsync().ConfigureAwait(false))
        {
            --_refCount;
            if (_refCount == 0)
            {
                var connection = _connection;
                _connection = null;
                if (connection is not null)
                {
                    await connection.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>Subscribes the specified observer, incrementing the reference count and connecting to the source if this is the first subscriber.</summary>
    /// <param name="observer">The observer to receive elements from the connectable source.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An async disposable that decrements the reference count on disposal.</returns>
    [DebuggerStepThrough]
    async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        using (await _gate.EnterAsync(cancellationToken).ConfigureAwait(false))
        {
            ++_refCount;
            var needConnect = _refCount == 1;
            RefCountWitness<T> refCountWitness = new(this, observer);
            var subcription = await ((IObservableAsync<T>)source)
                .SubscribeAsync(refCountWitness, cancellationToken)
                .ConfigureAwait(false);
            if (needConnect && !refCountWitness.HasDisposed)
            {
                SingleAssignmentDisposableAsync connection = new();
                _connection = connection;
                await connection
                    .SetDisposableAsync(await source.ConnectAsync(cancellationToken).ConfigureAwait(false))
                    .ConfigureAwait(false);
            }

            return subcription;
        }
    }
}
