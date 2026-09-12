// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async;

/// <summary>An asynchronous observable that multicasts a cold source through a signal, leaving the caller to decide when the source subscription is established.</summary>
/// <typeparam name="T">The type of elements produced by the observable sequence.</typeparam>
/// <remarks>Subscribing attaches the observer to the signal without touching the source, so observers that subscribe
/// before <see cref="ConnectAsync"/> all share the one source subscription it creates.</remarks>
[System.Diagnostics.DebuggerDisplay("ConnectableSignalAsync: State = {State}")]
public sealed class ConnectableSignalAsync<T> : IObservableAsync<T>, IDisposable
{
    /// <summary>Initializes a new instance of the <see cref="ConnectableSignalAsync{T}"/> class.</summary>
    /// <param name="source">The source signal to multicast.</param>
    /// <param name="signal">The signal that multicasts source notifications to the subscribed observers.</param>
    public ConnectableSignalAsync(IObservableAsync<T> source, ISignalAsync<T> signal) =>
        State = new(source, signal);

    /// <summary>Gets the mutable connection state owned by this wrapper.</summary>
    private ConnectableSignalAsyncState<T> State { get; }

    /// <summary>Subscribes the signal to the source, or returns the live connection when one exists.</summary>
    /// <param name="cancellationToken">A token that cancels connection establishment.</param>
    /// <returns>A handle whose disposal drops the source subscription, allowing a later call to reconnect.</returns>
    /// <exception cref="OperationCanceledException">This instance has been disposed, or
    /// <paramref name="cancellationToken"/> was cancelled.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<IAsyncDisposable> ConnectAsync(CancellationToken cancellationToken) =>
        ConnectableSignalAsyncHelper.ConnectAsync(State, cancellationToken);

    /// <summary>Drops any live connection and blocks further ones.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SuppressMessage(
        "Concurrency",
        "PSH1315:A blocking wait on an awaitable that may not be done",
        Justification = "The synchronous dispose contract leaves no way to await teardown of the async connection state.")]
    public void Dispose() => ConnectableSignalAsyncHelper.Dispose(State);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        ConnectableSignalAsyncHelper.SubscribeAsync(State, observer, cancellationToken);
}
