// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Represents an asynchronous observable sequence that can be connected to a data source, allowing control over when
/// the subscription to the underlying resource is established.
/// </summary>
/// <typeparam name="T">The type of elements produced by the observable sequence.</typeparam>
/// <remarks>A connectable observable enables explicit control over the connection to the data source, which can
/// be useful for sharing a single subscription among multiple observers or for deferring the start of data emission
/// until explicitly connected. Implementations may vary in how connections are managed and whether multiple connections
/// are supported concurrently.</remarks>
[System.Diagnostics.DebuggerDisplay("ConnectableSignalAsync: State = {State}")]
public sealed class ConnectableSignalAsync<T> : IObservableAsync<T>, IDisposable
{
    /// <summary>Initializes a new instance of the <see cref="ConnectableSignalAsync{T}"/> class.</summary>
    /// <param name="source">The source signal to multicast.</param>
    /// <param name="signal">The signal used to broadcast notifications to multiple observers.</param>
    public ConnectableSignalAsync(IObservableAsync<T> source, ISignalAsync<T> signal) =>
        State = new(source, signal);

    /// <summary>Gets the mutable connection state owned by this wrapper.</summary>
    private ConnectableSignalAsyncState<T> State { get; }

    /// <summary>
    /// Asynchronously establishes a connection to the target resource and returns a disposable handle for managing the
    /// connection's lifetime.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous connection operation.</param>
    /// <returns>A value task that represents the asynchronous operation. The result contains an <see cref="IAsyncDisposable"/>
    /// that should be disposed to close the connection.</returns>
    /// <remarks>The returned <see cref="IAsyncDisposable"/> must be disposed when the connection is no longer
    /// needed to ensure proper resource cleanup. Multiple calls to this method may result in multiple independent
    /// connections, depending on the implementation.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<IAsyncDisposable> ConnectAsync(CancellationToken cancellationToken) =>
        ConnectableSignalAsyncHelper.ConnectAsync(State, cancellationToken);

    /// <summary>Releases all resources used by the current instance of the class.</summary>
    /// <remarks>Call this method when you are finished using the object to release managed resources.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SuppressMessage(
        "Concurrency",
        "PSH1315:A blocking wait on an awaitable that may not be done",
        Justification =
            "IDisposable.Dispose is intrinsically synchronous; this method must tear down async connection state on the sync dispose path.")]
    public void Dispose() => ConnectableSignalAsyncHelper.Dispose(State);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        ConnectableSignalAsyncHelper.SubscribeAsync(State, observer, cancellationToken);
}
