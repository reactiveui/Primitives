// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Represents an asynchronous observable sequence that can be connected to a data source, allowing control over when
/// the subscription to the underlying resource is established.
/// </summary>
/// <remarks>A connectable observable enables explicit control over the connection to the data source, which can
/// be useful for sharing a single subscription among multiple observers or for deferring the start of data emission
/// until explicitly connected. Implementations may vary in how connections are managed and whether multiple connections
/// are supported concurrently.</remarks>
/// <typeparam name="T">The type of elements produced by the observable sequence.</typeparam>
public abstract class ConnectableSignalAsync<T> : SignalAsync<T>
{
    /// <summary>
    /// Asynchronously establishes a connection to the target resource and returns a disposable handle for managing the
    /// connection's lifetime.
    /// </summary>
    /// <remarks>The returned <see cref="IAsyncDisposable"/> must be disposed when the connection is no longer
    /// needed to ensure proper resource cleanup. Multiple calls to this method may result in multiple independent
    /// connections, depending on the implementation.</remarks>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous connection operation.</param>
    /// <returns>A value task that represents the asynchronous operation. The result contains an <see cref="IAsyncDisposable"/>
    /// that should be disposed to close the connection.</returns>
    public abstract ValueTask<IAsyncDisposable> ConnectAsync(CancellationToken cancellationToken);
}
