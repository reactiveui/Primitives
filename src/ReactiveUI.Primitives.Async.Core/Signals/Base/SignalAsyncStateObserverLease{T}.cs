// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Subscription handle that removes an observer from its owning signal when disposed.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
/// <param name="state">The signal state that owns the observer list.</param>
/// <param name="observer">The observer to remove when the lease is disposed.</param>
internal sealed class SignalAsyncStateObserverLease<T>(
    SignalAsyncState<T> state,
    IObserverAsync<T> observer) : IAsyncDisposable
{
    /// <summary>Indicates whether the lease has already removed its observer.</summary>
    private int _disposed;

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return default;
        }

        state.Remove(observer);
        return default;
    }
}
