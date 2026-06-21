// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Subscription handle that removes an observer from a stateless replay signal when disposed.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
/// <param name="state">The stateless replay signal state that owns the observer list.</param>
/// <param name="observer">The observer to remove when the lease is disposed.</param>
internal sealed class StatelessReplayLatestSignalAsyncStateObserverLease<T>(
    StatelessReplayLatestSignalAsyncState<T> state,
    IObserverAsync<T> observer) : IAsyncDisposable
{
    /// <summary>Indicates whether the lease has already removed its observer.</summary>
    private int _disposed;

    /// <inheritdoc/>
    public ValueTask DisposeAsync() =>
        Interlocked.Exchange(ref _disposed, 1) != 0
            ? default
            : RemoveObserverAndResetAsync();

    /// <summary>Removes the observer and restores the initial value when the last observer leaves.</summary>
    /// <returns>A task that represents the asynchronous removal operation.</returns>
    [ExcludeFromCodeCoverage]
    private async ValueTask RemoveObserverAndResetAsync()
    {
        if (state.IsDisposed)
        {
            return;
        }

        try
        {
            using (await state.Gate.EnterAsync(state.DisposedCts.Token).ConfigureAwait(false))
            {
                state.Observers = state.Observers.Remove(observer);
                if (state.Observers.IsEmpty)
                {
                    state.Value = state.InitialValue;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // The signal was disposed while removal was waiting to enter the gate.
        }
        catch (ObjectDisposedException)
        {
            // The gate was disposed while removal was waiting to enter it.
        }
    }
}
