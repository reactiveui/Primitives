// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Mutable state for a completing async signal.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
[SuppressMessage(
    "Style",
    "SST1802:Replace set accessor with init",
    Justification = "This record is the mutable state container for the flat helper implementation.")]
internal sealed record SignalAsyncState<T>
{
    /// <summary>The lock used to synchronize mutable state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Gets the currently subscribed observers.</summary>
    public ImmutableArray<IObserverAsync<T>> Observers { get; internal set; } = [];

    /// <summary>Gets the completion result, or null if the signal has not completed.</summary>
    public Result? Result { get; internal set; }

    /// <summary>Gets a stable observer snapshot when the signal is still active.</summary>
    /// <param name="observers">Receives the observers to notify when the signal is active.</param>
    /// <returns>true if observers should be notified; otherwise, false.</returns>
    internal bool TryGetObservers(out ImmutableArray<IObserverAsync<T>> observers)
    {
        lock (_gate)
        {
            if (Result is not null)
            {
                observers = [];
                return false;
            }

            observers = Observers;
            return true;
        }
    }

    /// <summary>Marks the signal as completed and returns the observers to notify.</summary>
    /// <param name="result">The completion result to store.</param>
    /// <param name="observers">Receives the observers subscribed at completion time.</param>
    /// <returns>true if this call completed the signal; otherwise, false.</returns>
    internal bool TryComplete(Result result, out ImmutableArray<IObserverAsync<T>> observers)
    {
        lock (_gate)
        {
            if (Result is not null)
            {
                observers = [];
                return false;
            }

            Result = result;
            observers = Observers;
            Observers = [];
            return true;
        }
    }

    /// <summary>Adds an observer unless the signal has already completed.</summary>
    /// <param name="observer">The observer to subscribe.</param>
    /// <returns>The existing completion result when the signal is already completed; otherwise, null.</returns>
    internal Result? Subscribe(IObserverAsync<T> observer)
    {
        lock (_gate)
        {
            var result = Result;
            if (result is null)
            {
                Observers = Observers.Add(observer);
            }

            return result;
        }
    }

    /// <summary>Removes an observer from the active subscription list.</summary>
    /// <param name="observer">The observer to remove.</param>
    internal void Remove(IObserverAsync<T> observer)
    {
        lock (_gate)
        {
            Observers = Observers.Remove(observer);
        }
    }
}
