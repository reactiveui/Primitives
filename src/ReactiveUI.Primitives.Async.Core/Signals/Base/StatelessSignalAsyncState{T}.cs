// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Mutable state for a stateless async signal.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
internal sealed class StatelessSignalAsyncState<T>
{
    /// <summary>The lock used to synchronize observer list updates.</summary>
    private readonly Lock _gate = new();

    /// <summary>Gets or sets the currently subscribed observers.</summary>
    internal ImmutableArray<IObserverAsync<T>> Observers { get; set; } = [];

    /// <summary>Gets a stable snapshot of the current observers.</summary>
    /// <returns>The observers subscribed when the snapshot is taken.</returns>
    internal ImmutableArray<IObserverAsync<T>> Snapshot()
    {
        lock (_gate)
        {
            return Observers;
        }
    }

    /// <summary>Removes all observers from the state.</summary>
    internal void Clear()
    {
        lock (_gate)
        {
            Observers = [];
        }
    }

    /// <summary>Adds an observer to the active subscription list.</summary>
    /// <param name="observer">The observer to add.</param>
    internal void Add(IObserverAsync<T> observer)
    {
        lock (_gate)
        {
            Observers = Observers.Add(observer);
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
