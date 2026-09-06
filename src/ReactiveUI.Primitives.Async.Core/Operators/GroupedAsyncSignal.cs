// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Represents an asynchronous observable sequence that is grouped by a specified key.</summary>
/// <typeparam name="TKey">The type of the key used to group elements in the observable sequence.</typeparam>
/// <typeparam name="TValue">The type of the elements contained in the grouped observable sequence.</typeparam>
/// <remarks>Each instance corresponds to a group within the parent observable, identified by its key. Observers
/// can subscribe to receive elements belonging to the group associated with the specified key.</remarks>
[System.Diagnostics.DebuggerDisplay("GroupedAsyncSignal: Key = {Key}")]
public sealed class GroupedAsyncSignal<TKey, TValue> : IObservableAsync<TValue>
{
    /// <summary>Initializes a new instance of the <see cref="GroupedAsyncSignal{TKey,TValue}"/> class.</summary>
    /// <param name="key">The key associated with this grouped observable.</param>
    /// <param name="signalValues">The signal value stream backing this group.</param>
    /// <param name="disposables">The parent coordinator collection that tracks group subscriptions.</param>
    /// <param name="parentDisposedToken">The token canceled when the parent grouping coordinator is disposed.</param>
    internal GroupedAsyncSignal(
        TKey key,
        IObservableAsync<TValue> signalValues,
        MultipleDisposableAsync disposables,
        CancellationToken parentDisposedToken) =>
        State = new(key, signalValues, disposables, parentDisposedToken);

    /// <summary>Gets the key associated with the current object.</summary>
    public TKey Key => State.Key;

    /// <summary>Gets the state shared by this grouped observable and its helper operations.</summary>
    private GroupedAsyncSignalState<TKey, TValue> State { get; }

    /// <summary>Subscribes the observer to values for this group.</summary>
    /// <param name="observer">The observer that receives group values.</param>
    /// <param name="cancellationToken">A token that can cancel subscription establishment.</param>
    /// <returns>The subscription to this group's value stream.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<TValue>.SubscribeAsync(
        IObserverAsync<TValue> observer,
        CancellationToken cancellationToken) =>
        GroupedAsyncSignalHelper.SubscribeAsync(State, observer, cancellationToken);
}
