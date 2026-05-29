// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Represents an asynchronous observable sequence that is grouped by a specified key.
/// </summary>
/// <remarks>Each instance corresponds to a group within the parent observable, identified by its key. Observers
/// can subscribe to receive elements belonging to the group associated with the specified key.</remarks>
/// <typeparam name="TKey">The type of the key used to group elements in the observable sequence.</typeparam>
/// <typeparam name="TValue">The type of the elements contained in the grouped observable sequence.</typeparam>
public abstract class GroupedAsyncObservable<TKey, TValue> : ObservableAsync<TValue>
{
    /// <summary>
    /// Gets the key associated with the current object.
    /// </summary>
    public abstract TKey Key { get; }
}
