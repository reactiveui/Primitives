// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Stores the state needed to subscribe observers to one grouped signal.</summary>
/// <typeparam name="TKey">The type of the key used to identify the group.</typeparam>
/// <typeparam name="TValue">The type of elements contained in the group.</typeparam>
/// <param name="key">The key associated with the grouped observable.</param>
/// <param name="signalValues">The value stream backing the grouped observable.</param>
/// <param name="disposables">The parent-owned collection that tracks group subscriptions.</param>
/// <param name="parentDisposedToken">The token canceled when the parent grouping coordinator is disposed.</param>
internal sealed class GroupedAsyncSignalState<TKey, TValue>(
    TKey key,
    IObservableAsync<TValue> signalValues,
    MultipleDisposableAsync disposables,
    CancellationToken parentDisposedToken)
{
    /// <summary>Gets the key associated with the grouped observable.</summary>
    internal TKey Key { get; } = key;

    /// <summary>Gets the value stream backing the grouped observable.</summary>
    internal IObservableAsync<TValue> SignalValues { get; } = signalValues;

    /// <summary>Gets the parent-owned collection that tracks group subscriptions.</summary>
    internal MultipleDisposableAsync Disposables { get; } = disposables;

    /// <summary>Gets the token canceled when the parent grouping coordinator is disposed.</summary>
    internal CancellationToken ParentDisposedToken { get; } = parentDisposedToken;
}
