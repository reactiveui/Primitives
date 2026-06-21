// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Stores the state needed to subscribe observers to one grouped signal.</summary>
/// <typeparam name="TKey">The type of the key used to identify the group.</typeparam>
/// <typeparam name="TValue">The type of elements contained in the group.</typeparam>
/// <param name="Key">The key associated with the grouped observable.</param>
/// <param name="SignalValues">The value stream backing the grouped observable.</param>
/// <param name="Disposables">The parent-owned collection that tracks group subscriptions.</param>
/// <param name="ParentDisposedToken">The token canceled when the parent grouping coordinator is disposed.</param>
internal sealed record GroupedAsyncSignalState<TKey, TValue>(
    TKey Key,
    IObservableAsync<TValue> SignalValues,
    MultipleDisposableAsync Disposables,
    CancellationToken ParentDisposedToken);
