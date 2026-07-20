// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Advanced;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides grouped-signal subscription operations over flat state records.</summary>
internal static class GroupedAsyncSignalHelper
{
    /// <summary>Subscribes an observer to a grouped signal and tracks the subscription in the parent collection.</summary>
    /// <typeparam name="TKey">The type of the key used to identify the group.</typeparam>
    /// <typeparam name="TValue">The type of elements contained in the group.</typeparam>
    /// <param name="state">The grouped signal state to observe.</param>
    /// <param name="observer">The observer that receives group values.</param>
    /// <param name="cancellationToken">A token that can cancel subscription establishment.</param>
    /// <returns>The subscription to the grouped value stream.</returns>
    internal static async ValueTask<IAsyncDisposable> SubscribeAsync<TKey, TValue>(
        GroupedAsyncSignalState<TKey, TValue> state,
        IObserverAsync<TValue> observer,
        CancellationToken cancellationToken)
    {
        RelayWitnessAsync<TValue> wrap = new(observer);
        wrap.LinkUpstreamCancellation(state.ParentDisposedToken);
        if (observer is WitnessAsync<TValue> downstream)
        {
            downstream.LinkUpstreamCancellation(wrap.InternalDisposedToken);
        }

        var subscription = await state.SignalValues.SubscribeAsync(wrap, cancellationToken).ConfigureAwait(false);
        await state.Disposables.AddAsync(subscription).ConfigureAwait(false);
        return DisposableAsync.Create(
            (state, subscription),
            static async s =>
            {
                await s.state.Disposables.Remove(s.subscription).ConfigureAwait(false);
                await s.subscription.DisposeAsync().ConfigureAwait(false);
            });
    }
}
