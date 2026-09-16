// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Shared subscribe behaviour for the arity-specific <c>SyncLatest</c> coordinators.</summary>
internal static class SyncLatestCoordinator
{
    /// <summary>Subscribes to every source in index order, storing each subscription in the lifecycle's slots.</summary>
    /// <typeparam name="TResult">The downstream element type.</typeparam>
    /// <param name="coordinator">The coordinator whose sources are subscribed.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>A task representing the asynchronous subscribe operation.</returns>
    internal static async ValueTask SubscribeSourcesAsync<TResult>(ISyncLatestCoordinator<TResult> coordinator, CancellationToken cancellationToken)
    {
        var subscriptions = coordinator.Lifecycle.Subscriptions;
        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i] = await coordinator.SubscribeAtAsync(i, cancellationToken).ConfigureAwait(false);
        }
    }
}
