// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>
/// Provides a helper for safely subscribing an <see cref="IAsyncDisposable"/> subscription,
/// ensuring the subscription is disposed if the subscribe action throws.
/// </summary>
internal static class SubscriptionHelper
{
    /// <summary>Executes <paramref name="subscribeAsync"/> and returns <paramref name="subscription"/>.</summary>
    /// <param name="subscription">The subscription to manage.</param>
    /// <param name="subscribeAsync">The async action that wires up the subscription.</param>
    /// <returns>The subscription if successful.</returns>
    internal static async ValueTask<IAsyncDisposable> SubscribeAndDisposeOnFailureAsync(
        IAsyncDisposable subscription,
        Func<ValueTask> subscribeAsync)
    {
        try
        {
            await subscribeAsync().ConfigureAwait(false);
        }
        catch
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return subscription;
    }
}
