// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Async.Helpers;

/// <summary>Provides a helper for safely subscribing an <see cref="IAsyncDisposable"/> subscription, ensuring the subscription is disposed if the subscribe action throws.</summary>
public static class SubscriptionHelper
{
    /// <summary>Runs <paramref name="subscribeAsync"/>, disposing <paramref name="subscription"/> and rethrowing if it fails, so a half-built subscription is never handed back.</summary>
    /// <param name="subscription">The subscription to manage.</param>
    /// <param name="subscribeAsync">The async action that wires up the subscription.</param>
    /// <returns>The subscription, once wiring succeeded.</returns>
    internal static async ValueTask<IAsyncDisposable> SubscribeAndDisposeOnFailureAsync(
        IAsyncDisposable subscription,
        Func<ValueTask> subscribeAsync)
    {
        ExceptionDispatchInfo failure;
        try
        {
            await subscribeAsync().ConfigureAwait(false);
            return subscription;
        }
        catch (Exception e)
        {
            failure = ExceptionDispatchInfo.Capture(e);
        }

        await subscription.DisposeAsync().ConfigureAwait(false);
        return CapturedFailure.Rethrow<IAsyncDisposable>(failure);
    }
}
