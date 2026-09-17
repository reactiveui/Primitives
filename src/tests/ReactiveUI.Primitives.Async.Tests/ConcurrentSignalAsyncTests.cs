// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests the independent lifetimes of concurrent signals and observer leases.</summary>
public sealed class ConcurrentSignalAsyncTests
{
    /// <summary>Signal disposal preserves subscriptions until their own leases are disposed.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task DisposeAsync_ActiveSubscription_DoesNotDisposeObserverLease()
    {
        const int IgnoredValue = 2;
        ConcurrentSignalAsync<int> signal = new();
        List<int> values = [];
        await using var subscription = await signal.SubscribeAsync(values.Add);

        await signal.DisposeAsync();
        await signal.OnNextAsync(1, CancellationToken.None);
        await subscription.DisposeAsync();
        await signal.OnNextAsync(IgnoredValue, CancellationToken.None);

        await Assert.That(values).IsCollectionEqualTo([1]);
    }
}
