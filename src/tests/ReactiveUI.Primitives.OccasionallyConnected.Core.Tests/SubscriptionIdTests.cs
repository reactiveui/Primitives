// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="SubscriptionId"/>.</summary>
public sealed class SubscriptionIdTests
{
    /// <summary>Verifies new subscription identifiers are non-empty.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task NewCreatesNonEmptyIdentifier()
    {
        var subscriptionId = SubscriptionId.New();

        await Assert.That(subscriptionId.Value).IsNotEqualTo(Guid.Empty);
    }

    /// <summary>Verifies new subscription identifiers are unique across calls.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task NewCreatesDistinctIdentifiers()
    {
        var first = SubscriptionId.New();
        var second = SubscriptionId.New();

        await Assert.That(first).IsNotEqualTo(second);
    }
}
