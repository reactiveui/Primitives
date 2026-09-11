// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemoteSubscribeRequest"/>.</summary>
public sealed class RemoteSubscribeRequestTests
{
    /// <summary>Verifies the subscription identifier is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsSubscriptionIdentifier()
    {
        var subscriptionId = SubscriptionId.New();
        var request = new RemoteSubscribeRequest(new("sensor/temperature"), subscriptionId, "cursor-1", StartPosition.Latest);
        await Assert.That(request.SubscriptionId).IsEqualTo(subscriptionId);
    }
}
