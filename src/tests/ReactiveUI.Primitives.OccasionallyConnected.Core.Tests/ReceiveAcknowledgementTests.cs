// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ReceiveAcknowledgement"/>.</summary>
public sealed class ReceiveAcknowledgementTests
{
    /// <summary>Verifies the cursor is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsCursor()
    {
        var subscriptionId = SubscriptionId.New();
        StreamId streamId = new("sensor/temperature");
        var acknowledgement = new ReceiveAcknowledgement(subscriptionId, streamId, "cursor-2");

        await Assert.That(acknowledgement.SubscriptionId).IsEqualTo(subscriptionId);
        await Assert.That(acknowledgement.StreamId).IsEqualTo(streamId);
        await Assert.That(acknowledgement.Cursor).IsEqualTo("cursor-2");
    }
}
