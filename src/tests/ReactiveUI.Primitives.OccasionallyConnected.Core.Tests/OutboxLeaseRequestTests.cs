// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OutboxLeaseRequest"/>.</summary>
public sealed class OutboxLeaseRequestTests
{
    /// <summary>The maximum leased bytes.</summary>
    private const int MaximumBytes = 2048;

    /// <summary>The maximum leased operations.</summary>
    private const int MaximumOperations = 10;

    /// <summary>Verifies the maximum byte count is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsMaximumBytes()
    {
        StreamId streamId = new("sensor/temperature");
        var leaseDuration = TimeSpan.FromMinutes(1);
        var request = new OutboxLeaseRequest(streamId, MaximumOperations, MaximumBytes, leaseDuration);

        await Assert.That(request.StreamId).IsEqualTo(streamId);
        await Assert.That(request.MaximumOperations).IsEqualTo(MaximumOperations);
        await Assert.That(request.MaximumBytes).IsEqualTo(MaximumBytes);
        await Assert.That(request.LeaseDuration).IsEqualTo(leaseDuration);
    }
}
