// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LocalSnapshotRecoveryCaptureRequest"/>.</summary>
public sealed class LocalSnapshotRecoveryCaptureRequestTests
{
    /// <summary>The stream used by request tests.</summary>
    private static readonly StreamId Stream = new("capture-stream");

    /// <summary>The subscription used by request tests.</summary>
    private static readonly SubscriptionId Subscription = SubscriptionId.New();

    /// <summary>Verifies capture requests contain only store-local fences and limits.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RequestCarriesStoreFencesAndLimits()
    {
        var limits = new SnapshotRecoveryLimits { MaximumPendingOperations = 1 };

        var request = new LocalSnapshotRecoveryCaptureRequest { StreamId = Stream, SubscriptionId = Subscription, Limits = limits };

        await Assert.That(request.StreamId).IsEqualTo(Stream);
        await Assert.That(request.SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(request.Limits).IsSameReferenceAs(limits);
    }
}
