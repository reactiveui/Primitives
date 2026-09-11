// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="NegotiatedCapabilities"/>.</summary>
public sealed class NegotiatedCapabilitiesTests
{
    /// <summary>The heartbeat timeout in minutes.</summary>
    private const int HeartbeatMinutes = 10;

    /// <summary>The maximum batch bytes.</summary>
    private const int MaximumBytes = 2048;

    /// <summary>The maximum batch operations.</summary>
    private const int MaximumOperations = 10;

    /// <summary>The receive timeout in minutes.</summary>
    private const int ReceiveMinutes = 5;

    /// <summary>Verifies features are retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsFeatures()
    {
        var protocolVersion = new Version(1, 0);
        var serverRetention = TimeSpan.FromMinutes(ReceiveMinutes);
        var inboxRetention = TimeSpan.FromMinutes(HeartbeatMinutes);
        var capabilities = new NegotiatedCapabilities(
            protocolVersion,
            RemoteTransportCapabilities.BatchPush,
            MaximumOperations,
            MaximumBytes,
            serverRetention,
            inboxRetention);

        await Assert.That(capabilities.ProtocolVersion).IsSameReferenceAs(protocolVersion);
        await Assert.That(capabilities.Features).IsEqualTo(RemoteTransportCapabilities.BatchPush);
        await Assert.That(capabilities.MaximumBatchOperations).IsEqualTo(MaximumOperations);
        await Assert.That(capabilities.MaximumBatchBytes).IsEqualTo(MaximumBytes);
        await Assert.That(capabilities.ServerIdempotencyRetention).IsEqualTo(serverRetention);
        await Assert.That(capabilities.ClientInboxRetentionRequired).IsEqualTo(inboxRetention);
    }
}
