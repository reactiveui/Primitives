// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="SyncOperation"/>.</summary>
public sealed class SyncOperationTests
{
    /// <summary>The policy priority.</summary>
    private const int Priority = 3;

    /// <summary>Verifies the effective policy is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsPolicy()
    {
        var policy = new OperationPolicy(DeliveryGuarantee.ExactlyOnce, OperationDurability.Durable, Priority, ConflictPolicy.Merge);
        var operation = CreateOperation() with { Policy = policy };
        await Assert.That(operation.Policy).IsEqualTo(policy);
        await Assert.That(SyncOperation.DefaultPolicy).IsEqualTo(OperationPolicy.Default);
    }

    /// <summary>Creates a representative synchronization operation.</summary>
    /// <returns>A synchronization operation.</returns>
    private static SyncOperation CreateOperation() => new()
    {
        OperationId = OperationId.New(),
        StreamId = new("sensor/temperature"),
        ClientSequence = 1,
        TimestampUtc = DateTimeOffset.UnixEpoch,
        Type = SyncOperationType.Append,
        Payload = new("reading", 1, "application/json", ReadOnlyMemory<byte>.Empty, "sha256-empty"),
        Policy = OperationPolicy.Default,
        Metadata = new Dictionary<string, string>(),
    };
}
