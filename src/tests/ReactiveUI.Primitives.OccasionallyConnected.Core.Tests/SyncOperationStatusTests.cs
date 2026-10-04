// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="SyncOperationStatus"/>.</summary>
public sealed class SyncOperationStatusTests
{
    /// <summary>Verifies the operation state is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsState()
    {
        var operationId = OperationId.New();
        StreamId streamId = new("sensor/temperature");
        var changedAt = DateTimeOffset.UnixEpoch;
        var status = new SyncOperationStatus(
            operationId,
            streamId,
            SyncOperationState.Synchronized,
            1,
            changedAt,
            "OC.Done");

        await Assert.That(status.OperationId).IsEqualTo(operationId);
        await Assert.That(status.StreamId).IsEqualTo(streamId);
        await Assert.That(status.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(status.Attempt).IsEqualTo(1);
        await Assert.That(status.ChangedAtUtc).IsEqualTo(changedAt);
        await Assert.That(status.ReasonCode).IsEqualTo("OC.Done");
    }
}
