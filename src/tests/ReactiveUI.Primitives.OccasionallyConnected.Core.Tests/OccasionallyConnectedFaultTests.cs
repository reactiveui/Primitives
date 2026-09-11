// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedFault"/>.</summary>
public sealed class OccasionallyConnectedFaultTests
{
    /// <summary>Verifies the fault code is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsCode()
    {
        StreamId streamId = new("sensor/temperature");
        var operationId = OperationId.New();
        var exception = new InvalidOperationException("failure");
        var occurredAt = DateTimeOffset.UnixEpoch;
        var fault = new OccasionallyConnectedFault(
            "OC.Fault",
            "message",
            occurredAt,
            streamId,
            operationId,
            exception);

        await Assert.That(fault.Code).IsEqualTo("OC.Fault");
        await Assert.That(fault.Message).IsEqualTo("message");
        await Assert.That(fault.OccurredAtUtc).IsEqualTo(occurredAt);
        await Assert.That(fault.StreamId).IsEqualTo(streamId);
        await Assert.That(fault.OperationId).IsEqualTo(operationId);
        await Assert.That(fault.Exception).IsSameReferenceAs(exception);
    }
}
