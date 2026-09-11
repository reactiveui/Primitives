// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ServerState"/>.</summary>
public sealed class ServerStateTests
{
    /// <summary>Verifies the state payload is retained by reference.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsStatePayload()
    {
        var payload = new PayloadEnvelope("reading", 1, "application/json", ReadOnlyMemory<byte>.Empty, "sha256-empty");
        StreamId streamId = new("sensor/temperature");
        var state = new ServerState(streamId, "v1", payload);

        await Assert.That(state.StreamId).IsEqualTo(streamId);
        await Assert.That(state.Version).IsEqualTo("v1");
        await Assert.That(state.State).IsSameReferenceAs(payload);
    }
}
