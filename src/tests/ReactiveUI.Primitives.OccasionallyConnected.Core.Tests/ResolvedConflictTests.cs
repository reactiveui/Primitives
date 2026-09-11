// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ResolvedConflict"/>.</summary>
public sealed class ResolvedConflictTests
{
    /// <summary>Verifies the resolved payload is retained by reference.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsResolvedPayload()
    {
        var operationId = OperationId.New();
        var payload = new PayloadEnvelope("reading", 1, "application/json", ReadOnlyMemory<byte>.Empty, "sha256-empty");
        var conflict = new ResolvedConflict(operationId, "OC.Merge", payload);

        await Assert.That(conflict.OperationId).IsEqualTo(operationId);
        await Assert.That(conflict.ResolutionCode).IsEqualTo("OC.Merge");
        await Assert.That(conflict.ResolvedPayload).IsSameReferenceAs(payload);
    }
}
