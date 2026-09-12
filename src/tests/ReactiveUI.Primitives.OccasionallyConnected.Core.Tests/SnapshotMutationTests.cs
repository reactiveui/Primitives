// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="SnapshotMutation"/>.</summary>
public sealed class SnapshotMutationTests
{
    /// <summary>The expected snapshot revision.</summary>
    private const long ExpectedRevision = 7;

    /// <summary>Verifies supplied values are retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsValues()
    {
        var payload = CreatePayload();
        StreamId streamId = new("sensor/temperature");
        var mutation = new SnapshotMutation(streamId, payload, 1, ExpectedRevision);

        await Assert.That(mutation.StreamId).IsEqualTo(streamId);
        await Assert.That(mutation.State).IsSameReferenceAs(payload);
        await Assert.That(mutation.FormatVersion).IsEqualTo(1);
        await Assert.That(mutation.ExpectedRevision).IsEqualTo(ExpectedRevision);
        await Assert.That(mutation.AuthoritativeState).IsNull();
        var confirmed = new PayloadEnvelope("reading", 1, "application/json", new byte[] { 1 }, "confirmed");
        var updated = mutation with { AuthoritativeState = confirmed };
        await Assert.That(updated.AuthoritativeState).IsSameReferenceAs(confirmed);
        await Assert.That(updated.State).IsSameReferenceAs(payload);
        await Assert.That(mutation.AuthoritativeState).IsNull();
    }

    /// <summary>Verifies the compatibility constructor defaults the expected revision.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompatibilityConstructorDefaultsExpectedRevision() =>
        await Assert.That(new SnapshotMutation(new("sensor/temperature"), CreatePayload(), 1).ExpectedRevision).IsEqualTo(0);

    /// <summary>Creates a representative payload.</summary>
    /// <returns>A payload envelope.</returns>
    private static PayloadEnvelope CreatePayload() =>
        new("reading", 1, "application/json", ReadOnlyMemory<byte>.Empty, "sha256-empty");
}
