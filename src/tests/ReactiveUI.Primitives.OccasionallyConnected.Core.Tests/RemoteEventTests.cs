// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemoteEvent"/>.</summary>
public sealed class RemoteEventTests
{
    /// <summary>The expected metadata count after construction.</summary>
    private const int MetadataCount = 1;

    /// <summary>Verifies event metadata is copied from caller-owned dictionaries.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorCopiesMetadata()
    {
        var metadata = new Dictionary<string, string> { ["source"] = "server" };
        var operationId = OperationId.New();
        var payload = CreatePayload();
        var remoteEvent = new RemoteEvent(
            Guid.NewGuid(),
            new("sensor/temperature"),
            "cursor-1",
            DateTimeOffset.UnixEpoch,
            operationId,
            payload,
            metadata);

        metadata.Add("extra", "ignored");

        await Assert.That(remoteEvent.Metadata).Count().IsEqualTo(MetadataCount);
        await Assert.That(remoteEvent.CausedByOperationId).IsEqualTo(operationId);
        await Assert.That(remoteEvent.Payload).IsSameReferenceAs(payload);
    }

    /// <summary>Verifies null metadata is rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullMetadata()
    {
        var action = static () => new RemoteEvent(
            Guid.NewGuid(),
            new("sensor/temperature"),
            "cursor-1",
            DateTimeOffset.UnixEpoch,
            null,
            CreatePayload(),
            null!);

        await Assert.That(action).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Creates a representative payload envelope.</summary>
    /// <returns>A payload envelope.</returns>
    private static PayloadEnvelope CreatePayload() => new("reading", 1, "application/json", ReadOnlyMemory<byte>.Empty, "sha256-empty");
}
