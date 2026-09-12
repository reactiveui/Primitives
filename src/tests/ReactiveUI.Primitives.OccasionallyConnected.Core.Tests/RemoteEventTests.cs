// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemoteEvent"/>.</summary>
public sealed class RemoteEventTests
{
    /// <summary>The test stream identifier.</summary>
    private const string StreamName = "sensor/temperature";

    /// <summary>The test server cursor.</summary>
    private const string Cursor = "cursor-1";

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
            new(StreamName),
            Cursor,
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
            new(StreamName),
            Cursor,
            DateTimeOffset.UnixEpoch,
            null,
            CreatePayload(),
            null!);

        await Assert.That(action).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies matching origin data can be added without changing legacy construction.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OriginMatchesCauseAndLegacyEventsRemainSupported()
    {
        var operationId = OperationId.New();
        var legacy = CreateRemoteEvent(operationId);
        var origin = new RemoteEventOrigin("client", operationId);
        var withOrigin = legacy with { Origin = origin };

        await Assert.That(legacy.Origin).IsNull();
        await Assert.That(withOrigin.Origin).IsEqualTo(origin);
        await Assert.That(withOrigin.CausedByOperationId).IsEqualTo(operationId);
    }

    /// <summary>Verifies origin data cannot be assigned with a mismatched or absent event cause.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OriginRequiresMatchingCause()
    {
        var operationId = OperationId.New();
        var origin = new RemoteEventOrigin("client", operationId);
        var mismatched = CreateRemoteEvent(OperationId.New());
        var withoutCause = CreateRemoteEvent(null);

        await Assert.That(() => mismatched with { Origin = origin }).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => withoutCause with { Origin = origin }).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates a representative remote event.</summary>
    /// <param name="causedByOperationId">The optional causing operation.</param>
    /// <returns>The event.</returns>
    private static RemoteEvent CreateRemoteEvent(OperationId? causedByOperationId) =>
        new(
            Guid.NewGuid(),
            new(StreamName),
            Cursor,
            DateTimeOffset.UnixEpoch,
            causedByOperationId,
            CreatePayload(),
            new Dictionary<string, string>());

    /// <summary>Creates a representative payload envelope.</summary>
    /// <returns>A payload envelope.</returns>
    private static PayloadEnvelope CreatePayload() => new("reading", 1, "application/json", ReadOnlyMemory<byte>.Empty, "sha256-empty");
}
