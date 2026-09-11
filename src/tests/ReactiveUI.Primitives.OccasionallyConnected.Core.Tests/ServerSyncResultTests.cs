// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ServerSyncResult"/>.</summary>
public sealed class ServerSyncResultTests
{
    /// <summary>Verifies events are copied from caller-owned collections.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorCopiesProducedEvents()
    {
        var events = new List<RemoteEvent> { CreateEvent() };
        var result = new ServerSyncResult(new(Guid.NewGuid(), [], null, null), events);
        events.Add(CreateEvent());
        await Assert.That(result.ProducedEvents).Count().IsEqualTo(1);
    }

    /// <summary>Verifies null events are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullProducedEvents()
    {
        RemoteSyncResult syncResult = new(Guid.NewGuid(), [], null, null);
        await Assert.That(() => new ServerSyncResult(syncResult, null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Creates a representative remote event.</summary>
    /// <returns>A remote event.</returns>
    private static RemoteEvent CreateEvent() =>
        new(
            Guid.NewGuid(),
            new("sensor/temperature"),
            "cursor",
            DateTimeOffset.UnixEpoch,
            OperationId.New(),
            new("reading", 1, "application/json", ReadOnlyMemory<byte>.Empty, "sha256-empty"),
            new Dictionary<string, string>());
}
