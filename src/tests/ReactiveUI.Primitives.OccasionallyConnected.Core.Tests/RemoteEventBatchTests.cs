// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemoteEventBatch"/>.</summary>
public sealed class RemoteEventBatchTests
{
    /// <summary>The stream name.</summary>
    private const string StreamName = "sensor/temperature";

    /// <summary>Verifies events are copied from caller-owned collections.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorCopiesEvents()
    {
        var events = new List<RemoteEvent> { CreateEvent() };
        var batch = new RemoteEventBatch(Guid.NewGuid(), new(StreamName), "cursor-1", "cursor-2", events);
        events.Add(CreateEvent());
        await Assert.That(batch.Events).Count().IsEqualTo(1);
    }

    /// <summary>Verifies null events are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullEvents()
    {
        var constructor = typeof(RemoteEventBatch).GetConstructors().Single();
        var exception = await Assert.That(() => constructor.Invoke([Guid.NewGuid(), new StreamId(StreamName), null, "cursor-1", null]))
            .ThrowsExactly<TargetInvocationException>();

        await Assert.That(exception?.InnerException).IsTypeOf<ArgumentNullException>();
    }

    /// <summary>Creates a representative remote event.</summary>
    /// <returns>A remote event.</returns>
    private static RemoteEvent CreateEvent() =>
        new(
            Guid.NewGuid(),
            new(StreamName),
            "cursor",
            DateTimeOffset.UnixEpoch,
            OperationId.New(),
            new("reading", 1, "application/json", ReadOnlyMemory<byte>.Empty, "sha256-empty"),
            new Dictionary<string, string>());
}
