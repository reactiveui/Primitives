// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemoteMessage{T}"/>.</summary>
public sealed class RemoteMessageTests
{
    /// <summary>The representative measurement.</summary>
    private const int Measurement = 42;

    /// <summary>The representative server cursor.</summary>
    private const string ServerCursor = "cursor-1";

    /// <summary>Verifies generic values participate in record value equality and preserved constructor state.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecordUsesValueSemanticsForGenericValue()
    {
        var eventId = Guid.NewGuid();
        var streamId = new StreamId("sensor/temperature");
        var value = new Reading(Measurement, "C");
        var first = new RemoteMessage<Reading>(eventId, streamId, ServerCursor, DateTimeOffset.UnixEpoch, value);
        var second = new RemoteMessage<Reading>(eventId, streamId, ServerCursor, DateTimeOffset.UnixEpoch, value);

        await Assert.That(first).IsEqualTo(second);
        await Assert.That(first.Value).IsEqualTo(value);
        await Assert.That(first.ServerCursor).IsEqualTo(ServerCursor);
        await Assert.That(first.EventId).IsEqualTo(eventId);
        await Assert.That(first.StreamId).IsEqualTo(streamId);
        await Assert.That(first.CommittedAtUtc).IsEqualTo(DateTimeOffset.UnixEpoch);
    }

    /// <summary>Defines a representative decoded value.</summary>
    /// <param name="Measurement">The measured value.</param>
    /// <param name="Unit">The measurement unit.</param>
    private sealed record Reading(int Measurement, string Unit);
}
