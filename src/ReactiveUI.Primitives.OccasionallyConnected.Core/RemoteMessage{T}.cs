// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a decoded remote message after local commit and deduplication.</summary>
/// <typeparam name="T">The decoded message value type.</typeparam>
/// <param name="EventId">The server event identifier.</param>
/// <param name="StreamId">The stream that produced the message.</param>
/// <param name="ServerCursor">The server cursor assigned to the message.</param>
/// <param name="CommittedAtUtc">The server commit timestamp.</param>
/// <param name="Value">The decoded message value.</param>
[System.Diagnostics.DebuggerDisplay("{EventId,nq} {StreamId,nq}")]
public sealed record RemoteMessage<T>(
    Guid EventId,
    StreamId StreamId,
    string ServerCursor,
    DateTimeOffset CommittedAtUtc,
    T Value);
