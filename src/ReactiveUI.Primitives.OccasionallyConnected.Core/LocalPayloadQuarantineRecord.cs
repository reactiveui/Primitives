// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a durable quarantine marker for one affected stream.</summary>
/// <param name="QuarantineId">The quarantine marker identifier.</param>
/// <param name="StreamId">The affected stream identifier.</param>
/// <param name="SubscriptionId">The durable subscription identifier, when known.</param>
/// <param name="OperationId">The affected local operation identifier, when any.</param>
/// <param name="EventId">The affected remote event identifier, when any.</param>
/// <param name="Source">The source record kind.</param>
/// <param name="Reason">The stable quarantine reason.</param>
/// <param name="ReasonCode">The optional stable reason code.</param>
/// <param name="Cursor">The server cursor associated with the bad record, when known.</param>
/// <param name="Evidence">The bounded payload evidence.</param>
/// <param name="ObservedAtUtc">The time at which the quarantine was observed.</param>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq}: {Reason,nq}")]
public sealed record LocalPayloadQuarantineRecord(
    Guid QuarantineId,
    StreamId StreamId,
    SubscriptionId? SubscriptionId,
    OperationId? OperationId,
    Guid? EventId,
    LocalPayloadQuarantineSource Source,
    LocalPayloadQuarantineReason Reason,
    string? ReasonCode,
    string? Cursor,
    LocalPayloadQuarantineEvidence Evidence,
    DateTimeOffset ObservedAtUtc);
