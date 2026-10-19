// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Requests a durable quarantine marker for an affected stream payload.</summary>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq}: {Reason,nq}")]
public sealed record LocalPayloadQuarantineRequest
{
    /// <summary>Gets the affected stream identifier.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the durable subscription identifier, when known.</summary>
    public SubscriptionId? SubscriptionId { get; init; }

    /// <summary>Gets the affected local operation identifier, when any.</summary>
    public OperationId? OperationId { get; init; }

    /// <summary>Gets the affected remote event identifier, when any.</summary>
    public Guid? EventId { get; init; }

    /// <summary>Gets the source record kind.</summary>
    public required LocalPayloadQuarantineSource Source { get; init; }

    /// <summary>Gets the stable quarantine reason.</summary>
    public required LocalPayloadQuarantineReason Reason { get; init; }

    /// <summary>Gets an optional stable reason code supplied by the failing component.</summary>
    public string? ReasonCode { get; init; }

    /// <summary>Gets the payload envelope evidence, when an envelope could be formed.</summary>
    public PayloadEnvelope? Envelope { get; init; }

    /// <summary>Gets bounded evidence captured from the persisted record.</summary>
    public LocalPayloadQuarantineEvidence? Evidence { get; init; }

    /// <summary>Gets the server cursor associated with the bad record, when known.</summary>
    public string? Cursor { get; init; }

    /// <summary>Gets the time at which the quarantine was observed.</summary>
    public required DateTimeOffset ObservedAtUtc { get; init; }
}
