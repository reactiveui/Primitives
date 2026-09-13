// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Describes a server-owned event proposal before cursor and timestamp stamping.</summary>
[System.Diagnostics.DebuggerDisplay("{EventId,nq} {Payload.ContractId,nq}")]
public sealed record ServerProducedEvent
{
    /// <summary>Gets the stable event identifier.</summary>
    public required Guid EventId { get; init; }

    /// <summary>Gets the event payload.</summary>
    public required PayloadEnvelope Payload { get; init; }

    /// <summary>Gets the event metadata.</summary>
    public IReadOnlyDictionary<string, string> Metadata
    {
        get;
        init => field = ServerCollectionCopy.Dictionary(value, nameof(Metadata));
    } = ServerCollectionCopy.EmptyDictionary;
}
