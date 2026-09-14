// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Collects immutable stream snapshot fields without extending positional constructor arity.</summary>
internal sealed record ServerCommitSnapshotOptions
{
    /// <summary>Gets the authenticated stream key.</summary>
    internal required ServerStreamKey StreamKey { get; init; }

    /// <summary>Gets the stream revision.</summary>
    internal required long Revision { get; init; }

    /// <summary>Gets the optional canonical server state.</summary>
    internal required ServerState? State { get; init; }

    /// <summary>Gets the optional last-write stamp.</summary>
    internal required ServerWriteStamp? LastWriteStamp { get; init; }

    /// <summary>Gets the complete requested replay entries.</summary>
    internal required IReadOnlyList<ServerLedgerEntry> Entries { get; init; }

    /// <summary>Gets the last retained server cursor.</summary>
    internal required string? LastCursor { get; init; }

    /// <summary>Gets the last sidecar event sequence.</summary>
    internal required long LastEventSequence { get; init; }

    /// <summary>Gets the complete durable receive group frontier.</summary>
    internal required long LastGroupSequence { get; init; }
}
