// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores validated immutable inputs needed inside the journal gate.</summary>
internal sealed class ServerCommitValidationResult
{
    /// <summary>Gets or sets the stream key.</summary>
    internal required ServerStreamKey StreamKey { get; init; }

    /// <summary>Gets or sets the expected revision.</summary>
    internal required long ExpectedRevision { get; init; }

    /// <summary>Gets or sets the optional new state.</summary>
    internal required ServerState? NewState { get; init; }

    /// <summary>Gets or sets the optional new write stamp.</summary>
    internal required ServerWriteStamp? NewWriteStamp { get; init; }

    /// <summary>Gets or sets the prepared entries.</summary>
    internal required ServerLedgerEntry[] Entries { get; init; }

    /// <summary>Gets or sets the operation keys to return after commit.</summary>
    internal required ServerOperationKey[] OperationKeys { get; init; }

    /// <summary>Gets or sets the per-entry logical bytes.</summary>
    internal required long[] EntryBytes { get; init; }

    /// <summary>Gets or sets the total ledger logical bytes.</summary>
    internal required long LedgerBytes { get; init; }

    /// <summary>Gets or sets the event count.</summary>
    internal required int EventCount { get; init; }

    /// <summary>Gets or sets the final server cursor produced by the commit.</summary>
    internal required string? LastCursor { get; init; }

    /// <summary>Gets or sets the final server cursor logical bytes.</summary>
    internal required long LastCursorBytes { get; init; }

    /// <summary>Gets or sets the new state logical bytes.</summary>
    internal required long StateBytes { get; init; }
}
