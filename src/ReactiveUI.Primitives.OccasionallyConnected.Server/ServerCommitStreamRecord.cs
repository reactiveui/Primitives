// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores retained state for one journal stream.</summary>
internal sealed class ServerCommitStreamRecord
{
    /// <summary>Gets or sets the stream revision.</summary>
    internal long Revision { get; set; }

    /// <summary>Gets or sets the canonical state.</summary>
    internal ServerState? State { get; set; }

    /// <summary>Gets or sets the last-write stamp.</summary>
    internal ServerWriteStamp? LastWriteStamp { get; set; }

    /// <summary>Gets or sets the retained state byte count.</summary>
    internal long StateBytes { get; set; }

    /// <summary>Gets or sets the last cursor.</summary>
    internal string? LastCursor { get; set; }

    /// <summary>Gets or sets the retained last cursor byte count.</summary>
    internal long LastCursorBytes { get; set; }

    /// <summary>Gets or sets the last event sequence.</summary>
    internal long LastEventSequence { get; set; }

    /// <summary>Gets or sets the last complete operation group sequence.</summary>
    internal long LastGroupSequence { get; set; }

    /// <summary>Gets or sets a value indicating whether earlier group order is not reconstructable.</summary>
    internal bool HasReceiveHistoryGap { get; set; }

    /// <summary>Gets the terminal ledger rows.</summary>
    internal Dictionary<ServerOperationKey, ServerCommitLedgerRow> Ledger { get; } = [];

    /// <summary>Gets retained terminal rows that have durable receive group order.</summary>
    internal List<ServerCommitLedgerRow> Groups { get; } = [];

    /// <summary>Gets the retained event rows.</summary>
    internal List<ServerCommitEventRow> Events { get; } = [];

    /// <summary>Gets the retained event identifiers.</summary>
    internal HashSet<Guid> EventIds { get; } = [];

    /// <summary>Gets the retained event cursors.</summary>
    internal HashSet<string> Cursors { get; } = [with(StringComparer.Ordinal)];
}
