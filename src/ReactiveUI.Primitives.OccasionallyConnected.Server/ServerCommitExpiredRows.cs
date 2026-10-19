// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores projected expired rows for an atomic cleanup.</summary>
internal sealed class ServerCommitExpiredRows
{
    /// <summary>Gets terminal ledger rows to remove.</summary>
    internal List<ServerCommitLedgerRow> LedgerRows { get; } = [];

    /// <summary>Gets event rows to remove.</summary>
    internal List<ServerCommitEventRow> EventRows { get; } = [];

    /// <summary>Gets or sets logical bytes to reclaim.</summary>
    internal long LogicalBytes { get; set; }
}
