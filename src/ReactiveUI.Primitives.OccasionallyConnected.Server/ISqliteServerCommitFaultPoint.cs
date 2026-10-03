// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Observes named SQLite server journal checkpoints so crash tests can stop a process at an exact durability boundary.</summary>
/// <remarks>
/// Production code always uses <see cref="NoOpSqliteServerCommitFaultPoint.Instance"/>. Only the internal
/// <see cref="SqliteServerCommitJournal"/> constructor can supply another implementation.
/// </remarks>
internal interface ISqliteServerCommitFaultPoint
{
    /// <summary>Reports that the journal reached a named write checkpoint.</summary>
    /// <param name="checkpoint">The reached checkpoint.</param>
    void Reached(SqliteServerCommitCheckpoint checkpoint);
}
