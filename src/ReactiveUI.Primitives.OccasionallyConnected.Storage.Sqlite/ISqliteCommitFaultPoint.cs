// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Observes named SQLite write checkpoints so crash tests can stop a process at an exact durability boundary.</summary>
/// <remarks>
/// Production code always uses <see cref="NoOpSqliteCommitFaultPoint.Instance"/>. Only the internal
/// <see cref="SqliteLocalStoreAdapter"/> constructor can supply another implementation.
/// </remarks>
internal interface ISqliteCommitFaultPoint
{
    /// <summary>Reports that the store reached a named write checkpoint.</summary>
    /// <param name="checkpoint">The reached checkpoint.</param>
    /// <remarks>The call runs on the SQLite worker thread while the store gate is held.</remarks>
    void Reached(SqliteCommitCheckpoint checkpoint);
}
