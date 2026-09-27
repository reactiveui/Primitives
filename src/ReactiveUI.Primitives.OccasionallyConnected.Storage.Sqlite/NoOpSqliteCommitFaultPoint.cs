// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Ignores every SQLite write checkpoint.</summary>
internal sealed class NoOpSqliteCommitFaultPoint : ISqliteCommitFaultPoint
{
    /// <summary>Initializes a new instance of the <see cref="NoOpSqliteCommitFaultPoint"/> class.</summary>
    private NoOpSqliteCommitFaultPoint()
    {
    }

    /// <summary>Gets the shared no-op instance.</summary>
    internal static NoOpSqliteCommitFaultPoint Instance { get; } = new();

    /// <inheritdoc/>
    public void Reached(SqliteCommitCheckpoint checkpoint)
    {
    }
}
