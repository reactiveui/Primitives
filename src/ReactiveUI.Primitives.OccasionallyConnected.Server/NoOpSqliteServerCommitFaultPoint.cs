// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Ignores every SQLite server journal checkpoint.</summary>
internal sealed class NoOpSqliteServerCommitFaultPoint : ISqliteServerCommitFaultPoint
{
    /// <summary>Initializes a new instance of the <see cref="NoOpSqliteServerCommitFaultPoint"/> class.</summary>
    private NoOpSqliteServerCommitFaultPoint()
    {
    }

    /// <summary>Gets the shared no-op instance.</summary>
    internal static NoOpSqliteServerCommitFaultPoint Instance { get; } = new();

    /// <inheritdoc/>
    public void Reached(SqliteServerCommitCheckpoint checkpoint)
    {
    }
}
