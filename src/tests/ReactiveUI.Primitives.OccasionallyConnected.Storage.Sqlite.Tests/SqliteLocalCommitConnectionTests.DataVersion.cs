// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests SQLite data-version observation.</summary>
public sealed partial class SqliteLocalCommitConnectionTests
{
    /// <summary>Verifies another connection's commit advances the connection-local version.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task DataVersionChangesAfterAnotherConnectionCommits()
    {
        using var database = TempDatabase.Create();
        using var observed = new SqliteDatabase(database.Path);
        using var writer = new SqliteDatabase(database.Path);

        long before;
        using (var transaction = observed.BeginTransaction())
        {
            before = SqliteLocalCommitConnection.GetDataVersion(observed, transaction);
        }

        using (var command = writer.CreateStatement())
        {
            command.SetSql("CREATE TABLE external_commit (value INTEGER NOT NULL);");
            _ = command.Execute();
        }

        using var later = observed.BeginTransaction();
        var after = SqliteLocalCommitConnection.GetDataVersion(observed, later);

        await Assert.That(after).IsNotEqualTo(before);
    }
}
