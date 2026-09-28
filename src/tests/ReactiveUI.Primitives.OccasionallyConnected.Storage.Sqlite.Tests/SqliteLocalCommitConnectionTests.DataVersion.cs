// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
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
        var connectionString = new SqliteConnectionStringBuilder { DataSource = database.Path, Pooling = false }.ToString();
        await using var observed = new SqliteConnection(connectionString);
        await using var writer = new SqliteConnection(connectionString);
        await observed.OpenAsync();
        await writer.OpenAsync();

        long before;
        await using (var transaction = (SqliteTransaction)await observed.BeginTransactionAsync())
        {
            before = SqliteLocalCommitConnection.GetDataVersion(observed, transaction);
        }

        await using (var command = writer.CreateCommand())
        {
            command.CommandText = "CREATE TABLE external_commit (value INTEGER NOT NULL);";
            _ = await command.ExecuteNonQueryAsync();
        }

        await using var later = (SqliteTransaction)await observed.BeginTransactionAsync();
        var after = SqliteLocalCommitConnection.GetDataVersion(observed, later);

        await Assert.That(after).IsNotEqualTo(before);
    }
}
