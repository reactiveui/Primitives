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

    /// <summary>Verifies the trusted version belongs to the read snapshot, not a later external WAL commit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DataVersionPinsReadSnapshotUntilTransactionEnds()
    {
        using var database = TempDatabase.Create();
        using var writer = new SqliteDatabase(database.Path);
        writer.Execute("PRAGMA journal_mode = WAL; CREATE TABLE data (value INTEGER); INSERT INTO data VALUES (1);");
        using var observed = new SqliteDatabase(database.Path);
        long before;
        long during;
        object? snapshotValue;
        using (var transaction = observed.BeginTransaction(deferred: true))
        {
            before = SqliteLocalCommitConnection.GetDataVersion(observed, transaction);
            writer.Execute("UPDATE data SET value = 2;");
            during = SqliteLocalCommitConnection.GetDataVersion(observed, transaction);
            using var selected = observed.CreateStatement();
            selected.UseTransaction(transaction);
            selected.SetSql("SELECT value FROM data;");
            snapshotValue = selected.Scalar();
            transaction.Commit();
        }

        using var later = observed.BeginTransaction(deferred: true);
        var after = SqliteLocalCommitConnection.GetDataVersion(observed, later);
        await Assert.That(during).IsEqualTo(before);
        await Assert.That(snapshotValue).IsEqualTo(1L);
        await Assert.That(after).IsNotEqualTo(before);
    }
}
