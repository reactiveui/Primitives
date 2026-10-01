// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests native transaction ownership and rollback.</summary>
public sealed class SqliteTransactionTests
{
    /// <summary>The native in-memory database path.</summary>
    private const string MemoryPath = ":memory:";

    /// <summary>Verifies disposing an uncommitted transaction preserves the previously committed state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeRollsBackAndCommitEndsTransactionOwnership()
    {
        using var database = new SqliteDatabase(MemoryPath);
        database.Execute("CREATE TABLE data (value INTEGER);");
        using (var transaction = database.BeginTransaction())
        {
            database.Execute("INSERT INTO data VALUES (1);");
        }

        using var statement = database.CreateStatement();
        statement.SetSql("SELECT count(*) FROM data;");
        await Assert.That(statement.Scalar()).IsEqualTo(0L);
        using var committed = database.BeginTransaction();
        statement.UseTransaction(committed);
        database.Execute("INSERT INTO data VALUES (1);");
        committed.Commit();
        await Assert.That(committed.Connection).IsNull();
        await Assert.That(() => committed.Commit()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => committed.Rollback()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => statement.UseTransaction(committed)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(statement.Scalar()).IsEqualTo(1L);
    }

    /// <summary>Verifies nesting and mismatched database transactions are rejected before native execution.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task NestedAndForeignTransactionsAreRejected()
    {
        using var first = new SqliteDatabase(MemoryPath);
        using var second = new SqliteDatabase(MemoryPath);
        using var transaction = first.BeginTransaction(deferred: true);
        using var statement = second.CreateStatement();
        await Assert.That(() => first.BeginTransaction()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => statement.UseTransaction(transaction)).ThrowsExactly<InvalidOperationException>();
        statement.UseTransaction(null);
        transaction.Rollback();
    }
}
