// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests operation-local transaction and cancellation cleanup.</summary>
public sealed class SqliteStoreConnectionScopeTests
{
    /// <summary>Checks scope cleanup rolls back a canceled transaction once.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ScopeRollsBackCanceledWorkAndLeavesConnectionUsable()
    {
        using var database = new SqliteDatabase(":memory:");
        database.Execute("CREATE TABLE data (value INTEGER);");
        var actual = RunCanceledScope(database);
        await Assert.That(database.Transaction).IsNull();
        await Assert.That(actual).IsEqualTo(0L);
        await Assert.That(database.IsDisposed).IsFalse();
    }

    /// <summary>Checks a failed native cleanup retires the connection once.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FailedCleanupRetiresConnectionOnce()
    {
        var database = new SqliteDatabase(":memory:");
        try
        {
            var retired = false;
            var scope = new SqliteStoreConnectionScope(database, failed =>
            {
                retired = true;
                failed.Dispose();
            });
            database.Dispose();
            Exception? failure = null;
            try
            {
                scope.Dispose();
            }
            catch (ObjectDisposedException exception)
            {
                failure = exception;
            }

            scope.Dispose();
            await Assert.That(failure).IsNotNull();
            await Assert.That(retired).IsTrue();
        }
        finally
        {
            database.Dispose();
        }
    }

    /// <summary>Runs a synchronous borrower so cancellation cleanup rolls back native work.</summary>
    /// <param name="database">The native database.</param>
    /// <returns>The row count after rollback.</returns>
    private static object? RunCanceledScope(SqliteDatabase database)
    {
        using var cancellation = new CancellationTokenSource();
        var scope = new SqliteStoreConnectionScope(database, static failed => failed.Dispose());
        database.SetCancellation(cancellation.Token);
        _ = database.BeginTransaction();
        database.Execute("INSERT INTO data VALUES (1);");
        cancellation.Cancel();
        scope.Dispose();
        scope.Dispose();
        using var count = database.CreateStatement();
        count.SetSql("SELECT COUNT(*) FROM data;");
        return count.Scalar();
    }
}
