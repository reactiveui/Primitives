// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests operation-local transaction, cancellation and gate cleanup.</summary>
public sealed class SqliteStoreConnectionScopeTests
{
    /// <summary>Checks scope cleanup rolls back a canceled transaction and releases its gate once.</summary>
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

    /// <summary>Checks a failed native cleanup retires the connection and releases the borrower gate.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FailedCleanupRetiresConnectionAndReleasesGate()
    {
        var database = new SqliteDatabase(":memory:");
        try
        {
            var gate = new object();
            var retired = false;
            Monitor.Enter(gate);
            var scope = new SqliteStoreConnectionScope(database, gate, failed =>
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

            var ownsGate = Monitor.IsEntered(gate);
            scope.Dispose();
            await Assert.That(failure).IsNotNull();
            await Assert.That(retired).IsTrue();
            await Assert.That(ownsGate).IsFalse();
        }
        finally
        {
            database.Dispose();
        }
    }

    /// <summary>Runs a synchronous borrower so cancellation cleanup releases its thread-affine gate.</summary>
    /// <param name="database">The native database.</param>
    /// <returns>The row count after rollback.</returns>
    private static object? RunCanceledScope(SqliteDatabase database)
    {
        using var cancellation = new CancellationTokenSource();
        var gate = new object();
        Monitor.Enter(gate);
        var scope = new SqliteStoreConnectionScope(database, gate, static failed => failed.Dispose());
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
