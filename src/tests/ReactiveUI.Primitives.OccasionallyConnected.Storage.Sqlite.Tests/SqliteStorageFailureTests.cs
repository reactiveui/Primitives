// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests <see cref="SqliteStorageFailure"/>.</summary>
public sealed class SqliteStorageFailureTests
{
    /// <summary>The SQLite constraint error code.</summary>
    private const int SqliteConstraint = 19;

    /// <summary>The number of bytes that forces SQLite to request a new page.</summary>
    private const int OversizedBlobBytes = 65_536;

    /// <summary>Verifies a full disk maps to a durable storage failure and keeps its source exception.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSqliteReportsFull_ThenWriteFailureIsTyped()
    {
        var source = new SqliteException("database or disk is full", SqliteStorageFailure.SqliteFull);
        DurableStorageException? observed = null;
        try
        {
            _ = SqliteStorageFailure.Run<int>(_ => throw source, CancellationToken.None);
        }
        catch (DurableStorageException exception)
        {
            observed = exception;
        }

        await Assert.That(observed).IsNotNull();
        await Assert.That(observed!.Failure).IsEqualTo(DurableStorageFailure.StorageFull);
        await Assert.That(observed.InnerException).IsSameReferenceAs(source);
    }

    /// <summary>Verifies an I/O error is classified while unrelated SQLite errors retain their type.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSqliteReportsIoError_ThenOnlyStorageErrorsAreMapped()
    {
        await Assert.That(SqliteStorageFailure.TryClassify(SqliteStorageFailure.SqliteIoError, out var failure)).IsTrue();
        await Assert.That(failure).IsEqualTo(DurableStorageFailure.InputOutput);

        var constraint = new SqliteException("constraint", SqliteConstraint);
        Action action = () => _ = SqliteStorageFailure.Run<int>(_ => throw constraint, CancellationToken.None);
        await Assert.That(action).ThrowsExactly<SqliteException>();
    }

    /// <summary>Verifies an I/O error maps to its kind and an existing durable failure is left intact.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStorageFailureIsAlreadyTyped_ThenItIsNotWrappedAgain()
    {
        var source = new SqliteException("I/O error", SqliteStorageFailure.SqliteIoError);
        var typed = new DurableStorageException("storage failed", DurableStorageFailure.InputOutput, source);
        var wrapped = new InvalidOperationException("outer", typed);
        var result = SqliteStorageFailure.FindStorageFailure(wrapped, out var failure);

        await Assert.That(result).IsNull();
        await Assert.That(failure).IsEqualTo(DurableStorageFailure.Unknown);
        await Assert.That(DurableStorageException.IsInChain(wrapped)).IsTrue();

        DurableStorageException? mapped = null;
        try
        {
            _ = SqliteStorageFailure.Run<int>(_ => throw source, CancellationToken.None);
        }
        catch (DurableStorageException exception)
        {
            mapped = exception;
        }

        await Assert.That(mapped).IsNotNull();
        await Assert.That(mapped!.Failure).IsEqualTo(DurableStorageFailure.InputOutput);
    }

    /// <summary>Verifies a real SQLite page limit failure is typed and its transaction rolls back.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSqlitePageLimitIsReached_ThenWriteRollsBack()
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = ":memory:" }.ToString();
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync();
        await using (var setup = connection.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE payloads (value BLOB NOT NULL); PRAGMA max_page_count = 2;";
            _ = await setup.ExecuteNonQueryAsync();
        }

        DurableStorageException? observed = null;
        await using (var transaction = (SqliteTransaction)await connection.BeginTransactionAsync())
        {
            try
            {
                _ = SqliteStorageFailure.Run(
                    token =>
                    {
                        token.ThrowIfCancellationRequested();
                        using var command = connection.CreateCommand();
                        command.Transaction = transaction;
                        command.CommandText = "INSERT INTO payloads (value) VALUES (zeroblob($size));";
                        _ = command.Parameters.AddWithValue("$size", OversizedBlobBytes);
                        return command.ExecuteNonQuery();
                    },
                    CancellationToken.None);
            }
            catch (DurableStorageException exception)
            {
                observed = exception;
            }
        }

        await Assert.That(observed).IsNotNull();
        await Assert.That(observed!.Failure).IsEqualTo(DurableStorageFailure.StorageFull);
        await using var count = connection.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM payloads;";
        await Assert.That(Convert.ToInt64(await count.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture)).IsEqualTo(0);
    }
}
