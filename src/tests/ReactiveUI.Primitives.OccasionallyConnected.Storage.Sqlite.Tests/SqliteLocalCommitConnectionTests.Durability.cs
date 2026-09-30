// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>WAL durability retry tests for <see cref="SqliteLocalCommitConnection"/>.</summary>
public sealed partial class SqliteLocalCommitConnectionTests
{
    /// <summary>The interval used to observe an active durability wait.</summary>
    private static readonly TimeSpan BusyWaitObservationDelay = TimeSpan.FromMilliseconds(1500);

    /// <summary>The bounded interval after which the durability retry is canceled.</summary>
    private static readonly TimeSpan DurabilityCancellationDelay = TimeSpan.FromMilliseconds(100);

    /// <summary>Verifies WAL durability waits for an in-flight writer after startup selects short lock polling.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDurabilitySeesStartupWriter_ThenItRetriesPastTheCommandTimeout()
    {
        using var database = TempDatabase.Create();
        await using var writer = SqliteLocalCommitConnection.OpenConnection(database.Path);
        await using var durability = SqliteLocalCommitConnection.OpenConnection(database.Path);
        await CreateStartupLockTableAsync(writer);
        await using var transaction = (SqliteTransaction)await writer.BeginTransactionAsync();
        await InsertStartupLockAsync(writer, transaction);

        SqliteLocalCommitConnection.ConfigureLockPolling(durability);
        var configure = Task.Run(() => SqliteLocalCommitConnection.ConfigureDurabilityAfterOwnershipValidation(durability, CancellationToken.None));
        await Task.Delay(BusyWaitObservationDelay);
        await Assert.That(configure.IsCompleted).IsFalse();

        await transaction.CommitAsync();
        await configure;
    }

    /// <summary>Verifies a writer-held WAL transition observes cancellation while retrying lock contention.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDurabilityWaitIsCanceled_ThenDurabilityConfigurationIsCanceled()
    {
        using var database = TempDatabase.Create();
        await using var writer = SqliteLocalCommitConnection.OpenConnection(database.Path);
        await using var durability = SqliteLocalCommitConnection.OpenConnection(database.Path);
        await CreateStartupLockTableAsync(writer);
        await using var transaction = (SqliteTransaction)await writer.BeginTransactionAsync();
        await InsertStartupLockAsync(writer, transaction);

        SqliteLocalCommitConnection.ConfigureLockPolling(durability);
        using var cancellation = new CancellationTokenSource(DurabilityCancellationDelay);
        Action configure = () => SqliteLocalCommitConnection.ConfigureDurabilityAfterOwnershipValidation(durability, cancellation.Token);

        await Assert.That(configure).ThrowsExactly<OperationCanceledException>();
    }

    /// <summary>Verifies durability verification failures are not retried as writer lock contention.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDurabilityVerificationFails_ThenFailureIsNotRetried()
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = ":memory:", Mode = SqliteOpenMode.Memory, Pooling = false }.ToString();
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync();
        SqliteLocalCommitConnection.ConfigureLockPolling(connection);
        Action configure = () => SqliteLocalCommitConnection.ConfigureDurabilityAfterOwnershipValidation(connection, CancellationToken.None);

        await Assert.That(configure).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates the table used to hold a SQLite writer lock.</summary>
    /// <param name="connection">The writer connection.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task CreateStartupLockTableAsync(SqliteConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE startup_lock (value INTEGER NOT NULL);";
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>Writes within an open transaction to retain the SQLite writer lock.</summary>
    /// <param name="connection">The writer connection.</param>
    /// <param name="transaction">The open writer transaction.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task InsertStartupLockAsync(SqliteConnection connection, SqliteTransaction transaction)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO startup_lock (value) VALUES (1);";
        await Assert.That(command.ExecuteNonQueryAsync()).IsEqualTo(1);
    }
}
