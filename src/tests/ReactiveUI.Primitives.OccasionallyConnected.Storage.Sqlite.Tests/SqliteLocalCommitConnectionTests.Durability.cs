// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;
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
    /// <remarks>Isolates the real writer deadline from unrelated database flushes and coverage instrumentation.</remarks>
    [Test]
    [NotInParallel]
    public async Task WhenDurabilitySeesStartupWriter_ThenItRetriesPastTheCommandTimeout()
    {
        using var database = TempDatabase.Create();
        using var writer = SqliteLocalCommitConnection.OpenConnection(database.Path);
        using var durability = SqliteLocalCommitConnection.OpenConnection(database.Path);
        CreateStartupLockTable(writer);
        using var transaction = writer.BeginTransaction();
        await InsertStartupLockAsync(writer, transaction);

        SqliteLocalCommitConnection.ConfigureLockPolling(durability);
        using var configureEntered = new ManualResetEventSlim();
        using var observerReady = new ManualResetEventSlim();
        var configure = Task.Factory.StartNew(
            ConfigureDurabilityOnDedicatedThread,
            (durability, configureEntered, observerReady),
            CancellationToken.None,
            TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default);
        var observation = Task.Factory.StartNew(
            ObserveAndReleaseStartupWriter,
            (transaction, configure, configureEntered, observerReady),
            CancellationToken.None,
            TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default);
        await Task.WhenAll(configure, observation);
        var completedWhileWriterHeld = await observation;
        await Assert.That(completedWhileWriterHeld).IsFalse();
    }

    /// <summary>Verifies a writer-held WAL transition observes cancellation while retrying lock contention.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDurabilityWaitIsCanceled_ThenDurabilityConfigurationIsCanceled()
    {
        using var database = TempDatabase.Create();
        using var writer = SqliteLocalCommitConnection.OpenConnection(database.Path);
        using var durability = SqliteLocalCommitConnection.OpenConnection(database.Path);
        CreateStartupLockTable(writer);
        using var transaction = writer.BeginTransaction();
        await InsertStartupLockAsync(writer, transaction);

        SqliteLocalCommitConnection.ConfigureLockPolling(durability);
        using var cancellation = new CancellationTokenSource();
        using var configureEntered = new ManualResetEventSlim();
        var cancel = Task.Factory.StartNew(
            CancelDurabilityOnDedicatedThread,
            (cancellation, configureEntered),
            CancellationToken.None,
            TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default);
        Action configure = () =>
        {
            configureEntered.Set();
            SqliteLocalCommitConnection.ConfigureDurabilityAfterOwnershipValidation(durability, cancellation.Token);
        };

        try
        {
            await Assert.That(configure).ThrowsExactly<OperationCanceledException>();
        }
        finally
        {
            configureEntered.Set();
            await cancel;
        }
    }

    /// <summary>Verifies durability verification failures are not retried as writer lock contention.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDurabilityVerificationFails_ThenFailureIsNotRetried()
    {
        using var connection = new SqliteDatabase(":memory:");

        SqliteLocalCommitConnection.ConfigureLockPolling(connection);
        Action configure = () => SqliteLocalCommitConnection.ConfigureDurabilityAfterOwnershipValidation(connection, CancellationToken.None);

        await Assert.That(configure).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Cancels the synchronous lock wait independently of test-runner timer scheduling.</summary>
    /// <param name="state">The cancellation source and durability-entry signal.</param>
    private static void CancelDurabilityOnDedicatedThread(object? state)
    {
        var (cancellation, entered) = ((CancellationTokenSource, ManualResetEventSlim))state!;
        entered.Wait();
        Thread.Sleep(DurabilityCancellationDelay);
        cancellation.Cancel();
    }

    /// <summary>Runs the blocking durability retry without occupying a test-runner worker.</summary>
    /// <param name="state">The durability connection, entry signal, and observer-ready signal.</param>
    private static void ConfigureDurabilityOnDedicatedThread(object? state)
    {
        var (connection, entered, observerReady) = ((SqliteDatabase, ManualResetEventSlim, ManualResetEventSlim))state!;
        observerReady.Wait();
        entered.Set();
        SqliteLocalCommitConnection.ConfigureDurabilityAfterOwnershipValidation(connection, CancellationToken.None);
    }

    /// <summary>Observes the retry and releases its writer independently of thread-pool scheduling.</summary>
    /// <param name="state">The writer transaction, durability task, entry signal, and observer-ready signal.</param>
    /// <returns>Whether durability completed while the writer was held.</returns>
    private static bool ObserveAndReleaseStartupWriter(object? state)
    {
        var (transaction, configure, entered, observerReady) = ((SqliteTransaction, Task, ManualResetEventSlim, ManualResetEventSlim))state!;
        try
        {
            observerReady.Set();
            entered.Wait();
            Thread.Sleep(BusyWaitObservationDelay);
            return configure.IsCompleted;
        }
        finally
        {
            transaction.Commit();
        }
    }

    /// <summary>Creates the table used to hold a SQLite writer lock.</summary>
    /// <param name="connection">The writer connection.</param>
    private static void CreateStartupLockTable(SqliteDatabase connection)
    {
        using var command = connection.CreateStatement();
        command.SetSql("CREATE TABLE startup_lock (value INTEGER NOT NULL);");
        _ = command.Execute();
    }

    /// <summary>Writes within an open transaction to retain the SQLite writer lock.</summary>
    /// <param name="connection">The writer connection.</param>
    /// <param name="transaction">The open writer transaction.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task InsertStartupLockAsync(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("INSERT INTO startup_lock (value) VALUES (1);");
        await Assert.That(command.Execute()).IsEqualTo(1);
    }
}
