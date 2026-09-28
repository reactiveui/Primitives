// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests the local store startup retry after a killed SQLite writer.</summary>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The primary SQLite I/O error code.</summary>
    private const int SqliteIoError = 10;

    /// <summary>The extended SQLite write error code.</summary>
    private const int SqliteIoErrorWrite = 778;

    /// <summary>The extended SQLite truncate error code.</summary>
    private const int SqliteIoErrorTruncate = 1546;

    /// <summary>The bounded number of startup retries.</summary>
    private const int StartupRetryCount = 4;

    /// <summary>The in-memory SQLite source used by retry tests.</summary>
    private const string InMemorySource = ":memory:";

    /// <summary>The injected transient failure message.</summary>
    private const string WalTruncateMessage = "WAL truncate";

    /// <summary>Verifies a failed validation disposes its connection and retries with a fresh one.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetryValidatedInitializationConnectionRetriesWalTruncateWithFreshConnection()
    {
        var attempts = 0;
        var delays = 0;
        SqliteConnection? first = null;
        await using var recovered = SqliteLocalCommitStore.RetryValidatedInitializationConnection(
            () =>
            {
                attempts++;
                var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = InMemorySource }.ToString());
                connection.Open();
                first ??= connection;
                return connection;
            },
            _ =>
            {
                if (attempts == 1)
                {
                    throw new SqliteException(WalTruncateMessage, SqliteIoError, SqliteIoErrorTruncate);
                }
            },
            () => delays++,
            CancellationToken.None);

        await Assert.That(attempts).IsEqualTo(delays + 1);
        await Assert.That(delays).IsEqualTo(1);
        await Assert.That(first?.State).IsEqualTo(System.Data.ConnectionState.Closed);
        await Assert.That(recovered.State).IsEqualTo(System.Data.ConnectionState.Open);
    }

    /// <summary>Verifies a WAL truncate error during connection open also retries.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetryValidatedInitializationConnectionRetriesOpenFailure()
    {
        var attempts = 0;
        var delays = 0;
        await using var recovered = SqliteLocalCommitStore.RetryValidatedInitializationConnection(
            () =>
            {
                attempts++;
                return attempts == 1
                    ? throw new SqliteException(WalTruncateMessage, SqliteIoError, SqliteIoErrorTruncate)
                    : new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = InMemorySource }.ToString());
            },
            static _ => { },
            () => delays++,
            CancellationToken.None);

        await Assert.That(attempts).IsEqualTo(delays + 1);
        await Assert.That(delays).IsEqualTo(1);
    }

    /// <summary>Verifies another I/O error during validation closes the failed connection.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetryValidatedInitializationConnectionDisposesFailedValidation()
    {
        var delays = 0;
        SqliteConnection? failed = null;
        Action open = () => _ = SqliteLocalCommitStore.RetryValidatedInitializationConnection(
            () =>
            {
                failed = new(new SqliteConnectionStringBuilder { DataSource = InMemorySource }.ToString());
                failed.Open();
                return failed;
            },
            static _ => throw new SqliteException("write failure", SqliteIoError, SqliteIoErrorWrite),
            () => delays++,
            CancellationToken.None);

        await Assert.That(open).ThrowsExactly<SqliteException>();
        await Assert.That(failed?.State).IsEqualTo(System.Data.ConnectionState.Closed);
        await Assert.That(delays).IsEqualTo(0);
    }

    /// <summary>Verifies unrelated I/O errors surface without another attempt.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetryValidatedInitializationConnectionPropagatesOtherIoErrors()
    {
        var attempts = 0;
        var delays = 0;
        Action open = () => _ = SqliteLocalCommitStore.RetryValidatedInitializationConnection(
            () =>
            {
                attempts++;
                throw new SqliteException("write failure", SqliteIoError, SqliteIoErrorWrite);
            },
            static _ => { },
            () => delays++,
            CancellationToken.None);

        await Assert.That(open).ThrowsExactly<SqliteException>();
        await Assert.That(attempts).IsEqualTo(1);
        await Assert.That(delays).IsEqualTo(0);
    }

    /// <summary>Verifies a lasting WAL truncate error surfaces after the retry bound.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetryValidatedInitializationConnectionPropagatesPersistentWalTruncate()
    {
        var attempts = 0;
        var delays = 0;
        Action open = () => _ = SqliteLocalCommitStore.RetryValidatedInitializationConnection(
            () =>
            {
                attempts++;
                throw new SqliteException(WalTruncateMessage, SqliteIoError, SqliteIoErrorTruncate);
            },
            static _ => { },
            () => delays++,
            CancellationToken.None);

        await Assert.That(open).ThrowsExactly<SqliteException>();
        await Assert.That(attempts).IsEqualTo(StartupRetryCount + 1);
        await Assert.That(delays).IsEqualTo(StartupRetryCount);
    }
}
