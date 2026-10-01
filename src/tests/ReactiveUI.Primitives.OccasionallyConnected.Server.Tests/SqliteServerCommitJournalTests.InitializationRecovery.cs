// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests the journal startup retry after a killed SQLite writer.</summary>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>The primary SQLite I/O error code.</summary>
    private const int SqliteIoError = 10;

    /// <summary>The extended SQLite write error code.</summary>
    private const int SqliteIoErrorWrite = 778;

    /// <summary>The extended SQLite truncate error code.</summary>
    private const int SqliteIoErrorTruncate = 1546;

    /// <summary>The bounded number of startup retries.</summary>
    private const int StartupRetryCount = 4;

    /// <summary>Verifies a transient WAL truncate error opens a fresh connection.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetryInitializationConnectionRetriesWalTruncateWithFreshConnection()
    {
        var attempts = 0;
        var delays = 0;
        using var connection = SqliteServerCommitJournal.RetryInitializationConnection(
            () =>
            {
                attempts++;
                return attempts == 1
                    ? throw new SqliteDatabaseException("WAL truncate", SqliteIoError, SqliteIoErrorTruncate)
                    : new SqliteDatabase(":memory:");
            },
            () => delays++);

        await Assert.That(attempts).IsEqualTo(delays + 1);
        await Assert.That(delays).IsEqualTo(1);
    }

    /// <summary>Verifies unrelated I/O errors are not retried.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetryInitializationConnectionPropagatesOtherIoErrors()
    {
        var attempts = 0;
        var delays = 0;
        Action open = () => _ = SqliteServerCommitJournal.RetryInitializationConnection(
            () =>
            {
                attempts++;
                throw new SqliteDatabaseException("write failure", SqliteIoError, SqliteIoErrorWrite);
            },
            () => delays++);

        await Assert.That(open).ThrowsExactly<SqliteDatabaseException>();
        await Assert.That(attempts).IsEqualTo(1);
        await Assert.That(delays).IsEqualTo(0);
    }

    /// <summary>Verifies a lasting WAL truncate error surfaces after the retry bound.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetryInitializationConnectionPropagatesPersistentWalTruncate()
    {
        var attempts = 0;
        var delays = 0;
        Action open = () => _ = SqliteServerCommitJournal.RetryInitializationConnection(
            () =>
            {
                attempts++;
                throw new SqliteDatabaseException("WAL truncate", SqliteIoError, SqliteIoErrorTruncate);
            },
            () => delays++);

        await Assert.That(open).ThrowsExactly<SqliteDatabaseException>();
        await Assert.That(attempts).IsEqualTo(StartupRetryCount + 1);
        await Assert.That(delays).IsEqualTo(StartupRetryCount);
    }
}
