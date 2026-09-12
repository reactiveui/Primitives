// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitConnection"/>.</summary>
public sealed class SqliteLocalCommitConnectionTests
{
    /// <summary>The SQLite busy error code.</summary>
    private const int SqliteBusy = 5;

    /// <summary>The SQLite locked error code.</summary>
    private const int SqliteLocked = 6;

    /// <summary>A non-locking SQLite constraint error code.</summary>
    private const int SqliteConstraint = 19;

    /// <summary>Verifies failed opens surface SQLite failure after cleaning up the connection.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDatabasePathIsDirectory_ThenOpenConnectionThrowsSqliteException()
    {
        using var database = TempDatabase.Create();
        _ = Directory.CreateDirectory(database.Path);

        Action action = () => SqliteLocalCommitConnection.OpenConnection(database.Path);

        await Assert.That(action).ThrowsExactly<SqliteException>();
    }

    /// <summary>Verifies only busy and locked SQLite errors are classified as retryable lock contention.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSqliteErrorIsBusyOrLocked_ThenItIsRetryableContention()
    {
        var busy = new SqliteException("busy", SqliteBusy);
        var locked = new SqliteException("locked", SqliteLocked);
        var constraint = new SqliteException("constraint", SqliteConstraint);

        await Assert.That(SqliteLocalCommitConnection.IsBusyOrLocked(busy)).IsTrue();
        await Assert.That(SqliteLocalCommitConnection.IsBusyOrLocked(locked)).IsTrue();
        await Assert.That(SqliteLocalCommitConnection.IsBusyOrLocked(constraint)).IsFalse();
    }

    /// <summary>Temporary database file helper.</summary>
    private sealed class TempDatabase : IDisposable
    {
        /// <summary>The temporary directory path.</summary>
        private readonly string _directory;

        /// <summary>Initializes a new instance of the <see cref="TempDatabase"/> class.</summary>
        /// <param name="directory">The temporary directory path.</param>
        private TempDatabase(string directory)
        {
            _directory = directory;
            Path = System.IO.Path.Combine(directory, "local.db");
        }

        /// <summary>Gets the SQLite database path.</summary>
        public string Path { get; }

        /// <summary>Creates a new temporary database helper.</summary>
        /// <returns>The temporary database helper.</returns>
        public static TempDatabase Create()
        {
            var directory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "rxui-oc-sqlite-local", Guid.NewGuid().ToString("N"));
            _ = System.IO.Directory.CreateDirectory(directory);
            return new(directory);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (!System.IO.Directory.Exists(_directory))
            {
                return;
            }

            System.IO.Directory.Delete(_directory, true);
        }
    }
}
