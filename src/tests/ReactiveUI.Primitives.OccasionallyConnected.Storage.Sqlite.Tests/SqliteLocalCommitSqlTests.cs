// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitSql"/>.</summary>
public sealed class SqliteLocalCommitSqlTests
{
    /// <summary>The store identity used by SQL helper tests.</summary>
    private const string StoreIdentity = "client-alpha";

    /// <summary>The cursor used by SQL helper tests.</summary>
    private const string Cursor = "cursor-a";

    /// <summary>The stream identity used by SQL helper tests.</summary>
    private static readonly StreamId Stream = new("sensor/temperature");

    /// <summary>The subscription identity used by SQL helper tests.</summary>
    private static readonly SubscriptionId Subscription = SubscriptionId.New();

    /// <summary>Verifies missing stream rows fail closed when a caller requires durable stream state.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStreamRowIsMissing_ThenReadStreamStateFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using (var transaction = (SqliteTransaction)await connection.BeginTransactionAsync())
        {
            SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
            await transaction.CommitAsync();
        }

        await using var readTransaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        Action action = () => SqliteLocalCommitSql.ReadStreamState(connection, readTransaction, StoreIdentity, new("sensor/missing"));

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies inbox insertion converts primary-key duplicate violations to local apply errors.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenInboxPrimaryKeyAlreadyExists_ThenInsertInboxEventThrowsInvalidOperationException()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
        EnsureStream(connection, transaction);
        var remoteEvent = CreateRemoteEvent(Cursor);
        SqliteLocalCommitSql.InsertInboxEvent(connection, transaction, StoreIdentity, remoteEvent, DateTimeOffset.UnixEpoch);

        Action duplicate = () => SqliteLocalCommitSql.InsertInboxEvent(connection, transaction, StoreIdentity, remoteEvent, DateTimeOffset.UnixEpoch);

        await Assert.That(duplicate).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies inbox insertion converts unique-index duplicate violations to local apply errors.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenInboxUniqueIndexRejectsInsert_ThenInsertInboxEventThrowsInvalidOperationException()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
        EnsureStream(connection, transaction);
        CreateInboxServerCursorUniqueIndex(connection, transaction);
        SqliteLocalCommitSql.InsertInboxEvent(connection, transaction, StoreIdentity, CreateRemoteEvent(Cursor), DateTimeOffset.UnixEpoch);

        Action duplicate = () => SqliteLocalCommitSql.InsertInboxEvent(connection, transaction, StoreIdentity, CreateRemoteEvent(Cursor), DateTimeOffset.UnixEpoch);

        await Assert.That(duplicate).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies non-duplicate inbox constraint failures remain SQLite failures.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenInboxForeignKeyRejectsInsert_ThenInsertInboxEventPreservesSqliteException()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();
        SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);

        Action missingStream = () => SqliteLocalCommitSql.InsertInboxEvent(connection, transaction, StoreIdentity, CreateRemoteEvent(Cursor), DateTimeOffset.UnixEpoch);

        await Assert.That(missingStream).ThrowsExactly<SqliteException>();
    }

    /// <summary>Opens a raw SQLite connection with pooling disabled.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The open connection.</returns>
    private static SqliteConnection OpenRawConnection(string path)
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = path, Pooling = false }.ToString();
        var connection = new SqliteConnection(connectionString);
        connection.Open();
        return connection;
    }

    /// <summary>Creates a representative remote event.</summary>
    /// <param name="serverCursor">The server cursor.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateRemoteEvent(string serverCursor) =>
        new(Guid.NewGuid(), Stream, serverCursor, DateTimeOffset.UnixEpoch, null, CreatePayload("remote"), new Dictionary<string, string>());

    /// <summary>Creates a representative payload envelope.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreatePayload(string text) =>
        new("reading", 1, "application/json", System.Text.Encoding.UTF8.GetBytes(text), $"hash-{text}");

    /// <summary>Ensures the stream row required by inbox foreign keys exists.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void EnsureStream(SqliteConnection connection, SqliteTransaction transaction)
    {
        SqliteSubscriptionIdentitySql.InsertSubscriptionIdentityIfMissing(connection, transaction, StoreIdentity, Stream, Subscription);
        SqliteLocalCommitSql.EnsureStreamRow(connection, transaction, StoreIdentity, Stream, Subscription);
    }

    /// <summary>Creates a unique index used to exercise SQLite unique constraint mapping.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateInboxServerCursorUniqueIndex(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "CREATE UNIQUE INDEX oc_inbox_cursor_unique ON oc_inbox (store_identity, stream_id, server_cursor);";
        _ = command.ExecuteNonQuery();
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
