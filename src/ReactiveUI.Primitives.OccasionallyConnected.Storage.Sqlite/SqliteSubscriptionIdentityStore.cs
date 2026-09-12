// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Data;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Stores durable subscription identities in SQLite.</summary>
internal sealed class SqliteSubscriptionIdentityStore : IDisposable
{
    /// <summary>The supported SQLite schema version.</summary>
    private const int CurrentSchemaVersion = 1;

    /// <summary>The metadata key for the schema version.</summary>
    private const string SchemaVersionKey = "schema_version";

    /// <summary>The invalid schema exception message.</summary>
    private const string InvalidSchemaMessage = "The SQLite identity schema is invalid.";

    /// <summary>The metadata table name.</summary>
    private const string MetadataTableName = "oc_metadata";

    /// <summary>The subscription identity table name.</summary>
    private const string SubscriptionIdentitiesTableName = "oc_subscription_identities";

    /// <summary>The SQL definition for the metadata table.</summary>
    private const string MetadataTableSql = "CREATE TABLE oc_metadata (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);";

    /// <summary>The SQL definition for the subscription identity table.</summary>
    private const string SubscriptionIdentitiesTableSql = """
        CREATE TABLE oc_subscription_identities (
            store_identity TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            subscription_id TEXT NOT NULL,
            PRIMARY KEY (store_identity, stream_id));
        """;

    /// <summary>The SQLite database path.</summary>
    private readonly string _databasePath;

    /// <summary>The per-instance gate.</summary>
    private readonly Lock _gate = new();

    /// <summary>The initialized durable store identity partition.</summary>
    private string? _storeIdentity;

    /// <summary>A value indicating whether this instance has been disposed.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="SqliteSubscriptionIdentityStore"/> class.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <exception cref="ArgumentNullException"><paramref name="databasePath"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="databasePath"/> is empty or is not a real file path.</exception>
    internal SqliteSubscriptionIdentityStore(string databasePath)
    {
        ArgumentExceptionHelper.ThrowIfNull(databasePath);
        ThrowIfBlank(databasePath, nameof(databasePath), "The SQLite database path cannot be empty.");
        ThrowIfUnsupportedPath(databasePath);

        _databasePath = Path.GetFullPath(databasePath);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
        }
    }

    /// <summary>Initializes the SQLite identity schema.</summary>
    /// <param name="initialization">The initialization requirements.</param>
    /// <param name="cancellationToken">The token used to cancel before persistence commits.</param>
    /// <exception cref="ArgumentNullException"><paramref name="initialization"/> is null.</exception>
    /// <exception cref="ArgumentException">The initialization requirements are invalid.</exception>
    /// <exception cref="InvalidOperationException">The requested or existing schema is not supported.</exception>
    /// <exception cref="NotSupportedException">Authenticated encryption at rest is required but unavailable.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled before a commit.</exception>
    internal void Initialize(LocalStoreInitialization initialization, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(initialization);
        ValidateInitialization(initialization);
        if (initialization.RequireAuthenticatedEncryptionAtRest)
        {
            throw new NotSupportedException("SQLite authenticated encryption at rest has not been configured for this store.");
        }

        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            ThrowIfDisposed();
            ThrowIfStoreIdentityConflicts(initialization.StoreIdentity);
            cancellationToken.ThrowIfCancellationRequested();
            EnsureDirectoryExists();

            using var connection = OpenConnection();
            SqliteConnectionSettings.ConfigureBusyTimeout(connection);
            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
            var userVersion = GetUserVersion(connection, transaction);
            if (userVersion == 0 && !HasUserTables(connection, transaction))
            {
                CreateSchema(connection, transaction);
            }
            else
            {
                ValidateSchemaVersion(userVersion);
                ValidateExistingSchema(connection, transaction);
            }

            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            SqliteConnectionSettings.ConfigureDurability(connection);
            _storeIdentity = initialization.StoreIdentity;
        }
    }

    /// <summary>Gets or creates the durable subscription identifier for a stream.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="preferredId">The preferred subscription identifier.</param>
    /// <param name="cancellationToken">The token used to cancel before persistence commits.</param>
    /// <returns>The durable subscription identifier.</returns>
    /// <exception cref="ArgumentException"><paramref name="streamId"/> or <paramref name="preferredId"/> is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or the stored identity conflicts.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled before a commit.</exception>
    internal SubscriptionId GetOrCreateSubscriptionId(StreamId streamId, SubscriptionId? preferredId, CancellationToken cancellationToken)
    {
        ValidateLookup(streamId, preferredId);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            ThrowIfDisposed();
            var storeIdentity = _storeIdentity
                ?? throw new InvalidOperationException("The SQLite identity store must be initialized before subscription identities are resolved.");
            cancellationToken.ThrowIfCancellationRequested();

            using var connection = OpenConnection();
            SqliteConnectionSettings.ConfigureBusyTimeout(connection);
            SqliteConnectionSettings.ConfigureDurability(connection);
            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
            var candidate = preferredId ?? SubscriptionId.New();
            InsertSubscriptionIdentityIfMissing(connection, transaction, storeIdentity, streamId, candidate);
            var stored = SelectSubscriptionIdentity(connection, transaction, storeIdentity, streamId);
            ThrowIfPreferredMismatch(preferredId, stored);

            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            return stored;
        }
    }

    /// <summary>Validates initialization input.</summary>
    /// <param name="initialization">The initialization requirements.</param>
    /// <exception cref="ArgumentException"><paramref name="initialization"/> has a blank store identity.</exception>
    /// <exception cref="InvalidOperationException">The requested schema version is unsupported.</exception>
    private static void ValidateInitialization(LocalStoreInitialization initialization)
    {
        ThrowIfBlank(initialization.StoreIdentity, nameof(initialization), "StoreIdentity must be non-empty.");
        if (initialization.RequiredSchemaVersion == CurrentSchemaVersion)
        {
            return;
        }

        throw new InvalidOperationException("The requested SQLite identity schema version is not supported.");
    }

    /// <summary>Validates identity lookup input.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="preferredId">The preferred subscription identifier.</param>
    /// <exception cref="ArgumentException"><paramref name="streamId"/> or <paramref name="preferredId"/> is invalid.</exception>
    private static void ValidateLookup(StreamId streamId, SubscriptionId? preferredId)
    {
        if (streamId.Value is null || streamId.Value.Length == 0)
        {
            throw new ArgumentException("StreamId must be non-empty.", nameof(streamId));
        }

        if (preferredId is not { Value: { } value } || value != Guid.Empty)
        {
            return;
        }

        throw new ArgumentException("SubscriptionId must be non-empty when supplied.", nameof(preferredId));
    }

    /// <summary>Rejects unsupported non-file SQLite path forms.</summary>
    /// <param name="databasePath">The requested database path.</param>
    /// <exception cref="ArgumentException"><paramref name="databasePath"/> is not a normal file path.</exception>
    private static void ThrowIfUnsupportedPath(string databasePath)
    {
        if (!string.Equals(databasePath, ":memory:", StringComparison.OrdinalIgnoreCase)
            && !databasePath.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        throw new ArgumentException("The SQLite database path must identify a real file.", nameof(databasePath));
    }

    /// <summary>Throws when text is null, empty, or white space.</summary>
    /// <param name="value">The value to validate.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <param name="message">The exception message.</param>
    /// <exception cref="ArgumentException"><paramref name="value"/> is blank.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null.</exception>
    private static void ThrowIfBlank(string? value, string parameterName, string message)
    {
        ArgumentExceptionHelper.ThrowIfNull(value, parameterName);
        for (var index = 0; index < value.Length; index++)
        {
            if (!char.IsWhiteSpace(value[index]))
            {
                return;
            }
        }

        throw new ArgumentException(message, parameterName);
    }

    /// <summary>Throws when an explicit preferred identifier conflicts with storage.</summary>
    /// <param name="preferredId">The preferred identifier.</param>
    /// <param name="stored">The stored identifier.</param>
    /// <exception cref="InvalidOperationException">The identifiers differ.</exception>
    private static void ThrowIfPreferredMismatch(SubscriptionId? preferredId, SubscriptionId stored)
    {
        if (!preferredId.HasValue || stored == preferredId.Value)
        {
            return;
        }

        throw new InvalidOperationException("The stored subscription identity does not match the requested identity.");
    }

    /// <summary>Creates the SQLite schema.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    private static void CreateSchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = "PRAGMA user_version = 1;";
            _ = command.ExecuteNonQuery();
        }

        using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = MetadataTableSql;
            _ = command.ExecuteNonQuery();
        }

        using var subscriptionCommand = connection.CreateCommand();
        subscriptionCommand.Transaction = transaction;
        subscriptionCommand.CommandText = SubscriptionIdentitiesTableSql;
        _ = subscriptionCommand.ExecuteNonQuery();

        InsertMetadata(
            connection,
            transaction,
            SchemaVersionKey,
            CurrentSchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>Validates the version advertised by SQLite.</summary>
    /// <param name="userVersion">The SQLite user version.</param>
    /// <exception cref="InvalidOperationException"><paramref name="userVersion"/> is unsupported.</exception>
    private static void ValidateSchemaVersion(long userVersion)
    {
        if (userVersion == CurrentSchemaVersion)
        {
            return;
        }

        throw new InvalidOperationException("The SQLite identity schema version is not supported.");
    }

    /// <summary>Validates an existing database schema.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <exception cref="InvalidOperationException">The existing schema is invalid or unsupported.</exception>
    private static void ValidateExistingSchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateTableDefinition(connection, transaction, MetadataTableName, MetadataTableSql);
        var schemaVersion = SelectMetadata(connection, transaction, SchemaVersionKey);
        if (schemaVersion == CurrentSchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture))
        {
            ValidateTableDefinition(connection, transaction, SubscriptionIdentitiesTableName, SubscriptionIdentitiesTableSql);
            return;
        }

        throw new InvalidOperationException("The SQLite identity metadata schema version is not supported.");
    }

    /// <summary>Validates that a table uses the expected SQL definition.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="expectedSql">The expected SQL definition.</param>
    /// <exception cref="InvalidOperationException">The table definition does not match the supported schema.</exception>
    private static void ValidateTableDefinition(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string tableName,
        string expectedSql)
    {
        var actualSql = ReadTableDefinition(connection, transaction, tableName);
        if (TextEqualsOrdinalIgnoreCase(actualSql, NormalizeCreateTableSql(expectedSql)))
        {
            return;
        }

        throw new InvalidOperationException(InvalidSchemaMessage);
    }

    /// <summary>Compares schema text without a content-dependent early return.</summary>
    /// <param name="left">The first normalized definition.</param>
    /// <param name="right">The second normalized definition.</param>
    /// <returns>Whether the definitions match, ignoring ordinal case.</returns>
    private static bool TextEqualsOrdinalIgnoreCase(string left, string right)
    {
        if (left.Length != right.Length)
        {
            return false;
        }

        var result = 0;
        for (var index = 0; index < left.Length; index++)
        {
            result |= char.ToUpperInvariant(left[index]) ^ char.ToUpperInvariant(right[index]);
        }

        return result == 0;
    }

    /// <summary>Reads a table definition from SQLite metadata.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>The normalized table definition.</returns>
    /// <exception cref="InvalidOperationException">The table definition could not be read.</exception>
    private static string ReadTableDefinition(SqliteConnection connection, SqliteTransaction transaction, string tableName)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = $name;";
        _ = command.Parameters.AddWithValue("$name", tableName);
        if (command.ExecuteScalar() is string tableSql)
        {
            return NormalizeCreateTableSql(tableSql);
        }

        throw new InvalidOperationException(InvalidSchemaMessage);
    }

    /// <summary>Normalizes create-table SQL for schema comparison.</summary>
    /// <param name="sql">The SQL text.</param>
    /// <returns>The normalized SQL text.</returns>
    private static string NormalizeCreateTableSql(string sql)
    {
        var builder = new StringBuilder(sql.Length);
        var pendingSpace = false;
        foreach (var character in sql)
        {
            if (char.IsWhiteSpace(character))
            {
                pendingSpace = builder.Length > 0;
                continue;
            }

            if (pendingSpace)
            {
                _ = builder.Append(' ');
                pendingSpace = false;
            }

            _ = builder.Append(character);
        }

        return builder.ToString().TrimEnd(';');
    }

    /// <summary>Returns whether the database already has user tables.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <returns>Whether at least one user table exists.</returns>
    private static bool HasUserTables(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';";
        return SqliteIdentityStoreData.ReadHasUserTables(command.ExecuteScalar());
    }

    /// <summary>Gets the SQLite schema version.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <returns>The schema version.</returns>
    /// <exception cref="InvalidOperationException">The SQLite schema version could not be read.</exception>
    private static long GetUserVersion(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "PRAGMA user_version;";
        return SqliteIdentityStoreData.ReadUserVersion(command.ExecuteScalar());
    }

    /// <summary>Inserts a metadata entry.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    private static void InsertMetadata(SqliteConnection connection, SqliteTransaction transaction, string key, string value)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO oc_metadata (key, value) VALUES ($key, $value);";
        _ = command.Parameters.AddWithValue("$key", key);
        _ = command.Parameters.AddWithValue("$value", value);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Selects a metadata value.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="key">The metadata key.</param>
    /// <returns>The metadata value.</returns>
    /// <exception cref="InvalidOperationException">The requested metadata key is missing.</exception>
    private static string SelectMetadata(SqliteConnection connection, SqliteTransaction transaction, string key)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT value FROM oc_metadata WHERE key = $key;";
        _ = command.Parameters.AddWithValue("$key", key);
        return SqliteIdentityStoreData.ReadMetadataValue(command.ExecuteScalar());
    }

    /// <summary>Inserts a stream identity mapping when one does not already exist.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="storeIdentity">The durable store identity partition.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    private static void InsertSubscriptionIdentityIfMissing(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId,
        SubscriptionId subscriptionId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_subscription_identities
                (store_identity, stream_id, subscription_id)
            VALUES
                ($storeIdentity, $streamId, $subscriptionId)
            ON CONFLICT (store_identity, stream_id) DO NOTHING;
            """;
        _ = command.Parameters.AddWithValue("$storeIdentity", storeIdentity);
        _ = command.Parameters.AddWithValue("$streamId", streamId.Value);
        _ = command.Parameters.AddWithValue("$subscriptionId", subscriptionId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Selects a persisted subscription identity.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="storeIdentity">The durable store identity partition.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The persisted subscription identity.</returns>
    /// <exception cref="InvalidOperationException">The persisted identity row is missing or malformed.</exception>
    private static SubscriptionId SelectSubscriptionIdentity(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT subscription_id FROM oc_subscription_identities
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue("$storeIdentity", storeIdentity);
        _ = command.Parameters.AddWithValue("$streamId", streamId.Value);
        return SqliteIdentityStoreData.ReadSubscriptionId(command.ExecuteScalar());
    }

    /// <summary>Opens a SQLite connection with pooling disabled.</summary>
    /// <returns>The open SQLite connection.</returns>
    private SqliteConnection OpenConnection()
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = _databasePath, Mode = SqliteOpenMode.ReadWriteCreate, Pooling = false }.ToString();

        var connection = new SqliteConnection(connectionString);
        try
        {
            connection.Open();
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    /// <summary>Ensures the database directory exists.</summary>
    private void EnsureDirectoryExists() => _ = Directory.CreateDirectory(SqliteIdentityStoreData.GetDirectoryForCreate(_databasePath));

    /// <summary>Throws when initialization tries to switch this instance to a different durable partition.</summary>
    /// <param name="storeIdentity">The requested store identity.</param>
    /// <exception cref="InvalidOperationException">This instance was already initialized for another store identity.</exception>
    private void ThrowIfStoreIdentityConflicts(string storeIdentity)
    {
        if (_storeIdentity is null || string.Equals(_storeIdentity, storeIdentity, StringComparison.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException("The SQLite identity store has already been initialized for another store identity.");
    }

    /// <summary>Throws when this instance has been disposed.</summary>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
}
