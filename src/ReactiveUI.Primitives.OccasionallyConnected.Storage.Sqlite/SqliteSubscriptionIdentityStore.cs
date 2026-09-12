// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Data;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Stores durable subscription identities in SQLite.</summary>
internal sealed class SqliteSubscriptionIdentityStore : IDisposable
{
    /// <summary>The supported SQLite schema version.</summary>
    private const int CurrentSchemaVersion = SqliteStoreSchema.IdentitySchemaVersion;

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
                SqliteStoreSchema.CreateIdentitySchema(connection, transaction);
            }
            else
            {
                SqliteStoreSchema.ValidateExistingSchemaForIdentityFacade(connection, transaction, userVersion);
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
        SqliteSubscriptionIdentitySql.ValidateLookup(streamId, preferredId);
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
            SqliteSubscriptionIdentitySql.InsertSubscriptionIdentityIfMissing(connection, transaction, storeIdentity, streamId, candidate);
            var stored = SqliteSubscriptionIdentitySql.SelectSubscriptionIdentity(connection, transaction, storeIdentity, streamId);
            SqliteSubscriptionIdentitySql.ThrowIfPreferredMismatch(preferredId, stored);

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
