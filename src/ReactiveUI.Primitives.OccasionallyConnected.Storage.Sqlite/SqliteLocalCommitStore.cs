// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Data;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Persists local commit and recovery state in SQLite.</summary>
internal sealed class SqliteLocalCommitStore : IDisposable
{
    /// <summary>The first valid client sequence.</summary>
    private const long FirstClientSequence = 1;

    /// <summary>The SQLite database path.</summary>
    private readonly string _databasePath;

    /// <summary>The clock used for commit timestamps.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>The per-instance gate.</summary>
    private readonly Lock _gate = new();

    /// <summary>The initialized durable store identity partition.</summary>
    private string? _storeIdentity;

    /// <summary>A value indicating whether this instance has been disposed.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="SqliteLocalCommitStore"/> class.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    internal SqliteLocalCommitStore(string databasePath)
        : this(databasePath, TimeProvider.System)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SqliteLocalCommitStore"/> class.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="timeProvider">The clock used for commit timestamps.</param>
    internal SqliteLocalCommitStore(string databasePath, TimeProvider timeProvider)
    {
        ArgumentExceptionHelper.ThrowIfNull(databasePath);
        ArgumentExceptionHelper.ThrowIfNull(timeProvider);
        SqliteLocalCommitValidation.ThrowIfBlank(databasePath, nameof(databasePath), "The SQLite database path cannot be empty.");
        SqliteLocalCommitValidation.ThrowIfUnsupportedPath(databasePath);

        _databasePath = Path.GetFullPath(databasePath);
        _timeProvider = timeProvider;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
        }
    }

    /// <summary>Initializes schema version three explicitly.</summary>
    /// <param name="initialization">The initialization requirements.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="ArgumentNullException">The initialization requirements are null.</exception>
    /// <exception cref="ArgumentException">The initialization requirements are invalid.</exception>
    /// <exception cref="InvalidOperationException">The requested or existing SQLite schema state is invalid.</exception>
    /// <exception cref="NotSupportedException">Authenticated encryption at rest is required but unavailable.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    internal void Initialize(LocalStoreInitialization initialization, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(initialization);
        SqliteLocalCommitValidation.ValidateInitialization(initialization);
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
            _ = Directory.CreateDirectory(SqliteIdentityStoreData.GetDirectoryForCreate(_databasePath));
            using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
            SqliteLocalCommitConnection.ConfigureLockPolling(connection);
            SqliteLocalCommitConnection.ValidateOwnershipBeforeDurability(connection);
            SqliteConnectionSettings.ConfigureDurability(connection);
            using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
            var userVersion = SqliteLocalCommitConnection.GetUserVersion(connection, transaction);
            if (userVersion == 0 && !SqliteLocalCommitConnection.HasUserTables(connection, transaction))
            {
                SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
            }
            else if (userVersion == SqliteStoreSchema.IdentitySchemaVersion)
            {
                SqliteStoreSchema.MigrateIdentityToLocalCommit(connection, transaction);
            }
            else if (userVersion == SqliteStoreSchema.LegacyLocalCommitSchemaVersion)
            {
                SqliteStoreSchema.MigrateLegacyLocalCommitToCurrent(connection, transaction);
            }
            else
            {
                SqliteStoreSchema.ValidateExistingSchemaForLocalCommit(connection, transaction, userVersion);
            }

            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            _storeIdentity = initialization.StoreIdentity;
        }
    }

    /// <summary>Gets or creates the durable subscription identifier for a stream.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="preferredId">The preferred subscription identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The subscription identifier.</returns>
    /// <exception cref="ArgumentException">The stream or preferred subscription identifier is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or stored identity state conflicts.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    internal SubscriptionId GetOrCreateSubscriptionId(StreamId streamId, SubscriptionId? preferredId, CancellationToken cancellationToken)
    {
        SqliteSubscriptionIdentitySql.ValidateLookup(streamId, preferredId);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ThrowIfDisposed();
            var storeIdentity = GetInitializedStoreIdentity();
            cancellationToken.ThrowIfCancellationRequested();
            using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
            SqliteLocalCommitConnection.ConfigureLockPolling(connection);
            SqliteConnectionSettings.ConfigureOperationalConnection(connection);
            using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
            var candidate = preferredId ?? SubscriptionId.New();
            SqliteSubscriptionIdentitySql.InsertSubscriptionIdentityIfMissing(connection, transaction, storeIdentity, streamId, candidate);
            var stored = SqliteSubscriptionIdentitySql.SelectSubscriptionIdentity(connection, transaction, storeIdentity, streamId);
            SqliteSubscriptionIdentitySql.ThrowIfPreferredMismatch(preferredId, stored);
            SqliteLocalCommitSql.EnsureStreamRow(connection, transaction, storeIdentity, streamId, stored);
            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            return stored;
        }
    }

    /// <summary>Atomically commits a local operation, snapshot, and next sequence.</summary>
    /// <param name="operation">The operation to commit.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The local commit result.</returns>
    /// <exception cref="ArgumentException">The operation or snapshot mutation is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or the durable stream state rejects the commit.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    internal LocalCommitResult CommitLocalOperation(
        SyncOperation operation,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateCommitInput(operation, snapshotMutation);
        cancellationToken.ThrowIfCancellationRequested();
        var fingerprint = SqliteCommitFingerprint.Compute(operation, snapshotMutation);
        var committedAtUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            ThrowIfDisposed();
            var storeIdentity = GetInitializedStoreIdentity();
            cancellationToken.ThrowIfCancellationRequested();
            using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
            SqliteLocalCommitConnection.ConfigureLockPolling(connection);
            SqliteConnectionSettings.ConfigureOperationalConnection(connection);
            using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
            if (SqliteLocalCommitSql.TryReadCommittedResult(connection, transaction, storeIdentity, operation, snapshotMutation, fingerprint, out var existing) && existing is not null)
            {
                transaction.Commit();
                return existing;
            }

            var subscriptionId = SqliteLocalCommitSql.SelectSubscriptionId(connection, transaction, storeIdentity, operation.StreamId);
            SqliteLocalCommitSql.EnsureStreamRow(connection, transaction, storeIdentity, operation.StreamId, subscriptionId);
            var stream = SqliteLocalCommitSql.ReadStreamState(connection, transaction, storeIdentity, operation.StreamId);
            if (stream.NextClientSequence != operation.ClientSequence)
            {
                throw new InvalidOperationException("The operation client sequence does not match the next durable sequence.");
            }

            var currentRevision = SqliteLocalCommitSql.ReadSnapshotRevision(connection, transaction, storeIdentity, operation.StreamId);
            if (currentRevision != snapshotMutation.ExpectedRevision)
            {
                throw new InvalidOperationException("The snapshot revision does not match the expected revision.");
            }

            var nextRevision = snapshotMutation.ExpectedRevision + 1;
            SqliteLocalCommitSql.InsertOutboxOperation(connection, transaction, storeIdentity, operation, nextRevision, fingerprint, committedAtUtc);
            SqliteLocalCommitSql.InsertOperationMetadata(connection, transaction, storeIdentity, operation);
            SqliteLocalCommitSql.UpsertSnapshot(connection, transaction, storeIdentity, snapshotMutation, nextRevision, stream.ServerCursor, committedAtUtc);
            SqliteLocalCommitSql.UpdateNextClientSequence(connection, transaction, storeIdentity, operation.StreamId, operation.ClientSequence + 1);
            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            return new(operation.OperationId, operation.ClientSequence, nextRevision, committedAtUtc);
        }
    }

    /// <summary>Recovers a stream snapshot and pending outbox operations.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The recovered stream.</returns>
    /// <exception cref="ArgumentException">The stream or subscription identifier is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or recovered data is invalid.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before recovery completes.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    internal RecoveredStream RecoverStream(StreamId streamId, SubscriptionId subscriptionId, CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateRecoveryInput(streamId, subscriptionId);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ThrowIfDisposed();
            var storeIdentity = GetInitializedStoreIdentity();
            using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
            SqliteLocalCommitConnection.ConfigureLockPolling(connection);
            SqliteConnectionSettings.ConfigureOperationalConnection(connection);
            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: true);
            var storedSubscriptionId = SqliteLocalCommitSql.SelectSubscriptionId(connection, transaction, storeIdentity, streamId);
            if (storedSubscriptionId != subscriptionId)
            {
                throw new InvalidOperationException("The recovered subscription identity does not match the requested identity.");
            }

            var hasStream = SqliteLocalCommitSql.TryReadStreamState(connection, transaction, storeIdentity, streamId, out var storedStream);
            var stream = hasStream
                ? storedStream
                : new SqliteLocalStreamState(FirstClientSequence, null);
            var snapshot = SqliteLocalCommitSql.ReadSnapshot(connection, transaction, storeIdentity, streamId);
            var pending = SqliteLocalCommitSql.ReadPendingOperations(connection, transaction, storeIdentity, streamId);
            if (!hasStream && (snapshot is not null || pending.Count != 0))
            {
                throw new InvalidOperationException("Committed data has no durable stream state.");
            }

            if (snapshot is not null && snapshot.ServerCursor != stream.ServerCursor)
            {
                throw new InvalidOperationException("The snapshot cursor does not match the durable stream cursor.");
            }

            foreach (var operation in pending)
            {
                if (operation.ClientSequence >= stream.NextClientSequence)
                {
                    throw new InvalidOperationException("A pending operation reaches or exceeds the next durable client sequence.");
                }
            }

            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            return new(subscriptionId, stream.ServerCursor, snapshot, pending, [], stream.NextClientSequence);
        }
    }

    /// <summary>Returns remote event identifiers that are not in the durable inbox for the stream.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="eventIds">The candidate remote event identifiers.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The unapplied event identifiers in candidate order.</returns>
    /// <exception cref="ArgumentException">The stream or event identifiers are invalid.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or stored inbox data is invalid.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before lookup completes.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    internal IReadOnlyList<Guid> GetUnappliedEventIds(
        StreamId streamId,
        IReadOnlyList<Guid> eventIds,
        CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateInboxLookupInput(streamId, eventIds);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ThrowIfDisposed();
            var storeIdentity = GetInitializedStoreIdentity();
            cancellationToken.ThrowIfCancellationRequested();
            using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
            SqliteLocalCommitConnection.ConfigureLockPolling(connection);
            SqliteConnectionSettings.ConfigureOperationalConnection(connection);
            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: true);
            List<Guid> unapplied = [with(capacity: eventIds.Count)];
            for (var index = 0; index < eventIds.Count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var eventId = eventIds[index];
                if (!SqliteLocalCommitSql.IsInboxEventApplied(connection, transaction, storeIdentity, streamId, eventId))
                {
                    unapplied.Add(eventId);
                }
            }

            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            return unapplied;
        }
    }

    /// <summary>Atomically applies a remote batch, records inbox identifiers, advances the cursor, and stores a snapshot.</summary>
    /// <param name="batch">The remote event batch.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The remote apply result.</returns>
    /// <exception cref="ArgumentException">The batch or mutation is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or the durable stream state rejects the apply.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    internal RemoteApplyResult ApplyRemoteBatch(
        RemoteEventBatch batch,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateRemoteApplyInput(batch, snapshotMutation);
        cancellationToken.ThrowIfCancellationRequested();
        var committedAtUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            ThrowIfDisposed();
            var storeIdentity = GetInitializedStoreIdentity();
            cancellationToken.ThrowIfCancellationRequested();
            using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
            SqliteLocalCommitConnection.ConfigureLockPolling(connection);
            SqliteConnectionSettings.ConfigureOperationalConnection(connection);
            using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
            var subscriptionId = SqliteLocalCommitSql.SelectSubscriptionId(connection, transaction, storeIdentity, batch.StreamId);
            SqliteLocalCommitSql.EnsureStreamRow(connection, transaction, storeIdentity, batch.StreamId, subscriptionId);
            var stream = SqliteLocalCommitSql.ReadStreamState(connection, transaction, storeIdentity, batch.StreamId);
            if (!string.Equals(stream.ServerCursor, batch.PreviousCursor, StringComparison.Ordinal))
            {
                throw new InvalidOperationException("The SQLite stream cursor does not match the remote batch previous cursor.");
            }

            var currentRevision = SqliteLocalCommitSql.ReadSnapshotRevision(connection, transaction, storeIdentity, batch.StreamId);
            if (currentRevision != snapshotMutation.ExpectedRevision)
            {
                throw new InvalidOperationException("The snapshot revision does not match the expected revision.");
            }

            var nextRevision = snapshotMutation.ExpectedRevision + 1;
            for (var index = 0; index < batch.Events.Count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                SqliteLocalCommitSql.InsertInboxEvent(connection, transaction, storeIdentity, batch.Events[index], committedAtUtc);
            }

            SqliteLocalCommitSql.UpsertSnapshot(connection, transaction, storeIdentity, snapshotMutation, nextRevision, batch.NextCursor, committedAtUtc);
            SqliteLocalCommitSql.UpdateServerCursor(connection, transaction, storeIdentity, batch.StreamId, batch.PreviousCursor, batch.NextCursor);
            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            return new(batch.NextCursor, batch.Events.Count, 0, nextRevision);
        }
    }

    /// <summary>Throws when initialization tries to switch this instance to a different durable partition.</summary>
    /// <param name="storeIdentity">The requested store identity.</param>
    /// <exception cref="InvalidOperationException">This instance has already been initialized for another store identity.</exception>
    private void ThrowIfStoreIdentityConflicts(string storeIdentity)
    {
        if (_storeIdentity is null || string.Equals(_storeIdentity, storeIdentity, StringComparison.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException("The SQLite local commit store has already been initialized for another store identity.");
    }

    /// <summary>Gets the initialized store identity.</summary>
    /// <returns>The store identity.</returns>
    /// <exception cref="InvalidOperationException">This instance has not been initialized.</exception>
    private string GetInitializedStoreIdentity() =>
        _storeIdentity ?? throw new InvalidOperationException("The SQLite local commit store must be initialized before use.");

    /// <summary>Throws when this instance has been disposed.</summary>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
}
