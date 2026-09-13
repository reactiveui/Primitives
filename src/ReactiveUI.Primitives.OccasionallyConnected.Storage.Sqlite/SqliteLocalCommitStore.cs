// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
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

    /// <summary>The maximum receive event and completion counts admitted by the durable SQLite store.</summary>
    private const int MaximumReceiveBatchEntries = 128;

    /// <summary>The expired lease exception message.</summary>
    private const string ExpiredLeaseMessage = "The SQLite outbox lease is expired.";

    /// <summary>The SQLite database path.</summary>
    private readonly string _databasePath;

    /// <summary>The clock used for commit timestamps.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>The per-instance gate.</summary>
    private readonly Lock _gate = new();

    /// <summary>The initialized durable store identity partition.</summary>
    private string? _storeIdentity;

    /// <summary>The initialized client identity binding.</summary>
    private string? _clientId;

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

    /// <summary>Initializes schema version four explicitly.</summary>
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
        var clientId = SqliteClientIdentityBinding.ValidateClientId(initialization.ClientId, nameof(initialization));
        if (initialization.RequireAuthenticatedEncryptionAtRest)
        {
            throw new NotSupportedException("SQLite authenticated encryption at rest has not been configured for this store.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ThrowIfDisposed();
            ThrowIfStoreIdentityConflicts(initialization.StoreIdentity);
            ThrowIfClientIdentityConflicts(clientId);
            cancellationToken.ThrowIfCancellationRequested();
            _ = Directory.CreateDirectory(SqliteIdentityStoreData.GetDirectoryForCreate(_databasePath));
            using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
            SqliteLocalCommitConnection.ConfigureLockPolling(connection);
            SqliteLocalCommitConnection.ValidateOwnershipBeforeDurability(connection);
            SqliteConnectionSettings.ConfigureDurability(connection);
            using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
            var userVersion = SqliteLocalCommitConnection.GetUserVersion(connection, transaction);
            InitializeSchema(connection, transaction, userVersion);

            cancellationToken.ThrowIfCancellationRequested();
            clientId = SqliteClientIdentityBinding.BindOrValidate(connection, transaction, initialization.StoreIdentity, clientId);
            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            _storeIdentity = initialization.StoreIdentity;
            _clientId = clientId;
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
            SqliteLocalCommitSql.InsertOutboxAuthoritativeMutation(connection, transaction, storeIdentity, operation.OperationId, snapshotMutation.AuthoritativeState);
            SqliteLocalCommitSql.InsertOperationMetadata(connection, transaction, storeIdentity, operation);
            SqliteLocalCommitSql.InsertInitialOperationState(connection, transaction, storeIdentity, operation, committedAtUtc);
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
            var replay = SqliteLocalCommitSql.ReadReplayOperations(connection, transaction, storeIdentity, streamId);
            if (!hasStream && (snapshot is not null || pending.Count != 0 || replay.Count != 0))
            {
                throw new InvalidOperationException("Committed data has no durable stream state.");
            }

            if (snapshot is not null && snapshot.ServerCursor != stream.ServerCursor)
            {
                throw new InvalidOperationException("The snapshot cursor does not match the durable stream cursor.");
            }

            ValidateRecoveredSequences(pending, replay, stream.NextClientSequence);

            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            var result = new RecoveredStream(subscriptionId, stream.ServerCursor, snapshot, pending, [], stream.NextClientSequence);
            return result with { ReplayOperations = replay };
        }
    }

    /// <summary>Leases at most one pending operation batch for upload.</summary>
    /// <param name="request">The lease request.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The leased batch, or null when no pending operation fits.</returns>
    /// <exception cref="ArgumentException">The lease request is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or the durable lease state is invalid.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    internal LeasedOperationBatch? LeasePendingOperationBatch(OutboxLeaseRequest request, CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateLeaseRequest(request);
        cancellationToken.ThrowIfCancellationRequested();
        var leaseId = Guid.NewGuid();
        var nowUtc = _timeProvider.GetUtcNow();
        var expiresAtUtc = CheckedAdd(nowUtc, request.LeaseDuration);
        lock (_gate)
        {
            ThrowIfDisposed();
            var storeIdentity = GetInitializedStoreIdentity();
            cancellationToken.ThrowIfCancellationRequested();
            using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
            SqliteLocalCommitConnection.ConfigureLockPolling(connection);
            SqliteConnectionSettings.ConfigureOperationalConnection(connection);
            using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
            var operations = SqliteLocalCommitSql.SelectLeaseableOperationIds(connection, transaction, storeIdentity, request, nowUtc, cancellationToken);
            if (operations.Count == 0)
            {
                transaction.Commit();
                return null;
            }

            SqliteLocalCommitSql.ReclaimSelectedLeaseRows(connection, transaction, storeIdentity, operations);
            SqliteLocalCommitSql.InsertLeaseMembership(connection, transaction, storeIdentity, leaseId, expiresAtUtc, operations);
            var leasedOperations = SqliteLocalCommitSql.ReadLeasedOperations(connection, transaction, storeIdentity, leaseId);
            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            return new(leaseId, expiresAtUtc, leasedOperations);
        }
    }

    /// <summary>Extends an active outbox lease.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="extension">The lease extension duration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="ArgumentException">The lease identifier or extension is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or the lease is not current.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    internal void RenewLease(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateLeaseRenewalInput(leaseId, extension);
        cancellationToken.ThrowIfCancellationRequested();
        var nowUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            ThrowIfDisposed();
            var storeIdentity = GetInitializedStoreIdentity();
            cancellationToken.ThrowIfCancellationRequested();
            using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
            SqliteLocalCommitConnection.ConfigureLockPolling(connection);
            SqliteConnectionSettings.ConfigureOperationalConnection(connection);
            using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
            var currentExpiry = SqliteLocalCommitSql.ValidateLeaseMembership(connection, transaction, storeIdentity, leaseId);
            if (currentExpiry <= nowUtc)
            {
                throw new InvalidOperationException(ExpiredLeaseMessage);
            }

            var expiresAtUtc = CheckedAdd(currentExpiry, extension);
            SqliteLocalCommitSql.RenewLease(connection, transaction, storeIdentity, leaseId, expiresAtUtc);
            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
        }
    }

    /// <summary>Extends an active outbox lease.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="extension">The lease extension duration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A completed value task.</returns>
    internal ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken)
    {
        RenewLease(leaseId, extension, cancellationToken);
        return default;
    }

    /// <summary>Releases an outbox lease.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="ArgumentException">The lease identifier is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or the lease membership is incomplete.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    internal void ReleaseLease(Guid leaseId, CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateLeaseId(leaseId);
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
            _ = SqliteLocalCommitSql.ValidateLeaseMembership(connection, transaction, storeIdentity, leaseId);
            SqliteLocalCommitSql.ReleaseLease(connection, transaction, storeIdentity, leaseId);
            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
        }
    }

    /// <summary>Releases an outbox lease.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A completed value task.</returns>
    internal ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken)
    {
        ReleaseLease(leaseId, cancellationToken);
        return default;
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

    /// <summary>Compacts eligible terminal outbox, dead-letter, and inbox rows transactionally.</summary>
    /// <param name="request">The compaction request.</param>
    /// <param name="retention">The retention policy.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The compaction result.</returns>
    /// <exception cref="ArgumentNullException">The request or retention policy is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The request or retention policy is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or durable state is invalid.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    internal CompactionResult Compact(
        CompactionRequest request,
        RetentionOptions retention,
        CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateCompactionInput(request, retention);
        cancellationToken.ThrowIfCancellationRequested();
        var nowUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            ThrowIfDisposed();
            var storeIdentity = GetInitializedStoreIdentity();
            cancellationToken.ThrowIfCancellationRequested();
            using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
            SqliteLocalCommitConnection.ConfigureLockPolling(connection);
            SqliteConnectionSettings.ConfigureOperationalConnection(connection);
            using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
            var result = SqliteLocalCommitSql.Compact(
                connection,
                transaction,
                storeIdentity,
                request,
                retention,
                nowUtc,
                cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            return result;
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
        RemoteEventBatchValidator.Validate(batch, MaximumReceiveBatchEntries, MaximumReceiveBatchEntries);
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
            var appliedCount = 0;
            var duplicateCount = 0;
            for (var index = 0; index < batch.Events.Count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var remoteEvent = batch.Events[index];
                if (SqliteLocalCommitSql.IsInboxEventApplied(connection, transaction, storeIdentity, batch.StreamId, remoteEvent.EventId))
                {
                    duplicateCount++;
                    continue;
                }

                SqliteLocalCommitSql.InsertInboxEvent(connection, transaction, storeIdentity, remoteEvent, committedAtUtc);
                appliedCount++;
            }

            SqliteLocalCommitSql.UpsertSnapshot(connection, transaction, storeIdentity, snapshotMutation, nextRevision, batch.NextCursor, committedAtUtc);
            SqliteLocalCommitSql.UpdateServerCursor(connection, transaction, storeIdentity, batch.StreamId, batch.PreviousCursor, batch.NextCursor);
            SqliteLocalCommitSql.MarkReceiveInclusions(connection, transaction, storeIdentity, _clientId, batch, snapshotMutation);
            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            return new(batch.NextCursor, appliedCount, duplicateCount, nextRevision);
        }
    }

    /// <summary>Gets the latest durable status recorded for an operation.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The operation status, if one is recorded.</returns>
    internal SyncOperationStatus? GetOperationStatus(OperationId operationId, CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateOperationId(operationId, nameof(operationId));
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
            var status = SqliteLocalCommitSql.ReadOperationStatus(connection, transaction, storeIdentity, operationId);
            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            return status;
        }
    }

    /// <summary>Gets the durable retry state recorded for an operation.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The retry state, if one is recorded.</returns>
    internal RetryState? GetRetryState(OperationId operationId, CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateOperationId(operationId, nameof(operationId));
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
            var retryState = SqliteLocalCommitSql.ReadRetryState(connection, transaction, storeIdentity, operationId);
            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
            return retryState;
        }
    }

    /// <summary>Records the durable attempt barrier before remote I/O.</summary>
    /// <param name="leaseId">The owning lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="nextAttempt">The attempt about to be sent.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The barrier decision.</returns>
    /// <exception cref="ArgumentException">The lease or operation identifier is invalid.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The attempt number is not positive.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or the lease is not current.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    internal AttemptBarrierResult TryBeginRemoteAttempt(
        Guid leaseId,
        OperationId operationId,
        int nextAttempt,
        CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateAttemptBarrierInput(leaseId, operationId, nextAttempt);
        cancellationToken.ThrowIfCancellationRequested();
        var storeIdentity = GetInitializedStoreIdentityForOperation();
        using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
        SqliteLocalCommitConnection.ConfigureLockPolling(connection);
        SqliteConnectionSettings.ConfigureOperationalConnection(connection);
        using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
        var nowUtc = _timeProvider.GetUtcNow();
        var leaseExpiry = SqliteLocalCommitSql.ValidateLeaseMembership(connection, transaction, storeIdentity, leaseId);
        if (leaseExpiry <= nowUtc)
        {
            throw new InvalidOperationException(ExpiredLeaseMessage);
        }

        var decision = SqliteLocalCommitSql.TryBeginRemoteAttempt(
            connection,
            transaction,
            storeIdentity,
            leaseId,
            operationId,
            nextAttempt,
            nowUtc);
        cancellationToken.ThrowIfCancellationRequested();
        transaction.Commit();
        return decision;
    }

    /// <summary>Applies remote synchronization results to the currently leased batch.</summary>
    /// <param name="leaseId">The owning lease identifier.</param>
    /// <param name="result">The remote result.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A completed value task.</returns>
    /// <exception cref="ArgumentException">The lease identifier is invalid.</exception>
    /// <exception cref="ArgumentNullException">The result is null.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or the lease is not current.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    /// <exception cref="SyncBatchValidationException">The result does not exactly match the leased batch.</exception>
    internal ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken)
    {
        ApplySyncResult(leaseId, result, cancellationToken);
        return default;
    }

    /// <summary>Saves durable retry state for an operation.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="retryState">The retry state.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal void SaveRetryState(OperationId operationId, RetryState retryState, CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateRetryStateInput(operationId, retryState);
        cancellationToken.ThrowIfCancellationRequested();
        var nowUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            ThrowIfDisposed();
            var storeIdentity = GetInitializedStoreIdentity();
            cancellationToken.ThrowIfCancellationRequested();
            using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
            SqliteLocalCommitConnection.ConfigureLockPolling(connection);
            SqliteConnectionSettings.ConfigureOperationalConnection(connection);
            using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
            SqliteLocalCommitSql.SaveRetryState(connection, transaction, storeIdentity, operationId, retryState, nowUtc);
            cancellationToken.ThrowIfCancellationRequested();
            transaction.Commit();
        }
    }

    /// <summary>Saves durable retry state for an operation after a retryable decision or ambiguous attempt.</summary>
    /// <param name="operationId">The operation identifier that owns the retry state.</param>
    /// <param name="retryState">The retry state to save.</param>
    /// <param name="cancellationToken">The token used to cancel retry state persistence.</param>
    /// <returns>A task representing the operation.</returns>
    internal ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken)
    {
        SaveRetryState(operationId, retryState, cancellationToken);
        return default;
    }

    /// <summary>Applies remote synchronization results to the currently leased batch.</summary>
    /// <param name="leaseId">The owning lease identifier.</param>
    /// <param name="result">The remote result.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="ArgumentException">The lease identifier is invalid.</exception>
    /// <exception cref="ArgumentNullException">The result is null.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or the lease is not current.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    /// <exception cref="SyncBatchValidationException">The result does not exactly match the leased batch.</exception>
    internal void ApplySyncResult(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateSyncResultInput(leaseId, result);
        cancellationToken.ThrowIfCancellationRequested();
        var storeIdentity = GetInitializedStoreIdentityForOperation();
        using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
        SqliteLocalCommitConnection.ConfigureLockPolling(connection);
        SqliteConnectionSettings.ConfigureOperationalConnection(connection);
        using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
        var nowUtc = _timeProvider.GetUtcNow();
        var leaseExpiry = SqliteLocalCommitSql.ValidateLeaseMembership(connection, transaction, storeIdentity, leaseId);
        if (leaseExpiry <= nowUtc)
        {
            throw new InvalidOperationException(ExpiredLeaseMessage);
        }

        var operations = SqliteLocalCommitSql.ReadLeasedOperations(connection, transaction, storeIdentity, leaseId);
        ValidateResultCountForLeasedBatch(operations, result);
        SyncBatchValidator.Validate(new(leaseId, operations), result);
        SqliteLocalCommitSql.ValidateStatusOnlyReconciliation(connection, transaction, storeIdentity, operations, result);
        SqliteLocalCommitSql.ApplySyncResult(connection, transaction, storeIdentity, result, nowUtc);
        SqliteLocalCommitSql.ReleaseLease(connection, transaction, storeIdentity, leaseId);
        cancellationToken.ThrowIfCancellationRequested();
        transaction.Commit();
    }

    /// <summary>Applies remote synchronization results and replacement snapshots to the currently leased batch.</summary>
    /// <param name="leaseId">The owning lease identifier.</param>
    /// <param name="result">The remote result.</param>
    /// <param name="snapshotMutations">The replacement snapshots for affected streams.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The committed snapshots.</returns>
    /// <exception cref="ArgumentException">The lease identifier is invalid.</exception>
    /// <exception cref="ArgumentNullException">A required value is null.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or the reconciliation is stale.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    /// <exception cref="SyncBatchValidationException">The result does not exactly match the leased batch.</exception>
    internal IReadOnlyList<LocalSnapshot> ApplySyncResult(
        Guid leaseId,
        RemoteSyncResult result,
        IReadOnlyList<SnapshotMutation> snapshotMutations,
        CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateSyncResultInput(leaseId, result);
        ArgumentExceptionHelper.ThrowIfNull(snapshotMutations);
        cancellationToken.ThrowIfCancellationRequested();
        var storeIdentity = GetInitializedStoreIdentityForOperation();
        using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
        SqliteLocalCommitConnection.ConfigureLockPolling(connection);
        SqliteConnectionSettings.ConfigureOperationalConnection(connection);
        using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
        var nowUtc = _timeProvider.GetUtcNow();
        var leaseExpiry = SqliteLocalCommitSql.ValidateLeaseMembership(connection, transaction, storeIdentity, leaseId);
        if (leaseExpiry <= nowUtc)
        {
            throw new InvalidOperationException(ExpiredLeaseMessage);
        }

        var operations = SqliteLocalCommitSql.ReadLeasedOperations(connection, transaction, storeIdentity, leaseId);
        ValidateResultCountForLeasedBatch(operations, result);
        SyncBatchValidator.Validate(new(leaseId, operations), result);
        var committedSnapshots = SqliteLocalCommitSql.CreateResultReconciliationSnapshots(
            connection,
            transaction,
            storeIdentity,
            operations,
            result,
            snapshotMutations,
            nowUtc);
        SqliteLocalCommitSql.ApplySyncResult(connection, transaction, storeIdentity, result, nowUtc);
        SqliteLocalCommitSql.ReleaseLease(connection, transaction, storeIdentity, leaseId);
        for (var index = 0; index < committedSnapshots.Count; index++)
        {
            var snapshot = committedSnapshots[index];
            SqliteLocalCommitSql.UpsertSnapshot(
                connection,
                transaction,
                storeIdentity,
                new(snapshot.StreamId, snapshot.State, snapshot.FormatVersion, snapshot.Revision - 1) { AuthoritativeState = snapshot.AuthoritativeState },
                snapshot.Revision,
                snapshot.ServerCursor,
                snapshot.SavedAtUtc);
        }

        var receipt = new ReadOnlyCollection<LocalSnapshot>(committedSnapshots);
        cancellationToken.ThrowIfCancellationRequested();
        transaction.Commit();
        return receipt;
    }

    /// <summary>Rejects malformed result sizes before the shared validator allocates membership dictionaries.</summary>
    /// <param name="operations">The bounded leased operations.</param>
    /// <param name="result">The remote result.</param>
    /// <exception cref="SyncBatchValidationException">The result count does not match the leased batch count.</exception>
    private static void ValidateResultCountForLeasedBatch(List<SyncOperation> operations, RemoteSyncResult result)
    {
        if (result.Operations.Count == operations.Count)
        {
            return;
        }

        throw result.Operations.Count < operations.Count
            ? new SyncBatchValidationException(SyncBatchValidationError.OmittedOperationResult, "The synchronization result omitted one or more operation results.")
            : new SyncBatchValidationException(SyncBatchValidationError.UnknownOperationResult, "The synchronization result contains an unknown operation result.");
    }

    /// <summary>Creates, migrates, or validates the local commit schema.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The active transaction.</param>
    /// <param name="userVersion">The current user version.</param>
    private static void InitializeSchema(SqliteConnection connection, SqliteTransaction transaction, long userVersion)
    {
        if (userVersion == 0 && !SqliteLocalCommitConnection.HasUserTables(connection, transaction))
        {
            SqliteStoreSchema.CreateLocalCommitSchema(connection, transaction);
            return;
        }

        if (userVersion == SqliteStoreSchema.IdentitySchemaVersion)
        {
            SqliteStoreSchema.MigrateIdentityToLocalCommit(connection, transaction);
            return;
        }

        if (userVersion == SqliteStoreSchema.LegacyLocalCommitSchemaVersion)
        {
            SqliteStoreSchema.MigrateLegacyLocalCommitToCurrent(connection, transaction);
            return;
        }

        if (userVersion == SqliteStoreSchema.RemoteApplySchemaVersion)
        {
            SqliteStoreSchema.MigrateRemoteApplyToCurrent(connection, transaction);
            return;
        }

        if (userVersion == SqliteStoreSchema.LeaseSchemaVersion)
        {
            SqliteStoreSchema.MigrateLeaseSchemaToCurrent(connection, transaction);
            return;
        }

        if (userVersion == SqliteStoreSchema.PreAuthoritativeLocalCommitSchemaVersion)
        {
            SqliteStoreSchema.MigratePreAuthoritativeLocalCommitToCurrent(connection, transaction);
            return;
        }

        if (userVersion == SqliteStoreSchema.AuthoritativeLocalCommitSchemaVersion)
        {
            SqliteStoreSchema.MigrateAuthoritativeLocalCommitToCurrent(connection, transaction);
            return;
        }

        SqliteStoreSchema.ValidateExistingSchemaForLocalCommit(connection, transaction, userVersion);
    }

    /// <summary>Validates recovered operation sequence fences.</summary>
    /// <param name="pending">The upload-pending operations.</param>
    /// <param name="replay">The replay-visible operations.</param>
    /// <param name="nextClientSequence">The next durable sequence.</param>
    /// <exception cref="InvalidOperationException">Recovered sequence data is invalid.</exception>
    private static void ValidateRecoveredSequences(
        List<SyncOperation> pending,
        List<SyncOperation> replay,
        long nextClientSequence)
    {
        for (var index = 0; index < pending.Count; index++)
        {
            if (pending[index].ClientSequence >= nextClientSequence)
            {
                throw new InvalidOperationException("A pending operation reaches or exceeds the next durable client sequence.");
            }
        }

        for (var index = 0; index < replay.Count; index++)
        {
            if (replay[index].ClientSequence >= nextClientSequence)
            {
                throw new InvalidOperationException("A replay operation reaches or exceeds the next durable client sequence.");
            }
        }
    }

    /// <summary>Adds a duration to a UTC timestamp and rejects overflow.</summary>
    /// <param name="timestamp">The timestamp.</param>
    /// <param name="duration">The duration.</param>
    /// <returns>The summed timestamp.</returns>
    /// <exception cref="ArgumentException">The resulting timestamp is outside the supported range.</exception>
    private static DateTimeOffset CheckedAdd(DateTimeOffset timestamp, TimeSpan duration)
    {
        try
        {
            return timestamp.Add(duration);
        }
        catch (ArgumentOutOfRangeException exception)
        {
            throw new ArgumentException("The SQLite outbox lease expiry is outside the supported timestamp range.", nameof(duration), exception);
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

    /// <summary>Throws when initialization tries to switch this instance to a different client identity binding.</summary>
    /// <param name="clientId">The requested client identity.</param>
    /// <exception cref="InvalidOperationException">This instance has already been initialized for another client identity.</exception>
    private void ThrowIfClientIdentityConflicts(string? clientId)
    {
        if (_clientId is null || (clientId is not null && string.Equals(_clientId, clientId, StringComparison.Ordinal)))
        {
            return;
        }

        throw new InvalidOperationException("The SQLite local commit store has already been initialized for another client identity.");
    }

    /// <summary>Gets the initialized store identity after validating this instance is available.</summary>
    /// <returns>The store identity.</returns>
    /// <exception cref="InvalidOperationException">This instance has not been initialized.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    private string GetInitializedStoreIdentityForOperation()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            return GetInitializedStoreIdentity();
        }
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
