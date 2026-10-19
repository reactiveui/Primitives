// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Persists local commit and recovery state in SQLite.</summary>
/// <content>Commits a validated local operation on the exclusively borrowed connection.</content>
internal sealed partial class SqliteLocalCommitStore
{
    /// <summary>Atomically commits a local operation, snapshot, and next sequence.</summary>
    /// <param name="operation">The operation to commit.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The local commit result.</returns>
    /// <exception cref="ArgumentException">The operation or snapshot mutation is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or the durable stream state rejects the commit.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteDatabaseException">SQLite rejects the operation.</exception>
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
            return WithStoreConnection(
                storeIdentity,
                connection => CommitLocalOperationLocked(
                    connection,
                    operation,
                    snapshotMutation,
                    fingerprint,
                    committedAtUtc,
                    storeIdentity,
                    cancellationToken),
                cancellationToken);
        }
    }

    /// <summary>Commits a local operation while the caller retains the connection gate.</summary>
    /// <param name="connection">The exclusively borrowed native connection.</param>
    /// <param name="operation">The validated operation.</param>
    /// <param name="snapshotMutation">The validated snapshot mutation.</param>
    /// <param name="fingerprint">The immutable intent fingerprint.</param>
    /// <param name="committedAtUtc">The commit timestamp.</param>
    /// <param name="storeIdentity">The initialized store identity.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable commit result.</returns>
    /// <exception cref="InvalidOperationException">The durable sequence or snapshot revision differs from the request.</exception>
    private LocalCommitResult CommitLocalOperationLocked(
        SqliteDatabase connection,
        SyncOperation operation,
        SnapshotMutation snapshotMutation,
        byte[] fingerprint,
        DateTimeOffset committedAtUtc,
        string storeIdentity,
        CancellationToken cancellationToken)
    {
        ConfigureLocalCommitConnection(connection);
        using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
        SqliteLocalCommitSql.ThrowIfStreamQuarantined(connection, transaction, storeIdentity, operation.StreamId);
        var query = new SqliteCommittedResultQuery(
            operation,
            snapshotMutation,
            fingerprint,
            _maximumReadPayloadBytes);
        if (SqliteLocalCommitSql.TryReadCommittedResult(connection, transaction, storeIdentity, query, out var existing)
            && existing is not null)
        {
            CommitWithOperationStateIntegrity(transaction);
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
        SqliteOutboxCapacitySql.EnsureCapacityFor(connection, transaction, storeIdentity, operation, _outboxOptions);
        SqliteLocalCommitSql.InsertOutboxOperation(connection, transaction, storeIdentity, operation, nextRevision, fingerprint, committedAtUtc);
        SqliteLocalCommitSql.InsertOutboxAuthoritativeMutation(
            connection,
            transaction,
            storeIdentity,
            operation.OperationId,
            snapshotMutation.AuthoritativeState);
        SqliteLocalCommitSql.InsertOperationMetadata(connection, transaction, storeIdentity, operation);
        SqliteLocalCommitSql.InsertInitialOperationState(connection, transaction, storeIdentity, operation, committedAtUtc);
        SqliteLocalCommitSql.UpsertSnapshot(connection, transaction, storeIdentity, snapshotMutation, nextRevision, stream.ServerCursor, committedAtUtc);
        SqliteLocalCommitSql.UpdateNextClientSequence(connection, transaction, storeIdentity, operation.StreamId, operation.ClientSequence + 1);
        cancellationToken.ThrowIfCancellationRequested();
        CommitAtCheckpoints(transaction, SqliteCommitCheckpoint.LocalCommitBeforeCommit, SqliteCommitCheckpoint.LocalCommitAfterCommit);
        return new(operation.OperationId, operation.ClientSequence, nextRevision, committedAtUtc);
    }
}
