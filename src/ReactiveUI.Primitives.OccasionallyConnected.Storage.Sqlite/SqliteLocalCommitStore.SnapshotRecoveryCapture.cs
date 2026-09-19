// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Data;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Persists local commit and recovery state in SQLite.</summary>
internal sealed partial class SqliteLocalCommitStore
{
    /// <summary>Captures a bounded snapshot recovery view without mutating durable state.</summary>
    /// <param name="request">The capture request.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The bounded capture.</returns>
    internal LocalSnapshotRecoveryCapture CaptureSnapshotRecovery(
        LocalSnapshotRecoveryCaptureRequest request,
        CancellationToken cancellationToken)
    {
        ValidateSnapshotRecoveryCaptureRequest(request);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            return CaptureSnapshotRecoveryLocked(request, cancellationToken);
        }
    }

    /// <summary>Captures snapshot recovery state while holding the store gate.</summary>
    /// <param name="request">The capture request.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The bounded capture.</returns>
    /// <exception cref="InvalidOperationException">The stored snapshot recovery state is invalid.</exception>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The capture exceeds a configured limit.</exception>
    private LocalSnapshotRecoveryCapture CaptureSnapshotRecoveryLocked(
        LocalSnapshotRecoveryCaptureRequest request,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var storeIdentity = GetInitializedStoreIdentity();
        using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
        SqliteLocalCommitConnection.ConfigureLockPolling(connection);
        SqliteConnectionSettings.ConfigureOperationalConnection(connection);
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: true);
        var stream = SqliteLocalCommitSql.PreflightSnapshotRecoveryCapture(
            connection,
            transaction,
            storeIdentity,
            request,
            cancellationToken);
        var payloadRows = ReadSnapshotRecoveryCapturePayloadRows(connection, transaction, storeIdentity, request.StreamId, _maximumReadPayloadBytes);
        if (payloadRows.HasRows && stream is null)
        {
            throw new InvalidOperationException("Committed data has no durable stream state.");
        }

        if (stream is { } durableStream && payloadRows.Snapshot is { } snapshot && snapshot.ServerCursor != durableStream.ServerCursor)
        {
            throw new InvalidOperationException("The snapshot cursor does not match the durable stream cursor.");
        }

        var recoveredStream = stream is null
            ? new RecoveredStream(request.SubscriptionId, null, null, [], [], FirstClientSequence)
            : CreateRecoveredStream(request.SubscriptionId, stream.Value, in payloadRows);
        var capture = new LocalSnapshotRecoveryCapture
        {
            StreamId = request.StreamId,
            SubscriptionId = request.SubscriptionId,
            ServerCursor = recoveredStream.ServerCursor,
            Snapshot = recoveredStream.Snapshot,
            NextClientSequence = recoveredStream.NextClientSequence,
            PendingOperations = recoveredStream.PendingOperations,
            ReplayOperations = recoveredStream.ReplayOperations,
        };
        cancellationToken.ThrowIfCancellationRequested();
        transaction.Commit();
        return capture;
    }
}
