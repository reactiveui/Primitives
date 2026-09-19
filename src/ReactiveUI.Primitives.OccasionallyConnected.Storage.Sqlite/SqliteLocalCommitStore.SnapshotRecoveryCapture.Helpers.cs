// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Persists local commit and recovery state in SQLite.</summary>
internal sealed partial class SqliteLocalCommitStore
{
    /// <summary>Validates request shape before opening SQLite resources.</summary>
    /// <param name="request">The request.</param>
    private static void ValidateSnapshotRecoveryCaptureRequest(LocalSnapshotRecoveryCaptureRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        SqliteLocalCommitValidation.ValidateRecoveryInput(request.StreamId, request.SubscriptionId);
        ArgumentExceptionHelper.ThrowIfNull(request.Limits);
        request.Limits.Validate();
    }

    /// <summary>Reads only payload-bearing rows needed by capture, excluding dead letters.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    /// <returns>The recovered payload rows.</returns>
    private static SqliteRecoveredPayloadRows ReadSnapshotRecoveryCapturePayloadRows(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId,
        long maximumPayloadBytes) =>
        new(
            SqliteLocalCommitSql.ReadSnapshot(connection, transaction, storeIdentity, streamId, maximumPayloadBytes),
            SqliteLocalCommitSql.ReadPendingOperations(connection, transaction, storeIdentity, streamId, maximumPayloadBytes),
            SqliteLocalCommitSql.ReadReplayOperations(connection, transaction, storeIdentity, streamId, maximumPayloadBytes),
            []);
}
