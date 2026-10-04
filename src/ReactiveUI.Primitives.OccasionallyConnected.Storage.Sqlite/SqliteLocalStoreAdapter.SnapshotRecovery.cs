// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Persists occasionally connected stream state in SQLite on a bounded single-command worker.</summary>
public sealed partial class SqliteLocalStoreAdapter
{
    /// <inheritdoc/>
    public async ValueTask<LocalSnapshotRecoveryResult> ApplySnapshotRecoveryAsync(
        LocalSnapshotRecoveryMutation mutation,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(mutation);
        cancellationToken.ThrowIfCancellationRequested();
        ReserveCapture(_workerCapacityBytes);
        try
        {
            SqliteLocalCommitValidation.ValidateSnapshotRecoveryMutationShape(mutation);
            cancellationToken.ThrowIfCancellationRequested();
            var retainedBytes = _sizing.SnapshotRecoveryBytes(mutation);
            return await ExecuteAsync(
                token => _store.ApplySnapshotRecovery(mutation, token),
                retainedBytes,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReleaseCapture(_workerCapacityBytes);
        }
    }
}
