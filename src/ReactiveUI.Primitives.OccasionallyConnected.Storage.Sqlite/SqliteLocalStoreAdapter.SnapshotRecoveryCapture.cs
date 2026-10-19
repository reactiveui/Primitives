// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Provides bounded snapshot recovery capture through the SQLite worker.</summary>
public sealed partial class SqliteLocalStoreAdapter
{
    /// <inheritdoc/>
    public async ValueTask<LocalSnapshotRecoveryCapture> CaptureSnapshotRecoveryAsync(
        LocalSnapshotRecoveryCaptureRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        ReserveCapture(_workerCapacityBytes);
        try
        {
            return await ExecuteAsync(
                token => _store.CaptureSnapshotRecovery(request, token),
                _workerCapacityBytes,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReleaseCapture(_workerCapacityBytes);
        }
    }
}
