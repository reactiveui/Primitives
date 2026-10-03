// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Exactly-once guarantee expiry and downgrade support for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapter : ILocalDeliveryGuaranteeStore
{
    /// <inheritdoc/>
    public ValueTask<SyncOperationStatus> ExpireDeliveryGuaranteeAsync(
        Guid leaseId,
        OperationId operationId,
        CancellationToken cancellationToken) =>
        new(ExecuteAsync(
            token => _store.ExpireDeliveryGuarantee(leaseId, operationId, token),
            _sizing.AttemptBarrierBytes,
            cancellationToken));

    /// <inheritdoc/>
    public ValueTask<SyncOperationStatus> DowngradeDeliveryGuaranteeAsync(
        Guid leaseId,
        OperationId operationId,
        RetryState retryState,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(retryState);
        return new(ExecuteAsync(
            token => _store.DowngradeDeliveryGuarantee(leaseId, operationId, retryState, token),
            _sizing.AttemptBarrierBytes + _sizing.RetryStateBytes(retryState),
            cancellationToken));
    }
}
