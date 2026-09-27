// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Persists exactly-once guarantee expiry and explicit at-least-once downgrade decisions.</summary>
/// <remarks>
/// A store that implements this contract lets the sync engine apply
/// <see cref="ExactlyOnceExpiryBehavior"/> durably when a previously attempted exactly-once operation outlives the
/// negotiated deduplication window. Both transitions keep the operation's original policy, so a resend still matches
/// the server idempotency fingerprint.
/// </remarks>
public interface ILocalDeliveryGuaranteeStore
{
    /// <summary>Durably moves a leased exactly-once operation to <see cref="SyncOperationState.GuaranteeExpired"/>.</summary>
    /// <param name="leaseId">The lease that owns the operation.</param>
    /// <param name="operationId">The exactly-once operation whose guarantee window expired.</param>
    /// <param name="cancellationToken">The cancellation token observed before persistence commits.</param>
    /// <returns>The durable status after the transition, carrying <see cref="SyncReasonCodes.GuaranteeExpired"/>.</returns>
    ValueTask<SyncOperationStatus> ExpireDeliveryGuaranteeAsync(
        Guid leaseId,
        OperationId operationId,
        CancellationToken cancellationToken);

    /// <summary>Durably records an explicit at-least-once downgrade for a leased exactly-once operation.</summary>
    /// <param name="leaseId">The lease that owns the operation.</param>
    /// <param name="operationId">The exactly-once operation whose guarantee window expired.</param>
    /// <param name="retryState">The fresh at-least-once retry anchor that replaces the expired exactly-once anchor.</param>
    /// <param name="cancellationToken">The cancellation token observed before persistence commits.</param>
    /// <returns>The durable status after the transition, carrying <see cref="SyncReasonCodes.GuaranteeDowngraded"/>.</returns>
    ValueTask<SyncOperationStatus> DowngradeDeliveryGuaranteeAsync(
        Guid leaseId,
        OperationId operationId,
        RetryState retryState,
        CancellationToken cancellationToken);
}
