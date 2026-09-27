// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Exactly-once guarantee expiry and downgrade support for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
internal sealed partial class InMemoryLocalStoreAdapter : ILocalDeliveryGuaranteeStore
{
    /// <inheritdoc/>
    public ValueTask<SyncOperationStatus> ExpireDeliveryGuaranteeAsync(
        Guid leaseId,
        OperationId operationId,
        CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateLeaseId(leaseId);
        InMemoryLocalStoreAdapterValidation.ValidateOperationId(operationId, nameof(operationId));
        cancellationToken.ThrowIfCancellationRequested();
        var nowUtc = _timeProvider.GetUtcNow();
        SyncOperationStatus status;
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            var record = GetGuaranteeTransitionRecord(leaseId, operationId, nowUtc);
            status = CreateStatus(record.Operation, SyncOperationState.GuaranteeExpired, record.Attempt, nowUtc, SyncReasonCodes.GuaranteeExpired);
            ApplyGuaranteeTransition(record, status, record.RetryState, nowUtc);
        }

        return new(status);
    }

    /// <inheritdoc/>
    public ValueTask<SyncOperationStatus> DowngradeDeliveryGuaranteeAsync(
        Guid leaseId,
        OperationId operationId,
        RetryState retryState,
        CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateLeaseId(leaseId);
        InMemoryLocalStoreAdapterValidation.ValidateOperationId(operationId, nameof(operationId));
        InMemoryLocalStoreAdapterValidation.ValidateRetryState(retryState);
        cancellationToken.ThrowIfCancellationRequested();
        var nowUtc = _timeProvider.GetUtcNow();
        SyncOperationStatus status;
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            var record = GetGuaranteeTransitionRecord(leaseId, operationId, nowUtc);
            status = CreateStatus(record.Operation, record.Status.State, record.Attempt, nowUtc, SyncReasonCodes.GuaranteeDowngraded);
            ApplyGuaranteeTransition(record, status, retryState, record.TerminalAtUtc);
        }

        return new(status);
    }

    /// <summary>Keeps a durable downgrade marker while the operation stays in flight.</summary>
    /// <param name="current">The current durable status.</param>
    /// <param name="next">The next durable state.</param>
    /// <param name="reasonCode">The reason code requested by the transition.</param>
    /// <returns>The reason code to persist.</returns>
    private static string? KeepDowngradeMarker(SyncOperationStatus current, SyncOperationState next, string? reasonCode) =>
        StringComparer.Ordinal.Equals(current.ReasonCode, SyncReasonCodes.GuaranteeDowngraded)
        && next is SyncOperationState.QueuedForUpload or SyncOperationState.Uploading
            ? current.ReasonCode
            : reasonCode;

    /// <summary>Gets a leased exactly-once operation that may change its delivery guarantee.</summary>
    /// <param name="leaseId">The owning lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="nowUtc">The sampled current timestamp.</param>
    /// <returns>The operation record.</returns>
    /// <exception cref="InvalidOperationException">The lease does not own an in-flight exactly-once operation.</exception>
    private OperationRecord GetGuaranteeTransitionRecord(Guid leaseId, OperationId operationId, DateTimeOffset nowUtc)
    {
        var lease = GetActiveLease(leaseId, nowUtc);
        ThrowIfLeaseQuarantined(lease);
        if (!lease.Owns(operationId))
        {
            throw new InvalidOperationException("The lease does not own the operation.");
        }

        var record = _operations[operationId];
        ThrowIfStreamQuarantined(record.Operation.StreamId);
        if (record.Operation.Policy.DeliveryGuarantee != DeliveryGuarantee.ExactlyOnce)
        {
            throw new InvalidOperationException("Only exactly-once operations can expire or downgrade their delivery guarantee.");
        }

        if (IsDefinitiveTerminal(record.Status.State) || IsBlockingHead(record.Status.State))
        {
            throw new InvalidOperationException("The operation cannot change its delivery guarantee from its current state.");
        }

        return record;
    }

    /// <summary>Applies one guarantee transition with retained-capacity accounting.</summary>
    /// <param name="record">The operation record.</param>
    /// <param name="status">The new durable status.</param>
    /// <param name="retryState">The retry state to keep.</param>
    /// <param name="terminalAtUtc">The terminal timestamp to keep.</param>
    private void ApplyGuaranteeTransition(
        OperationRecord record,
        SyncOperationStatus status,
        RetryState? retryState,
        DateTimeOffset? terminalAtUtc)
    {
        var capacity = CapacityDifference(
            OperationRecordCapacity(record),
            OperationRecordCapacity(record, status, retryState, record.LeaseId, record.LeaseExpiresAtUtc, terminalAtUtc));
        EnsureCapacityFor(capacity);
        record.Status = status;
        record.RetryState = retryState;
        record.TerminalAtUtc = terminalAtUtc;
        ApplyCapacity(capacity);
    }
}
