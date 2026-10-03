// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.OccasionallyConnected.Reactive;
#else
namespace ReactiveUI.Primitives.OccasionallyConnected;
#endif

/// <summary>Delivery-guarantee outcomes for the <see cref="SyncEngine"/> upload pump.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>The fault message published when an exactly-once operation stops at its window.</summary>
    private const string GuaranteeExpiredFaultMessage = "An exactly-once operation outlived its deduplication window and stopped.";

    /// <summary>The fault message published when an exactly-once operation explicitly falls back to at-least-once.</summary>
    private const string GuaranteeDowngradedFaultMessage =
        "An exactly-once operation outlived its deduplication window and continues under at-least-once retry.";

    /// <summary>The fault message published when an at-most-once attempt loses its outcome.</summary>
    private const string AtMostOnceAmbiguousFaultMessage = "An at-most-once operation lost its outcome and will not be resent.";

    /// <summary>Describes the outcome of pre-send exactly-once retry anchor preparation.</summary>
    private enum UploadAnchorPreparation
    {
        /// <summary>Every leased operation may be sent.</summary>
        Proceed = 0,

        /// <summary>The lease was released and a fault was published.</summary>
        Faulted = 1,

        /// <summary>An operation stopped as guarantee-expired and the lease was released.</summary>
        GuaranteeExpired = 2,
    }

    /// <summary>Checks whether a status carries the durable at-least-once downgrade marker.</summary>
    /// <param name="status">The durable status.</param>
    /// <returns><see langword="true"/> when the operation was explicitly downgraded.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsGuaranteeDowngraded(SyncOperationStatus? status) =>
        StringComparer.Ordinal.Equals(status?.ReasonCode, SyncReasonCodes.GuaranteeDowngraded);

    /// <summary>Gets the later of an optional current due time and a candidate due time.</summary>
    /// <param name="current">The current latest due time.</param>
    /// <param name="candidate">The candidate due time.</param>
    /// <returns>The later due time.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DateTimeOffset GetLatestDueUtc(DateTimeOffset? current, DateTimeOffset candidate) =>
        current is { } value && value >= candidate ? value : candidate;

    /// <summary>Checks whether the store can durably apply the configured exactly-once expiry behaviour.</summary>
    /// <param name="operation">The leased operation.</param>
    /// <param name="guaranteeStore">The durable guarantee store, when available.</param>
    /// <returns><see langword="true"/> when the operation is exactly-once and the store supports guarantee transitions.</returns>
    private bool CanApplyExactlyOnceExpiry(SyncOperation operation, [NotNullWhen(true)] out ILocalDeliveryGuaranteeStore? guaranteeStore)
    {
        guaranteeStore = operation.Policy.DeliveryGuarantee == DeliveryGuarantee.ExactlyOnce
            ? _options.Store as ILocalDeliveryGuaranteeStore
            : null;
        return guaranteeStore is not null;
    }

    /// <summary>Applies <see cref="ExactlyOnceExpiryBehavior"/> to a leased operation that reached its deduplication window.</summary>
    /// <param name="guaranteeStore">The durable guarantee store.</param>
    /// <param name="leaseId">The active lease identifier.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="operation">The leased operation.</param>
    /// <param name="age">The retry anchor age.</param>
    /// <returns>Whether the operation may proceed, was faulted, or stopped as guarantee-expired.</returns>
    private async Task<UploadAnchorPreparation> ApplyExactlyOnceExpiryAsync(
        ILocalDeliveryGuaranteeStore guaranteeStore,
        Guid leaseId,
        StreamId streamId,
        SyncOperation operation,
        TimeSpan age)
    {
        var lookup = await TryGetUploadRetryAnchorStatusAsync(leaseId, streamId, operation.OperationId).ConfigureAwait(false);
        if (lookup.Faulted)
        {
            return UploadAnchorPreparation.Faulted;
        }

        if (IsGuaranteeDowngraded(lookup.Status))
        {
            if (age < _options.Options.Retry.MaximumRetryAge)
            {
                return UploadAnchorPreparation.Proceed;
            }

            await ReleaseExpiredUploadRetryWindowAsync(leaseId, streamId).ConfigureAwait(false);
            return UploadAnchorPreparation.Faulted;
        }

        try
        {
            return _options.Options.ExactlyOnceExpiryBehavior == ExactlyOnceExpiryBehavior.FallbackToAtLeastOnce
                ? await DowngradeExactlyOnceAsync(guaranteeStore, leaseId, streamId, operation).ConfigureAwait(false)
                : await ExpireExactlyOnceAsync(guaranteeStore, leaseId, streamId, operation).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            await ReleaseFaultedUploadLeaseAsync(leaseId, streamId, exception).ConfigureAwait(false);
            return UploadAnchorPreparation.Faulted;
        }
    }

    /// <summary>Emits the downgrade fault, then durably records the explicit at-least-once fallback.</summary>
    /// <param name="guaranteeStore">The durable guarantee store.</param>
    /// <param name="leaseId">The active lease identifier.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="operation">The leased operation.</param>
    /// <returns>The proceed outcome.</returns>
    private async Task<UploadAnchorPreparation> DowngradeExactlyOnceAsync(
        ILocalDeliveryGuaranteeStore guaranteeStore,
        Guid leaseId,
        StreamId streamId,
        SyncOperation operation)
    {
        var retryAnchor = RetryState.Start(_options.TimeProvider.GetUtcNow());
        var status = await guaranteeStore
            .DowngradeDeliveryGuaranteeAsync(leaseId, operation.OperationId, retryAnchor, CancellationToken.None)
            .ConfigureAwait(false);
        PublishOperationFault(
            SyncReasonCodes.GuaranteeDowngraded,
            GuaranteeDowngradedFaultMessage,
            streamId,
            operation.OperationId,
            FaultSeverity.Warning);
        _operationStates.Publish(status);
        return UploadAnchorPreparation.Proceed;
    }

    /// <summary>Durably stops an exactly-once operation as guarantee-expired and reports it.</summary>
    /// <param name="guaranteeStore">The durable guarantee store.</param>
    /// <param name="leaseId">The active lease identifier.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="operation">The leased operation.</param>
    /// <returns>The guarantee-expired outcome.</returns>
    private async Task<UploadAnchorPreparation> ExpireExactlyOnceAsync(
        ILocalDeliveryGuaranteeStore guaranteeStore,
        Guid leaseId,
        StreamId streamId,
        SyncOperation operation)
    {
        var status = await guaranteeStore
            .ExpireDeliveryGuaranteeAsync(leaseId, operation.OperationId, CancellationToken.None)
            .ConfigureAwait(false);
        try
        {
            await _options.Store.ReleaseLeaseAsync(leaseId, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception releaseException)
        {
            PublishFault("OC.Engine.UploadLeaseRelease", "A guarantee-expired upload lease release failed.", streamId, releaseException);
        }

        _operationStates.Publish(status);
        PublishOperationFault(
            SyncReasonCodes.GuaranteeExpired,
            GuaranteeExpiredFaultMessage,
            streamId,
            operation.OperationId,
            FaultSeverity.Error);
        return UploadAnchorPreparation.GuaranteeExpired;
    }

    /// <summary>Reports leased at-most-once operations whose durable attempt barrier made the outcome ambiguous.</summary>
    /// <param name="lease">The failed lease.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <returns>The operations that must not be retried.</returns>
    private async Task<HashSet<OperationId>?> ReportAmbiguousAtMostOnceOperationsAsync(LeasedOperationBatch lease, StreamId streamId)
    {
        HashSet<OperationId>? ambiguous = null;
        for (var i = 0; i < lease.Operations.Count; i++)
        {
            var operation = lease.Operations[i];
            if (operation.Policy.DeliveryGuarantee != DeliveryGuarantee.AtMostOnce)
            {
                continue;
            }

            var status = await _options.Store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None).ConfigureAwait(false);
            if (status is not { State: SyncOperationState.Ambiguous })
            {
                continue;
            }

            ambiguous ??= [];
            _ = ambiguous.Add(operation.OperationId);
            _operationStates.Publish(status);
            PublishOperationFault(
                SyncReasonCodes.AtMostOnceAmbiguous,
                AtMostOnceAmbiguousFaultMessage,
                streamId,
                operation.OperationId,
                FaultSeverity.Error);
        }

        return ambiguous;
    }

    /// <summary>Gets the retry decision for one failed operation, honouring exactly-once window expiry and explicit downgrade.</summary>
    /// <param name="operation">The failed operation.</param>
    /// <param name="failure">The classified failure.</param>
    /// <param name="state">The durable retry state.</param>
    /// <param name="retryPolicy">The retry policy bounded by leased capabilities.</param>
    /// <param name="execution">The upload execution context.</param>
    /// <returns>The retry decision and, when the window must be evaluated before the next send, its due time.</returns>
    private async Task<UploadFailureDecision> GetUploadFailureDecisionAsync(
        SyncOperation operation,
        RetryFailure failure,
        RetryState state,
        RetryPolicy retryPolicy,
        PreparedUploadExecution execution)
    {
        var decision = retryPolicy.GetDecision(failure, state);
        if (decision.Kind != RetryDecisionKind.Stop
            || decision.StopReason != RetryStopReason.RetryAgeExhausted
            || execution.ExactlyOnceWindow is not { } window
            || execution.RetryOptions.MaximumRetryAge < window
            || !CanApplyExactlyOnceExpiry(operation, out _))
        {
            return new(decision, ExpiryDueUtc: null);
        }

        var status = await _options.Store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None).ConfigureAwait(false);
        if (IsGuaranteeDowngraded(status))
        {
            var fallbackPolicy = new RetryPolicy(_options.Options.Retry, _options.TimeProvider, _options.RetryRandomSource);
            return new(fallbackPolicy.GetDecision(failure, state), ExpiryDueUtc: null);
        }

        return new(decision, state.StartedUtc + window);
    }

    /// <summary>Publishes a sanitized fault for one operation's delivery-guarantee outcome.</summary>
    /// <param name="code">The stable fault code.</param>
    /// <param name="message">The stable diagnostic message.</param>
    /// <param name="streamId">The related stream.</param>
    /// <param name="operationId">The related operation.</param>
    /// <param name="severity">The fault severity.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PublishOperationFault(string code, string message, StreamId streamId, OperationId operationId, FaultSeverity severity) =>
        _faults.Publish(new(
            code,
            message,
            _options.TimeProvider.GetUtcNow(),
            streamId,
            operationId,
            Exception: null) { Category = FaultCategory.Transport, Severity = severity, IsTransient = false });

    /// <summary>Describes one failed operation's retry decision.</summary>
    /// <param name="Decision">The retry decision.</param>
    /// <param name="ExpiryDueUtc">The exactly-once window end that must be evaluated before the next send, when deferred.</param>
    private readonly record struct UploadFailureDecision(RetryDecision Decision, DateTimeOffset? ExpiryDueUtc);

    /// <summary>Describes how one failed operation is scheduled for retry.</summary>
    /// <param name="Stopped">Whether the whole lease stops retrying.</param>
    /// <param name="Reported">Whether the stop was already reported as a fault.</param>
    /// <param name="Retried">Whether retry state was persisted.</param>
    /// <param name="DueUtc">The next due time requested by the operation.</param>
    private readonly record struct OperationRetrySchedule(bool Stopped, bool Reported, bool Retried, DateTimeOffset? DueUtc);
}
