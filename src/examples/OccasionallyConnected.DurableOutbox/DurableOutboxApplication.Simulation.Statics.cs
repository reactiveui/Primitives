// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Static simulation helpers for <see cref="DurableOutboxApplication"/>.</summary>
internal sealed partial class DurableOutboxApplication
{
    /// <summary>Selects a pending operation or returns a command failure.</summary>
    /// <param name="session">The recovered store session.</param>
    /// <param name="operationId">The requested operation id.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The selected operation or failure.</returns>
    private static async ValueTask<OperationSelection> SelectOperationOrFailureAsync(
        StoreSession session,
        OperationId operationId,
        CancellationToken cancellationToken)
    {
        var selected = SelectOperation(session.Recovered, operationId);
        if (selected is not null)
        {
            return new SelectedOperationSelection(selected);
        }

        var status = operationId.Value == Guid.Empty
            ? null
            : await session.Store.GetOperationStatusAsync(operationId, cancellationToken).ConfigureAwait(false);
        var failure = status?.State == SyncOperationState.Ambiguous
            ? Error(AtMostOnceAmbiguousMessage, exitCode: 2) with { OperationId = operationId }
            : Error("No pending operation was available for the requested local simulation.", exitCode: 2)
                with { OperationId = operationId };
        return new FailedOperationSelection(failure);
    }

    /// <summary>Rejects a retry of an ambiguous at-most-once operation.</summary>
    /// <param name="operation">The pending operation.</param>
    /// <param name="status">The current durable status.</param>
    /// <returns>A failure result when retry is denied; otherwise <see langword="null"/>.</returns>
    private static OutboxCommandResult? RejectAmbiguousAtMostOnce(SyncOperation operation, SyncOperationStatus status)
    {
        var isAmbiguous = status.State == SyncOperationState.Ambiguous;
        var isAtMostOnce = operation.Policy.DeliveryGuarantee == DeliveryGuarantee.AtMostOnce;
        return isAmbiguous && isAtMostOnce
            ? Error(AtMostOnceAmbiguousMessage, exitCode: 2) with { OperationId = operation.OperationId }
            : null;
    }

    /// <summary>Leases the selected operation when it is next in durable order.</summary>
    /// <param name="store">The SQLite store adapter.</param>
    /// <param name="operation">The selected operation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The lease batch or failure.</returns>
    private static async ValueTask<LeaseSelection> LeaseSelectedOperationAsync(
        ILocalStoreAdapter store,
        SyncOperation operation,
        CancellationToken cancellationToken)
    {
        var lease = await LeaseSingleAsync(store, cancellationToken).ConfigureAwait(false);
        if (lease is null)
        {
            return new FailedLeaseSelection(
                Error("No lease could be acquired for the pending operation.", exitCode: 2)
                    with { OperationId = operation.OperationId });
        }

        if (ContainsOperation(lease, operation.OperationId))
        {
            return new SelectedLeaseSelection(lease);
        }

        await store.ReleaseLeaseAsync(lease.LeaseId, cancellationToken).ConfigureAwait(false);
        var failure = Error("The requested operation is not the next leased operation; drain earlier work first.", exitCode: 2)
            with { OperationId = operation.OperationId };
        return new FailedLeaseSelection(failure);
    }

    /// <summary>Begins a durable remote attempt for a leased operation.</summary>
    /// <param name="store">The SQLite store adapter.</param>
    /// <param name="batch">The lease batch.</param>
    /// <param name="operation">The operation being attempted.</param>
    /// <param name="status">The current durable status.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The attempt barrier or failure.</returns>
    private static async ValueTask<BarrierSelection> BeginAttemptAsync(
        ILocalStoreAdapter store,
        LeasedOperationBatch batch,
        SyncOperation operation,
        SyncOperationStatus status,
        CancellationToken cancellationToken)
    {
        var barrier = await store.TryBeginRemoteAttemptAsync(
            batch.LeaseId,
            operation.OperationId,
            status.Attempt + 1,
            cancellationToken).ConfigureAwait(false);
        if (barrier.MaySend)
        {
            return new(barrier, null);
        }

        await store.ReleaseLeaseAsync(batch.LeaseId, cancellationToken).ConfigureAwait(false);
        return new(barrier, Error(DescribeDeniedAttempt(barrier), exitCode: 2) with { OperationId = operation.OperationId });
    }

    /// <summary>Completes a lost-response simulation.</summary>
    /// <param name="session">The recovered store session.</param>
    /// <param name="operation">The selected operation.</param>
    /// <param name="barrier">The durable attempt barrier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The command result.</returns>
    private static async ValueTask<OutboxCommandResult> CompleteLostResponseAsync(
        StoreSession session,
        SyncOperation operation,
        AttemptBarrierResult barrier,
        CancellationToken cancellationToken)
    {
        var status = await session.Store.GetOperationStatusAsync(operation.OperationId, cancellationToken).ConfigureAwait(false);
        var output = FormatLines(
            "local simulation: response lost after durable attempt barrier",
            string.Create(CultureInfo.InvariantCulture, $"{OperationLabel}: {operation.OperationId.Value}"),
            string.Create(CultureInfo.InvariantCulture, $"attempt: {barrier.Attempt}"),
            string.Create(CultureInfo.InvariantCulture, $"{StateLabel}: {status?.State.ToString() ?? UnknownText}"),
            string.Create(CultureInfo.InvariantCulture, $"reason: {status?.ReasonCode ?? AtMostOnceAmbiguousReason}"),
            "server acknowledgement: not recorded");
        return Ok(output, operation.OperationId);
    }

    /// <summary>Completes an accepted-response simulation.</summary>
    /// <param name="session">The recovered store session.</param>
    /// <param name="operation">The selected operation.</param>
    /// <param name="batch">The lease batch.</param>
    /// <param name="barrier">The durable attempt barrier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The command result.</returns>
    private static async ValueTask<OutboxCommandResult> CompleteAcceptedAsync(
        StoreSession session,
        SyncOperation operation,
        LeasedOperationBatch batch,
        AttemptBarrierResult barrier,
        CancellationToken cancellationToken)
    {
        await session.Store.ApplySyncResultAsync(
            batch.LeaseId,
            new(batch.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, SampleServerVersion)], null, null),
            cancellationToken).ConfigureAwait(false);
        return await CompleteAttemptedOperationAsync(session, operation, barrier, SimulatedAttemptOutcome.Accepted, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Formats the final status for an accepted or rejected local simulation.</summary>
    /// <param name="session">The recovered store session.</param>
    /// <param name="operation">The selected operation.</param>
    /// <param name="barrier">The durable attempt barrier.</param>
    /// <param name="outcome">The simulated outcome.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The command result.</returns>
    private static async ValueTask<OutboxCommandResult> CompleteAttemptedOperationAsync(
        StoreSession session,
        SyncOperation operation,
        AttemptBarrierResult barrier,
        SimulatedAttemptOutcome outcome,
        CancellationToken cancellationToken)
    {
        var finalStatus = await session.Store.GetOperationStatusAsync(operation.OperationId, cancellationToken).ConfigureAwait(false);
        var output = FormatLines(
            string.Create(CultureInfo.InvariantCulture, $"local simulation: {outcome}"),
            string.Create(CultureInfo.InvariantCulture, $"{OperationLabel}: {operation.OperationId.Value}"),
            string.Create(CultureInfo.InvariantCulture, $"attempt: {barrier.Attempt}"),
            string.Create(CultureInfo.InvariantCulture, $"{StateLabel}: {finalStatus?.State.ToString() ?? UnknownText}"),
            "server acknowledgement: simulated locally; no transport or real server was used");
        return Ok(output, operation.OperationId);
    }

    /// <summary>Selects the requested pending operation.</summary>
    /// <param name="recovered">The recovered stream state.</param>
    /// <param name="requested">The requested operation id, or empty for the first pending operation.</param>
    /// <returns>The selected operation, or <see langword="null"/>.</returns>
    private static SyncOperation? SelectOperation(RecoveredStream recovered, OperationId requested)
    {
        if (requested.Value == Guid.Empty)
        {
            return recovered.PendingOperations.Count == 0 ? null : recovered.PendingOperations[0];
        }

        for (var index = 0; index < recovered.PendingOperations.Count; index++)
        {
            var operation = recovered.PendingOperations[index];
            if (operation.OperationId == requested)
            {
                return operation;
            }
        }

        return null;
    }

    /// <summary>Checks whether a lease contains a selected operation.</summary>
    /// <param name="batch">The lease batch.</param>
    /// <param name="operationId">The operation id.</param>
    /// <returns><see langword="true"/> when the operation is leased.</returns>
    private static bool ContainsOperation(LeasedOperationBatch batch, OperationId operationId)
    {
        for (var index = 0; index < batch.Operations.Count; index++)
        {
            if (batch.Operations[index].OperationId == operationId)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Leases at most one pending operation from SQLite.</summary>
    /// <param name="store">The SQLite store adapter.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The first lease batch, or <see langword="null"/>.</returns>
    private static async ValueTask<LeasedOperationBatch?> LeaseSingleAsync(
        ILocalStoreAdapter store,
        CancellationToken cancellationToken)
    {
        var request = new OutboxLeaseRequest(
            TemperatureStream,
            MaximumLeaseOperations,
            LeaseCapacityBytes,
            TimeSpan.FromMinutes(LeaseDurationMinutes));
        var batches = store.LeasePendingOperationsAsync(request, cancellationToken);
        await using var enumerator = batches.GetAsyncEnumerator(cancellationToken);
        return await enumerator.MoveNextAsync().ConfigureAwait(false) ? enumerator.Current : null;
    }

    /// <summary>Describes a denied attempt barrier.</summary>
    /// <param name="barrier">The denied barrier result.</param>
    /// <returns>The human-readable denial message.</returns>
    private static string DescribeDeniedAttempt(AttemptBarrierResult barrier) =>
        $"The durable store denied the send attempt: {barrier.ReasonCode ?? UnknownText}";
}
