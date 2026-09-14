// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Simulation helpers for <see cref="DurableOutboxApplication"/>.</summary>
internal sealed partial class DurableOutboxApplication
{
    /// <summary>Runs an explicitly local simulation of a remote attempt result.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="operationId">The operation to attempt, or empty to pick the next pending operation.</param>
    /// <param name="outcome">The simulated result.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The command result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when lease selection returns an invalid success shape.</exception>
    internal async ValueTask<OutboxCommandResult> SimulateAttemptAsync(
        string databasePath,
        OperationId operationId,
        SimulatedAttemptOutcome outcome,
        CancellationToken cancellationToken)
    {
        await using var session = await OpenSessionAsync(databasePath, cancellationToken).ConfigureAwait(false);
        var selection = await SelectOperationOrFailureAsync(session, operationId, cancellationToken).ConfigureAwait(false);
        if (selection.TryGetFailure(out var failure))
        {
            return failure;
        }

        var selected = ((SelectedOperationSelection)selection).Operation;
        var currentStatus = await session.Store.GetOperationStatusAsync(selected.OperationId, cancellationToken).ConfigureAwait(false);
        if (currentStatus is null)
        {
            return Error(MissingDurableStatusMessage, exitCode: 2) with { OperationId = selected.OperationId };
        }

        if (currentStatus.Attempt == int.MaxValue)
        {
            return Error(AttemptOverflowMessage, exitCode: 2) with { OperationId = selected.OperationId };
        }

        var ambiguousFailure = RejectAmbiguousAtMostOnce(selected, currentStatus);
        if (ambiguousFailure is not null)
        {
            return ambiguousFailure;
        }

        var lease = await LeaseSelectedOperationAsync(session.Store, selected, cancellationToken).ConfigureAwait(false);
        if (lease.TryGetFailure(out var leaseFailure))
        {
            return leaseFailure;
        }

        var batch = ((SelectedLeaseSelection)lease).Batch;
        var barrier = await BeginAttemptAsync(session.Store, batch, selected, currentStatus, cancellationToken).ConfigureAwait(false);
        return barrier.Failure ?? outcome switch
        {
            SimulatedAttemptOutcome.LostResponse => await CompleteLostResponseAsync(
                session,
                selected,
                barrier.Result,
                cancellationToken).ConfigureAwait(false),
            SimulatedAttemptOutcome.Rejected => await CompleteRejectedAsync(
                session,
                selected,
                batch,
                barrier.Result,
                cancellationToken).ConfigureAwait(false),
            _ => await CompleteAcceptedAsync(session, selected, batch, barrier.Result, cancellationToken).ConfigureAwait(false),
        };
    }

    /// <summary>Completes a rejected-response simulation with an atomic snapshot rebuild.</summary>
    /// <param name="session">The recovered store session.</param>
    /// <param name="operation">The selected operation.</param>
    /// <param name="batch">The lease batch.</param>
    /// <param name="barrier">The durable attempt barrier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The command result.</returns>
    private async ValueTask<OutboxCommandResult> CompleteRejectedAsync(
        StoreSession session,
        SyncOperation operation,
        LeasedOperationBatch batch,
        AttemptBarrierResult barrier,
        CancellationToken cancellationToken)
    {
        var snapshot = session.Recovered.Snapshot;
        if (snapshot is null)
        {
            return Error(MissingRejectionSnapshotMessage, exitCode: 2) with { OperationId = operation.OperationId };
        }

        var authoritativePayload = snapshot.AuthoritativeState;
        if (authoritativePayload is null)
        {
            return Error(MissingRejectionAuthoritativeSnapshotMessage, exitCode: 2) with
            {
                OperationId = operation.OperationId,
            };
        }

        var result = new RemoteSyncResult(
            batch.LeaseId,
            [new(operation.OperationId, OperationResultKind.Rejected, SampleRejectedReason, SampleServerVersion)],
            serverCursor: null,
            retryAfter: null);
        var rebuilt = await RebuildSnapshotExcludingAsync(
            session.Recovered,
            authoritativePayload,
            operation.OperationId,
            cancellationToken)
            .ConfigureAwait(false);
        var snapshotPayload = await _serializer.SerializeAsync(
            SnapshotContract,
            PayloadSchemaVersion,
            rebuilt,
            cancellationToken).ConfigureAwait(false);
        var mutation = CreateSnapshotMutation(snapshotPayload, authoritativePayload, snapshot.Revision);
        _ = await session.Store.ApplySyncResultAsync(batch.LeaseId, result, [mutation], cancellationToken).ConfigureAwait(false);
        return await CompleteAttemptedOperationAsync(session, operation, barrier, SimulatedAttemptOutcome.Rejected, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Rebuilds the optimistic snapshot while excluding one rejected operation.</summary>
    /// <param name="recovered">The recovered stream state.</param>
    /// <param name="authoritativePayload">The authoritative checkpoint payload.</param>
    /// <param name="rejectedOperationId">The rejected operation id.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The rebuilt snapshot.</returns>
    private async ValueTask<TemperatureSnapshot> RebuildSnapshotExcludingAsync(
        RecoveredStream recovered,
        PayloadEnvelope authoritativePayload,
        OperationId rejectedOperationId,
        CancellationToken cancellationToken)
    {
        var state = (TemperatureSnapshot)await _serializer.DeserializeAsync(
            authoritativePayload,
            typeof(TemperatureSnapshot),
            cancellationToken).ConfigureAwait(false);
        for (var index = 0; index < recovered.ReplayOperations.Count; index++)
        {
            var operation = recovered.ReplayOperations[index];
            if (operation.OperationId == rejectedOperationId)
            {
                continue;
            }

            var value = await _serializer.DeserializeAsync(
                operation.Payload,
                typeof(TemperatureReading),
                cancellationToken).ConfigureAwait(false);
            state = state.Apply((TemperatureReading)value);
        }

        return state;
    }

    /// <summary>Represents operation selection or failure.</summary>
    private abstract record OperationSelection
    {
        /// <summary>Tries to get a command failure for the selection.</summary>
        /// <param name="failure">The command failure when selection failed.</param>
        /// <returns><see langword="true"/> when selection failed.</returns>
        public abstract bool TryGetFailure([NotNullWhen(true)] out OutboxCommandResult? failure);
    }

    /// <summary>Represents a successful operation selection.</summary>
    /// <param name="Operation">The selected operation.</param>
    private sealed record SelectedOperationSelection(SyncOperation Operation) : OperationSelection
    {
        /// <inheritdoc/>
        public override bool TryGetFailure([NotNullWhen(true)] out OutboxCommandResult? failure)
        {
            failure = null;
            return false;
        }
    }

    /// <summary>Represents a failed operation selection.</summary>
    /// <param name="Failure">The command failure.</param>
    private sealed record FailedOperationSelection(OutboxCommandResult Failure) : OperationSelection
    {
        /// <inheritdoc/>
        public override bool TryGetFailure([NotNullWhen(true)] out OutboxCommandResult? failure)
        {
            failure = Failure;
            return true;
        }
    }

    /// <summary>Represents lease selection or failure.</summary>
    private abstract record LeaseSelection
    {
        /// <summary>Tries to get a command failure for the selection.</summary>
        /// <param name="failure">The command failure when selection failed.</param>
        /// <returns><see langword="true"/> when selection failed.</returns>
        public abstract bool TryGetFailure([NotNullWhen(true)] out OutboxCommandResult? failure);
    }

    /// <summary>Represents a successful lease selection.</summary>
    /// <param name="Batch">The selected lease batch.</param>
    private sealed record SelectedLeaseSelection(LeasedOperationBatch Batch) : LeaseSelection
    {
        /// <inheritdoc/>
        public override bool TryGetFailure([NotNullWhen(true)] out OutboxCommandResult? failure)
        {
            failure = null;
            return false;
        }
    }

    /// <summary>Represents a failed lease selection.</summary>
    /// <param name="Failure">The command failure.</param>
    private sealed record FailedLeaseSelection(OutboxCommandResult Failure) : LeaseSelection
    {
        /// <inheritdoc/>
        public override bool TryGetFailure([NotNullWhen(true)] out OutboxCommandResult? failure)
        {
            failure = Failure;
            return true;
        }
    }

    /// <summary>Represents attempt barrier selection or failure.</summary>
    /// <param name="Result">The attempt barrier result.</param>
    /// <param name="Failure">The command failure.</param>
    private sealed record BarrierSelection(AttemptBarrierResult Result, OutboxCommandResult? Failure);
}
