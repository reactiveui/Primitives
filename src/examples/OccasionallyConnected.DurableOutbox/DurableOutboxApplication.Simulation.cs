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
        if (!selection.TryGetValue(out var selected, out var failure))
        {
            return failure;
        }

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
        if (!lease.TryGetValue(out var batch, out var leaseFailure))
        {
            return leaseFailure;
        }

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

    /// <summary>Holds either a selected value or the command failure that prevented selection.</summary>
    /// <typeparam name="T">The selected value type.</typeparam>
    private sealed class Selection<T>
        where T : class
    {
        /// <summary>The selected value, or <see langword="null"/> when selection failed.</summary>
        private readonly T? _value;

        /// <summary>The command failure, or <see langword="null"/> when selection succeeded.</summary>
        private readonly OutboxCommandResult? _failure;

        /// <summary>Initializes a new instance of the <see cref="Selection{T}"/> class.</summary>
        /// <param name="value">The selected value.</param>
        /// <param name="failure">The command failure.</param>
        private Selection(T? value, OutboxCommandResult? failure)
        {
            _value = value;
            _failure = failure;
        }

        /// <summary>Creates a successful selection.</summary>
        /// <param name="value">The selected value.</param>
        /// <returns>The selection.</returns>
        public static Selection<T> Selected(T value) => new(value, null);

        /// <summary>Creates a failed selection.</summary>
        /// <param name="failure">The command failure.</param>
        /// <returns>The selection.</returns>
        public static Selection<T> Failed(OutboxCommandResult failure) => new(null, failure);

        /// <summary>Tries to get the selected value.</summary>
        /// <param name="value">The selected value when selection succeeded.</param>
        /// <param name="failure">The command failure when selection failed.</param>
        /// <returns><see langword="true"/> when selection succeeded.</returns>
        /// <exception cref="InvalidOperationException">The selection holds neither a value nor a failure.</exception>
        public bool TryGetValue([NotNullWhen(true)] out T? value, [NotNullWhen(false)] out OutboxCommandResult? failure)
        {
            if (_value is { } selected)
            {
                value = selected;
                failure = null;
                return true;
            }

            value = null;
            failure = _failure ?? throw new InvalidOperationException("The selection holds neither a value nor a failure.");
            return false;
        }
    }

    /// <summary>Represents attempt barrier selection or failure.</summary>
    /// <param name="Result">The attempt barrier result.</param>
    /// <param name="Failure">The command failure.</param>
    private sealed record BarrierSelection(AttemptBarrierResult Result, OutboxCommandResult? Failure);
}
