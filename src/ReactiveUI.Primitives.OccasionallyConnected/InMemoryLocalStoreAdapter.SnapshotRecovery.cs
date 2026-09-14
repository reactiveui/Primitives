// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Applies process-local snapshot recovery transactions.</summary>
internal sealed partial class InMemoryLocalStoreAdapter
{
    /// <summary>The bounded planning collections created by one recovery request.</summary>
    private const int SnapshotRecoveryCapturedCollectionCount = 5;

    /// <summary>The minimum records reserved by an empty recovery request.</summary>
    private const int SnapshotRecoveryMinimumReservationRecords = 1;

    /// <summary>The aggregate disposition and result reservations of active recovery requests.</summary>
    private CapacityUsage _snapshotRecoveryUsage;

    /// <inheritdoc/>
    public ValueTask<LocalSnapshotRecoveryResult> ApplySnapshotRecoveryAsync(
        LocalSnapshotRecoveryMutation mutation,
        CancellationToken cancellationToken) =>
        new(ApplySnapshotRecoveryCore(mutation, cancellationToken));

    /// <summary>Applies snapshot recovery synchronously behind the ValueTask interface.</summary>
    /// <param name="mutation">The recovery mutation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The recovery result.</returns>
    private LocalSnapshotRecoveryResult ApplySnapshotRecoveryCore(
        LocalSnapshotRecoveryMutation mutation,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(mutation);
        var count = mutation.OperationDispositions.Count;
        var reservation = ReserveSnapshotRecovery(count, cancellationToken);
        try
        {
            var dispositions = CaptureSnapshotRecoveryDispositions(mutation.OperationDispositions, count, cancellationToken);
            ValidateSnapshotRecoveryMutationShape(mutation);
            cancellationToken.ThrowIfCancellationRequested();
            var nowUtc = _timeProvider.GetUtcNow();
            lock (_gate)
            {
                ThrowIfReady(cancellationToken);
                ThrowIfStreamQuarantined(mutation.StreamId);
                var stream = GetStream(mutation.StreamId);
                ValidateSnapshotRecoveryVersion(stream, mutation);
                var nextRevision = checked(mutation.ExpectedRevision + 1);
                var nextSnapshot = new LocalSnapshot(
                    mutation.StreamId,
                    mutation.SnapshotFormatVersion,
                    mutation.Checkpoint.FrontierCursor,
                    mutation.OptimisticState,
                    nextRevision,
                    nowUtc) { AuthoritativeState = mutation.Checkpoint.ClientState };
                var plan = CreateSnapshotRecoveryPlan(stream, mutation, dispositions, nextSnapshot, nowUtc, cancellationToken);

                EnsureCapacityFor(plan.Capacity);
                cancellationToken.ThrowIfCancellationRequested();
                ApplySnapshotRecoveryPlan(stream, mutation, nextSnapshot, in plan);
                return new()
                {
                    Snapshot = nextSnapshot,
                    IncludedOperationCount = plan.IncludedOperationCount,
                    TerminalOperationCount = plan.TerminalOperationCount,
                    PreservedPendingOperationCount = plan.PreservedPendingOperationCount,
                };
            }
        }
        finally
        {
            ReleaseSnapshotRecovery(reservation);
        }
    }

    /// <summary>Applies the fully validated and admitted recovery plan.</summary>
    /// <param name="stream">The stream record.</param>
    /// <param name="mutation">The recovery mutation.</param>
    /// <param name="nextSnapshot">The snapshot to store.</param>
    /// <param name="plan">The recovery plan.</param>
    private void ApplySnapshotRecoveryPlan(
        StreamRecord stream,
        LocalSnapshotRecoveryMutation mutation,
        LocalSnapshot nextSnapshot,
        in SnapshotRecoveryPlan plan)
    {
        for (var index = 0; index < plan.ExpiredLeaseIds.Length; index++)
        {
            _ = _leases.Remove(plan.ExpiredLeaseIds[index]);
        }

        for (var index = 0; index < plan.Changes.Length; index++)
        {
            var change = plan.Changes[index];
            change.Record.Status = change.Status;
            change.Record.RetryState = change.RetryState;
            change.Record.LeaseId = change.LeaseId;
            change.Record.LeaseExpiresAtUtc = change.LeaseExpiresAtUtc;
            change.Record.TerminalAtUtc = change.TerminalAtUtc;
            if (change.AddInclusion)
            {
                _ = _includedOperations.Add(change.Record.Operation.OperationId);
            }
        }

        ApplyCapacity(plan.Capacity);
        stream.Snapshot = nextSnapshot;
        stream.ServerCursor = mutation.Checkpoint.FrontierCursor;
    }

    /// <summary>Builds a recovery plan after all live fences have passed.</summary>
    /// <param name="stream">The stream record.</param>
    /// <param name="mutation">The recovery mutation.</param>
    /// <param name="dispositions">The owned dispositions.</param>
    /// <param name="nextSnapshot">The snapshot to store.</param>
    /// <param name="nowUtc">The sampled time.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The recovery plan.</returns>
    private SnapshotRecoveryPlan CreateSnapshotRecoveryPlan(
        StreamRecord stream,
        LocalSnapshotRecoveryMutation mutation,
        SnapshotRecoveryDisposition[] dispositions,
        LocalSnapshot nextSnapshot,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken)
    {
        var pending = GetSnapshotRecoveryPendingRecords(mutation.StreamId, cancellationToken);
        ValidateSnapshotRecoveryDispositions(pending, dispositions);
        var expiredLeaseIds = GetSnapshotRecoveryExpiredLeaseIds(pending, nowUtc);
        var capacity = AddCapacity(
            new(0, checked(StringBytes(mutation.Checkpoint.FrontierCursor) - StringBytes(stream.ServerCursor))),
            CapacityDifference(LocalSnapshotCapacity(stream.Snapshot), LocalSnapshotCapacity(nextSnapshot)));
        var changes = new SnapshotRecoveryOperationChange[dispositions.Length];
        var included = 0;
        var terminal = 0;
        var preserved = 0;
        for (var index = 0; index < dispositions.Length; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var record = pending[index];
            var disposition = dispositions[index];
            var change = CreateSnapshotRecoveryOperationChange(record, in disposition, expiredLeaseIds, nowUtc);
            changes[index] = change;
            capacity = AddCapacity(
                capacity,
                CapacityDifference(
                    OperationRecordCapacity(record),
                    OperationRecordCapacity(
                        record,
                        change.Status,
                        change.RetryState,
                        change.LeaseId,
                        change.LeaseExpiresAtUtc,
                        change.TerminalAtUtc)));
            if (change.AddInclusion)
            {
                capacity = AddCapacity(capacity, InclusionCapacity());
            }

            CountSnapshotRecoveryDisposition(disposition.Kind, ref included, ref terminal, ref preserved);
        }

        for (var index = 0; index < expiredLeaseIds.Length; index++)
        {
            var leaseCapacity = LeaseRecordCapacity(_leases[expiredLeaseIds[index]]);
            capacity = AddCapacity(capacity, new(checked(-leaseCapacity.Records), checked(-leaseCapacity.EncodedBytes)));
        }

        return new(capacity, changes, expiredLeaseIds, included, terminal, preserved);
    }

    /// <summary>Creates the operation mutation for one recovery disposition.</summary>
    /// <param name="record">The operation record.</param>
    /// <param name="disposition">The disposition.</param>
    /// <param name="expiredLeaseIds">The expired leases committed with recovery.</param>
    /// <param name="nowUtc">The sampled time.</param>
    /// <returns>The operation change.</returns>
    private SnapshotRecoveryOperationChange CreateSnapshotRecoveryOperationChange(
        OperationRecord record,
        in SnapshotRecoveryDisposition disposition,
        Guid[] expiredLeaseIds,
        DateTimeOffset nowUtc)
    {
        var clearLease = record.LeaseId.HasValue && Array.IndexOf(expiredLeaseIds, record.LeaseId.Value) >= 0;
        var leaseId = clearLease ? null : record.LeaseId;
        var leaseExpiresAtUtc = clearLease ? null : record.LeaseExpiresAtUtc;
        var status = record.Status;
        var retryState = record.RetryState;
        var terminalAtUtc = record.TerminalAtUtc;
        var addInclusion = false;
        if (disposition.Kind == SnapshotOperationDispositionKind.IncludedAccepted)
        {
            status = CreateStatus(record.Operation, GetResultState(disposition.ResultKind), record.Attempt, nowUtc, disposition.ReasonCode);
            retryState = null;
            leaseId = null;
            leaseExpiresAtUtc = null;
            terminalAtUtc = GetTerminalTimestamp(status, record.TerminalAtUtc, nowUtc);
            addInclusion = !_includedOperations.Contains(record.Operation.OperationId);
        }
        else if (disposition.Kind == SnapshotOperationDispositionKind.TerminalRejected)
        {
            status = CreateStatus(record.Operation, SyncOperationState.Rejected, record.Attempt, nowUtc, disposition.ReasonCode);
            retryState = null;
            leaseId = null;
            leaseExpiresAtUtc = null;
            terminalAtUtc = GetTerminalTimestamp(status, record.TerminalAtUtc, nowUtc);
        }

        return new(record, status, retryState, leaseId, leaseExpiresAtUtc, terminalAtUtc, addInclusion);
    }

    /// <summary>Finds expired leases after rejecting active ownership.</summary>
    /// <param name="pending">The pending records.</param>
    /// <param name="nowUtc">The sampled time.</param>
    /// <returns>The expired leases to remove during commit.</returns>
    /// <exception cref="InvalidOperationException">A pending operation is actively leased.</exception>
    private Guid[] GetSnapshotRecoveryExpiredLeaseIds(List<OperationRecord> pending, DateTimeOffset nowUtc)
    {
        List<Guid> expiredLeaseIds = [];
        for (var index = 0; index < pending.Count; index++)
        {
            var leaseId = pending[index].LeaseId;
            if (!leaseId.HasValue)
            {
                continue;
            }

            var lease = _leases[leaseId.Value];
            if (lease.ExpiresAtUtc > nowUtc)
            {
                throw new InvalidOperationException("A snapshot recovery operation is still owned by an active lease.");
            }

            if (!expiredLeaseIds.Contains(leaseId.Value))
            {
                expiredLeaseIds.Add(leaseId.Value);
            }
        }

        return [.. expiredLeaseIds];
    }

    /// <summary>Returns pending records for recovery using the same durable recovery convention.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The pending records.</returns>
    private List<OperationRecord> GetSnapshotRecoveryPendingRecords(StreamId streamId, CancellationToken cancellationToken)
    {
        var records = GetStreamOperations(streamId);
        List<OperationRecord> pending = [];
        for (var index = 0; index < records.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (ShouldRecoverPendingOperation(records[index]))
            {
                pending.Add(records[index]);
            }
        }

        return pending;
    }

    /// <summary>Reserves finite recovery capture capacity before allocating owned buffers.</summary>
    /// <param name="count">The caller's advertised disposition count.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The reservation to release after recovery.</returns>
    /// <exception cref="InvalidOperationException">The store is not initialized.</exception>
    /// <exception cref="ObjectDisposedException">The store is disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled.</exception>
    /// <exception cref="QueueCapacityExceededException">Active recovery planning exceeds configured bounds.</exception>
    /// <remarks>
    /// <see cref="LocalSnapshotRecoveryMutation"/> already owns the public disposition list. This reservation bounds
    /// adapter-owned planning collections created for the transaction: validated dispositions, pending record
    /// references, operation changes, duplicate tracking, and expired lease identifiers. Encoded bytes are logical
    /// GUID and count fields used to share the same finite admission model as retained store records.
    /// </remarks>
    private CapacityUsage ReserveSnapshotRecovery(int count, CancellationToken cancellationToken)
    {
        var records = Math.Max(SnapshotRecoveryMinimumReservationRecords, checked(count * SnapshotRecoveryCapturedCollectionCount));
        var encodedBytes = checked(
            (SnapshotRecoveryCapturedCollectionCount * Int32EncodedBytes)
            + (SnapshotRecoveryCapturedCollectionCount * GuidEncodedBytes * count));
        var reservation = new CapacityUsage(records, encodedBytes);
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            if (reservation.Records <= _maximumRecordCount - _snapshotRecoveryUsage.Records
                && reservation.EncodedBytes <= _maximumEncodedBytes - _snapshotRecoveryUsage.EncodedBytes)
            {
                _snapshotRecoveryUsage = new(
                    _snapshotRecoveryUsage.Records + reservation.Records,
                    _snapshotRecoveryUsage.EncodedBytes + reservation.EncodedBytes);
                return reservation;
            }

            var canFitWhenEmpty = reservation.Records <= _maximumRecordCount && reservation.EncodedBytes <= _maximumEncodedBytes;
            throw new QueueCapacityExceededException("The in-memory snapshot recovery capacity would be exceeded.", canFitWhenEmpty);
        }
    }

    /// <summary>Releases a snapshot recovery capture reservation.</summary>
    /// <param name="reservation">The admitted recovery capacity.</param>
    private void ReleaseSnapshotRecovery(CapacityUsage reservation)
    {
        lock (_gate)
        {
            _snapshotRecoveryUsage = new(
                _snapshotRecoveryUsage.Records - reservation.Records,
                _snapshotRecoveryUsage.EncodedBytes - reservation.EncodedBytes);
        }
    }

    /// <summary>Describes one validated recovery disposition without nullable proof lookups.</summary>
    /// <param name="OperationId">The operation identifier.</param>
    /// <param name="Kind">The disposition kind.</param>
    /// <param name="ResultKind">The proven result kind, or a placeholder for unknown dispositions.</param>
    /// <param name="ReasonCode">The proven result reason code.</param>
    private readonly record struct SnapshotRecoveryDisposition(
        OperationId OperationId,
        SnapshotOperationDispositionKind Kind,
        OperationResultKind ResultKind,
        string? ReasonCode);

    /// <summary>Describes one operation change inside a snapshot recovery transaction.</summary>
    /// <param name="Record">The operation record.</param>
    /// <param name="Status">The final status.</param>
    /// <param name="RetryState">The final retry state.</param>
    /// <param name="LeaseId">The final lease identifier.</param>
    /// <param name="LeaseExpiresAtUtc">The final lease expiry.</param>
    /// <param name="TerminalAtUtc">The final terminal timestamp.</param>
    /// <param name="AddInclusion">Whether an inclusion marker is added.</param>
    private readonly record struct SnapshotRecoveryOperationChange(
        OperationRecord Record,
        SyncOperationStatus Status,
        RetryState? RetryState,
        Guid? LeaseId,
        DateTimeOffset? LeaseExpiresAtUtc,
        DateTimeOffset? TerminalAtUtc,
        bool AddInclusion);

    /// <summary>Describes one fully validated recovery transaction.</summary>
    /// <param name="Capacity">The retained capacity delta.</param>
    /// <param name="Changes">The operation changes.</param>
    /// <param name="ExpiredLeaseIds">The expired leases removed by the commit.</param>
    /// <param name="IncludedOperationCount">The included operation count.</param>
    /// <param name="TerminalOperationCount">The terminal operation count.</param>
    /// <param name="PreservedPendingOperationCount">The preserved pending operation count.</param>
    private readonly record struct SnapshotRecoveryPlan(
        CapacityUsage Capacity,
        SnapshotRecoveryOperationChange[] Changes,
        Guid[] ExpiredLeaseIds,
        int IncludedOperationCount,
        int TerminalOperationCount,
        int PreservedPendingOperationCount);
}
