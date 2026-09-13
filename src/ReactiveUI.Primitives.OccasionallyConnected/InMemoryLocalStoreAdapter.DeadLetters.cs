// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores occasionally connected stream state in this process.</summary>
/// <content>Atomic local dead-letter reconciliation.</content>
internal sealed partial class InMemoryLocalStoreAdapter
{
    /// <inheritdoc/>
    public ValueTask<LocalSnapshot> DeadLetterOperationAsync(
        Guid leaseId,
        OperationId operationId,
        string reasonCode,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateDeadLetterInput(leaseId, operationId, reasonCode, snapshotMutation);
        cancellationToken.ThrowIfCancellationRequested();
        var nowUtc = _timeProvider.GetUtcNow();
        LocalSnapshot committed;
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            committed = CommitDeadLetterOperation(leaseId, operationId, reasonCode, snapshotMutation, nowUtc, cancellationToken);
        }

        return new(committed);
    }

    /// <summary>Returns the capacity delta for one dead-letter transition.</summary>
    /// <param name="lease">The current lease record.</param>
    /// <param name="record">The operation record.</param>
    /// <param name="nextStatus">The next operation status.</param>
    /// <param name="currentSnapshot">The current snapshot.</param>
    /// <param name="nextSnapshot">The replacement snapshot.</param>
    /// <returns>The retained capacity delta.</returns>
    private static CapacityUsage GetDeadLetterCapacityDelta(
        LeaseRecord lease,
        OperationRecord record,
        SyncOperationStatus nextStatus,
        LocalSnapshot currentSnapshot,
        LocalSnapshot nextSnapshot)
    {
        var remainingLeaseMembers = lease.OperationIds.Count - 1;
        var leaseAfter = remainingLeaseMembers == 0 ? default : LeaseRecordCapacity(remainingLeaseMembers);
        var capacity = CapacityDifference(LeaseRecordCapacity(lease), leaseAfter);
        capacity = AddCapacity(
            capacity,
            CapacityDifference(
                OperationRecordCapacity(record),
                OperationRecordCapacity(record, nextStatus, null, null, null, nextStatus.ChangedAtUtc)));
        return AddCapacity(capacity, CapacityDifference(LocalSnapshotCapacity(currentSnapshot), LocalSnapshotCapacity(nextSnapshot)));
    }

    /// <summary>Creates the committed dead-letter snapshot.</summary>
    /// <param name="operation">The target operation.</param>
    /// <param name="stream">The stream record.</param>
    /// <param name="current">The current snapshot.</param>
    /// <param name="mutation">The replacement mutation.</param>
    /// <param name="nowUtc">The sampled commit time.</param>
    /// <returns>The committed snapshot.</returns>
    /// <exception cref="InvalidOperationException">The authoritative checkpoint or revision fence is invalid.</exception>
    private static LocalSnapshot CreateDeadLetterSnapshot(
        SyncOperation operation,
        StreamRecord stream,
        LocalSnapshot current,
        SnapshotMutation mutation,
        DateTimeOffset nowUtc)
    {
        var authoritative = current.AuthoritativeState
            ?? throw new InvalidOperationException("The stream requires an authoritative checkpoint before dead-letter reconciliation.");
        if (current.Revision == mutation.ExpectedRevision)
        {
            return CreateDeadLetterSnapshot(operation, stream, mutation, nowUtc, authoritative);
        }

        throw new InvalidOperationException("The optimistic snapshot changed before dead-letter reconciliation.");
    }

    /// <summary>Creates the committed dead-letter snapshot after revision validation.</summary>
    /// <param name="operation">The target operation.</param>
    /// <param name="stream">The stream record.</param>
    /// <param name="mutation">The replacement mutation.</param>
    /// <param name="nowUtc">The sampled commit time.</param>
    /// <param name="authoritative">The preserved authoritative payload.</param>
    /// <returns>The committed snapshot.</returns>
    /// <exception cref="InvalidOperationException">The authoritative checkpoint replacement is invalid.</exception>
    private static LocalSnapshot CreateDeadLetterSnapshot(
        SyncOperation operation,
        StreamRecord stream,
        SnapshotMutation mutation,
        DateTimeOffset nowUtc,
        PayloadEnvelope authoritative)
    {
        if (mutation.AuthoritativeState is not null && !PayloadEnvelopeComparison.ContentEquals(authoritative, mutation.AuthoritativeState))
        {
            throw new InvalidOperationException("A dead-letter transition cannot replace the authoritative checkpoint.");
        }

        return new(
            operation.StreamId,
            mutation.FormatVersion,
            stream.ServerCursor,
            mutation.State,
            checked(mutation.ExpectedRevision + 1),
            nowUtc) { AuthoritativeState = authoritative };
    }

    /// <summary>Validates operation-specific dead-letter constraints.</summary>
    /// <param name="record">The operation record.</param>
    /// <param name="mutation">The replacement mutation.</param>
    /// <exception cref="InvalidOperationException">The operation is mismatched.</exception>
    private static void ValidateDeadLetterOperation(OperationRecord record, SnapshotMutation mutation)
    {
        if (IsTerminalForDeadLetter(record.Status.State))
        {
            throw new InvalidOperationException("The operation state is terminal.");
        }

        if (record.Status.State != SyncOperationState.QueuedForUpload || record.Status.Attempt != 0)
        {
            throw new InvalidOperationException("The operation has prior upload attempt evidence.");
        }

        if (record.Operation.StreamId == mutation.StreamId)
        {
            return;
        }

        throw new InvalidOperationException("The dead-letter snapshot targets a different stream.");
    }

    /// <summary>Determines whether a state rejects local dead-letter transition.</summary>
    /// <param name="state">The operation state.</param>
    /// <returns>Whether the state is terminal.</returns>
    private static bool IsTerminalForDeadLetter(SyncOperationState state) =>
        state is SyncOperationState.Conflict
            or SyncOperationState.Synchronized
            or SyncOperationState.Rejected
            or SyncOperationState.DeadLettered
            or SyncOperationState.Ambiguous
            or SyncOperationState.GuaranteeExpired;

    /// <summary>Commits one dead-letter transition and replacement snapshot while holding the store gate.</summary>
    /// <param name="leaseId">The owning lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="reasonCode">The stable reason code.</param>
    /// <param name="mutation">The replacement snapshot mutation.</param>
    /// <param name="nowUtc">The sampled commit timestamp.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The committed snapshot.</returns>
    /// <exception cref="InvalidOperationException">The lease, operation, or snapshot fence is invalid.</exception>
    private LocalSnapshot CommitDeadLetterOperation(
        Guid leaseId,
        OperationId operationId,
        string reasonCode,
        SnapshotMutation mutation,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken)
    {
        var target = ValidateDeadLetterTarget(leaseId, operationId, mutation, nowUtc);
        var nextStatus = CreateStatus(target.Record.Operation, SyncOperationState.DeadLettered, target.Record.Attempt, nowUtc, reasonCode);
        var capacity = GetDeadLetterCapacityDelta(target.Lease, target.Record, nextStatus, target.CurrentSnapshot, target.NextSnapshot);
        EnsureCapacityFor(capacity);
        cancellationToken.ThrowIfCancellationRequested();
        target.Record.Status = nextStatus;
        target.Record.RetryState = null;
        target.Record.LeaseId = null;
        target.Record.LeaseExpiresAtUtc = null;
        target.Record.TerminalAtUtc = nowUtc;
        RemoveDeadLetterLeaseMember(leaseId, operationId, target.Lease);
        target.Stream.Snapshot = target.NextSnapshot;
        ApplyCapacity(capacity);
        return target.NextSnapshot;
    }

    /// <summary>Removes one dead-lettered operation from its lease.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="lease">The lease record.</param>
    private void RemoveDeadLetterLeaseMember(Guid leaseId, OperationId operationId, LeaseRecord lease)
    {
        _ = lease.Remove(operationId);
        if (lease.OperationIds.Count > 0)
        {
            return;
        }

        _ = _leases.Remove(leaseId);
    }

    /// <summary>Validates the target and prepares the replacement snapshot.</summary>
    /// <param name="leaseId">The owning lease.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="mutation">The requested replacement.</param>
    /// <param name="nowUtc">The sampled commit timestamp.</param>
    /// <returns>The validated target records and snapshot.</returns>
    /// <exception cref="InvalidOperationException">The target or snapshot is invalid.</exception>
    private DeadLetterTarget ValidateDeadLetterTarget(
        Guid leaseId,
        OperationId operationId,
        SnapshotMutation mutation,
        DateTimeOffset nowUtc)
    {
        var lease = GetActiveLease(leaseId, nowUtc);
        if (!lease.Owns(operationId))
        {
            throw new InvalidOperationException("The active lease does not own the operation.");
        }

        var record = GetOperation(operationId);
        if (_includedOperations.Contains(operationId))
        {
            throw new InvalidOperationException("A dead-letter transition contradicts authoritative operation inclusion.");
        }

        ValidateDeadLetterOperation(record, mutation);
        var stream = GetStream(record.Operation.StreamId);
        var current = stream.Snapshot;
        ArgumentExceptionHelper.ThrowIfNull(current);
        var nextSnapshot = CreateDeadLetterSnapshot(record.Operation, stream, current, mutation, nowUtc);
        return new(lease, record, stream, current, nextSnapshot);
    }

    /// <summary>One validated dead-letter target.</summary>
    /// <param name="Lease">The active lease.</param>
    /// <param name="Record">The operation record.</param>
    /// <param name="Stream">The stream record.</param>
    /// <param name="CurrentSnapshot">The current snapshot.</param>
    /// <param name="NextSnapshot">The replacement snapshot.</param>
    private readonly record struct DeadLetterTarget(
        LeaseRecord Lease,
        OperationRecord Record,
        StreamRecord Stream,
        LocalSnapshot CurrentSnapshot,
        LocalSnapshot NextSnapshot);
}
