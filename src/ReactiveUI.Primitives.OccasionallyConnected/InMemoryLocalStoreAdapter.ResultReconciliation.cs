// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores occasionally connected stream state in this process.</summary>
/// <content>Atomic upload result reconciliation.</content>
internal sealed partial class InMemoryLocalStoreAdapter
{
    /// <summary>Whether one bounded result capture owns transient reconciliation capacity.</summary>
    private bool _resultReconciliationActive;

    /// <summary>Atomically applies upload decisions and replacement optimistic snapshots.</summary>
    /// <param name="leaseId">The active upload lease.</param>
    /// <param name="result">The complete upload result.</param>
    /// <param name="snapshotMutations">The replacements for streams losing optimistic operations.</param>
    /// <param name="cancellationToken">The token observed before commit.</param>
    /// <returns>The snapshots committed with the operation decisions.</returns>
    /// <exception cref="InvalidOperationException">The lease, result, or snapshot fences are invalid.</exception>
    public ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
        Guid leaseId,
        RemoteSyncResult result,
        IReadOnlyList<SnapshotMutation> snapshotMutations,
        CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateLeaseId(leaseId);
        ArgumentExceptionHelper.ThrowIfNull(result);
        ArgumentExceptionHelper.ThrowIfNull(snapshotMutations);
        ValidateResultCaptureCount(result);
        ReserveResultReconciliation(cancellationToken);
        IReadOnlyList<LocalSnapshot> committed;
        try
        {
            var mutations = CaptureResultMutations(snapshotMutations, cancellationToken);
            var nowUtc = _timeProvider.GetUtcNow();
            lock (_gate)
            {
                ThrowIfReady(cancellationToken);
                committed = CommitResultReconciliation(leaseId, result, mutations, nowUtc, cancellationToken);
            }
        }
        finally
        {
            lock (_gate)
            {
                _resultReconciliationActive = false;
            }
        }

        return new(committed);
    }

    /// <summary>Commits a validated result with all affected optimistic snapshots under the store gate.</summary>
    /// <param name="leaseId">The active lease.</param>
    /// <param name="result">The exact batch result.</param>
    /// <param name="mutations">The owned snapshot mutations.</param>
    /// <param name="nowUtc">The sampled commit time.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The committed snapshots.</returns>
    private System.Collections.ObjectModel.ReadOnlyCollection<LocalSnapshot> CommitResultReconciliation(
        Guid leaseId,
        RemoteSyncResult result,
        SnapshotMutation[] mutations,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken)
    {
        var lease = GetActiveLease(leaseId, nowUtc);
        SyncBatchValidator.Validate(new(leaseId, GetLeaseOperations(lease)), result);
        var statuses = CreateStatusesFromResult(result, nowUtc);
        var requiredStreams = GetReconciliationStreams(statuses);
        var snapshots = CreateReconciledSnapshots(mutations, requiredStreams, nowUtc);
        var capacity = GetSyncResultCapacityDelta(leaseId, statuses, nowUtc);
        foreach (var snapshot in snapshots)
        {
            capacity = AddCapacity(capacity, CapacityDifference(LocalSnapshotCapacity(GetStream(snapshot.StreamId).Snapshot), LocalSnapshotCapacity(snapshot)));
        }

        EnsureCapacityFor(capacity);
        cancellationToken.ThrowIfCancellationRequested();
        ApplySyncResultMutations(statuses, nowUtc);
        _ = _leases.Remove(leaseId);
        foreach (var snapshot in snapshots)
        {
            GetStream(snapshot.StreamId).Snapshot = snapshot;
        }

        ApplyCapacity(capacity);
        return snapshots;
    }

    /// <summary>Identifies streams whose optimistic replay membership will shrink.</summary>
    /// <param name="statuses">The validated result statuses.</param>
    /// <returns>The streams requiring one replacement snapshot.</returns>
    /// <exception cref="InvalidOperationException">The result contradicts authoritative inclusion.</exception>
    private HashSet<StreamId> GetReconciliationStreams(Dictionary<OperationId, SyncOperationStatus> statuses)
    {
        HashSet<StreamId> streams = [];
        foreach (var pair in statuses)
        {
            if (pair.Value.State != SyncOperationState.Rejected)
            {
                continue;
            }

            if (_includedOperations.Contains(pair.Key))
            {
                throw new InvalidOperationException("A rejection contradicts authoritative operation inclusion.");
            }

            _ = streams.Add(_operations[pair.Key].Operation.StreamId);
        }

        return streams;
    }

    /// <summary>Prepares snapshot replacements while preserving authoritative checkpoints and receive cursors.</summary>
    /// <param name="mutations">The owned mutations.</param>
    /// <param name="requiredStreams">The streams requiring replacement.</param>
    /// <param name="nowUtc">The sampled commit time.</param>
    /// <returns>The owned read-only snapshots.</returns>
    /// <exception cref="InvalidOperationException">The replacement set or revision fence is invalid.</exception>
    private System.Collections.ObjectModel.ReadOnlyCollection<LocalSnapshot> CreateReconciledSnapshots(
        SnapshotMutation[] mutations,
        HashSet<StreamId> requiredStreams,
        DateTimeOffset nowUtc)
    {
        if (mutations.Length != requiredStreams.Count)
        {
            throw new InvalidOperationException("Each affected stream requires exactly one snapshot replacement.");
        }

        List<LocalSnapshot> snapshots = [with(capacity: mutations.Length)];
        foreach (var mutation in mutations)
        {
            if (!requiredStreams.Remove(mutation.StreamId))
            {
                throw new InvalidOperationException("A snapshot replacement is duplicated or unrelated to this result.");
            }

            var stream = GetStream(mutation.StreamId);
            var current = stream.Snapshot;
            ArgumentExceptionHelper.ThrowIfNull(current);
            var authoritative = current.AuthoritativeState ?? throw new InvalidOperationException("The stream requires an authoritative checkpoint before reconciliation.");
            if (current.Revision != mutation.ExpectedRevision)
            {
                throw new InvalidOperationException("The optimistic snapshot changed before result reconciliation.");
            }

            if (mutation.AuthoritativeState is { } supplied && !PayloadEnvelopeComparison.ContentEquals(authoritative, supplied))
            {
                throw new InvalidOperationException("An upload result cannot replace the authoritative checkpoint.");
            }

            snapshots.Add(new(
                mutation.StreamId,
                mutation.FormatVersion,
                stream.ServerCursor,
                mutation.State,
                checked(current.Revision + 1),
                nowUtc) { AuthoritativeState = authoritative });
        }

        return new(snapshots);
    }

    /// <summary>Reserves a single bounded capture before accessing caller collection callbacks.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="QueueCapacityExceededException">Another result capture owns the transient budget.</exception>
    private void ReserveResultReconciliation(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            if (_resultReconciliationActive)
            {
                throw new QueueCapacityExceededException("A result reconciliation capture is already active.", true);
            }

            _resultReconciliationActive = true;
        }
    }

    /// <summary>Bounds remote decisions before allocating result dictionaries.</summary>
    /// <param name="result">The immutable remote result.</param>
    /// <exception cref="QueueCapacityExceededException">The result count exceeds the configured record capacity.</exception>
    private void ValidateResultCaptureCount(RemoteSyncResult result)
    {
        if (result.Operations.Count <= _maximumRecordCount)
        {
            return;
        }

        throw new QueueCapacityExceededException("The remote result count exceeds the transient budget.", false);
    }

    /// <summary>Copies and validates bounded mutation references without holding the store gate.</summary>
    /// <param name="mutations">The caller collection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The owned mutation array.</returns>
    /// <exception cref="QueueCapacityExceededException">The count or logical encoded bytes exceed the transient budget.</exception>
    private SnapshotMutation[] CaptureResultMutations(IReadOnlyList<SnapshotMutation> mutations, CancellationToken cancellationToken)
    {
        var count = mutations.Count;
        if (count < 0 || count > _maximumRecordCount)
        {
            throw new QueueCapacityExceededException("The result mutation count exceeds the transient budget.", false);
        }

        var result = new SnapshotMutation[count];
        var bytes = (long)Int32EncodedBytes;
        for (var index = 0; index < count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var mutation = mutations[index];
            InMemoryLocalStoreAdapterValidation.ValidateSnapshotMutation(mutation);
            bytes = checked(bytes + SnapshotMutationCapacityBytes(mutation));
            if (bytes > _maximumEncodedBytes)
            {
                throw new QueueCapacityExceededException("The result mutation bytes exceed the transient budget.", false);
            }

            result[index] = mutation;
        }

        return result;
    }

    /// <summary>Rejects status-only results that require an optimistic snapshot replacement.</summary>
    /// <param name="statuses">The validated operation results.</param>
    /// <exception cref="InvalidOperationException">A rejection contradicts inclusion or requires a snapshot rebuild.</exception>
    private void ValidateStatusOnlyReconciliation(Dictionary<OperationId, SyncOperationStatus> statuses)
    {
        foreach (var pair in statuses)
        {
            if (pair.Value.State is not SyncOperationState.Rejected and not SyncOperationState.DeadLettered)
            {
                continue;
            }

            if (_includedOperations.Contains(pair.Key))
            {
                throw new InvalidOperationException("A rejection contradicts authoritative operation inclusion.");
            }

            var record = _operations[pair.Key];
            var snapshot = GetStream(record.Operation.StreamId).Snapshot;
            ArgumentExceptionHelper.ThrowIfNull(snapshot);
            if (snapshot.AuthoritativeState is not null)
            {
                throw new InvalidOperationException("Removing an optimistic operation requires an atomic snapshot replacement.");
            }
        }
    }
}
