// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Microsoft.JSInterop;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB;

/// <summary>Implements IndexedDB-backed durable stream, outbox, and lifecycle operations.</summary>
public sealed partial class IndexedDbLocalStoreAdapter
{
    /// <inheritdoc/>
    public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
        StreamId streamId,
        SubscriptionId? preferredId,
        CancellationToken cancellationToken)
    {
#if NET5_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId.Value);
#else
        ArgumentExceptionHelper.ThrowIfNullOrWhiteSpace(streamId.Value);
#endif
        if (preferredId is { Value: var value } && value == Guid.Empty)
        {
            throw new ArgumentException("Preferred subscription id must be non-empty.", nameof(preferredId));
        }

        return GetOrCreateSubscriptionIdCoreAsync(streamId, preferredId, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask<RecoveredStream> RecoverStreamAsync(
        StreamId streamId,
        SubscriptionId subscriptionId,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            var state = await LoadStateAsync(cancellationToken).ConfigureAwait(false);
            if (!state.Streams.TryGetValue(streamId.Value, out var stream) || stream.SubscriptionId is null)
            {
                throw new InvalidOperationException("The stream has no durable subscription identity.");
            }

            if (stream.SubscriptionId != subscriptionId)
            {
                throw new InvalidOperationException("The recovered subscription identity does not match the stored identity.");
            }

            var pendingOperations = new List<SyncOperation>();
            var replayOperations = new List<SyncOperation>();
            foreach (var operation in stream.Operations.Values)
            {
                if (!operation.Terminal)
                {
                    pendingOperations.Add(operation.Operation);
                }

                if (!state.IncludedOperations.Contains(operation.Operation.OperationId.Value)
                    && operation.Status.State is not SyncOperationState.Rejected and not SyncOperationState.DeadLettered)
                {
                    replayOperations.Add(operation.Operation);
                }
            }

            replayOperations.Sort(static (left, right) => left.ClientSequence.CompareTo(right.ClientSequence));
            return new(
                subscriptionId,
                stream.Cursor,
                stream.Snapshot,
                pendingOperations.ToArray(),
                stream.DeadLetters,
                stream.NextSequence) { ReplayOperations = replayOperations.ToArray() };
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<LocalCommitResult> CommitLocalOperationAsync(
        SyncOperation operation,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        ArgumentExceptionHelper.ThrowIfNull(snapshotMutation);
        if (operation.StreamId != snapshotMutation.StreamId)
        {
            throw new ArgumentException("The operation and snapshot must target the same stream.", nameof(snapshotMutation));
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            return await ExecuteMutationAsync(
                state =>
                {
                    var stream = state.Streams.TryGetValue(operation.StreamId.Value, out var existing)
                        ? existing
                        : new StreamState();
                    var expectedRevision = IndexedDbStoreHelpers.ValidateLocalCommit(stream, operation, snapshotMutation);

                    var nextStream = new StreamState(stream)
                    {
                        NextSequence = checked(operation.ClientSequence + 1),
                        Snapshot = CreateSnapshot(snapshotMutation, stream.Cursor, expectedRevision + 1, stream.Snapshot?.AuthoritativeState),
                    };
                    nextStream.Operations[operation.OperationId.Value] = new OperationState
                    {
                        Operation = operation,
                        Status = new(
                            operation.OperationId,
                            operation.StreamId,
                            SyncOperationState.SavedLocally,
                            0,
                            _timeProvider.GetUtcNow(),
                            null),
                    };
                    state.Streams[operation.StreamId.Value] = nextStream;
                    var result = new LocalCommitResult(
                        operation.OperationId,
                        operation.ClientSequence,
                        expectedRevision + 1,
                        _timeProvider.GetUtcNow());
                    return new MutationOutcome<LocalCommitResult>(state, result, true);
                },
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
        OutboxLeaseRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        IndexedDbStoreHelpers.ValidateLeaseRequest(request);
        return LeasePendingOperationsCoreAsync(request, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken) =>
        _ = await ApplySyncResultAsync(leaseId, result, [], cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
        Guid leaseId,
        RemoteSyncResult result,
        IReadOnlyList<SnapshotMutation> snapshotMutations,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(result);
        ArgumentExceptionHelper.ThrowIfNull(snapshotMutations);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            return await ExecuteMutationAsync(
                state => ApplySyncResultMutation(state, leaseId, result, snapshotMutations),
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<LocalSnapshot> DeadLetterOperationAsync(
        Guid leaseId,
        OperationId operationId,
        string reasonCode,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(reasonCode);
        ArgumentExceptionHelper.ThrowIfNull(snapshotMutation);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            return await ExecuteMutationAsync(
                state =>
                {
                    var (lease, op, stream) = ValidateDeadLetterOperation(state, leaseId, operationId, snapshotMutation);
                    var now = _timeProvider.GetUtcNow();
                    var authoritativeState = IndexedDbStoreHelpers.PreserveAuthoritativeState(stream.Snapshot, snapshotMutation);
                    stream.Snapshot = CreateSnapshot(
                        snapshotMutation,
                        stream.Snapshot?.ServerCursor,
                        snapshotMutation.ExpectedRevision + 1,
                        authoritativeState);
                    op.Terminal = true;
                    op.Status = op.Status with
                    {
                        State = SyncOperationState.DeadLettered,
                        ReasonCode = reasonCode,
                        ChangedAtUtc = now,
                    };
                    stream.DeadLetters.Add(new(op.Operation, reasonCode, op.Status.Attempt, now));
                    op.LeaseId = null;
                    op.LeaseExpiry = null;
                    _ = lease.OperationIds.Remove(operationId.Value);
                    return new MutationOutcome<LocalSnapshot>(state, stream.Snapshot, true);
                },
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
        StreamId streamId,
        IReadOnlyList<Guid> eventIds,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(eventIds);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            var state = await LoadStateAsync(cancellationToken).ConfigureAwait(false);
            var unapplied = new List<Guid>(eventIds.Count);
            foreach (var eventId in eventIds)
            {
                if (!state.Inbox.Contains($"{streamId.Value}|{eventId}"))
                {
                    unapplied.Add(eventId);
                }
            }

            return unapplied.ToArray();
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
        RemoteEventBatch batch,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(batch);
        ArgumentExceptionHelper.ThrowIfNull(snapshotMutation);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            return await ExecuteMutationAsync(
                state =>
                {
                    var stream = IndexedDbStoreHelpers.GetStream(state, batch.StreamId);
                    IndexedDbStoreHelpers.ValidateRemoteApplyFence(stream, batch, snapshotMutation);
                    var applied = IndexedDbStoreHelpers.ApplyRemoteEvents(state, batch);
                    stream.Cursor = batch.NextCursor;
                    stream.Snapshot = CreateSnapshot(
                        snapshotMutation,
                        batch.NextCursor,
                        snapshotMutation.ExpectedRevision + 1,
                        snapshotMutation.AuthoritativeState);
                    IndexedDbStoreHelpers.ApplyCompletedOperations(state, batch);
                    var result = new RemoteApplyResult(
                        batch.NextCursor,
                        applied,
                        batch.Events.Count - applied,
                        stream.Snapshot.Revision);
                    return new MutationOutcome<RemoteApplyResult>(state, result, true);
                },
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            var state = await LoadStateAsync(cancellationToken).ConfigureAwait(false);
            return IndexedDbStoreHelpers.FindOperationOrNull(state, operationId)?.Status;
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            var state = await LoadStateAsync(cancellationToken).ConfigureAwait(false);
            return IndexedDbStoreHelpers.FindOperationOrNull(state, operationId)?.RetryState;
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
        Guid leaseId,
        OperationId operationId,
        int nextAttempt,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            return await ExecuteMutationAsync(
                state =>
                {
                    var lease = GetLease(state, leaseId);
                    if (!lease.OperationIds.Contains(operationId.Value))
                    {
                        throw new InvalidOperationException("The lease does not own the operation.");
                    }

                    var op = IndexedDbStoreHelpers.FindOperation(state, operationId);
                    if (op.Status.State == SyncOperationState.Ambiguous
                        && op.Operation.Policy.DeliveryGuarantee == DeliveryGuarantee.AtMostOnce)
                    {
                        return new MutationOutcome<AttemptBarrierResult>(
                            state,
                            new(operationId, nextAttempt, false, "OC.AmbiguousAtMostOnce"),
                            false);
                    }

                    if (nextAttempt <= op.Status.Attempt)
                    {
                        return new MutationOutcome<AttemptBarrierResult>(
                            state,
                            new(operationId, nextAttempt, false, "OC.AttemptAlreadyRecorded"),
                            false);
                    }

                    op.Status = op.Status with
                    {
                        Attempt = nextAttempt,
                        State = op.Operation.Policy.DeliveryGuarantee == DeliveryGuarantee.AtMostOnce
                            ? SyncOperationState.Ambiguous
                            : SyncOperationState.Uploading,
                        ChangedAtUtc = _timeProvider.GetUtcNow(),
                    };
                    return new MutationOutcome<AttemptBarrierResult>(state, new(operationId, nextAttempt, true, null), true);
                },
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(retryState);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            _ = await ExecuteMutationAsync(
                state =>
                {
                    var op = IndexedDbStoreHelpers.FindOperation(state, operationId);
                    op.RetryState = retryState;
                    return new MutationOutcome<bool>(state, true, true);
                },
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken)
    {
        if (extension <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(extension), extension, "Lease extension must be positive.");
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            _ = await ExecuteMutationAsync(
                state =>
                {
                    var lease = GetLease(state, leaseId);
                    lease.ExpiresAtUtc = lease.ExpiresAtUtc.Add(extension);
                    foreach (var operationId in lease.OperationIds)
                    {
                        var operation = IndexedDbStoreHelpers.FindOperation(state, new(operationId));
                        operation.LeaseExpiry = lease.ExpiresAtUtc;
                    }

                    return new MutationOutcome<bool>(state, true, true);
                },
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            _ = await ExecuteMutationAsync(
                state =>
                {
                    var lease = GetLease(state, leaseId);
                    var releasedOperations = 0;
                    var now = _timeProvider.GetUtcNow();
                    foreach (var stream in state.Streams.Values)
                    {
                        foreach (var operation in stream.Operations.Values)
                        {
                            if (operation.LeaseId != leaseId)
                            {
                                continue;
                            }

                            operation.LeaseId = null;
                            operation.LeaseExpiry = null;
                            operation.Status = operation.Status with
                            {
                                State = SyncOperationState.QueuedForUpload,
                                ChangedAtUtc = now,
                            };
                            releasedOperations++;
                        }
                    }

                    if (releasedOperations != lease.OperationIds.Count)
                    {
                        throw new InvalidOperationException("The IndexedDB lease membership is incomplete.");
                    }

                    _ = state.Leases.Remove(leaseId);
                    return new MutationOutcome<bool>(state, true, true);
                },
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        if (request.TargetBytes < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), request.TargetBytes, "TargetBytes must not be negative.");
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            var storeIdentity = _storeIdentity!;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var current = await LoadStateAsync(cancellationToken).ConfigureAwait(false);
                var next = new StoreState(current);
                var candidates = IndexedDbStoreHelpers.CollectCompactionCandidates(next, request, out var retainedBytes);
                var removed = IndexedDbStoreHelpers.RemoveCompactionCandidates(next, candidates, request.TargetBytes, ref retainedBytes);
                if (removed == 0)
                {
                    return new(0, 0);
                }

                next.Generation = current.Generation + 1;
                var currentJson = SerializeState(current);
                var nextJson = SerializeState(next);
                cancellationToken.ThrowIfCancellationRequested();
                if (!await TryWriteStateAsync(storeIdentity, current.Generation, next, cancellationToken).ConfigureAwait(false))
                {
                    continue;
                }

                var bytesReclaimed = Math.Max(0, GetUtf8ByteCount(currentJson) - GetUtf8ByteCount(nextJson));
                return new(removed, bytesReclaimed);
            }
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <summary>Validates one dead-letter request against durable state.</summary>
    /// <param name="state">The durable store state.</param>
    /// <param name="leaseId">The lease that owns the operation.</param>
    /// <param name="operationId">The operation to dead-letter.</param>
    /// <param name="snapshotMutation">The replacement snapshot mutation.</param>
    /// <returns>The validated lease, operation, and stream state.</returns>
    /// <exception cref="InvalidOperationException">The request does not match the leased durable state.</exception>
    private (LeaseState Lease, OperationState Operation, StreamState Stream) ValidateDeadLetterOperation(
        StoreState state,
        Guid leaseId,
        OperationId operationId,
        SnapshotMutation snapshotMutation)
    {
        var lease = GetLease(state, leaseId);
        if (!lease.OperationIds.Contains(operationId.Value))
        {
            throw new InvalidOperationException("The lease does not own the operation.");
        }

        var operation = IndexedDbStoreHelpers.FindOperation(state, operationId);
        if (operation.Operation.StreamId != snapshotMutation.StreamId)
        {
            throw new InvalidOperationException("The replacement snapshot must target the leased operation's stream.");
        }

        if (state.IncludedOperations.Contains(operationId.Value))
        {
            throw new InvalidOperationException("An operation already included in the authoritative snapshot cannot be dead-lettered.");
        }

        if (!state.Streams.TryGetValue(snapshotMutation.StreamId.Value, out var stream)
            || stream.Snapshot?.Revision != snapshotMutation.ExpectedRevision)
        {
            throw new InvalidOperationException("The snapshot revision does not match durable state.");
        }

        return (lease, operation, stream);
    }

    /// <summary>Gets or creates the durable subscription identity for one stream.</summary>
    /// <param name="streamId">The logical stream identifier.</param>
    /// <param name="preferredId">The preferred subscription identifier, when the caller already knows it.</param>
    /// <param name="cancellationToken">The token used to cancel the mutation.</param>
    /// <returns>The durable subscription identifier.</returns>
    private async ValueTask<SubscriptionId> GetOrCreateSubscriptionIdCoreAsync(
        StreamId streamId,
        SubscriptionId? preferredId,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            return await ExecuteMutationAsync<SubscriptionId>(
                state =>
                {
                    if (!state.Streams.TryGetValue(streamId.Value, out var stream) || stream.SubscriptionId is null)
                    {
                        var nextStream = stream is null ? new StreamState() : new StreamState(stream);
                        nextStream.SubscriptionId = preferredId ?? SubscriptionId.New();
                        state.Streams[streamId.Value] = nextStream;
                        return new(state, nextStream.SubscriptionId.Value, true);
                    }

                    if (preferredId.HasValue && stream.SubscriptionId != preferredId)
                    {
                        throw new InvalidOperationException("The preferred subscription identity does not match the stored identity.");
                    }

                    return new(state, stream.SubscriptionId.Value, false);
                },
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <summary>Applies a remote sync result and any required replacement snapshots.</summary>
    /// <param name="state">The copied durable store state.</param>
    /// <param name="leaseId">The lease identifier being completed.</param>
    /// <param name="result">The remote sync result.</param>
    /// <param name="snapshotMutations">The replacement snapshots required by rejected optimistic writes.</param>
    /// <returns>The replacement snapshots persisted during the mutation.</returns>
    private MutationOutcome<IReadOnlyList<LocalSnapshot>> ApplySyncResultMutation(
        StoreState state,
        Guid leaseId,
        RemoteSyncResult result,
        IReadOnlyList<SnapshotMutation> snapshotMutations)
    {
        var lease = GetLease(state, leaseId);
        IndexedDbStoreHelpers.ValidateResult(state, lease, result);
        var requiredRebuildStreams = IndexedDbStoreHelpers.ApplySyncResultOperations(state, result, _timeProvider.GetUtcNow());
        IndexedDbStoreHelpers.ValidateSnapshotMutationPresence(requiredRebuildStreams, snapshotMutations);
        var snapshots = new List<LocalSnapshot>();
        var mutationStreams = ApplySnapshotMutations(state, snapshotMutations, snapshots);
        IndexedDbStoreHelpers.ValidateSnapshotCoverage(requiredRebuildStreams, mutationStreams);
        _ = state.Leases.Remove(leaseId);
        return new(state, snapshots, true);
    }

    /// <summary>Applies the provided replacement snapshots and records the affected streams.</summary>
    /// <param name="state">The copied durable store state.</param>
    /// <param name="snapshotMutations">The replacement snapshot mutations to apply.</param>
    /// <param name="snapshots">The list that collects persisted replacement snapshots.</param>
    /// <returns>The stream identifiers updated by the replacement snapshots.</returns>
    private List<string> ApplySnapshotMutations(
        StoreState state,
        IReadOnlyList<SnapshotMutation> snapshotMutations,
        List<LocalSnapshot> snapshots)
    {
        var mutationStreams = new List<string>(snapshotMutations.Count);
        foreach (var mutation in snapshotMutations)
        {
            mutationStreams.Add(mutation.StreamId.Value);
            ApplySnapshotMutation(state, mutation, snapshots, mutationStreams.Count > 1);
        }

        return mutationStreams;
    }

    /// <summary>Applies one replacement snapshot mutation.</summary>
    /// <param name="state">The copied durable store state.</param>
    /// <param name="mutation">The replacement snapshot mutation.</param>
    /// <param name="snapshots">The list that collects persisted replacement snapshots.</param>
    /// <param name="checkForDuplicates">Whether to validate duplicate stream identifiers by scanning the list.</param>
    /// <exception cref="InvalidOperationException">The mutation duplicates a stream identifier or mismatches durable state.</exception>
    private void ApplySnapshotMutation(
        StoreState state,
        SnapshotMutation mutation,
        List<LocalSnapshot> snapshots,
        bool checkForDuplicates)
    {
        if (checkForDuplicates && snapshots.Exists(snapshot => snapshot.StreamId == mutation.StreamId))
        {
            throw new InvalidOperationException("Replacement snapshots must not contain duplicate stream identifiers.");
        }

        if (!state.Streams.TryGetValue(mutation.StreamId.Value, out var stream)
            || stream.Snapshot?.Revision != mutation.ExpectedRevision)
        {
            throw new InvalidOperationException("The snapshot revision does not match durable state.");
        }

        var authoritativeState = IndexedDbStoreHelpers.PreserveAuthoritativeState(stream.Snapshot, mutation);
        var snapshot = CreateSnapshot(mutation, stream.Snapshot.ServerCursor, mutation.ExpectedRevision + 1, authoritativeState);
        stream.Snapshot = snapshot;
        snapshots.Add(snapshot);
    }

    /// <summary>Creates and yields the next eligible persisted outbox lease.</summary>
    /// <param name="request">The requested operation and byte limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The leased batch, or an empty sequence when no operation is eligible.</returns>
    private async IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsCoreAsync(
        OutboxLeaseRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var batch = await LeaseNextOperationsAsync(request, cancellationToken).ConfigureAwait(false);
        if (batch is not null)
        {
            yield return batch;
        }
    }

    /// <summary>Creates and persists one lease for the next eligible operations.</summary>
    /// <param name="request">The requested operation and byte limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The leased batch, or <see langword="null"/> when no operation is eligible.</returns>
    private async ValueTask<LeasedOperationBatch?> LeaseNextOperationsAsync(
        OutboxLeaseRequest request,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            return await ExecuteMutationAsync<LeasedOperationBatch?>(
                state =>
                {
                    var now = _timeProvider.GetUtcNow();
                    var selected = IndexedDbStoreHelpers.SelectPendingOperations(state, request, now);
                    if (selected.Count == 0)
                    {
                        return new(state, null, false);
                    }

                    var leaseId = Guid.NewGuid();
                    var expiry = now.Add(request.LeaseDuration);
                    var operationIds = new List<Guid>(selected.Count);
                    var operations = new List<SyncOperation>(selected.Count);
                    foreach (var item in selected)
                    {
                        item.LeaseId = leaseId;
                        item.LeaseExpiry = expiry;
                        item.Status = item.Status with { State = SyncOperationState.Uploading, ChangedAtUtc = now };
                        operationIds.Add(item.Operation.OperationId.Value);
                        operations.Add(item.Operation);
                    }

                    var lease = new LeaseState { LeaseId = leaseId, ExpiresAtUtc = expiry };
                    lease.OperationIds.AddRange(operationIds);
                    state.Leases[leaseId] = lease;
                    return new(state, new LeasedOperationBatch(leaseId, expiry, operations.ToArray()), true);
                },
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _gate.Release();
        }
    }
}
