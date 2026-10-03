// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

/// <summary>Implements durable stream, outbox, and lifecycle operations.</summary>
public sealed partial class FileSystemLocalStoreAdapter
{
    /// <inheritdoc/>
    public async ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
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

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            if (!_state.Streams.TryGetValue(streamId.Value, out var stream) || stream.SubscriptionId is null)
            {
                var next = new StoreState(_state);
                stream = next.Streams.TryGetValue(streamId.Value, out var stored)
                    ? new StreamState(stored)
                    : new StreamState();
                stream.SubscriptionId = preferredId ?? SubscriptionId.New();
                next.Streams[streamId.Value] = stream;
                await PersistStateAsync(next, cancellationToken).ConfigureAwait(false);
            }
            else if (preferredId.HasValue && stream.SubscriptionId != preferredId)
            {
                throw new InvalidOperationException("The preferred subscription identity does not match the stored identity.");
            }

            return stream.SubscriptionId!.Value;
        }
        finally
        {
            _ = _gate.Release();
        }
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
            if (!_state.Streams.TryGetValue(streamId.Value, out var stream) || stream.SubscriptionId is null)
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

                if (!_state.IncludedOperations.Contains(operation.Operation.OperationId.Value)
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
                stream.NextSequence)
            { ReplayOperations = replayOperations.ToArray(), };
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
            var stream = _state.Streams.TryGetValue(operation.StreamId.Value, out var existing)
                ? existing
                : new StreamState();
            var expectedRevision = ValidateLocalCommit(stream, operation, snapshotMutation);

            var next = new StoreState(_state);
            var nextStream = new StreamState(stream)
            {
                NextSequence = checked(operation.ClientSequence + 1),
                Snapshot = new(
                    snapshotMutation.StreamId,
                    snapshotMutation.FormatVersion,
                    stream.Cursor,
                    snapshotMutation.State,
                    expectedRevision + 1,
                    _timeProvider.GetUtcNow())
                { AuthoritativeState = snapshotMutation.AuthoritativeState, },
            };
            nextStream.Operations[operation.OperationId.Value] = new OperationState
            {
                Operation = operation,
                Status = new(operation.OperationId, operation.StreamId, SyncOperationState.SavedLocally, 0, _timeProvider.GetUtcNow(), null),
            };
            next.Streams[operation.StreamId.Value] = nextStream;
            await AppendAsync(new(next), cancellationToken).ConfigureAwait(false);
            _state = next;
            return new(operation.OperationId, operation.ClientSequence, expectedRevision + 1, _timeProvider.GetUtcNow());
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
        ValidateLeaseRequest(request);
        return LeasePendingOperationsCoreAsync(request, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken) =>
        await ApplySyncResultAsync(leaseId, result, [], cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
        Guid leaseId,
        RemoteSyncResult result,
        IReadOnlyList<SnapshotMutation> snapshotMutations,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            var next = new StoreState(_state);
            var lease = GetLease(next, leaseId);
            FileSystemJournalHelpers.ValidateResult(next, lease, result);
            var now = _timeProvider.GetUtcNow();
            var changed = new List<LocalSnapshot>();
            foreach (var decision in result.Operations)
            {
                var op = FileSystemJournalHelpers.FindOperation(next, decision.OperationId);
                var state = decision.Kind switch
                {
                    OperationResultKind.Accepted => SyncOperationState.Synchronized,
                    OperationResultKind.Conflict => SyncOperationState.Conflict,
                    OperationResultKind.Rejected => SyncOperationState.Rejected,
                    _ => SyncOperationState.QueuedForUpload,
                };
                op.Status = op.Status with { State = state, ReasonCode = decision.ReasonCode, ChangedAtUtc = now };
                op.Terminal = state is SyncOperationState.Synchronized or SyncOperationState.Rejected;
                op.LeaseId = null;
                op.LeaseExpiry = null;
            }

            _ = next.Leases.Remove(leaseId);
            foreach (var mutation in snapshotMutations)
            {
                if (!next.Streams.TryGetValue(mutation.StreamId.Value, out var stream) || stream.Snapshot?.Revision != mutation.ExpectedRevision)
                {
                    throw new InvalidOperationException("The snapshot revision does not match durable state.");
                }

                var snapshot = CreateSnapshot(mutation, stream.Snapshot!.ServerCursor, mutation.ExpectedRevision + 1);
                stream.Snapshot = snapshot;
                changed.Add(snapshot);
            }

            await PersistStateAsync(next, cancellationToken).ConfigureAwait(false);
            return changed;
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
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            var next = new StoreState(_state);
            var lease = GetLease(next, leaseId);
            if (!lease.OperationIds.Contains(operationId.Value))
            {
                throw new InvalidOperationException("The lease does not own the operation.");
            }

            var op = FileSystemJournalHelpers.FindOperation(next, operationId);
            if (op.Operation.StreamId != snapshotMutation.StreamId)
            {
                throw new InvalidOperationException("The replacement snapshot must target the leased operation's stream.");
            }

            if (next.IncludedOperations.Contains(operationId.Value))
            {
                throw new InvalidOperationException("An operation already included in the authoritative snapshot cannot be dead-lettered.");
            }

            if (!next.Streams.TryGetValue(snapshotMutation.StreamId.Value, out var stream) || stream.Snapshot?.Revision != snapshotMutation.ExpectedRevision)
            {
                throw new InvalidOperationException("The snapshot revision does not match durable state.");
            }

            var now = _timeProvider.GetUtcNow();
            stream.Snapshot = CreateSnapshot(snapshotMutation, stream.Snapshot?.ServerCursor, snapshotMutation.ExpectedRevision + 1);
            op.Terminal = true;
            op.Status = op.Status with { State = SyncOperationState.DeadLettered, ReasonCode = reasonCode, ChangedAtUtc = now };
            stream.DeadLetters.Add(new(op.Operation, reasonCode, op.Status.Attempt, now));
            op.LeaseId = null;
            op.LeaseExpiry = null;
            _ = lease.OperationIds.Remove(operationId.Value);
            await PersistStateAsync(next, cancellationToken).ConfigureAwait(false);
            return stream.Snapshot;
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(StreamId streamId, IReadOnlyList<Guid> eventIds, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            var unapplied = new List<Guid>(eventIds.Count);
            foreach (var eventId in eventIds)
            {
                if (!_state.Inbox.Contains($"{streamId.Value}|{eventId}"))
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
    public async ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(RemoteEventBatch batch, SnapshotMutation snapshotMutation, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            var next = new StoreState(_state);
            var stream = FileSystemJournalHelpers.GetStream(next, batch.StreamId);
            ValidateRemoteApplyFence(stream, batch, snapshotMutation);
            var applied = ApplyRemoteEvents(next, batch);
            stream.Cursor = batch.NextCursor;
            stream.Snapshot = CreateSnapshot(snapshotMutation, batch.NextCursor, snapshotMutation.ExpectedRevision + 1);
            ApplyCompletedOperations(next, batch);
            await PersistStateAsync(next, cancellationToken).ConfigureAwait(false);
            return new(batch.NextCursor, applied, batch.Events.Count - applied, stream.Snapshot.Revision);
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
            return FileSystemJournalHelpers.FindOperationOrNull(_state, operationId)?.Status;
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
            return FileSystemJournalHelpers.FindOperationOrNull(_state, operationId)?.RetryState;
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(Guid leaseId, OperationId operationId, int nextAttempt, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            var next = new StoreState(_state);
            var lease = GetLease(next, leaseId);
            if (!lease.OperationIds.Contains(operationId.Value))
            {
                throw new InvalidOperationException("The lease does not own the operation.");
            }

            var op = FileSystemJournalHelpers.FindOperation(next, operationId);
            if (op.Status.State == SyncOperationState.Ambiguous
                && op.Operation.Policy.DeliveryGuarantee == DeliveryGuarantee.AtMostOnce)
            {
                return new(operationId, nextAttempt, false, "OC.AmbiguousAtMostOnce");
            }

            if (nextAttempt <= op.Status.Attempt)
            {
                return new(operationId, nextAttempt, false, "OC.AttemptAlreadyRecorded");
            }

            op.Status = op.Status with
            {
                Attempt = nextAttempt,
                State = op.Operation.Policy.DeliveryGuarantee == DeliveryGuarantee.AtMostOnce
                    ? SyncOperationState.Ambiguous
                    : SyncOperationState.Uploading,
                ChangedAtUtc = _timeProvider.GetUtcNow(),
            };
            await PersistStateAsync(next, cancellationToken).ConfigureAwait(false);
            return new(operationId, nextAttempt, true, null);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureInitialized();
            var next = new StoreState(_state);
            var op = FileSystemJournalHelpers.FindOperation(next, operationId);
            op.RetryState = retryState;
            await PersistStateAsync(next, cancellationToken).ConfigureAwait(false);
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
            var next = new StoreState(_state);
            var lease = GetLease(next, leaseId);
            lease.ExpiresAtUtc = lease.ExpiresAtUtc.Add(extension);
            foreach (var operationId in lease.OperationIds)
            {
                var operation = FileSystemJournalHelpers.FindOperation(next, new(operationId));
                operation.LeaseExpiry = lease.ExpiresAtUtc;
            }

            await PersistStateAsync(next, cancellationToken).ConfigureAwait(false);
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
            var next = new StoreState(_state);
            var lease = GetLease(next, leaseId);
            var releasedOperations = 0;
            var now = _timeProvider.GetUtcNow();
            foreach (var stream in next.Streams.Values)
            {
                foreach (var operation in stream.Operations.Values)
                {
                    if (operation.LeaseId != leaseId)
                    {
                        continue;
                    }

                    operation.LeaseId = null;
                    operation.LeaseExpiry = null;
                    operation.Status = operation.Status with { State = SyncOperationState.QueuedForUpload, ChangedAtUtc = now };
                    releasedOperations++;
                }
            }

            if (releasedOperations != lease.OperationIds.Count)
            {
                throw new InvalidOperationException("The filesystem lease membership is incomplete.");
            }

            _ = next.Leases.Remove(leaseId);
            await PersistStateAsync(next, cancellationToken).ConfigureAwait(false);
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
            var next = new StoreState(_state);
            var candidates = new List<(StreamState Stream, Guid OperationId, OperationState Operation)>();
            long retainedBytes = 0;
            foreach (var pair in next.Streams)
            {
                if (request.StreamId.HasValue && pair.Key != request.StreamId.Value.Value)
                {
                    continue;
                }

                foreach (var operation in pair.Value.Operations)
                {
                    retainedBytes += operation.Value.Operation.Payload.Payload.Length;
                    if (FileSystemJournalHelpers.CanCompact(next, operation.Value, request.RetainTerminalRecordsAfter))
                    {
                        candidates.Add((pair.Value, operation.Key, operation.Value));
                    }
                }
            }

            candidates.Sort(static (left, right) => left.Operation.Status.ChangedAtUtc.CompareTo(right.Operation.Status.ChangedAtUtc));
            long removed = 0;
            foreach (var candidate in candidates)
            {
                if (request.TargetBytes > 0 && retainedBytes <= request.TargetBytes)
                {
                    break;
                }

                _ = candidate.Stream.Operations.Remove(candidate.OperationId);
                _ = next.IncludedOperations.Remove(candidate.OperationId);
                retainedBytes -= candidate.Operation.Operation.Payload.Payload.Length;
                removed++;
            }

            cancellationToken.ThrowIfCancellationRequested();
            var bytesReclaimed = await RewriteJournalAsync(next, cancellationToken).ConfigureAwait(false);
            return new(removed, bytesReclaimed);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            await _disposeCompletion.Task.ConfigureAwait(false);
            return;
        }

        try
        {
            await _gate.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_journal is not null)
                {
                    await FileSystemJournalHelpers.DisposeAsync(_journal).ConfigureAwait(false);
                }

                _journal = null;

                if (_owner is not null)
                {
                    await FileSystemJournalHelpers.DisposeAsync(_owner).ConfigureAwait(false);
                }

                _owner = null;
            }
            finally
            {
                _ = _gate.Release();
                _gate.Dispose();
            }

            _ = _disposeCompletion.TrySetResult(true);
        }
        catch (Exception error)
        {
            _ = _disposeCompletion.TrySetException(error);
            throw;
        }
    }

    /// <summary>Validates the lease bounds before returning the asynchronous sequence.</summary>
    /// <param name="request">The requested operation and byte limits.</param>
    /// <exception cref="ArgumentOutOfRangeException">A lease bound is not positive.</exception>
    private static void ValidateLeaseRequest(OutboxLeaseRequest request)
    {
        if (request.MaximumOperations <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), request.MaximumOperations, "MaximumOperations must be positive.");
        }

        if (request.MaximumBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), request.MaximumBytes, "MaximumBytes must be positive.");
        }

        if (request.LeaseDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(request), request.LeaseDuration, "LeaseDuration must be positive.");
        }
    }

    /// <summary>Validates the sequence and revision preconditions for a local commit.</summary>
    /// <param name="stream">The current durable stream.</param>
    /// <param name="operation">The operation to commit.</param>
    /// <param name="snapshotMutation">The snapshot mutation to commit with the operation.</param>
    /// <returns>The expected durable snapshot revision.</returns>
    /// <exception cref="InvalidOperationException">A subscription, sequence, or revision precondition fails.</exception>
    private static long ValidateLocalCommit(
        StreamState stream,
        SyncOperation operation,
        SnapshotMutation snapshotMutation)
    {
        if (stream.SubscriptionId is null)
        {
            throw new InvalidOperationException("A durable subscription identity is required before committing a local operation.");
        }

        if (operation.ClientSequence != stream.NextSequence)
        {
            throw new InvalidOperationException($"Expected client sequence {stream.NextSequence}, received {operation.ClientSequence}.");
        }

        var expectedRevision = stream.Snapshot?.Revision ?? 0;
        if (snapshotMutation.ExpectedRevision != expectedRevision)
        {
            throw new InvalidOperationException($"Expected snapshot revision {expectedRevision}, received {snapshotMutation.ExpectedRevision}.");
        }

        return expectedRevision;
    }

    /// <summary>Selects the ordered, eligible operations for a new lease.</summary>
    /// <param name="state">The current durable state.</param>
    /// <param name="request">The requested operation and byte limits.</param>
    /// <param name="now">The current time used to test lease expiry.</param>
    /// <returns>The selected operations in client-sequence order.</returns>
    private static List<OperationState> SelectPendingOperations(
        StoreState state,
        OutboxLeaseRequest request,
        DateTimeOffset now)
    {
        var candidates = new List<OperationState>();
        foreach (var stream in state.Streams.Values)
        {
            foreach (var operation in stream.Operations.Values)
            {
                if (IsLeaseCandidate(operation, request, now))
                {
                    candidates.Add(operation);
                }
            }
        }

        candidates.Sort(static (left, right) => left.Operation.ClientSequence.CompareTo(right.Operation.ClientSequence));
        return TakeLeaseBatch(candidates, request);
    }

    /// <summary>Checks whether an operation may be included in a new lease.</summary>
    /// <param name="operation">The operation state.</param>
    /// <param name="request">The requested stream, if any.</param>
    /// <param name="now">The current time used to test lease expiry.</param>
    /// <returns><see langword="true"/> when the operation is eligible.</returns>
    private static bool IsLeaseCandidate(OperationState operation, OutboxLeaseRequest request, DateTimeOffset now) =>
        !operation.Terminal
        && (operation.LeaseId is null || operation.LeaseExpiry <= now)
        && (!request.StreamId.HasValue || operation.Operation.StreamId == request.StreamId.Value);

    /// <summary>Applies the operation count and payload byte limits to eligible operations.</summary>
    /// <param name="candidates">The eligible operations in sequence order.</param>
    /// <param name="request">The requested operation and byte limits.</param>
    /// <returns>The bounded operation batch.</returns>
    private static List<OperationState> TakeLeaseBatch(
        List<OperationState> candidates,
        OutboxLeaseRequest request)
    {
        var selected = new List<OperationState>();
        long selectedPayloadBytes = 0;
        foreach (var candidate in candidates)
        {
            if (selected.Count == request.MaximumOperations)
            {
                break;
            }

            var payloadBytes = candidate.Operation.Payload.PayloadLength;
            if (payloadBytes > request.MaximumBytes - selectedPayloadBytes)
            {
                break;
            }

            selected.Add(candidate);
            selectedPayloadBytes += payloadBytes;
        }

        return selected;
    }

    /// <summary>Checks that the remote batch and snapshot match the durable cursor and revision.</summary>
    /// <param name="stream">The durable stream state.</param>
    /// <param name="batch">The remote batch to apply.</param>
    /// <param name="snapshotMutation">The replacement snapshot.</param>
    /// <exception cref="ArgumentException">The batch and snapshot target different streams.</exception>
    /// <exception cref="InvalidOperationException">The snapshot revision or cursor does not match durable state.</exception>
    private static void ValidateRemoteApplyFence(
        StreamState stream,
        RemoteEventBatch batch,
        SnapshotMutation snapshotMutation)
    {
        if (batch.StreamId != snapshotMutation.StreamId)
        {
            throw new ArgumentException("The batch and snapshot must target the same stream.");
        }

        var snapshotRevision = stream.Snapshot?.Revision;
        var cursorMismatch = stream.Cursor != batch.PreviousCursor;
        if (snapshotRevision != snapshotMutation.ExpectedRevision || cursorMismatch)
        {
            throw new InvalidOperationException(
                "The remote apply fence does not match durable state "
                + $"(snapshot revision {snapshotRevision?.ToString() ?? "missing"}, "
                + $"expected {snapshotMutation.ExpectedRevision}; cursor mismatch: {cursorMismatch}).");
        }
    }

    /// <summary>Adds previously unseen remote events to the durable inbox.</summary>
    /// <param name="state">The copied durable store state.</param>
    /// <param name="batch">The remote batch to apply.</param>
    /// <returns>The number of newly applied events.</returns>
    private static int ApplyRemoteEvents(StoreState state, RemoteEventBatch batch)
    {
        var applied = 0;
        foreach (var remoteEvent in batch.Events)
        {
            if (state.Inbox.Add($"{batch.StreamId.Value}|{remoteEvent.EventId}"))
            {
                applied++;
            }
        }

        return applied;
    }

    /// <summary>Marks locally originated operations included in the remote batch as synchronized.</summary>
    /// <param name="state">The copied durable store state.</param>
    /// <param name="batch">The remote batch containing operation completions.</param>
    /// <exception cref="InvalidOperationException">A completion refers to an unknown local operation.</exception>
    private static void ApplyCompletedOperations(StoreState state, RemoteEventBatch batch)
    {
        foreach (var completion in batch.CompletedOperations)
        {
            if (completion.Origin.ClientId != state.ClientId)
            {
                continue;
            }

            if (!state.Streams.TryGetValue(batch.StreamId.Value, out var completedStream)
                || !completedStream.Operations.TryGetValue(completion.Origin.OperationId.Value, out var operation))
            {
                throw new InvalidOperationException("The remote batch completes an unknown local operation.");
            }

            _ = state.IncludedOperations.Add(completion.Origin.OperationId.Value);
            operation.Terminal = true;
            operation.Status = operation.Status with { State = SyncOperationState.Synchronized };
            operation.LeaseId = null;
            operation.LeaseExpiry = null;
        }
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
            var now = _timeProvider.GetUtcNow();
            var next = new StoreState(_state);
            var selected = SelectPendingOperations(next, request, now);
            if (selected.Count == 0)
            {
                return null;
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

            next.Leases[leaseId] = new LeaseState { LeaseId = leaseId, ExpiresAtUtc = expiry, OperationIds = operationIds, };
            await PersistStateAsync(next, cancellationToken).ConfigureAwait(false);
            return new(leaseId, expiry, operations.ToArray());
        }
        finally
        {
            _ = _gate.Release();
        }
    }
}
