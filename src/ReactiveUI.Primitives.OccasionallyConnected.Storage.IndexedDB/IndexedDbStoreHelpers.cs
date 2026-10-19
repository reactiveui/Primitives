// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB;

/// <summary>Provides stateless helpers for IndexedDB durable store state.</summary>
internal static class IndexedDbStoreHelpers
{
    /// <summary>Determines whether a terminal operation is safe to remove.</summary>
    /// <param name="state">The full state used to check leases and snapshot inclusion.</param>
    /// <param name="operation">The operation considered for removal.</param>
    /// <param name="cutoffUtc">The retention cutoff.</param>
    /// <returns><see langword="true"/> when the operation meets all compaction rules.</returns>
    internal static bool CanCompact(
        IndexedDbLocalStoreAdapter.StoreState state,
        IndexedDbLocalStoreAdapter.OperationState operation,
        DateTimeOffset cutoffUtc)
    {
        if (!operation.Terminal || operation.Status.ChangedAtUtc >= cutoffUtc)
        {
            return false;
        }

        foreach (var lease in state.Leases.Values)
        {
            if (lease.OperationIds.Contains(operation.Operation.OperationId.Value))
            {
                return false;
            }
        }

        return operation.Status.State is SyncOperationState.Rejected or SyncOperationState.DeadLettered
            || (operation.Status.State == SyncOperationState.Synchronized
                && state.IncludedOperations.Contains(operation.Operation.OperationId.Value));
    }

    /// <summary>Gets the durable state for a stream.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The stream state.</returns>
    /// <exception cref="InvalidOperationException">The stream has no durable subscription identity.</exception>
    internal static IndexedDbLocalStoreAdapter.StreamState GetStream(
        IndexedDbLocalStoreAdapter.StoreState state,
        StreamId streamId) =>
        state.Streams.TryGetValue(streamId.Value, out var stream)
            ? stream
            : throw new InvalidOperationException("The stream has no durable subscription identity.");

    /// <summary>Finds a durable operation or throws when it is missing.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The matching operation state.</returns>
    /// <exception cref="InvalidOperationException">The operation is not present in the durable store.</exception>
    internal static IndexedDbLocalStoreAdapter.OperationState FindOperation(
        IndexedDbLocalStoreAdapter.StoreState state,
        OperationId operationId) =>
        FindOperationOrNull(state, operationId)
            ?? throw new InvalidOperationException("The operation is not present in the durable store.");

    /// <summary>Finds an operation across the stored streams.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The matching operation, or <see langword="null"/> when it is absent.</returns>
    internal static IndexedDbLocalStoreAdapter.OperationState? FindOperationOrNull(
        IndexedDbLocalStoreAdapter.StoreState state,
        OperationId operationId)
    {
        foreach (var stream in state.Streams.Values)
        {
            foreach (var operation in stream.Operations.Values)
            {
                if (operation.Operation.OperationId == operationId)
                {
                    return operation;
                }
            }
        }

        return null;
    }

    /// <summary>Validates the lease bounds before returning the asynchronous sequence.</summary>
    /// <param name="request">The requested operation and byte limits.</param>
    /// <exception cref="ArgumentOutOfRangeException">A lease bound is not positive.</exception>
    internal static void ValidateLeaseRequest(OutboxLeaseRequest request)
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
    internal static long ValidateLocalCommit(
        IndexedDbLocalStoreAdapter.StreamState stream,
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

    /// <summary>Selects the next pending operations that fit the requested count and byte limits.</summary>
    /// <param name="state">The durable store state.</param>
    /// <param name="request">The requested lease limits.</param>
    /// <param name="now">The current time used for lease expiry checks.</param>
    /// <returns>The selected operations ordered by client sequence.</returns>
    internal static List<IndexedDbLocalStoreAdapter.OperationState> SelectPendingOperations(
        IndexedDbLocalStoreAdapter.StoreState state,
        OutboxLeaseRequest request,
        DateTimeOffset now)
    {
        var candidates = new List<IndexedDbLocalStoreAdapter.OperationState>();
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

    /// <summary>Validates the remote apply fence against durable snapshot and cursor state.</summary>
    /// <param name="stream">The durable stream state.</param>
    /// <param name="batch">The remote batch to apply.</param>
    /// <param name="snapshotMutation">The replacement snapshot mutation.</param>
    /// <exception cref="ArgumentException">The batch and snapshot target different streams.</exception>
    /// <exception cref="InvalidOperationException">The cursor or snapshot revision does not match durable state.</exception>
    internal static void ValidateRemoteApplyFence(
        IndexedDbLocalStoreAdapter.StreamState stream,
        RemoteEventBatch batch,
        SnapshotMutation snapshotMutation)
    {
        if (batch.StreamId != snapshotMutation.StreamId)
        {
            throw new ArgumentException("The batch and snapshot must target the same stream.");
        }

        var snapshotRevision = stream.Snapshot?.Revision ?? 0;
        var cursorMismatch = stream.Cursor != batch.PreviousCursor;
        if (snapshotRevision != snapshotMutation.ExpectedRevision || cursorMismatch)
        {
            throw new InvalidOperationException(
                "The remote apply fence does not match durable state "
                + $"(snapshot revision {snapshotRevision}, "
                + $"expected {snapshotMutation.ExpectedRevision}; cursor mismatch: {cursorMismatch}).");
        }
    }

    /// <summary>Adds previously unseen remote events to the durable inbox.</summary>
    /// <param name="state">The copied durable store state.</param>
    /// <param name="batch">The remote batch to apply.</param>
    /// <returns>The number of newly applied events.</returns>
    internal static int ApplyRemoteEvents(
        IndexedDbLocalStoreAdapter.StoreState state,
        RemoteEventBatch batch)
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
    internal static void ApplyCompletedOperations(
        IndexedDbLocalStoreAdapter.StoreState state,
        RemoteEventBatch batch)
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

    /// <summary>Validates that a remote sync result matches the complete leased batch.</summary>
    /// <param name="state">The store state.</param>
    /// <param name="lease">The active lease.</param>
    /// <param name="result">The remote sync result.</param>
    /// <exception cref="InvalidOperationException">The result does not exactly match the leased operation batch.</exception>
    internal static void ValidateResult(
        IndexedDbLocalStoreAdapter.StoreState state,
        IndexedDbLocalStoreAdapter.LeaseState lease,
        RemoteSyncResult result)
    {
        if (result.Operations.Count != lease.OperationIds.Count)
        {
            throw new InvalidOperationException("The remote result does not exactly match the leased operation batch.");
        }

        var seenIds = new HashSet<Guid>();
        foreach (var decision in result.Operations)
        {
            var operationId = decision.OperationId.Value;
            if (!seenIds.Add(operationId)
                || !lease.OperationIds.Contains(operationId)
                || FindOperationOrNull(state, decision.OperationId) is null)
            {
                throw new InvalidOperationException("The remote result does not exactly match the leased operation batch.");
            }
        }
    }

    /// <summary>Preserves an existing authoritative state unless the mutation repeats it exactly.</summary>
    /// <param name="snapshot">The current durable snapshot.</param>
    /// <param name="mutation">The replacement snapshot mutation.</param>
    /// <returns>The authoritative state to persist with the replacement snapshot.</returns>
    /// <exception cref="InvalidOperationException">The mutation proposes a mismatched authoritative state.</exception>
    internal static PayloadEnvelope? PreserveAuthoritativeState(LocalSnapshot? snapshot, SnapshotMutation mutation)
    {
        if (mutation.AuthoritativeState is null)
        {
            return snapshot?.AuthoritativeState;
        }

        if (snapshot?.AuthoritativeState is null || EqualityComparer<PayloadEnvelope>.Default.Equals(snapshot.AuthoritativeState, mutation.AuthoritativeState))
        {
            return mutation.AuthoritativeState;
        }

        throw new InvalidOperationException("The replacement snapshot authoritative state does not match durable state.");
    }

    /// <summary>Applies remote per-operation decisions and collects streams that need replacement snapshots.</summary>
    /// <param name="state">The copied durable store state.</param>
    /// <param name="result">The remote sync result.</param>
    /// <param name="changedAtUtc">The durable timestamp to stamp onto changed operations.</param>
    /// <returns>The stream identifiers that require replacement snapshots.</returns>
    internal static HashSet<string> ApplySyncResultOperations(
        IndexedDbLocalStoreAdapter.StoreState state,
        RemoteSyncResult result,
        DateTimeOffset changedAtUtc)
    {
        var requiredRebuildStreams = new HashSet<string>();
        foreach (var decision in result.Operations)
        {
            var operation = FindOperation(state, decision.OperationId);
            var stream = GetStream(state, operation.Operation.StreamId);
            TrackRejectedOperation(state, decision, operation, stream, requiredRebuildStreams);
            ApplySyncDecision(operation, decision, changedAtUtc);
        }

        return requiredRebuildStreams;
    }

    /// <summary>Validates that required snapshot replacements are present when optimistic work was rejected.</summary>
    /// <param name="requiredRebuildStreams">The streams that require replacement snapshots.</param>
    /// <param name="snapshotMutations">The provided replacement snapshot mutations.</param>
    /// <exception cref="InvalidOperationException">Rejected optimistic work requires replacement snapshots.</exception>
    internal static void ValidateSnapshotMutationPresence(
        HashSet<string> requiredRebuildStreams,
        IReadOnlyList<SnapshotMutation> snapshotMutations)
    {
        if (requiredRebuildStreams.Count > 0 && snapshotMutations.Count == 0)
        {
            throw new InvalidOperationException("Rejected optimistic work with a known authoritative checkpoint requires replacement snapshots.");
        }
    }

    /// <summary>Validates that every affected stream received a replacement snapshot.</summary>
    /// <param name="requiredRebuildStreams">The streams that require replacement snapshots.</param>
    /// <param name="mutationStreams">The streams covered by replacement snapshot mutations.</param>
    /// <exception cref="InvalidOperationException">An affected stream is missing a replacement snapshot.</exception>
    internal static void ValidateSnapshotCoverage(HashSet<string> requiredRebuildStreams, List<string> mutationStreams)
    {
        foreach (var streamId in requiredRebuildStreams)
        {
            if (!mutationStreams.Contains(streamId))
            {
                throw new InvalidOperationException("Rejected optimistic work requires one replacement snapshot per affected stream.");
            }
        }
    }

    /// <summary>Collects the compactable operations for one compaction pass.</summary>
    /// <param name="state">The durable store state.</param>
    /// <param name="request">The compaction request.</param>
    /// <param name="retainedBytes">The total retained payload bytes before compaction.</param>
    /// <returns>The ordered compaction candidates.</returns>
    internal static List<(IndexedDbLocalStoreAdapter.StreamState Stream, Guid OperationId, IndexedDbLocalStoreAdapter.OperationState Operation)> CollectCompactionCandidates(
        IndexedDbLocalStoreAdapter.StoreState state,
        CompactionRequest request,
        out long retainedBytes)
    {
        var candidates = new List<(IndexedDbLocalStoreAdapter.StreamState Stream, Guid OperationId, IndexedDbLocalStoreAdapter.OperationState Operation)>();
        retainedBytes = 0;
        foreach (var pair in state.Streams)
        {
            if (request.StreamId.HasValue && pair.Key != request.StreamId.Value.Value)
            {
                continue;
            }

            foreach (var operation in pair.Value.Operations)
            {
                retainedBytes += operation.Value.Operation.Payload.Payload.Length;
                if (CanCompact(state, operation.Value, request.RetainTerminalRecordsAfter))
                {
                    candidates.Add((pair.Value, operation.Key, operation.Value));
                }
            }
        }

        candidates.Sort(static (left, right) => left.Operation.Status.ChangedAtUtc.CompareTo(right.Operation.Status.ChangedAtUtc));
        return candidates;
    }

    /// <summary>Removes compactable operations until the requested target is satisfied.</summary>
    /// <param name="state">The durable store state.</param>
    /// <param name="candidates">The ordered compaction candidates.</param>
    /// <param name="targetBytes">The requested maximum retained payload bytes.</param>
    /// <param name="retainedBytes">The retained payload bytes before removal and the updated count after removal.</param>
    /// <returns>The number of removed operations.</returns>
    internal static long RemoveCompactionCandidates(
        IndexedDbLocalStoreAdapter.StoreState state,
        List<(IndexedDbLocalStoreAdapter.StreamState Stream, Guid OperationId, IndexedDbLocalStoreAdapter.OperationState Operation)> candidates,
        long targetBytes,
        ref long retainedBytes)
    {
        long removed = 0;
        foreach (var candidate in candidates)
        {
            if (targetBytes > 0 && retainedBytes <= targetBytes)
            {
                break;
            }

            _ = candidate.Stream.Operations.Remove(candidate.OperationId);
            _ = state.IncludedOperations.Remove(candidate.OperationId);
            retainedBytes -= candidate.Operation.Operation.Payload.Payload.Length;
            removed++;
        }

        return removed;
    }

    /// <summary>Determines whether one operation can be leased for upload.</summary>
    /// <param name="operation">The durable operation state.</param>
    /// <param name="request">The requested lease limits.</param>
    /// <param name="now">The current time used for lease expiry checks.</param>
    /// <returns>True when the operation may be leased; otherwise false.</returns>
    private static bool IsLeaseCandidate(
        IndexedDbLocalStoreAdapter.OperationState operation,
        OutboxLeaseRequest request,
        DateTimeOffset now) =>
        !operation.Terminal
        && (operation.LeaseId is null || operation.LeaseExpiry <= now)
        && (!request.StreamId.HasValue || operation.Operation.StreamId == request.StreamId.Value);

    /// <summary>Takes the next batch of operations that fits the configured count and byte limits.</summary>
    /// <param name="candidates">The ordered candidate operations.</param>
    /// <param name="request">The requested lease limits.</param>
    /// <returns>The selected lease batch.</returns>
    private static List<IndexedDbLocalStoreAdapter.OperationState> TakeLeaseBatch(
        List<IndexedDbLocalStoreAdapter.OperationState> candidates,
        OutboxLeaseRequest request)
    {
        var selected = new List<IndexedDbLocalStoreAdapter.OperationState>();
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

    /// <summary>Tracks the snapshot rebuild requirements for a rejected operation.</summary>
    /// <param name="state">The copied durable store state.</param>
    /// <param name="decision">The remote decision.</param>
    /// <param name="operation">The durable operation state.</param>
    /// <param name="stream">The durable stream state.</param>
    /// <param name="requiredRebuildStreams">The set of streams that need replacement snapshots.</param>
    /// <exception cref="InvalidOperationException">A rejected operation is already included in authoritative state.</exception>
    private static void TrackRejectedOperation(
        IndexedDbLocalStoreAdapter.StoreState state,
        OperationSyncResult decision,
        IndexedDbLocalStoreAdapter.OperationState operation,
        IndexedDbLocalStoreAdapter.StreamState stream,
        HashSet<string> requiredRebuildStreams)
    {
        if (decision.Kind != OperationResultKind.Rejected)
        {
            return;
        }

        if (stream.Snapshot?.AuthoritativeState is not null)
        {
            _ = requiredRebuildStreams.Add(operation.Operation.StreamId.Value);
        }

        if (state.IncludedOperations.Contains(operation.Operation.OperationId.Value))
        {
            throw new InvalidOperationException("A rejected operation cannot already be included in authoritative receive state.");
        }
    }

    /// <summary>Applies one remote decision to the durable operation state.</summary>
    /// <param name="operation">The durable operation state.</param>
    /// <param name="decision">The remote decision.</param>
    /// <param name="changedAtUtc">The durable timestamp for the status change.</param>
    private static void ApplySyncDecision(
        IndexedDbLocalStoreAdapter.OperationState operation,
        OperationSyncResult decision,
        DateTimeOffset changedAtUtc)
    {
        var nextState = GetResultState(decision.Kind);
        operation.Status = operation.Status with
        {
            ChangedAtUtc = changedAtUtc,
            ReasonCode = decision.ReasonCode,
            State = nextState,
        };
        operation.Terminal = nextState is SyncOperationState.Synchronized or SyncOperationState.Rejected;
        operation.LeaseId = null;
        operation.LeaseExpiry = null;
    }

    /// <summary>Maps one remote result kind to the corresponding durable operation state.</summary>
    /// <param name="kind">The remote result kind.</param>
    /// <returns>The durable operation state.</returns>
    private static SyncOperationState GetResultState(OperationResultKind kind) =>
        kind switch
        {
            OperationResultKind.Accepted => SyncOperationState.Synchronized,
            OperationResultKind.Conflict => SyncOperationState.Conflict,
            OperationResultKind.Rejected => SyncOperationState.Rejected,
            _ => SyncOperationState.QueuedForUpload,
        };
}
