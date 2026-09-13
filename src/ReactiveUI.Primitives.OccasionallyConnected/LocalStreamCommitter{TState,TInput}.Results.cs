// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates atomic local stream recovery and optimistic operation commits.</summary>
/// <content>Reconciles upload decisions with optimistic state.</content>
internal sealed partial class LocalStreamCommitter<TState, TInput>
{
    /// <summary>Rebuilds optimistic state when one leased operation is locally dead-lettered.</summary>
    /// <param name="leaseId">The active lease.</param>
    /// <param name="operationId">The operation to dead-letter.</param>
    /// <param name="reasonCode">The stable reason code.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The committed local state.</returns>
    /// <exception cref="InvalidOperationException">The target or recovered state cannot be reconciled safely.</exception>
    internal async ValueTask<LocalStreamCommitterState<TState>> DeadLetterOperationAsync(
        Guid leaseId,
        OperationId operationId,
        string reasonCode,
        CancellationToken cancellationToken)
    {
        ValidateDeadLetterRequest(leaseId, operationId, reasonCode);
        EnterExclusive();
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            ThrowIfNotRecovered();
            return await CommitDeadLetterOperationAsync(leaseId, operationId, reasonCode, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ExitExclusive();
        }
    }

    /// <summary>Rebuilds optimistic state when a leased operation is rejected.</summary>
    /// <param name="batch">The original leased batch.</param>
    /// <param name="result">The remote decisions.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The committed local state.</returns>
    /// <exception cref="InvalidOperationException">The result or recovered state cannot be reconciled safely.</exception>
    internal async ValueTask<LocalStreamCommitterState<TState>> ApplySyncResultAsync(
        SyncBatch batch,
        RemoteSyncResult result,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(batch);
        ArgumentExceptionHelper.ThrowIfNull(result);
        EnterExclusive();
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            ThrowIfNotRecovered();
            ValidateResultBatch(batch, result);
            var rejected = SelectRejectedOperations(result);
            if (rejected.Count == 0)
            {
                var unchanged = await _options.Dependencies.Store.ApplySyncResultAsync(batch.BatchId, result, [], cancellationToken).ConfigureAwait(false);
                ValidateUnchangedResult(unchanged);
                return Current;
            }

            return await CommitRejectedResultAsync(batch.BatchId, result, rejected, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ExitExclusive();
        }
    }

    /// <summary>Selects rejected identities from an already validated result.</summary>
    /// <param name="result">The validated result.</param>
    /// <returns>The identities excluded from optimistic replay.</returns>
    private static HashSet<OperationId> SelectRejectedOperations(RemoteSyncResult result)
    {
        HashSet<OperationId> rejected = [];
        foreach (var operation in result.Operations)
        {
            if (operation.Kind == OperationResultKind.Rejected)
            {
                _ = rejected.Add(operation.OperationId);
            }
        }

        return rejected;
    }

    /// <summary>Checks that a result retains a known authoritative checkpoint.</summary>
    /// <param name="actual">The returned authoritative payload.</param>
    /// <param name="expected">The prior authoritative payload.</param>
    /// <returns>Whether both checkpoints are known and identical.</returns>
    private static bool ResultAuthoritativeMatches(PayloadEnvelope? actual, PayloadEnvelope expected) =>
        actual is not null && PayloadEnvelopeComparison.ContentEquals(actual, expected);

    /// <summary>Validates a local dead-letter request before rebuilding projection state.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="reasonCode">The reason code.</param>
    private static void ValidateDeadLetterRequest(Guid leaseId, OperationId operationId, string reasonCode)
    {
        InMemoryLocalStoreAdapterValidation.ValidateLeaseId(leaseId);
        InMemoryLocalStoreAdapterValidation.ValidateOperationId(operationId, nameof(operationId));
        InMemoryLocalStoreAdapterValidation.ValidateDeadLetterReasonCode(reasonCode);
    }

    /// <summary>Checks bounds and stream identity before allocating result lookups.</summary>
    /// <param name="batch">The leased batch.</param>
    /// <param name="result">The remote result.</param>
    /// <exception cref="InvalidOperationException">The batch exceeds the bounded stream scope.</exception>
    private void ValidateResultBatch(SyncBatch batch, RemoteSyncResult result)
    {
        if (batch.Operations.Count > MaximumReplayOperations || result.Operations.Count > MaximumReplayOperations)
        {
            throw new InvalidOperationException("The result exceeds the bounded stream transaction size.");
        }

        SyncBatchValidator.Validate(batch, result, _options.MinimumPriority, _options.MaximumPriority);
        if (batch.Operations[0].StreamId == _options.StreamId)
        {
            return;
        }

        throw new InvalidOperationException("The upload batch belongs to another stream.");
    }

    /// <summary>Commits one rebuilt optimistic state after removing a dead-lettered operation.</summary>
    /// <param name="leaseId">The upload lease.</param>
    /// <param name="operationId">The operation to dead-letter.</param>
    /// <param name="reasonCode">The stable reason code.</param>
    /// <param name="cancellationToken">The precommit cancellation token.</param>
    /// <returns>The committed state.</returns>
    /// <exception cref="InvalidOperationException">The authoritative checkpoint, replay, or receipt is invalid.</exception>
    private async ValueTask<LocalStreamCommitterState<TState>> CommitDeadLetterOperationAsync(
        Guid leaseId,
        OperationId operationId,
        string reasonCode,
        CancellationToken cancellationToken)
    {
        var observed = Current;
        ThrowIfRevisionOverflow(observed.Revision);
        var authoritative = observed.AuthoritativePayload
            ?? throw new InvalidOperationException("Dead-letter reconciliation requires an authoritative checkpoint.");
        var recovered = await _options.Dependencies.Store.RecoverStreamAsync(_options.StreamId, _options.SubscriptionId, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateReplayRecovery(recovered, observed);
        HashSet<OperationId> excluded = [operationId];
        var prepared = await PrepareProjectionStateAsync(observed with { MaterializedPayload = authoritative }, cancellationToken).ConfigureAwait(false);
        var state = await ReplayResultOperationsAsync(prepared.State, recovered.ReplayOperations, excluded, cancellationToken).ConfigureAwait(false);
        var payload = await _options.Dependencies.Serializer
            .SerializeAsync(_options.Contracts.StateContractId, _options.Contracts.StateSchemaVersion, state, cancellationToken)
            .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateStatePayload(payload);
        var mutation = new SnapshotMutation(_options.StreamId, payload, _options.Contracts.SnapshotFormatVersion, observed.Revision);
        var snapshot = await _options.Dependencies.Store
            .DeadLetterOperationAsync(leaseId, operationId, reasonCode, mutation, cancellationToken)
            .ConfigureAwait(false);
        ValidateDeadLetterResult(snapshot, mutation, observed, authoritative);
        var next = observed with { State = state, Revision = snapshot.Revision, MaterializedPayload = payload };
        SwapCurrent(next);
        return next;
    }

    /// <summary>Commits a rebuilt optimistic state with the complete upload decisions.</summary>
    /// <param name="leaseId">The upload lease.</param>
    /// <param name="result">The validated upload decisions.</param>
    /// <param name="rejected">The operations excluded from replay.</param>
    /// <param name="cancellationToken">The precommit cancellation token.</param>
    /// <returns>The committed state.</returns>
    /// <exception cref="InvalidOperationException">The authoritative checkpoint is unknown.</exception>
    private async ValueTask<LocalStreamCommitterState<TState>> CommitRejectedResultAsync(
        Guid leaseId,
        RemoteSyncResult result,
        HashSet<OperationId> rejected,
        CancellationToken cancellationToken)
    {
        var observed = Current;
        ThrowIfRevisionOverflow(observed.Revision);
        var authoritative = observed.AuthoritativePayload ?? throw new InvalidOperationException("Result reconciliation requires an authoritative checkpoint.");
        var recovered = await _options.Dependencies.Store.RecoverStreamAsync(_options.StreamId, _options.SubscriptionId, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateReplayRecovery(recovered, observed);
        var prepared = await PrepareProjectionStateAsync(
            observed with { MaterializedPayload = authoritative },
            LocalPayloadQuarantineSource.Snapshot,
            null,
            observed.ServerCursor,
            cancellationToken).ConfigureAwait(false);
        var state = await ReplayResultOperationsAsync(prepared.State, recovered.ReplayOperations, rejected, cancellationToken).ConfigureAwait(false);
        var payload = await _options.Dependencies.Serializer
            .SerializeAsync(_options.Contracts.StateContractId, _options.Contracts.StateSchemaVersion, state, cancellationToken)
            .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateStatePayload(payload);
        var mutation = new SnapshotMutation(_options.StreamId, payload, _options.Contracts.SnapshotFormatVersion, observed.Revision);
        var snapshots = await _options.Dependencies.Store.ApplySyncResultAsync(leaseId, result, [mutation], cancellationToken).ConfigureAwait(false);
        ValidateReconciledResult(snapshots, mutation, observed, authoritative);
        var next = observed with { State = state, Revision = snapshots[0].Revision, MaterializedPayload = payload };
        SwapCurrent(next);
        return next;
    }

    /// <summary>Replays the retained operations in their persisted client sequence order.</summary>
    /// <param name="state">The isolated authoritative state.</param>
    /// <param name="operations">The retained replay operations.</param>
    /// <param name="rejected">The excluded identities.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The rebuilt optimistic state.</returns>
    /// <exception cref="InvalidOperationException">Replay ordering or stream identity is malformed.</exception>
    private async ValueTask<TState> ReplayResultOperationsAsync(
        TState state,
        IReadOnlyList<SyncOperation> operations,
        HashSet<OperationId> rejected,
        CancellationToken cancellationToken)
    {
        long previousSequence = 0;
        foreach (var operation in operations)
        {
            if (operation is null || operation.StreamId != _options.StreamId || operation.ClientSequence <= previousSequence)
            {
                throw new InvalidOperationException("Replay operations must belong to the stream and have strictly increasing client sequences.");
            }

            previousSequence = operation.ClientSequence;
            if (rejected.Contains(operation.OperationId))
            {
                continue;
            }

            ValidateRemotePayload(operation.Payload);
            var input = await DecodePersistedOutboxInputAsync(operation, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            state = _options.Dependencies.Projection.ApplyLocal(state, input, operation);
            cancellationToken.ThrowIfCancellationRequested();
        }

        return state;
    }

    /// <summary>Checks the store did not replace a snapshot for a status-only result.</summary>
    /// <param name="snapshots">The returned snapshots.</param>
    /// <exception cref="InvalidOperationException">The receipt violates the store contract.</exception>
    private void ValidateUnchangedResult(IReadOnlyList<LocalSnapshot> snapshots)
    {
        if (snapshots is not null && snapshots.Count == 0)
        {
            return;
        }

        _poisoned = true;
        throw new InvalidOperationException("The local store returned a malformed result receipt.");
    }

    /// <summary>Checks the committed snapshot before exposing rebuilt projection state.</summary>
    /// <param name="snapshots">The returned committed snapshots.</param>
    /// <param name="mutation">The prepared optimistic mutation.</param>
    /// <param name="observed">The prior committed state.</param>
    /// <param name="authoritative">The known authoritative checkpoint used to prepare the result.</param>
    /// <exception cref="InvalidOperationException">The receipt violates the store contract.</exception>
    private void ValidateReconciledResult(
        IReadOnlyList<LocalSnapshot> snapshots,
        SnapshotMutation mutation,
        LocalStreamCommitterState<TState> observed,
        PayloadEnvelope authoritative)
    {
        if (snapshots is not null && snapshots.Count == 1 && snapshots[0] is { } snapshot
            && snapshot.StreamId == _options.StreamId && snapshot.Revision == observed.Revision + 1
            && snapshot.FormatVersion == mutation.FormatVersion
            && string.Equals(snapshot.ServerCursor, observed.ServerCursor, StringComparison.Ordinal)
            && PayloadEnvelopeComparison.ContentEquals(snapshot.State, mutation.State)
            && ResultAuthoritativeMatches(snapshot.AuthoritativeState, authoritative))
        {
            return;
        }

        _poisoned = true;
        throw new InvalidOperationException("The local store returned a malformed result receipt.");
    }

    /// <summary>Checks the committed dead-letter snapshot before exposing rebuilt projection state.</summary>
    /// <param name="snapshot">The returned committed snapshot.</param>
    /// <param name="mutation">The prepared optimistic mutation.</param>
    /// <param name="observed">The prior committed state.</param>
    /// <param name="authoritative">The known authoritative checkpoint used to prepare the result.</param>
    /// <exception cref="InvalidOperationException">The receipt violates the store contract.</exception>
    private void ValidateDeadLetterResult(
        LocalSnapshot snapshot,
        SnapshotMutation mutation,
        LocalStreamCommitterState<TState> observed,
        PayloadEnvelope authoritative)
    {
        if (snapshot is not null
            && snapshot.StreamId == _options.StreamId && snapshot.Revision == observed.Revision + 1
            && snapshot.FormatVersion == mutation.FormatVersion
            && string.Equals(snapshot.ServerCursor, observed.ServerCursor, StringComparison.Ordinal)
            && PayloadEnvelopeComparison.ContentEquals(snapshot.State, mutation.State)
            && ResultAuthoritativeMatches(snapshot.AuthoritativeState, authoritative))
        {
            return;
        }

        _poisoned = true;
        throw new InvalidOperationException("The local store returned a malformed dead-letter receipt.");
    }
}
