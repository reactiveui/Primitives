// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates atomic local stream recovery and optimistic operation commits.</summary>
/// <content>Rebuilds optimistic state from authoritative state and ordered retained operations.</content>
internal sealed partial class LocalStreamCommitter<TState, TInput>
{
    /// <summary>The finite maximum number of replay operations decoded for one transaction.</summary>
    private const int MaximumReplayOperations = 10_000;

    /// <summary>Validates complete receive groups before allocating inbox or replay lookups.</summary>
    /// <param name="batch">The received batch.</param>
    /// <exception cref="InvalidOperationException">The complete receive batch is malformed or exceeds bounds.</exception>
    private static void ValidateCompleteRemoteBatch(RemoteEventBatch batch)
    {
        try
        {
            RemoteEventBatchValidator.Validate(batch, MaximumReplayOperations, MaximumReplayOperations);
        }
        catch (ArgumentException exception)
        {
            throw new InvalidOperationException("The complete remote batch is invalid.", exception);
        }
    }

    /// <summary>Checks recovery belongs to the exact revision used to prepare the remote transaction.</summary>
    /// <param name="recovered">The recovered store state.</param>
    /// <param name="observed">The current committer state.</param>
    /// <exception cref="InvalidOperationException">The store state changed or contains too many replay operations.</exception>
    private static void ValidateReplayRecovery(RecoveredStream recovered, LocalStreamCommitterState<TState> observed)
    {
        if (recovered is not null
            && recovered.SubscriptionId == observed.SubscriptionId
            && recovered.NextClientSequence == observed.NextClientSequence
            && (recovered.Snapshot?.Revision ?? 0) == observed.Revision
            && string.Equals(recovered.ServerCursor, observed.ServerCursor, StringComparison.Ordinal)
            && recovered.ReplayOperations.Count <= MaximumReplayOperations)
        {
            return;
        }

        throw new InvalidOperationException("The recovered replay state does not match the current bounded stream revision.");
    }

    /// <summary>Prepares isolated authoritative and optimistic states before committing either.</summary>
    /// <param name="batch">The complete received batch.</param>
    /// <param name="observed">The observed committed state.</param>
    /// <param name="events">The new remote events.</param>
    /// <param name="inputs">The decoded remote inputs.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The rebuilt materialized state and encoded authoritative checkpoint.</returns>
    /// <exception cref="InvalidOperationException">The authoritative checkpoint is unknown or recovery changed.</exception>
    private async ValueTask<(TState State, PayloadEnvelope Authoritative)> RebuildRemoteStateAsync(
        RemoteEventBatch batch,
        LocalStreamCommitterState<TState> observed,
        IReadOnlyList<RemoteEvent> events,
        IReadOnlyList<TInput> inputs,
        CancellationToken cancellationToken)
    {
        var recovered = await _options.Dependencies.Store.RecoverStreamAsync(_options.StreamId, _options.SubscriptionId, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateReplayRecovery(recovered, observed);
        var operations = SelectReplayOperations(recovered.ReplayOperations, batch);
        if (observed.AuthoritativePayload is null && observed.MaterializedPayload is not null && recovered.ReplayOperations.Count > 0)
        {
            throw new InvalidOperationException("The historical authoritative checkpoint is unknown; authoritative resynchronization is required before receiving events.");
        }

        List<TInput> replayInputs = [with(capacity: operations.Count)];
        foreach (var operation in operations)
        {
            ValidateRemotePayload(operation.Payload);
            replayInputs.Add(await DecodePersistedOutboxInputAsync(operation, cancellationToken).ConfigureAwait(false));
            cancellationToken.ThrowIfCancellationRequested();
        }

        var baseState = observed with { MaterializedPayload = observed.AuthoritativePayload ?? observed.MaterializedPayload };
        var prepared = await PrepareProjectionStateAsync(baseState, cancellationToken).ConfigureAwait(false);
        var authoritative = ApplyRemoteProjection(prepared.State, events, inputs);
        cancellationToken.ThrowIfCancellationRequested();
        var payload = await _options.Dependencies.Serializer
            .SerializeAsync(_options.Contracts.StateContractId, _options.Contracts.StateSchemaVersion, authoritative, cancellationToken)
            .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateStatePayload(payload);
        var replayState = await PrepareProjectionStateAsync(observed with { MaterializedPayload = payload }, cancellationToken).ConfigureAwait(false);
        var state = replayState.State;
        for (var index = 0; index < operations.Count; index++)
        {
            state = _options.Dependencies.Projection.ApplyLocal(state, replayInputs[index], operations[index]);
            cancellationToken.ThrowIfCancellationRequested();
        }

        return (state, payload);
    }

    /// <summary>Excludes complete authenticated own operations while preserving client sequence order.</summary>
    /// <param name="operations">The retained replay operations.</param>
    /// <param name="batch">The complete received batch.</param>
    /// <returns>The remaining ordered operations.</returns>
    /// <exception cref="InvalidOperationException">The replay operation ordering or stream identity is invalid.</exception>
    private List<SyncOperation> SelectReplayOperations(IReadOnlyList<SyncOperation> operations, RemoteEventBatch batch)
    {
        HashSet<OperationId> included = [];
        foreach (var completion in batch.CompletedOperations)
        {
            if (string.Equals(completion.Origin.ClientId, _options.ClientId, StringComparison.Ordinal))
            {
                _ = included.Add(completion.Origin.OperationId);
            }
        }

        List<SyncOperation> replay = [with(capacity: operations.Count)];
        long previousSequence = 0;
        foreach (var operation in operations)
        {
            if (operation is null || operation.StreamId != _options.StreamId || operation.ClientSequence <= previousSequence)
            {
                throw new InvalidOperationException("Replay operations must belong to the stream and have strictly increasing client sequences.");
            }

            previousSequence = operation.ClientSequence;
            if (!included.Contains(operation.OperationId))
            {
                replay.Add(operation);
            }
        }

        return replay;
    }
}
