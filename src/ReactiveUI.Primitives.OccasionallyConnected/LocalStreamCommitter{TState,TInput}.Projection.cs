// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates atomic local stream recovery and optimistic operation commits.</summary>
/// <content>Isolates projection state and persists the initial authoritative checkpoint.</content>
internal sealed partial class LocalStreamCommitter<TState, TInput>
{
    /// <summary>Decodes an isolated state instance before invoking application projection code.</summary>
    /// <param name="observed">The committed state snapshot.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The isolated state and its unchanged source payload.</returns>
    /// <exception cref="InvalidOperationException">The serializer returns the wrong state type.</exception>
    private async ValueTask<(TState State, PayloadEnvelope Payload)> PrepareProjectionStateAsync(
        LocalStreamCommitterState<TState> observed,
        CancellationToken cancellationToken)
    {
        var payload = observed.MaterializedPayload ?? await _options.Dependencies.Serializer
            .SerializeAsync(_options.Contracts.StateContractId, _options.Contracts.StateSchemaVersion, observed.State, cancellationToken)
            .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateStatePayload(payload);
        var decoded = await _options.Dependencies.Serializer.DeserializeAsync(payload, typeof(TState), cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        if (decoded is TState typed)
        {
            return (typed, payload);
        }

        throw new InvalidOperationException("The projection state decoded to the wrong state type.");
    }

    /// <summary>Commits prepared local projection state together with its operation and initial authoritative base.</summary>
    /// <param name="operation">The local operation.</param>
    /// <param name="decodedInput">The owned decoded input.</param>
    /// <param name="nextStateValue">The prepared projected value.</param>
    /// <param name="previousPayload">The payload before the local projection ran.</param>
    /// <param name="observed">The committed state snapshot.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The committed local result.</returns>
    private async ValueTask<LocalStreamCommitResult<TState, TInput>> CommitPreparedLocalAsync(
        SyncOperation operation,
        TInput decodedInput,
        TState nextStateValue,
        PayloadEnvelope previousPayload,
        LocalStreamCommitterState<TState> observed,
        CancellationToken cancellationToken)
    {
        var payload = await _options.Dependencies.Serializer
            .SerializeAsync(_options.Contracts.StateContractId, _options.Contracts.StateSchemaVersion, nextStateValue, cancellationToken)
            .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateStatePayload(payload);
        var initialAuthoritative = observed.MaterializedPayload is null ? previousPayload : null;
        var mutation = new SnapshotMutation(_options.StreamId, payload, _options.Contracts.SnapshotFormatVersion, observed.Revision) { AuthoritativeState = initialAuthoritative };
        var result = await _options.Dependencies.Store.CommitLocalOperationAsync(operation, mutation, cancellationToken).ConfigureAwait(false);
        ValidateStoreResult(result, operation, observed.Revision);
        var next = new LocalStreamCommitterState<TState>(
            _options.StreamId,
            observed.SubscriptionId,
            nextStateValue,
            result.SnapshotRevision,
            checked(operation.ClientSequence + 1),
            observed.ServerCursor) { MaterializedPayload = payload, AuthoritativePayload = observed.AuthoritativePayload ?? initialAuthoritative };
        SwapCurrent(next);
        var receipt = new PublishReceipt(result.OperationId, result.ClientSequence, SyncOperationState.SavedLocally, result.CommittedAtUtc);
        return new(receipt, operation, decodedInput, next);
    }
}
