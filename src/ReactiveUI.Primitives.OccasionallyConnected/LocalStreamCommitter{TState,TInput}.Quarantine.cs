// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates atomic local stream recovery and optimistic operation commits.</summary>
/// <content>Handles durable quarantine for corrupt persisted or received payloads.</content>
internal sealed partial class LocalStreamCommitter<TState, TInput>
{
    /// <summary>Maps schema failures onto durable quarantine reasons.</summary>
    /// <param name="reason">The schema failure reason.</param>
    /// <returns>The quarantine reason.</returns>
    private static LocalPayloadQuarantineReason MapQuarantineReason(PayloadSchemaFailureReason reason) =>
        reason switch
        {
            PayloadSchemaFailureReason.PayloadHashMismatch => LocalPayloadQuarantineReason.PayloadHashMismatch,
            PayloadSchemaFailureReason.MissingUpcaster
                or PayloadSchemaFailureReason.AmbiguousUpcaster
                or PayloadSchemaFailureReason.DowncastNotSupported
                or PayloadSchemaFailureReason.UpcasterContractMismatch
                or PayloadSchemaFailureReason.UpcasterFailed => LocalPayloadQuarantineReason.UpcastFailed,
            _ => LocalPayloadQuarantineReason.SchemaRejected,
        };

    /// <summary>Decodes recovered store state.</summary>
    /// <param name="recovered">The recovered stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The recovered committer state.</returns>
    /// <exception cref="InvalidOperationException">The recovered stream or snapshot is not safe to use.</exception>
    private async ValueTask<LocalStreamCommitterState<TState>> DecodeRecoveredStateAsync(
        RecoveredStream recovered,
        CancellationToken cancellationToken)
    {
        if (recovered is null)
        {
            throw new InvalidOperationException("Local store recovery returned no stream state.");
        }

        if (recovered.SubscriptionId != _options.SubscriptionId)
        {
            throw new InvalidOperationException("Recovered subscription identity does not match the configured subscription.");
        }

        ThrowIfRecoveredStreamQuarantined(recovered);
        if (recovered.Snapshot is null)
        {
            return DecodePristineRecovery(recovered);
        }

        ValidateSnapshotHeader(recovered.Snapshot);
        ValidateRecoveredCounters(recovered, recovered.Snapshot);
        var value = await DecodeRecoveredSnapshotPayloadAsync(
            recovered.Snapshot.State,
            recovered.ServerCursor,
            cancellationToken).ConfigureAwait(false);
        if (recovered.Snapshot.AuthoritativeState is { } authoritativeState)
        {
            _ = await DecodeRecoveredSnapshotPayloadAsync(authoritativeState, recovered.ServerCursor, cancellationToken).ConfigureAwait(false);
        }

        return new(
            _options.StreamId,
            recovered.SubscriptionId,
            value,
            recovered.Snapshot.Revision,
            recovered.NextClientSequence,
            recovered.ServerCursor) { MaterializedPayload = recovered.Snapshot.State, AuthoritativePayload = recovered.Snapshot.AuthoritativeState };
    }

    /// <summary>Decodes one persisted recovered snapshot payload and quarantines schema failures with original evidence.</summary>
    /// <param name="payload">The persisted snapshot payload.</param>
    /// <param name="serverCursor">The recovered server cursor.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The decoded value.</returns>
    /// <exception cref="InvalidOperationException">The recovered snapshot decoded to the wrong state type.</exception>
    private async ValueTask<TState> DecodeRecoveredSnapshotPayloadAsync(
        PayloadEnvelope payload,
        string? serverCursor,
        CancellationToken cancellationToken)
    {
        try
        {
            var value = await _options.Dependencies.Serializer
                .DeserializeAsync(payload, typeof(TState), cancellationToken)
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            if (value is TState typed)
            {
                return typed;
            }

            throw new InvalidOperationException("Recovered snapshot decoded to the wrong state type.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (PayloadSchemaException exception)
        {
            await QuarantineRecoveredSnapshotAsync(payload, serverCursor, exception, cancellationToken).ConfigureAwait(false);
            throw CreateQuarantinedStreamException("Recovered snapshot payload was quarantined.", exception);
        }
    }

    /// <summary>Throws when recovery already carries a quarantine marker.</summary>
    /// <param name="recovered">The recovered stream.</param>
    /// <exception cref="InvalidOperationException">The recovered stream is quarantined.</exception>
    private void ThrowIfRecoveredStreamQuarantined(RecoveredStream recovered)
    {
        if (recovered.Quarantine is null)
        {
            return;
        }

        _poisoned = true;
        throw new InvalidOperationException("The local stream is quarantined.");
    }

    /// <summary>Writes a quarantine marker for a recovered snapshot payload.</summary>
    /// <param name="payload">The recovered snapshot payload.</param>
    /// <param name="serverCursor">The recovered server cursor.</param>
    /// <param name="exception">The schema exception.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The quarantine marker cannot be written.</exception>
    private async ValueTask QuarantineRecoveredSnapshotAsync(
        PayloadEnvelope payload,
        string? serverCursor,
        PayloadSchemaException exception,
        CancellationToken cancellationToken) =>
        await QuarantinePayloadAsync(
            new()
            {
                StreamId = _options.StreamId,
                SubscriptionId = _options.SubscriptionId,
                Source = LocalPayloadQuarantineSource.Snapshot,
                Reason = MapQuarantineReason(exception.Reason),
                ReasonCode = exception.Reason.ToString(),
                Envelope = payload,
                Cursor = serverCursor,
                ObservedAtUtc = _options.Dependencies.TimeProvider.GetUtcNow(),
            },
            exception,
            cancellationToken).ConfigureAwait(false);

    /// <summary>Writes a quarantine marker for a received remote event payload.</summary>
    /// <param name="remoteEvent">The remote event.</param>
    /// <param name="exception">The schema exception.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The quarantine marker cannot be written.</exception>
    private async ValueTask QuarantineRemoteEventAsync(
        RemoteEvent remoteEvent,
        PayloadSchemaException exception,
        CancellationToken cancellationToken) =>
        await QuarantinePayloadAsync(
            new()
            {
                StreamId = _options.StreamId,
                SubscriptionId = _options.SubscriptionId,
                EventId = remoteEvent.EventId,
                Source = LocalPayloadQuarantineSource.RemoteEvent,
                Reason = MapQuarantineReason(exception.Reason),
                ReasonCode = exception.Reason.ToString(),
                Envelope = remoteEvent.Payload,
                Cursor = remoteEvent.ServerCursor,
                ObservedAtUtc = _options.Dependencies.TimeProvider.GetUtcNow(),
            },
            exception,
            cancellationToken).ConfigureAwait(false);

    /// <summary>Writes a quarantine marker for a persisted state payload used by replay or reconciliation.</summary>
    /// <param name="payload">The persisted state payload.</param>
    /// <param name="source">The source record kind.</param>
    /// <param name="operationId">The outbox operation identifier, when any.</param>
    /// <param name="cursor">The persisted cursor, when known.</param>
    /// <param name="exception">The schema exception.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The quarantine marker cannot be written.</exception>
    private async ValueTask QuarantinePersistedStateAsync(
        PayloadEnvelope payload,
        LocalPayloadQuarantineSource source,
        OperationId? operationId,
        string? cursor,
        PayloadSchemaException exception,
        CancellationToken cancellationToken) =>
        await QuarantinePayloadAsync(
            new()
            {
                StreamId = _options.StreamId,
                SubscriptionId = _options.SubscriptionId,
                OperationId = operationId,
                Source = source,
                Reason = MapQuarantineReason(exception.Reason),
                ReasonCode = exception.Reason.ToString(),
                Envelope = payload,
                Cursor = cursor,
                ObservedAtUtc = _options.Dependencies.TimeProvider.GetUtcNow(),
            },
            exception,
            cancellationToken).ConfigureAwait(false);

    /// <summary>Writes a quarantine marker for a persisted outbox operation payload.</summary>
    /// <param name="operation">The persisted operation.</param>
    /// <param name="exception">The schema exception.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The quarantine marker cannot be written.</exception>
    private async ValueTask QuarantinePersistedOutboxOperationAsync(
        SyncOperation operation,
        PayloadSchemaException exception,
        CancellationToken cancellationToken) =>
        await QuarantinePayloadAsync(
            new()
            {
                StreamId = _options.StreamId,
                SubscriptionId = _options.SubscriptionId,
                OperationId = operation.OperationId,
                Source = LocalPayloadQuarantineSource.OutboxOperation,
                Reason = MapQuarantineReason(exception.Reason),
                ReasonCode = exception.Reason.ToString(),
                Envelope = operation.Payload,
                ObservedAtUtc = _options.Dependencies.TimeProvider.GetUtcNow(),
            },
            exception,
            cancellationToken).ConfigureAwait(false);

    /// <summary>Decodes a persisted replay operation input and quarantines schema failures.</summary>
    /// <param name="operation">The persisted operation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The decoded input.</returns>
    /// <exception cref="InvalidOperationException">The persisted operation payload is quarantined or decoded to the wrong type.</exception>
    private async ValueTask<TInput> DecodePersistedOutboxInputAsync(
        SyncOperation operation,
        CancellationToken cancellationToken)
    {
        try
        {
            return await DecodeInputAsync(operation.Payload, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (PayloadSchemaException exception)
        {
            await QuarantinePersistedOutboxOperationAsync(operation, exception, cancellationToken).ConfigureAwait(false);
            throw CreateQuarantinedStreamException("Persisted outbox operation payload was quarantined.", exception);
        }
    }

    /// <summary>Writes a quarantine marker and poisons the committer when the marker cannot be written.</summary>
    /// <param name="request">The quarantine request.</param>
    /// <param name="cause">The schema failure that caused the marker.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The quarantine store is unavailable or fails.</exception>
    private async ValueTask QuarantinePayloadAsync(
        LocalPayloadQuarantineRequest request,
        PayloadSchemaException cause,
        CancellationToken cancellationToken)
    {
        if (_options.Dependencies.Store is not ILocalPayloadQuarantineStore quarantineStore)
        {
            _poisoned = true;
            throw new InvalidOperationException("The local store does not support payload quarantine.", cause);
        }

        _poisoned = true;
        try
        {
            _ = await quarantineStore.QuarantinePayloadAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _poisoned = true;
            throw new InvalidOperationException("The local payload quarantine marker could not be persisted.", exception);
        }
    }

    /// <summary>Creates the stable public failure for a quarantined stream.</summary>
    /// <param name="message">The message.</param>
    /// <param name="exception">The schema exception.</param>
    /// <returns>The stable failure.</returns>
    private InvalidOperationException CreateQuarantinedStreamException(string message, PayloadSchemaException exception)
    {
        _poisoned = true;
        return new(message, exception);
    }
}
