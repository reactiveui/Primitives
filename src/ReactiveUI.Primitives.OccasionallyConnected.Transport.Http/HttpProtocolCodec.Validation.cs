// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Encodes and decodes the bounded HTTP protocol DTOs.</summary>
internal sealed partial class HttpProtocolCodec
{
    /// <summary>Creates a synchronization batch validation exception.</summary>
    /// <param name="error">The validation error.</param>
    /// <param name="message">The validation message.</param>
    /// <returns>The exception.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncBatchValidationException CreateSyncBatchValidationException(SyncBatchValidationError error, string message) => new(error, message);

    /// <summary>Enforces one-stream batches with unique operation identifiers and ascending client sequences.</summary>
    /// <param name="operation">The operation being checked.</param>
    /// <param name="operationIds">The operation identifiers already seen in the batch.</param>
    /// <param name="clientSequences">The client sequences already seen in the batch.</param>
    /// <param name="streamId">The stream established by the first operation in the batch.</param>
    /// <param name="previousSequence">The previous client sequence used to enforce ascending order.</param>
    /// <exception cref="HttpRemoteTransportException">The operation changes stream, duplicates identity, duplicates sequence, or moves backward.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateBatchOperationOrder(
        SyncOperation operation,
        HashSet<OperationId> operationIds,
        HashSet<long> clientSequences,
        ref StreamId? streamId,
        ref long previousSequence)
    {
        if (streamId is not null && streamId.Value != operation.StreamId)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        streamId ??= operation.StreamId;
        if (!operationIds.Add(operation.OperationId)
            || !clientSequences.Add(operation.ClientSequence)
            || operation.ClientSequence < previousSequence)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        previousSequence = operation.ClientSequence;
    }

    /// <summary>Validates negotiated transport capabilities before sending or accepting a connect response.</summary>
    /// <param name="capabilities">Capability set advertised by the peer during connection negotiation.</param>
    /// <exception cref="HttpRemoteTransportException">The capability set advertises an unsupported protocol version, flag, or limit.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateCapabilities(NegotiatedCapabilities capabilities)
    {
        ArgumentExceptionHelper.ThrowIfNull(capabilities);
        ArgumentExceptionHelper.ThrowIfNull(capabilities.ProtocolVersion);
        if (capabilities.ProtocolVersion.Major != SupportedProtocolMajor
            || (capabilities.Features & ~KnownFeatures) != 0
            || capabilities.MaximumBatchOperations <= 0
            || capabilities.MaximumBatchBytes <= 0)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        ValidateRetention(capabilities.ServerIdempotencyRetention);
        ValidateRetention(capabilities.ClientInboxRetentionRequired);
        ValidateRetention(capabilities.EffectiveExactlyOnceWindow);
    }

    /// <summary>Rejects negative retention windows while allowing absent values.</summary>
    /// <param name="retention">The optional retention window.</param>
    /// <exception cref="HttpRemoteTransportException">The retention window is negative.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateRetention(TimeSpan? retention)
    {
        if (!retention.HasValue || retention.Value >= TimeSpan.Zero)
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Converts an optional retention window into protocol milliseconds.</summary>
    /// <param name="retention">The optional retention window.</param>
    /// <returns>The retention window in milliseconds, or <see langword="null"/> when absent.</returns>
    /// <exception cref="HttpRemoteTransportException">The retention window is negative or cannot be represented as milliseconds.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long? ToMilliseconds(TimeSpan? retention)
    {
        if (!retention.HasValue)
        {
            return null;
        }

        ValidateRetention(retention);
        return checked((long)retention.Value.TotalMilliseconds);
    }

    /// <summary>Validates a connect request before it is serialized or returned from decoding.</summary>
    /// <param name="request">Client connect request entering or leaving the HTTP wire contract.</param>
    /// <exception cref="HttpRemoteTransportException">The request contains an invalid version range, client text, or guarantee set.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateConnectRequest(TransportConnectRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ArgumentExceptionHelper.ThrowIfNull(request.SupportedProtocolVersions);
        ArgumentExceptionHelper.ThrowIfNull(request.Client);
        ArgumentExceptionHelper.ThrowIfNull(request.RequiredGuarantees);
        if (request.SupportedProtocolVersions.Minimum.CompareTo(request.SupportedProtocolVersions.Maximum) > 0)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        ValidateProtocolString(request.Client.ClientId);
        ValidateOptionalProtocolString(request.Client.TenantHint);
        if (request.RequiredGuarantees.Count > _limits.MaximumBatchOperations)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        foreach (var guarantee in request.RequiredGuarantees)
        {
            _ = ToDeliveryGuarantee((int)guarantee);
        }
    }

    /// <summary>Validates a push batch shape, operation count, and per-operation invariants.</summary>
    /// <param name="batch">Client operation batch entering or leaving the push request body.</param>
    /// <exception cref="HttpRemoteTransportException">The batch has an empty identifier, no operations, too many operations, or invalid operation ordering.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateBatch(SyncBatch batch)
    {
        ArgumentExceptionHelper.ThrowIfNull(batch);
        var count = batch.Operations.Count;
        if (batch.BatchId == Guid.Empty || count == 0)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        if (count > _limits.MaximumBatchOperations)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        HashSet<OperationId> operationIds = [];
        HashSet<long> clientSequences = [];
        StreamId? streamId = null;
        var previousSequence = 0L;
        for (var index = 0; index < count; index++)
        {
            var operation = batch.Operations[index];
            ValidateOperation(operation);
            ValidateBatchOperationOrder(operation, operationIds, clientSequences, ref streamId, ref previousSequence);
        }
    }

    /// <summary>Validates one pushed operation before wire conversion or batch acceptance.</summary>
    /// <param name="operation">Client mutation carried inside a push batch.</param>
    /// <exception cref="HttpRemoteTransportException">The operation contains invalid identity, sequence, stream, enum, policy, payload, or metadata values.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateOperation(SyncOperation operation)
    {
        if (operation is null
            || operation.OperationId.Value == Guid.Empty
            || operation.StreamId.Value is null
            || operation.ClientSequence <= 0
            || operation.Payload is null
            || operation.Policy is null)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        ValidateProtocolString(operation.StreamId.Value);
        _ = ToOperationType((int)operation.Type);
        TranslateProtocolExceptions(operation.Policy.Validate);

        ValidateOptionalProtocolString(operation.BaseVersion);
        ValidatePayload(operation.Payload);
        ValidateMetadata(operation.Metadata);
    }

    /// <summary>Validates a push result against the original batch before response serialization.</summary>
    /// <param name="batch">The original pushed batch.</param>
    /// <param name="result">The result returned for that batch.</param>
    /// <exception cref="HttpRemoteTransportException">The result exceeds configured limits or contains invalid protocol values.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateResult(SyncBatch batch, RemoteSyncResult result)
    {
        ArgumentExceptionHelper.ThrowIfNull(result);
        ValidateBatch(batch);
        if (result.BatchId != batch.BatchId)
        {
            throw CreateSyncBatchValidationException(
                SyncBatchValidationError.MismatchingBatchId,
                "The synchronization result batch identifier does not match the pushed batch.");
        }

        if (result.RetryAfter < TimeSpan.Zero)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        var count = result.Operations.Count;
        if (count > _limits.MaximumBatchOperations)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        HashSet<OperationId> expected = [];
        for (var index = 0; index < batch.Operations.Count; index++)
        {
            _ = expected.Add(batch.Operations[index].OperationId);
        }

        HashSet<OperationId> seen = [];
        for (var index = 0; index < count; index++)
        {
            var operation = result.Operations[index];
            ValidateOperationResult(operation);
            if (!seen.Add(operation.OperationId))
            {
                throw CreateSyncBatchValidationException(
                    SyncBatchValidationError.DuplicateOperationResult,
                    "The synchronization result contains a duplicate operation result.");
            }

            if (!expected.Contains(operation.OperationId))
            {
                throw CreateSyncBatchValidationException(
                    SyncBatchValidationError.UnknownOperationResult,
                    "The synchronization result contains an unknown operation result.");
            }
        }

        if (seen.Count != expected.Count)
        {
            throw CreateSyncBatchValidationException(
                SyncBatchValidationError.OmittedOperationResult,
                "The synchronization result omitted one or more operation results.");
        }

        ValidateOptionalProtocolString(result.ServerCursor);
    }

    /// <summary>Validates one operation result and its optional protocol strings.</summary>
    /// <param name="result">Result returned for one pushed operation.</param>
    /// <exception cref="HttpRemoteTransportException">The result contains an empty operation identifier, unknown kind, or oversized protocol text.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateOperationResult(OperationSyncResult result)
    {
        if (result is null || result.OperationId.Value == Guid.Empty)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        _ = ToOperationResultKind((int)result.Kind);
        ValidateOptionalProtocolString(result.ReasonCode);
        ValidateOptionalProtocolString(result.ServerVersion);
    }

    /// <summary>Validates a receive acknowledgement request before serialization or after decoding.</summary>
    /// <param name="acknowledgement">Client acknowledgement for a receive subscription cursor.</param>
    /// <exception cref="HttpRemoteTransportException">The acknowledgement contains an empty subscription, invalid stream, or invalid cursor.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateAcknowledgement(ReceiveAcknowledgement acknowledgement)
    {
        if (acknowledgement is null || acknowledgement.SubscriptionId.Value == Guid.Empty)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        ValidateProtocolString(acknowledgement.StreamId.Value);
        _ = new StreamId(acknowledgement.StreamId.Value);
        ValidateProtocolString(acknowledgement.Cursor);
    }

    /// <summary>Validates a parsed subscribe request before returning it to server code.</summary>
    /// <param name="request">Subscribe query parsed from the server receive endpoint URL.</param>
    /// <exception cref="HttpRemoteTransportException">The request contains an empty subscription, invalid stream, invalid cursor, or invalid start cursor.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateSubscribeRequest(RemoteSubscribeRequest request)
    {
        if (request.SubscriptionId.Value == Guid.Empty)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        ValidateProtocolString(request.StreamId.Value);
        ValidateOptionalProtocolString(request.Cursor);
        ValidateStartPosition(request.InitialPosition);
    }

    /// <summary>Validates the subscribe start position and any required cursor text.</summary>
    /// <param name="position">Initial subscription position selected by the subscribe query.</param>
    /// <exception cref="HttpRemoteTransportException">The cursor-based start position contains invalid cursor text.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateStartPosition(StartPosition position)
    {
        if (position.Kind != StartPositionKind.FromCursor)
        {
            return;
        }

        ValidateProtocolString(position.Cursor);
    }

    /// <summary>Validates a receive batch and its completion references before response serialization.</summary>
    /// <param name="batch">Server event batch prepared for a subscribe response body.</param>
    /// <exception cref="HttpRemoteTransportException">The batch contains invalid stream, cursor, event, completion, or aggregate count values.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateReceiveBatch(RemoteEventBatch batch)
    {
        if (batch is null)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        if (batch.Events.Count > _limits.MaximumEventsPerBatch
            || batch.CompletedOperations.Count > _limits.MaximumCompletedOperationsPerBatch)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        var completionEventIds = 0;
        for (var index = 0; index < batch.CompletedOperations.Count; index++)
        {
            var completion = batch.CompletedOperations[index]
                ?? throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            completionEventIds = checked(completionEventIds + completion.EventIds.Count);
            if (completionEventIds > _limits.MaximumEventsPerBatch)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
            }
        }

        for (var index = 0; index < batch.Events.Count; index++)
        {
            ValidateRemoteEvent(batch.Events[index]);
        }

        TranslateProtocolExceptions(() => RemoteEventBatchValidator.Validate(batch, _limits.MaximumEventsPerBatch, _limits.MaximumCompletedOperationsPerBatch));

        ValidateProtocolString(batch.StreamId.Value);
        ValidateOptionalProtocolString(batch.PreviousCursor);
        ValidateProtocolString(batch.NextCursor);
        for (var index = 0; index < batch.CompletedOperations.Count; index++)
        {
            var completion = batch.CompletedOperations[index];
            ValidateProtocolString(completion.Origin.ClientId);
        }
    }

    /// <summary>Validates one received event before DTO conversion.</summary>
    /// <param name="remoteEvent">Server event carried in a receive batch.</param>
    /// <exception cref="HttpRemoteTransportException">The event contains invalid identity, stream, cursor, origin, payload, or metadata values.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateRemoteEvent(RemoteEvent remoteEvent)
    {
        if (remoteEvent is null || remoteEvent.EventId == Guid.Empty)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        ValidateProtocolString(remoteEvent.StreamId.Value);
        ValidateProtocolString(remoteEvent.ServerCursor);
        if (remoteEvent.Origin is not null)
        {
            ValidateProtocolString(remoteEvent.Origin.ClientId);
        }

        ValidatePayload(remoteEvent.Payload);
        ValidateMetadata(remoteEvent.Metadata);
    }

    /// <summary>Validates payload envelope metadata and decoded payload length.</summary>
    /// <param name="payload">Domain payload envelope before HTTP DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The payload has invalid metadata or exceeds the configured decoded byte limit.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidatePayload(PayloadEnvelope payload)
    {
        if (payload.SchemaVersion <= 0)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        ValidateProtocolString(payload.ContractId);
        ValidateProtocolString(payload.ContentType);
        ValidateProtocolString(payload.PayloadHash);
        if (payload.PayloadLength <= _limits.MaximumPayloadBytes)
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Validates payload envelope text fields before base64 decoding.</summary>
    /// <param name="payload">Wire payload envelope before base64 payload decoding.</param>
    /// <exception cref="HttpRemoteTransportException">The payload text fields or schema version violate the protocol contract.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidatePayloadText(HttpProtocolJsonContext.PayloadEnvelopeWire payload)
    {
        ValidateProtocolString(payload.ContractId);
        ValidateProtocolString(payload.ContentType);
        ValidateProtocolString(payload.PayloadHash);
        if (payload.SchemaVersion > 0)
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Validates metadata count and strict UTF-8 key/value byte limits.</summary>
    /// <param name="metadata">Operation or event metadata entries carried on the wire.</param>
    /// <exception cref="HttpRemoteTransportException">The metadata contains null values or exceeds configured entry, key, or value limits.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateMetadata(IReadOnlyDictionary<string, string> metadata)
    {
        ArgumentExceptionHelper.ThrowIfNull(metadata);
        if (metadata.Count > _limits.MaximumMetadataEntries)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        foreach (var pair in metadata)
        {
            if (pair.Value is null)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            }

            ValidateProtocolString(pair.Key, _limits.MaximumMetadataKeyBytes);
            ValidateProtocolString(pair.Value, _limits.MaximumMetadataValueBytes);
        }
    }

    /// <summary>Estimates encoded operation size before JSON serialization allocates the final body.</summary>
    /// <param name="operation">The operation whose wire size is being estimated.</param>
    /// <returns>The estimated JSON byte count for the operation.</returns>
    /// <exception cref="HttpRemoteTransportException">The operation payload or metadata is invalid before size estimation.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long EstimateOperationBytes(SyncOperation operation)
    {
        ValidatePayload(operation.Payload);
        ValidateMetadata(operation.Metadata);
        checked
        {
            var payloadBase64Characters =
                ((operation.Payload.PayloadLength + Base64RoundingBytes) / Base64BlockInputBytes) * Base64BlockOutputCharacters;
            var size = OperationEstimateOverheadBytes + payloadBase64Characters;
            size += HttpProtocolCodecHelper.EstimateJsonStringBytes(operation.StreamId.Value);
            size += HttpProtocolCodecHelper.EstimateOptionalJsonStringBytes(operation.BaseVersion);
            size += HttpProtocolCodecHelper.EstimateJsonStringBytes(operation.Payload.ContractId);
            size += HttpProtocolCodecHelper.EstimateJsonStringBytes(operation.Payload.ContentType);
            size += HttpProtocolCodecHelper.EstimateJsonStringBytes(operation.Payload.PayloadHash);
            foreach (var pair in operation.Metadata)
            {
                size += HttpProtocolCodecHelper.EstimateJsonStringBytes(pair.Key);
                size += HttpProtocolCodecHelper.EstimateJsonStringBytes(pair.Value);
            }

            return size;
        }
    }
}
