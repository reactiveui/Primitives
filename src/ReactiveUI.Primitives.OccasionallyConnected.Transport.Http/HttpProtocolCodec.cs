// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Encodes and decodes the bounded HTTP protocol DTOs.</summary>
internal sealed class HttpProtocolCodec
{
    /// <summary>The base64 block input byte count.</summary>
    private const int Base64BlockInputBytes = 3;

    /// <summary>The base64 block output character count.</summary>
    private const int Base64BlockOutputCharacters = 4;

    /// <summary>The base64 rounding byte count.</summary>
    private const int Base64RoundingBytes = 2;

    /// <summary>The fixed per-operation estimate used before exact bounded JSON writing.</summary>
    private const long OperationEstimateOverheadBytes = 256;

    /// <summary>The adapter options.</summary>
    private readonly HttpRemoteTransportOptions _options;

    /// <summary>Initializes a new instance of the <see cref="HttpProtocolCodec"/> class.</summary>
    /// <param name="options">The adapter options.</param>
    internal HttpProtocolCodec(HttpRemoteTransportOptions options) => _options = options;

    /// <summary>Serializes a connect request.</summary>
    /// <param name="request">The connect request.</param>
    /// <returns>The request bytes.</returns>
    internal byte[] SerializeConnectRequest(TransportConnectRequest request)
    {
        var guarantees = new int[request.RequiredGuarantees.Count];
        var index = 0;
        foreach (var guarantee in request.RequiredGuarantees)
        {
            guarantees[index] = (int)guarantee;
            index++;
        }

        HttpProtocolJsonContext.ConnectRequestWire dto = new()
        {
            MinimumProtocolVersion = request.SupportedProtocolVersions.Minimum.ToString(),
            MaximumProtocolVersion = request.SupportedProtocolVersions.Maximum.ToString(),
            ClientId = request.Client.ClientId,
            TenantHint = request.Client.TenantHint,
            RequiredGuarantees = guarantees,
        };
        return Serialize(dto, HttpProtocolJsonContext.Default.ConnectRequestWireInfo);
    }

    /// <summary>Deserializes a connect response.</summary>
    /// <param name="bytes">The response bytes.</param>
    /// <returns>The negotiated capabilities.</returns>
    /// <exception cref="HttpRemoteTransportException">The response is malformed or violates protocol bounds.</exception>
    internal NegotiatedCapabilities DeserializeConnectResponse(byte[] bytes)
    {
        var dto = Deserialize(bytes, HttpProtocolJsonContext.Default.ConnectResponseWireInfo);
        if (!Version.TryParse(dto.ProtocolVersion, out var version))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        var features = (RemoteTransportCapabilities)dto.Features;
        return new(
            version,
            features,
            dto.MaximumBatchOperations,
            dto.MaximumBatchBytes,
            HttpProtocolCodecHelper.ToTimeSpan(dto.ServerIdempotencyRetentionMilliseconds),
            HttpProtocolCodecHelper.ToTimeSpan(dto.ClientInboxRetentionRequiredMilliseconds));
    }

    /// <summary>Serializes a push request.</summary>
    /// <param name="batch">The synchronization batch.</param>
    /// <returns>The request bytes.</returns>
    /// <exception cref="HttpRemoteTransportException">The batch cannot be encoded within configured bounds.</exception>
    internal byte[] SerializePushRequest(SyncBatch batch)
    {
        if (batch.Operations.Count > _options.MaximumBatchOperations)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        SyncBatchValidator.Validate(batch, CreateLocalAcceptedResult(batch));
        var operations = new HttpProtocolJsonContext.SyncOperationWire[batch.Operations.Count];
        long requestBudget = 0;
        for (var index = 0; index < batch.Operations.Count; index++)
        {
            var operation = batch.Operations[index];
            requestBudget = checked(requestBudget + EstimateOperationBytes(operation));
            if (requestBudget > _options.MaximumRequestBytes)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
            }

            operations[index] = ToDto(operation);
        }

        HttpProtocolJsonContext.PushRequestWire request = new() { BatchId = batch.BatchId, Operations = operations };
        return Serialize(request, HttpProtocolJsonContext.Default.PushRequestWireInfo);
    }

    /// <summary>Deserializes a push response and validates it against the pushed batch.</summary>
    /// <param name="batch">The pushed batch.</param>
    /// <param name="bytes">The response bytes.</param>
    /// <param name="retryAfter">The HTTP retry hint.</param>
    /// <returns>The remote synchronization result.</returns>
    /// <exception cref="HttpRemoteTransportException">The response is malformed or does not match the pushed batch.</exception>
    internal RemoteSyncResult DeserializePushResponse(SyncBatch batch, byte[] bytes, TimeSpan? retryAfter)
    {
        var dto = Deserialize(bytes, HttpProtocolJsonContext.Default.PushResponseWireInfo);
        if (dto.Operations.Length > _options.MaximumBatchOperations)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        var operations = new OperationSyncResult[dto.Operations.Length];
        for (var index = 0; index < dto.Operations.Length; index++)
        {
            operations[index] = HttpProtocolCodecHelper.ToOperationResult(dto.Operations[index]);
        }

        RemoteSyncResult result = new(dto.BatchId, operations, dto.ServerCursor, retryAfter);
        SyncBatchValidator.Validate(batch, result);
        return result;
    }

    /// <summary>Serializes an acknowledgement.</summary>
    /// <param name="acknowledgement">The acknowledgement.</param>
    /// <returns>The request bytes.</returns>
    internal byte[] SerializeAcknowledgement(ReceiveAcknowledgement acknowledgement)
    {
        var dto = new HttpProtocolJsonContext.AcknowledgeRequestWire
        {
            SubscriptionId = acknowledgement.SubscriptionId.Value,
            StreamId = acknowledgement.StreamId.Value,
            Cursor = acknowledgement.Cursor,
        };
        return Serialize(dto, HttpProtocolJsonContext.Default.AcknowledgeRequestWireInfo);
    }

    /// <summary>Deserializes a subscribe response into complete batches.</summary>
    /// <param name="bytes">The response bytes.</param>
    /// <param name="currentCursor">The caller's current receive cursor, when one has been durably applied.</param>
    /// <returns>The remote event batches.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal RemoteEventBatch[] DeserializeSubscribeResponse(byte[] bytes, string? currentCursor = null) =>
        DeserializeSubscribeResponse(bytes, expectedStreamId: null, currentCursor);

    /// <summary>Deserializes a subscribe response into complete batches for the requested stream.</summary>
    /// <param name="bytes">The response bytes.</param>
    /// <param name="expectedStreamId">The requested stream identifier, when the caller binds the response to a stream.</param>
    /// <param name="currentCursor">The caller's current receive cursor, when one has been durably applied.</param>
    /// <returns>The remote event batches.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal RemoteEventBatch[] DeserializeSubscribeResponse(byte[] bytes, StreamId? expectedStreamId, string? currentCursor = null)
    {
        if (bytes.Length == 0)
        {
            return [];
        }

        var dto = Deserialize(bytes, HttpProtocolJsonContext.Default.SubscribeResponseWireInfo);
        var batches = new RemoteEventBatch[dto.Batches.Length];
        for (var index = 0; index < dto.Batches.Length; index++)
        {
            batches[index] = ToBatch(dto.Batches[index]);
            ValidateStream(batches[index], expectedStreamId);
            RemoteEventBatchValidator.Validate(
                batches[index],
                _options.MaximumEventsPerBatch,
                _options.MaximumCompletedOperationsPerBatch);
            ValidateCursorContinuity(batches[index], ref currentCursor);
        }

        return batches;
    }

    /// <summary>Validates that a received batch belongs to the requested stream.</summary>
    /// <param name="batch">The received batch.</param>
    /// <param name="expectedStreamId">The expected stream, when stream binding is required.</param>
    /// <exception cref="HttpRemoteTransportException">The response includes a foreign stream.</exception>
    private static void ValidateStream(RemoteEventBatch batch, StreamId? expectedStreamId)
    {
        if (expectedStreamId is null)
        {
            return;
        }

        var expectedValue = expectedStreamId.Value.Value;
        if (!StringComparer.Ordinal.Equals(batch.StreamId.Value, expectedValue))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        for (var index = 0; index < batch.Events.Count; index++)
        {
            if (!StringComparer.Ordinal.Equals(batch.Events[index].StreamId.Value, expectedValue))
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            }
        }
    }

    /// <summary>Validates receive cursor continuity while allowing immediate duplicate candidates after a lost ACK.</summary>
    /// <param name="batch">The received batch.</param>
    /// <param name="currentCursor">The current receive cursor.</param>
    /// <exception cref="HttpRemoteTransportException">A new batch skipped the current receive cursor.</exception>
    private static void ValidateCursorContinuity(RemoteEventBatch batch, ref string? currentCursor)
    {
        if (currentCursor is null)
        {
            currentCursor = batch.NextCursor;
            return;
        }

        if (StringComparer.Ordinal.Equals(batch.NextCursor, currentCursor))
        {
            return;
        }

        if (!StringComparer.Ordinal.Equals(batch.PreviousCursor, currentCursor))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        currentCursor = batch.NextCursor;
    }

    /// <summary>Serializes a DTO and enforces the request byte bound.</summary>
    /// <typeparam name="T">The DTO type.</typeparam>
    /// <param name="dto">The DTO.</param>
    /// <param name="typeInfo">The generated type metadata.</param>
    /// <returns>The serialized bytes.</returns>
    /// <exception cref="HttpRemoteTransportException">The DTO cannot be encoded within configured bounds.</exception>
    private byte[] Serialize<T>(T dto, JsonTypeInfo<T> typeInfo)
    {
        HttpBoundedBufferWriter bufferWriter = new(_options.MaximumRequestBytes);
        using Utf8JsonWriter jsonWriter = new(bufferWriter);
        JsonSerializer.Serialize(jsonWriter, dto, typeInfo);
        jsonWriter.Flush();

        return bufferWriter.ToArray();
    }

    /// <summary>Deserializes a DTO after depth validation.</summary>
    /// <typeparam name="T">The DTO type.</typeparam>
    /// <param name="bytes">The serialized bytes.</param>
    /// <param name="typeInfo">The generated type metadata.</param>
    /// <returns>The DTO.</returns>
    /// <exception cref="HttpRemoteTransportException">The response is malformed or violates protocol bounds.</exception>
    private T Deserialize<T>(byte[] bytes, JsonTypeInfo<T> typeInfo)
    {
        ValidateJsonDepth(bytes);
        try
        {
            var dto = JsonSerializer.Deserialize(bytes, typeInfo);
            if (dto is not null)
            {
                return dto;
            }
        }
        catch (JsonException exception)
        {
            throw new HttpRemoteTransportException(
                HttpTransportFailureKind.ProtocolViolation,
                statusCode: null,
                retryAfter: null,
                innerException: exception);
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Validates JSON nesting depth before DTO materialization.</summary>
    /// <param name="bytes">The serialized JSON bytes.</param>
    /// <exception cref="HttpRemoteTransportException">The JSON exceeds configured depth limits.</exception>
    private void ValidateJsonDepth(byte[] bytes)
    {
        var reader = new Utf8JsonReader(bytes, new JsonReaderOptions { MaxDepth = _options.MaximumJsonDepth });
        try
        {
            var tokenCount = 0;
            while (reader.Read())
            {
                tokenCount++;
            }
        }
        catch (JsonException exception)
        {
            throw new HttpRemoteTransportException(
                HttpTransportFailureKind.ProtocolViolation,
                statusCode: null,
                retryAfter: null,
                innerException: exception);
        }
    }

    /// <summary>Creates a local accepted result for pre-validating the pushed batch shape.</summary>
    /// <param name="batch">The batch.</param>
    /// <returns>The local validation result.</returns>
    private RemoteSyncResult CreateLocalAcceptedResult(SyncBatch batch)
    {
        var operations = new OperationSyncResult[batch.Operations.Count];
        for (var index = 0; index < batch.Operations.Count; index++)
        {
            operations[index] = new(batch.Operations[index].OperationId, OperationResultKind.Accepted, null, null);
        }

        return new(batch.BatchId, operations, serverCursor: null, retryAfter: null);
    }

    /// <summary>Converts an operation to its HTTP DTO.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The operation DTO.</returns>
    private HttpProtocolJsonContext.SyncOperationWire ToDto(SyncOperation operation)
    {
        ValidateMetadata(operation.Metadata);
        HttpProtocolJsonContext.SyncOperationWire dto = new()
        {
            OperationId = operation.OperationId.Value,
            StreamId = operation.StreamId.Value,
            ClientSequence = operation.ClientSequence,
            TimestampUtc = operation.TimestampUtc,
            BaseVersion = operation.BaseVersion,
            Type = (int)operation.Type,
            Payload = ToDto(operation.Payload),
            Policy = new()
            {
                DeliveryGuarantee = (int)operation.Policy.DeliveryGuarantee,
                Durability = (int)operation.Policy.Durability,
                Priority = operation.Policy.Priority,
                ConflictPolicy = (int)operation.Policy.ConflictPolicy,
            },
            Metadata = HttpProtocolCodecHelper.ToDictionary(operation.Metadata),
        };
        return dto;
    }

    /// <summary>Converts a payload envelope to its HTTP DTO.</summary>
    /// <param name="payload">The payload.</param>
    /// <returns>The payload DTO.</returns>
    private HttpProtocolJsonContext.PayloadEnvelopeWire ToDto(PayloadEnvelope payload)
    {
        ValidatePayload(payload);
        HttpProtocolJsonContext.PayloadEnvelopeWire dto = new()
        {
            ContractId = payload.ContractId,
            SchemaVersion = payload.SchemaVersion,
            ContentType = payload.ContentType,
            Payload = Convert.ToBase64String(payload.Payload.ToArray()),
            PayloadHash = payload.PayloadHash,
        };
        return dto;
    }

    /// <summary>Converts a remote event batch DTO.</summary>
    /// <param name="dto">The DTO.</param>
    /// <returns>The remote event batch.</returns>
    private RemoteEventBatch ToBatch(HttpProtocolJsonContext.RemoteEventBatchWire dto)
    {
        var events = new RemoteEvent[dto.Events.Length];
        for (var index = 0; index < dto.Events.Length; index++)
        {
            events[index] = ToEvent(dto.Events[index]);
        }

        var completions = new RemoteOperationCompletion[dto.CompletedOperations.Length];
        for (var index = 0; index < dto.CompletedOperations.Length; index++)
        {
            completions[index] = HttpProtocolCodecHelper.ToCompletion(dto.CompletedOperations[index]);
        }

        return new(dto.BatchId, new(dto.StreamId), dto.PreviousCursor, dto.NextCursor, events) { CompletedOperations = completions };
    }

    /// <summary>Converts a remote event DTO.</summary>
    /// <param name="dto">The DTO.</param>
    /// <returns>The remote event.</returns>
    private RemoteEvent ToEvent(HttpProtocolJsonContext.RemoteEventWire dto)
    {
        ValidateMetadata(dto.Metadata);
        var operationId = dto.CausedByOperationId.HasValue
            ? new OperationId(dto.CausedByOperationId.Value)
            : (OperationId?)null;
        return new(dto.EventId, new(dto.StreamId), dto.ServerCursor, dto.CommittedAtUtc, operationId, ToPayload(dto.Payload), HttpProtocolCodecHelper.ToDictionary(dto.Metadata))
        {
            Origin = dto.Origin is null ? null : HttpProtocolCodecHelper.ToOrigin(dto.Origin),
        };
    }

    /// <summary>Converts a payload DTO.</summary>
    /// <param name="dto">The DTO.</param>
    /// <returns>The payload envelope.</returns>
    /// <exception cref="HttpRemoteTransportException">The payload is malformed or violates configured limits.</exception>
    private PayloadEnvelope ToPayload(HttpProtocolJsonContext.PayloadEnvelopeWire dto)
    {
        var maximumBase64Chars = checked(
            ((_options.MaximumPayloadBytes + Base64RoundingBytes) / Base64BlockInputBytes) * Base64BlockOutputCharacters);
        if (dto.Payload.Length > maximumBase64Chars)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        byte[] payload;
        try
        {
            payload = Convert.FromBase64String(dto.Payload);
        }
        catch (FormatException exception)
        {
            throw new HttpRemoteTransportException(
                HttpTransportFailureKind.ProtocolViolation,
                statusCode: null,
                retryAfter: null,
                innerException: exception);
        }

        var envelope = new PayloadEnvelope(dto.ContractId, dto.SchemaVersion, dto.ContentType, payload, dto.PayloadHash);
        ValidatePayload(envelope);
        return envelope;
    }

    /// <summary>Validates payload byte limits.</summary>
    /// <param name="payload">The payload envelope.</param>
    /// <exception cref="HttpRemoteTransportException">The payload exceeds configured limits.</exception>
    private void ValidatePayload(PayloadEnvelope payload)
    {
        if (payload.PayloadLength <= _options.MaximumPayloadBytes)
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Validates metadata count and UTF-8 byte limits.</summary>
    /// <param name="metadata">The metadata.</param>
    /// <exception cref="HttpRemoteTransportException">The metadata is malformed or violates configured limits.</exception>
    private void ValidateMetadata(IReadOnlyDictionary<string, string> metadata)
    {
        if (metadata.Count > _options.MaximumMetadataEntries)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        foreach (var pair in metadata)
        {
            if (pair.Value is null)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            }

            if (Encoding.UTF8.GetByteCount(pair.Key) > _options.MaximumMetadataKeyBytes
                || Encoding.UTF8.GetByteCount(pair.Value) > _options.MaximumMetadataValueBytes)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
            }
        }
    }

    /// <summary>Estimates encoded operation size before DTO payload conversion.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The conservative encoded byte estimate.</returns>
    /// <exception cref="HttpRemoteTransportException">The operation payload or metadata exceeds configured limits.</exception>
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
