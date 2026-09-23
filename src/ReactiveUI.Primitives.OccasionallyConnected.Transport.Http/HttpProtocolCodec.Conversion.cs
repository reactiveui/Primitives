// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Encodes and decodes the bounded HTTP protocol DTOs.</summary>
internal sealed partial class HttpProtocolCodec
{
    /// <summary>Converts a wire operation policy into the domain policy after enum validation.</summary>
    /// <param name="dto">The decoded wire policy values.</param>
    /// <returns>A domain operation policy with declared enum values.</returns>
    /// <exception cref="HttpRemoteTransportException">A policy enum value is outside the declared contract.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OperationPolicy ToPolicy(HttpProtocolJsonContext.OperationPolicyWire dto) =>
        new(ToDeliveryGuarantee(dto.DeliveryGuarantee), ToDurability(dto.Durability), dto.Priority, ToConflictPolicy(dto.ConflictPolicy));

    /// <summary>Converts an event origin into its wire representation.</summary>
    /// <param name="origin">The domain event origin to write.</param>
    /// <returns>The wire event origin object.</returns>
    /// <exception cref="HttpRemoteTransportException">The origin contains protocol text that violates configured limits.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpProtocolJsonContext.RemoteEventOriginWire ToOriginDto(RemoteEventOrigin origin) =>
        new() { ClientId = origin.ClientId, OperationId = origin.OperationId.Value };

    /// <summary>Copies a remote operation completion into a wire DTO with bounded event identifiers.</summary>
    /// <param name="completion">The domain completion entry to write.</param>
    /// <returns>The wire completion object with copied event identifiers.</returns>
    /// <exception cref="HttpRemoteTransportException">The completion origin or event identifiers violate protocol limits.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpProtocolJsonContext.RemoteOperationCompletionWire ToCompletionDto(RemoteOperationCompletion completion)
    {
        var count = completion.EventIds.Count;
        var eventIds = new Guid[count];
        for (var index = 0; index < count; index++)
        {
            eventIds[index] = completion.EventIds[index];
        }

        return new() { Origin = ToOriginDto(completion.Origin), EventIds = eventIds };
    }

    /// <summary>Converts a push operation wire DTO into the domain operation model.</summary>
    /// <param name="dto">The decoded wire operation object.</param>
    /// <returns>A domain synchronization operation.</returns>
    /// <exception cref="HttpRemoteTransportException">The operation contains invalid enum values, payload, or metadata.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private SyncOperation ToOperation(HttpProtocolJsonContext.SyncOperationWire dto) =>
        new()
        {
            OperationId = new(dto.OperationId),
            StreamId = new(dto.StreamId),
            ClientSequence = dto.ClientSequence,
            TimestampUtc = dto.TimestampUtc,
            BaseVersion = dto.BaseVersion,
            Type = ToOperationType(dto.Type),
            Payload = ToPayload(dto.Payload),
            Policy = ToPolicy(dto.Policy),
            Metadata = HttpProtocolCodecHelper.ToDictionary(dto.Metadata),
        };

    /// <summary>Converts a domain push operation into its wire DTO.</summary>
    /// <param name="operation">The domain operation to write.</param>
    /// <returns>The wire push operation object.</returns>
    /// <exception cref="HttpRemoteTransportException">The operation cannot be represented within configured HTTP protocol limits.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    /// <summary>Converts a payload envelope into its base64 wire DTO.</summary>
    /// <param name="payload">The payload envelope to encode.</param>
    /// <returns>The wire payload envelope with base64 payload bytes.</returns>
    /// <exception cref="HttpRemoteTransportException">The payload metadata or byte length violates configured limits.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    /// <summary>Converts one operation synchronization result into its wire DTO.</summary>
    /// <param name="result">The domain operation result to write.</param>
    /// <returns>The wire operation result object.</returns>
    /// <exception cref="HttpRemoteTransportException">The result identifier, enum, reason, or version violates protocol limits.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private HttpProtocolJsonContext.OperationSyncResultWire ToDto(OperationSyncResult result)
    {
        ValidateOperationResult(result);
        return new() { OperationId = result.OperationId.Value, Kind = (int)result.Kind, ReasonCode = result.ReasonCode, ServerVersion = result.ServerVersion };
    }

    /// <summary>Converts a remote event batch into its ordered wire DTO.</summary>
    /// <param name="batch">The domain receive batch to write.</param>
    /// <returns>The wire receive batch object with ordered events and completions.</returns>
    /// <exception cref="HttpRemoteTransportException">The batch contents violate configured protocol limits.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private HttpProtocolJsonContext.RemoteEventBatchWire ToDto(RemoteEventBatch batch)
    {
        var eventCount = batch.Events.Count;
        var events = new HttpProtocolJsonContext.RemoteEventWire[eventCount];
        for (var index = 0; index < eventCount; index++)
        {
            events[index] = ToDto(batch.Events[index]);
        }

        var completionCount = batch.CompletedOperations.Count;
        var completions = new HttpProtocolJsonContext.RemoteOperationCompletionWire[completionCount];
        for (var index = 0; index < completionCount; index++)
        {
            completions[index] = ToCompletionDto(batch.CompletedOperations[index]);
        }

        return new()
        {
            BatchId = batch.BatchId,
            StreamId = batch.StreamId.Value,
            PreviousCursor = batch.PreviousCursor,
            NextCursor = batch.NextCursor,
            Events = events,
            CompletedOperations = completions,
        };
    }

    /// <summary>Converts a remote event into its wire DTO.</summary>
    /// <param name="remoteEvent">The domain event to write.</param>
    /// <returns>The wire event object with optional origin references.</returns>
    /// <exception cref="HttpRemoteTransportException">The event identity, cursor, payload, or metadata violates protocol limits.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private HttpProtocolJsonContext.RemoteEventWire ToDto(RemoteEvent remoteEvent)
    {
        ValidateRemoteEvent(remoteEvent);
        return new()
        {
            EventId = remoteEvent.EventId,
            StreamId = remoteEvent.StreamId.Value,
            ServerCursor = remoteEvent.ServerCursor,
            CommittedAtUtc = remoteEvent.CommittedAtUtc,
            CausedByOperationId = remoteEvent.CausedByOperationId?.Value,
            Origin = remoteEvent.Origin is null ? null : ToOriginDto(remoteEvent.Origin),
            Payload = ToDto(remoteEvent.Payload),
            Metadata = HttpProtocolCodecHelper.ToDictionary(remoteEvent.Metadata),
        };
    }

    /// <summary>Converts domain sync operations into wire DTOs.</summary>
    /// <param name="operations">The operations.</param>
    /// <returns>The wire DTOs.</returns>
    private HttpProtocolJsonContext.SyncOperationWire[] ToSyncOperationDtos(IReadOnlyList<SyncOperation> operations)
    {
        var count = operations.Count;
        var values = new HttpProtocolJsonContext.SyncOperationWire[count];
        for (var index = 0; index < count; index++)
        {
            values[index] = ToDto(operations[index]);
        }

        return values;
    }

    /// <summary>Converts wire sync operations into domain operations.</summary>
    /// <param name="dtos">The wire DTOs.</param>
    /// <returns>The domain operations.</returns>
    private SyncOperation[] ToOperations(HttpProtocolJsonContext.SyncOperationWire[] dtos)
    {
        var count = dtos.Length;
        var values = new SyncOperation[count];
        for (var index = 0; index < count; index++)
        {
            values[index] = ToOperation(dtos[index]);
        }

        return values;
    }

    /// <summary>Converts a remote event batch DTO into the domain batch model.</summary>
    /// <param name="dto">The decoded wire receive batch object.</param>
    /// <returns>A domain receive batch with ordered events and completions.</returns>
    /// <exception cref="HttpRemoteTransportException">The decoded batch cannot be represented by the domain model.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private RemoteEventBatch ToBatch(HttpProtocolJsonContext.RemoteEventBatchWire dto)
    {
        var eventCount = dto.Events.Length;
        var events = new RemoteEvent[eventCount];
        for (var index = 0; index < eventCount; index++)
        {
            events[index] = ToEvent(dto.Events[index]);
        }

        var completionCount = dto.CompletedOperations.Length;
        var completions = new RemoteOperationCompletion[completionCount];
        for (var index = 0; index < completionCount; index++)
        {
            completions[index] = HttpProtocolCodecHelper.ToCompletion(dto.CompletedOperations[index]);
        }

        return new(dto.BatchId, new(dto.StreamId), dto.PreviousCursor, dto.NextCursor, events) { CompletedOperations = completions };
    }

    /// <summary>Converts a remote event DTO into the domain event model.</summary>
    /// <param name="dto">The decoded wire event object.</param>
    /// <returns>A domain remote event.</returns>
    /// <exception cref="HttpRemoteTransportException">The decoded event cannot be represented by the domain model.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    /// <summary>Decodes a base64 payload DTO into a bounded payload envelope.</summary>
    /// <param name="dto">The decoded wire payload envelope.</param>
    /// <returns>A domain payload envelope with decoded payload bytes.</returns>
    /// <exception cref="HttpRemoteTransportException">The payload base64 text, metadata, or decoded length violates configured limits.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private PayloadEnvelope ToPayload(HttpProtocolJsonContext.PayloadEnvelopeWire dto)
    {
        ValidatePayloadText(dto);
        var maximumBase64Chars = checked(
            (((long)_limits.MaximumPayloadBytes + Base64RoundingBytes) / Base64BlockInputBytes) * Base64BlockOutputCharacters);
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
            throw CreateProtocolViolation(exception);
        }

        var envelope = new PayloadEnvelope(dto.ContractId, dto.SchemaVersion, dto.ContentType, payload, dto.PayloadHash);
        ValidatePayload(envelope);
        return envelope;
    }
}
