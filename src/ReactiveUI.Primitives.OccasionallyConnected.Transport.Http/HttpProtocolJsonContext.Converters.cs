// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Authored converter-backed metadata for the HTTP protocol DTO allowlist.</summary>
internal sealed partial class HttpProtocolJsonContext
{
    /// <summary>The acknowledgement cursor JSON property name.</summary>
    private const string AcknowledgeCursorPropertyName = "cursor";

    /// <summary>The batch identifier JSON property name.</summary>
    private const string BatchIdPropertyName = "batchId";

    /// <summary>The batches JSON property name.</summary>
    private const string BatchesPropertyName = "batches";

    /// <summary>The base version JSON property name.</summary>
    private const string BaseVersionPropertyName = "baseVersion";

    /// <summary>The causing operation identifier JSON property name.</summary>
    private const string CausedByOperationIdPropertyName = "causedByOperationId";

    /// <summary>The client identifier JSON property name.</summary>
    private const string ClientIdPropertyName = "clientId";

    /// <summary>The required client inbox retention JSON property name.</summary>
    private const string ClientInboxRetentionRequiredMillisecondsPropertyName = "clientInboxRetentionRequiredMilliseconds";

    /// <summary>The client sequence JSON property name.</summary>
    private const string ClientSequencePropertyName = "clientSequence";

    /// <summary>The committed timestamp JSON property name.</summary>
    private const string CommittedAtUtcPropertyName = "committedAtUtc";

    /// <summary>The completed operations JSON property name.</summary>
    private const string CompletedOperationsPropertyName = "completedOperations";

    /// <summary>The conflict policy JSON property name.</summary>
    private const string ConflictPolicyPropertyName = "conflictPolicy";

    /// <summary>The content type JSON property name.</summary>
    private const string ContentTypePropertyName = "contentType";

    /// <summary>The contract identifier JSON property name.</summary>
    private const string ContractIdPropertyName = "contractId";

    /// <summary>The delivery guarantee JSON property name.</summary>
    private const string DeliveryGuaranteePropertyName = "deliveryGuarantee";

    /// <summary>The durability JSON property name.</summary>
    private const string DurabilityPropertyName = "durability";

    /// <summary>The duplicate property JSON exception message.</summary>
    private const string DuplicatePropertyMessage = "Duplicate JSON property name.";

    /// <summary>The event identifier JSON property name.</summary>
    private const string EventIdPropertyName = "eventId";

    /// <summary>The event identifiers JSON property name.</summary>
    private const string EventIdsPropertyName = "eventIds";

    /// <summary>The events JSON property name.</summary>
    private const string EventsPropertyName = "events";

    /// <summary>The expected array JSON exception message.</summary>
    private const string ExpectedArrayMessage = "Expected a JSON array.";

    /// <summary>The expected number JSON exception message.</summary>
    private const string ExpectedNumberMessage = "Expected a JSON number.";

    /// <summary>The expected object JSON exception message.</summary>
    private const string ExpectedObjectMessage = "Expected a JSON object.";

    /// <summary>The expected string JSON exception message.</summary>
    private const string ExpectedStringMessage = "Expected a JSON string.";

    /// <summary>The features JSON property name.</summary>
    private const string FeaturesPropertyName = "features";

    /// <summary>The operation result kind JSON property name.</summary>
    private const string KindPropertyName = "kind";

    /// <summary>The maximum batch bytes JSON property name.</summary>
    private const string MaximumBatchBytesPropertyName = "maximumBatchBytes";

    /// <summary>The maximum batch operations JSON property name.</summary>
    private const string MaximumBatchOperationsPropertyName = "maximumBatchOperations";

    /// <summary>The maximum protocol version JSON property name.</summary>
    private const string MaximumProtocolVersionPropertyName = "maximumProtocolVersion";

    /// <summary>The metadata JSON property name.</summary>
    private const string MetadataPropertyName = "metadata";

    /// <summary>The minimum protocol version JSON property name.</summary>
    private const string MinimumProtocolVersionPropertyName = "minimumProtocolVersion";

    /// <summary>The next cursor JSON property name.</summary>
    private const string NextCursorPropertyName = "nextCursor";

    /// <summary>The operation identifier JSON property name.</summary>
    private const string OperationIdPropertyName = "operationId";

    /// <summary>The operations JSON property name.</summary>
    private const string OperationsPropertyName = "operations";

    /// <summary>The origin JSON property name.</summary>
    private const string OriginPropertyName = "origin";

    /// <summary>The payload hash JSON property name.</summary>
    private const string PayloadHashPropertyName = "payloadHash";

    /// <summary>The payload JSON property name.</summary>
    private const string PayloadPropertyName = "payload";

    /// <summary>The policy JSON property name.</summary>
    private const string PolicyPropertyName = "policy";

    /// <summary>The previous cursor JSON property name.</summary>
    private const string PreviousCursorPropertyName = "previousCursor";

    /// <summary>The priority JSON property name.</summary>
    private const string PriorityPropertyName = "priority";

    /// <summary>The protocol version JSON property name.</summary>
    private const string ProtocolVersionPropertyName = "protocolVersion";

    /// <summary>The reason code JSON property name.</summary>
    private const string ReasonCodePropertyName = "reasonCode";

    /// <summary>The required guarantees JSON property name.</summary>
    private const string RequiredGuaranteesPropertyName = "requiredGuarantees";

    /// <summary>The schema version JSON property name.</summary>
    private const string SchemaVersionPropertyName = "schemaVersion";

    /// <summary>The server cursor JSON property name.</summary>
    private const string ServerCursorPropertyName = "serverCursor";

    /// <summary>The server idempotency retention JSON property name.</summary>
    private const string ServerIdempotencyRetentionMillisecondsPropertyName = "serverIdempotencyRetentionMilliseconds";

    /// <summary>The server version JSON property name.</summary>
    private const string ServerVersionPropertyName = "serverVersion";

    /// <summary>The stream identifier JSON property name.</summary>
    private const string StreamIdPropertyName = "streamId";

    /// <summary>The subscription identifier JSON property name.</summary>
    private const string SubscriptionIdPropertyName = "subscriptionId";

    /// <summary>The tenant hint JSON property name.</summary>
    private const string TenantHintPropertyName = "tenantHint";

    /// <summary>The timestamp JSON property name.</summary>
    private const string TimestampUtcPropertyName = "timestampUtc";

    /// <summary>The operation type JSON property name.</summary>
    private const string TypePropertyName = "type";

    /// <summary>The operation policy reader delegate.</summary>
    private static readonly Func<JsonElement, OperationPolicyWire> ReadOperationPolicyWireDelegate = ReadOperationPolicyWire;

    /// <summary>The operation result reader delegate.</summary>
    private static readonly Func<JsonElement, OperationSyncResultWire> ReadOperationSyncResultWireDelegate = ReadOperationSyncResultWire;

    /// <summary>The payload envelope reader delegate.</summary>
    private static readonly Func<JsonElement, PayloadEnvelopeWire> ReadPayloadEnvelopeWireDelegate = ReadPayloadEnvelopeWire;

    /// <summary>The remote event batch reader delegate.</summary>
    private static readonly Func<JsonElement, RemoteEventBatchWire> ReadRemoteEventBatchWireDelegate = ReadRemoteEventBatchWire;

    /// <summary>The remote event origin reader delegate.</summary>
    private static readonly Func<JsonElement, RemoteEventOriginWire> ReadRemoteEventOriginWireDelegate = ReadRemoteEventOriginWire;

    /// <summary>The remote event reader delegate.</summary>
    private static readonly Func<JsonElement, RemoteEventWire> ReadRemoteEventWireDelegate = ReadRemoteEventWire;

    /// <summary>The remote operation completion reader delegate.</summary>
    private static readonly Func<JsonElement, RemoteOperationCompletionWire> ReadRemoteOperationCompletionWireDelegate = ReadRemoteOperationCompletionWire;

    /// <summary>The synchronization operation reader delegate.</summary>
    private static readonly Func<JsonElement, SyncOperationWire> ReadSyncOperationWireDelegate = ReadSyncOperationWire;

    /// <summary>The operation result writer delegate.</summary>
    private static readonly Action<Utf8JsonWriter, OperationSyncResultWire> WriteOperationSyncResultWireDelegate = WriteOperationSyncResultWire;

    /// <summary>The remote event batch writer delegate.</summary>
    private static readonly Action<Utf8JsonWriter, RemoteEventBatchWire> WriteRemoteEventBatchWireDelegate = WriteRemoteEventBatchWire;

    /// <summary>The remote event origin writer delegate.</summary>
    private static readonly Action<Utf8JsonWriter, RemoteEventOriginWire> WriteRemoteEventOriginWireDelegate = WriteRemoteEventOriginWire;

    /// <summary>The remote event writer delegate.</summary>
    private static readonly Action<Utf8JsonWriter, RemoteEventWire> WriteRemoteEventWireDelegate = WriteRemoteEventWire;

    /// <summary>The remote operation completion writer delegate.</summary>
    private static readonly Action<Utf8JsonWriter, RemoteOperationCompletionWire> WriteRemoteOperationCompletionWireDelegate = WriteRemoteOperationCompletionWire;

    /// <summary>The synchronization operation writer delegate.</summary>
    private static readonly Action<Utf8JsonWriter, SyncOperationWire> WriteSyncOperationWireDelegate = WriteSyncOperationWire;

    /// <summary>Reads a connect request DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static ConnectRequestWire ReadConnectRequestWire(JsonElement element)
    {
        EnsureObject(element);
        return new()
        {
            MinimumProtocolVersion = GetString(element, MinimumProtocolVersionPropertyName),
            MaximumProtocolVersion = GetString(element, MaximumProtocolVersionPropertyName),
            ClientId = GetString(element, ClientIdPropertyName),
            TenantHint = GetOptionalString(element, TenantHintPropertyName),
            RequiredGuarantees = GetInt32Array(element, RequiredGuaranteesPropertyName),
        };
    }

    /// <summary>Writes a connect request DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteConnectRequestWire(Utf8JsonWriter writer, ConnectRequestWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(MinimumProtocolVersionPropertyName, value.MinimumProtocolVersion);
        writer.WriteString(MaximumProtocolVersionPropertyName, value.MaximumProtocolVersion);
        writer.WriteString(ClientIdPropertyName, value.ClientId);
        WriteOptionalString(writer, TenantHintPropertyName, value.TenantHint);
        WriteInt32Array(writer, RequiredGuaranteesPropertyName, value.RequiredGuarantees);
        writer.WriteEndObject();
    }

    /// <summary>Reads a connect response DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static ConnectResponseWire ReadConnectResponseWire(JsonElement element)
    {
        EnsureObject(element);
        return new()
        {
            ProtocolVersion = GetString(element, ProtocolVersionPropertyName),
            Features = GetInt32(element, FeaturesPropertyName),
            MaximumBatchOperations = GetInt32(element, MaximumBatchOperationsPropertyName),
            MaximumBatchBytes = GetInt64(element, MaximumBatchBytesPropertyName),
            ServerIdempotencyRetentionMilliseconds = GetOptionalInt64(element, ServerIdempotencyRetentionMillisecondsPropertyName),
            ClientInboxRetentionRequiredMilliseconds = GetOptionalInt64(element, ClientInboxRetentionRequiredMillisecondsPropertyName),
        };
    }

    /// <summary>Writes a connect response DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteConnectResponseWire(Utf8JsonWriter writer, ConnectResponseWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(ProtocolVersionPropertyName, value.ProtocolVersion);
        writer.WriteNumber(FeaturesPropertyName, value.Features);
        writer.WriteNumber(MaximumBatchOperationsPropertyName, value.MaximumBatchOperations);
        writer.WriteNumber(MaximumBatchBytesPropertyName, value.MaximumBatchBytes);
        WriteOptionalInt64(writer, ServerIdempotencyRetentionMillisecondsPropertyName, value.ServerIdempotencyRetentionMilliseconds);
        WriteOptionalInt64(writer, ClientInboxRetentionRequiredMillisecondsPropertyName, value.ClientInboxRetentionRequiredMilliseconds);
        writer.WriteEndObject();
    }

    /// <summary>Reads a push request DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static PushRequestWire ReadPushRequestWire(JsonElement element)
    {
        EnsureObject(element);
        return new() { BatchId = GetGuid(element, BatchIdPropertyName), Operations = GetArray(element, OperationsPropertyName, ReadSyncOperationWireDelegate) };
    }

    /// <summary>Writes a push request DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WritePushRequestWire(Utf8JsonWriter writer, PushRequestWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(BatchIdPropertyName, value.BatchId);
        WriteArray(writer, OperationsPropertyName, value.Operations, WriteSyncOperationWireDelegate);
        writer.WriteEndObject();
    }

    /// <summary>Reads a push response DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static PushResponseWire ReadPushResponseWire(JsonElement element)
    {
        EnsureObject(element);
        return new()
        {
            BatchId = GetGuid(element, BatchIdPropertyName),
            Operations = GetArray(element, OperationsPropertyName, ReadOperationSyncResultWireDelegate),
            ServerCursor = GetOptionalString(element, ServerCursorPropertyName),
        };
    }

    /// <summary>Writes a push response DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WritePushResponseWire(Utf8JsonWriter writer, PushResponseWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(BatchIdPropertyName, value.BatchId);
        WriteArray(writer, OperationsPropertyName, value.Operations, WriteOperationSyncResultWireDelegate);
        WriteOptionalString(writer, ServerCursorPropertyName, value.ServerCursor);
        writer.WriteEndObject();
    }

    /// <summary>Reads a subscribe response DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static SubscribeResponseWire ReadSubscribeResponseWire(JsonElement element)
    {
        EnsureObject(element);
        return new() { Batches = GetArray(element, BatchesPropertyName, ReadRemoteEventBatchWireDelegate) };
    }

    /// <summary>Writes a subscribe response DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteSubscribeResponseWire(Utf8JsonWriter writer, SubscribeResponseWire value)
    {
        writer.WriteStartObject();
        WriteArray(writer, BatchesPropertyName, value.Batches, WriteRemoteEventBatchWireDelegate);
        writer.WriteEndObject();
    }

    /// <summary>Reads an acknowledgement request DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static AcknowledgeRequestWire ReadAcknowledgeRequestWire(JsonElement element)
    {
        EnsureObject(element);
        return new() { SubscriptionId = GetGuid(element, SubscriptionIdPropertyName), StreamId = GetString(element, StreamIdPropertyName), Cursor = GetString(element, AcknowledgeCursorPropertyName) };
    }

    /// <summary>Writes an acknowledgement request DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteAcknowledgeRequestWire(Utf8JsonWriter writer, AcknowledgeRequestWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(SubscriptionIdPropertyName, value.SubscriptionId);
        writer.WriteString(StreamIdPropertyName, value.StreamId);
        writer.WriteString(AcknowledgeCursorPropertyName, value.Cursor);
        writer.WriteEndObject();
    }

    /// <summary>Reads a synchronization operation DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static SyncOperationWire ReadSyncOperationWire(JsonElement element)
    {
        EnsureObject(element);
        return new()
        {
            OperationId = GetGuid(element, OperationIdPropertyName),
            StreamId = GetString(element, StreamIdPropertyName),
            ClientSequence = GetInt64(element, ClientSequencePropertyName),
            TimestampUtc = GetDateTimeOffset(element, TimestampUtcPropertyName),
            BaseVersion = GetOptionalString(element, BaseVersionPropertyName),
            Type = GetInt32(element, TypePropertyName),
            Payload = GetObject(element, PayloadPropertyName, ReadPayloadEnvelopeWireDelegate),
            Policy = GetObject(element, PolicyPropertyName, ReadOperationPolicyWireDelegate),
            Metadata = GetStringDictionary(element, MetadataPropertyName),
        };
    }

    /// <summary>Writes a synchronization operation DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteSyncOperationWire(Utf8JsonWriter writer, SyncOperationWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(OperationIdPropertyName, value.OperationId);
        writer.WriteString(StreamIdPropertyName, value.StreamId);
        writer.WriteNumber(ClientSequencePropertyName, value.ClientSequence);
        writer.WriteString(TimestampUtcPropertyName, value.TimestampUtc);
        WriteOptionalString(writer, BaseVersionPropertyName, value.BaseVersion);
        writer.WriteNumber(TypePropertyName, value.Type);
        writer.WritePropertyName(PayloadPropertyName);
        WritePayloadEnvelopeWire(writer, value.Payload);
        writer.WritePropertyName(PolicyPropertyName);
        WriteOperationPolicyWire(writer, value.Policy);
        WriteStringDictionary(writer, MetadataPropertyName, value.Metadata);
        writer.WriteEndObject();
    }

    /// <summary>Reads an operation policy DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static OperationPolicyWire ReadOperationPolicyWire(JsonElement element)
    {
        EnsureObject(element);
        return new()
        {
            DeliveryGuarantee = GetInt32(element, DeliveryGuaranteePropertyName),
            Durability = GetInt32(element, DurabilityPropertyName),
            Priority = GetInt32(element, PriorityPropertyName),
            ConflictPolicy = GetInt32(element, ConflictPolicyPropertyName),
        };
    }

    /// <summary>Writes an operation policy DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteOperationPolicyWire(Utf8JsonWriter writer, OperationPolicyWire value)
    {
        writer.WriteStartObject();
        writer.WriteNumber(DeliveryGuaranteePropertyName, value.DeliveryGuarantee);
        writer.WriteNumber(DurabilityPropertyName, value.Durability);
        writer.WriteNumber(PriorityPropertyName, value.Priority);
        writer.WriteNumber(ConflictPolicyPropertyName, value.ConflictPolicy);
        writer.WriteEndObject();
    }

    /// <summary>Reads a payload envelope DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static PayloadEnvelopeWire ReadPayloadEnvelopeWire(JsonElement element)
    {
        EnsureObject(element);
        return new()
        {
            ContractId = GetString(element, ContractIdPropertyName),
            SchemaVersion = GetInt32(element, SchemaVersionPropertyName),
            ContentType = GetString(element, ContentTypePropertyName),
            Payload = GetString(element, PayloadPropertyName),
            PayloadHash = GetString(element, PayloadHashPropertyName),
        };
    }

    /// <summary>Writes a payload envelope DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WritePayloadEnvelopeWire(Utf8JsonWriter writer, PayloadEnvelopeWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(ContractIdPropertyName, value.ContractId);
        writer.WriteNumber(SchemaVersionPropertyName, value.SchemaVersion);
        writer.WriteString(ContentTypePropertyName, value.ContentType);
        writer.WriteString(PayloadPropertyName, value.Payload);
        writer.WriteString(PayloadHashPropertyName, value.PayloadHash);
        writer.WriteEndObject();
    }

    /// <summary>Reads an operation synchronization result DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static OperationSyncResultWire ReadOperationSyncResultWire(JsonElement element)
    {
        EnsureObject(element);
        return new()
        {
            OperationId = GetGuid(element, OperationIdPropertyName),
            Kind = GetInt32(element, KindPropertyName),
            ReasonCode = GetOptionalString(element, ReasonCodePropertyName),
            ServerVersion = GetOptionalString(element, ServerVersionPropertyName),
        };
    }

    /// <summary>Writes an operation synchronization result DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteOperationSyncResultWire(Utf8JsonWriter writer, OperationSyncResultWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(OperationIdPropertyName, value.OperationId);
        writer.WriteNumber(KindPropertyName, value.Kind);
        WriteOptionalString(writer, ReasonCodePropertyName, value.ReasonCode);
        WriteOptionalString(writer, ServerVersionPropertyName, value.ServerVersion);
        writer.WriteEndObject();
    }

    /// <summary>Reads a remote event batch DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static RemoteEventBatchWire ReadRemoteEventBatchWire(JsonElement element)
    {
        EnsureObject(element);
        return new()
        {
            BatchId = GetGuid(element, BatchIdPropertyName),
            StreamId = GetString(element, StreamIdPropertyName),
            PreviousCursor = GetOptionalString(element, PreviousCursorPropertyName),
            NextCursor = GetString(element, NextCursorPropertyName),
            Events = GetArray(element, EventsPropertyName, ReadRemoteEventWireDelegate),
            CompletedOperations = GetArray(element, CompletedOperationsPropertyName, ReadRemoteOperationCompletionWireDelegate),
        };
    }

    /// <summary>Writes a remote event batch DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteRemoteEventBatchWire(Utf8JsonWriter writer, RemoteEventBatchWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(BatchIdPropertyName, value.BatchId);
        writer.WriteString(StreamIdPropertyName, value.StreamId);
        WriteOptionalString(writer, PreviousCursorPropertyName, value.PreviousCursor);
        writer.WriteString(NextCursorPropertyName, value.NextCursor);
        WriteArray(writer, EventsPropertyName, value.Events, WriteRemoteEventWireDelegate);
        WriteArray(writer, CompletedOperationsPropertyName, value.CompletedOperations, WriteRemoteOperationCompletionWireDelegate);
        writer.WriteEndObject();
    }

    /// <summary>Reads a remote event DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static RemoteEventWire ReadRemoteEventWire(JsonElement element)
    {
        EnsureObject(element);
        return new()
        {
            EventId = GetGuid(element, EventIdPropertyName),
            StreamId = GetString(element, StreamIdPropertyName),
            ServerCursor = GetString(element, ServerCursorPropertyName),
            CommittedAtUtc = GetDateTimeOffset(element, CommittedAtUtcPropertyName),
            CausedByOperationId = GetOptionalGuid(element, CausedByOperationIdPropertyName),
            Origin = GetOptionalObject(element, OriginPropertyName, ReadRemoteEventOriginWireDelegate),
            Payload = GetObject(element, PayloadPropertyName, ReadPayloadEnvelopeWireDelegate),
            Metadata = GetStringDictionary(element, MetadataPropertyName),
        };
    }

    /// <summary>Writes a remote event DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteRemoteEventWire(Utf8JsonWriter writer, RemoteEventWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(EventIdPropertyName, value.EventId);
        writer.WriteString(StreamIdPropertyName, value.StreamId);
        writer.WriteString(ServerCursorPropertyName, value.ServerCursor);
        writer.WriteString(CommittedAtUtcPropertyName, value.CommittedAtUtc);
        WriteOptionalGuid(writer, CausedByOperationIdPropertyName, value.CausedByOperationId);
        WriteOptionalObject(writer, OriginPropertyName, value.Origin, WriteRemoteEventOriginWireDelegate);
        writer.WritePropertyName(PayloadPropertyName);
        WritePayloadEnvelopeWire(writer, value.Payload);
        WriteStringDictionary(writer, MetadataPropertyName, value.Metadata);
        writer.WriteEndObject();
    }

    /// <summary>Reads a remote event origin DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static RemoteEventOriginWire ReadRemoteEventOriginWire(JsonElement element)
    {
        EnsureObject(element);
        return new() { ClientId = GetString(element, ClientIdPropertyName), OperationId = GetGuid(element, OperationIdPropertyName) };
    }

    /// <summary>Writes a remote event origin DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteRemoteEventOriginWire(Utf8JsonWriter writer, RemoteEventOriginWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(ClientIdPropertyName, value.ClientId);
        writer.WriteString(OperationIdPropertyName, value.OperationId);
        writer.WriteEndObject();
    }

    /// <summary>Reads a remote operation completion DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static RemoteOperationCompletionWire ReadRemoteOperationCompletionWire(JsonElement element)
    {
        EnsureObject(element);
        return new() { Origin = GetObject(element, OriginPropertyName, ReadRemoteEventOriginWireDelegate), EventIds = GetGuidArray(element, EventIdsPropertyName) };
    }

    /// <summary>Writes a remote operation completion DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteRemoteOperationCompletionWire(Utf8JsonWriter writer, RemoteOperationCompletionWire value)
    {
        writer.WriteStartObject();
        writer.WritePropertyName(OriginPropertyName);
        WriteRemoteEventOriginWire(writer, value.Origin);
        WriteGuidArray(writer, EventIdsPropertyName, value.EventIds);
        writer.WriteEndObject();
    }
}
