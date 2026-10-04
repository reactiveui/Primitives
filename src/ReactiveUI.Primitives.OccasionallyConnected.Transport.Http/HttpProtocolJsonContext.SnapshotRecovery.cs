// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Provides snapshot recovery protocol JSON metadata.</summary>
internal sealed partial class HttpProtocolJsonContext
{
    /// <summary>The expired cursor JSON property name.</summary>
    private const string ExpiredCursorPropertyName = "expiredCursor";

    /// <summary>The client-state contract identifier JSON property name.</summary>
    private const string ClientStateContractIdPropertyName = "clientStateContractId";

    /// <summary>The client-state schema version JSON property name.</summary>
    private const string ClientStateSchemaVersionPropertyName = "clientStateSchemaVersion";

    /// <summary>The snapshot format version JSON property name.</summary>
    private const string SnapshotFormatVersionPropertyName = "snapshotFormatVersion";

    /// <summary>The pending operations JSON property name.</summary>
    private const string PendingOperationsPropertyName = "pendingOperations";

    /// <summary>The replay operations JSON property name.</summary>
    private const string ReplayOperationsPropertyName = "replayOperations";

    /// <summary>The maximum response bytes JSON property name.</summary>
    private const string MaximumResponseBytesPropertyName = "maximumResponseBytes";

    /// <summary>The snapshot status JSON property name.</summary>
    private const string StatusPropertyName = "status";

    /// <summary>The recovered checkpoint JSON property name.</summary>
    private const string CheckpointPropertyName = "checkpoint";

    /// <summary>The operation dispositions JSON property name.</summary>
    private const string OperationDispositionsPropertyName = "operationDispositions";

    /// <summary>The checkpoint frontier cursor JSON property name.</summary>
    private const string FrontierCursorPropertyName = "frontierCursor";

    /// <summary>The client-state payload JSON property name.</summary>
    private const string ClientStatePropertyName = "clientState";

    /// <summary>The observed timestamp JSON property name.</summary>
    private const string ObservedAtUtcPropertyName = "observedAtUtc";

    /// <summary>The operation disposition result JSON property name.</summary>
    private const string ResultPropertyName = "result";

    /// <summary>Reads snapshot operation disposition DTOs.</summary>
    private static readonly Func<JsonElement, SnapshotOperationDispositionWire> ReadSnapshotOperationDispositionWireDelegate = ReadSnapshotOperationDispositionWire;

    /// <summary>Writes snapshot operation disposition DTOs.</summary>
    private static readonly Action<Utf8JsonWriter, SnapshotOperationDispositionWire> WriteSnapshotOperationDispositionWireDelegate = WriteSnapshotOperationDispositionWire;

    /// <summary>Gets snapshot recovery request metadata.</summary>
    internal JsonTypeInfo<SnapshotRecoveryRequestWire> SnapshotRecoveryRequestWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<SnapshotRecoveryRequestWire>(ReadSnapshotRecoveryRequestWire, WriteSnapshotRecoveryRequestWire));

    /// <summary>Gets snapshot recovery response metadata.</summary>
    internal JsonTypeInfo<SnapshotRecoveryResponseWire> SnapshotRecoveryResponseWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<SnapshotRecoveryResponseWire>(ReadSnapshotRecoveryResponseWire, WriteSnapshotRecoveryResponseWire));

    /// <summary>Reads a snapshot recovery request DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static SnapshotRecoveryRequestWire ReadSnapshotRecoveryRequestWire(JsonElement element)
    {
        EnsureObject(element);
        return new()
        {
            StreamId = GetString(element, StreamIdPropertyName),
            SubscriptionId = GetGuid(element, SubscriptionIdPropertyName),
            ExpiredCursor = GetOptionalString(element, ExpiredCursorPropertyName),
            ClientStateContractId = GetString(element, ClientStateContractIdPropertyName),
            ClientStateSchemaVersion = GetInt32(element, ClientStateSchemaVersionPropertyName),
            SnapshotFormatVersion = GetInt32(element, SnapshotFormatVersionPropertyName),
            PendingOperations = GetArray(element, PendingOperationsPropertyName, ReadSyncOperationWireDelegate),
            ReplayOperations = GetArray(element, ReplayOperationsPropertyName, ReadSyncOperationWireDelegate),
            MaximumResponseBytes = GetInt64(element, MaximumResponseBytesPropertyName),
        };
    }

    /// <summary>Writes a snapshot recovery request DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteSnapshotRecoveryRequestWire(Utf8JsonWriter writer, SnapshotRecoveryRequestWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(StreamIdPropertyName, value.StreamId);
        writer.WriteString(SubscriptionIdPropertyName, value.SubscriptionId);
        WriteOptionalString(writer, ExpiredCursorPropertyName, value.ExpiredCursor);
        writer.WriteString(ClientStateContractIdPropertyName, value.ClientStateContractId);
        writer.WriteNumber(ClientStateSchemaVersionPropertyName, value.ClientStateSchemaVersion);
        writer.WriteNumber(SnapshotFormatVersionPropertyName, value.SnapshotFormatVersion);
        WriteArray(writer, PendingOperationsPropertyName, value.PendingOperations, WriteSyncOperationWireDelegate);
        WriteOptionalArray(writer, ReplayOperationsPropertyName, value.ReplayOperations, WriteSyncOperationWireDelegate);
        writer.WriteNumber(MaximumResponseBytesPropertyName, value.MaximumResponseBytes);
        writer.WriteEndObject();
    }

    /// <summary>Reads a snapshot recovery response DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static SnapshotRecoveryResponseWire ReadSnapshotRecoveryResponseWire(JsonElement element)
    {
        EnsureObject(element);
        return new()
        {
            Status = GetInt32(element, StatusPropertyName),
            Checkpoint = GetOptionalObject(element, CheckpointPropertyName, ReadRemoteSnapshotCheckpointWire),
            OperationDispositions = GetArray(element, OperationDispositionsPropertyName, ReadSnapshotOperationDispositionWireDelegate),
            ReasonCode = GetOptionalString(element, ReasonCodePropertyName),
        };
    }

    /// <summary>Writes a snapshot recovery response DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteSnapshotRecoveryResponseWire(Utf8JsonWriter writer, SnapshotRecoveryResponseWire value)
    {
        writer.WriteStartObject();
        writer.WriteNumber(StatusPropertyName, value.Status);
        WriteOptionalObject(writer, CheckpointPropertyName, value.Checkpoint, WriteRemoteSnapshotCheckpointWire);
        WriteArray(writer, OperationDispositionsPropertyName, value.OperationDispositions, WriteSnapshotOperationDispositionWireDelegate);
        WriteOptionalString(writer, ReasonCodePropertyName, value.ReasonCode);
        writer.WriteEndObject();
    }

    /// <summary>Reads a recovered checkpoint DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static RemoteSnapshotCheckpointWire ReadRemoteSnapshotCheckpointWire(JsonElement element)
    {
        EnsureObject(element);
        return new()
        {
            StreamId = GetString(element, StreamIdPropertyName),
            SubscriptionId = GetGuid(element, SubscriptionIdPropertyName),
            FrontierCursor = GetString(element, FrontierCursorPropertyName),
            ServerVersion = GetString(element, ServerVersionPropertyName),
            SnapshotFormatVersion = GetInt32(element, SnapshotFormatVersionPropertyName),
            ClientState = GetObject(element, ClientStatePropertyName, ReadPayloadEnvelopeWireDelegate),
            ObservedAtUtc = GetDateTimeOffset(element, ObservedAtUtcPropertyName),
        };
    }

    /// <summary>Writes a recovered checkpoint DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteRemoteSnapshotCheckpointWire(Utf8JsonWriter writer, RemoteSnapshotCheckpointWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(StreamIdPropertyName, value.StreamId);
        writer.WriteString(SubscriptionIdPropertyName, value.SubscriptionId);
        writer.WriteString(FrontierCursorPropertyName, value.FrontierCursor);
        writer.WriteString(ServerVersionPropertyName, value.ServerVersion);
        writer.WriteNumber(SnapshotFormatVersionPropertyName, value.SnapshotFormatVersion);
        writer.WritePropertyName(ClientStatePropertyName);
        WritePayloadEnvelopeWire(writer, value.ClientState);
        writer.WriteString(ObservedAtUtcPropertyName, value.ObservedAtUtc);
        writer.WriteEndObject();
    }

    /// <summary>Reads a snapshot operation disposition DTO.</summary>
    /// <param name="element">The JSON element.</param>
    /// <returns>The DTO.</returns>
    private static SnapshotOperationDispositionWire ReadSnapshotOperationDispositionWire(JsonElement element)
    {
        EnsureObject(element);
        return new()
        {
            OperationId = GetGuid(element, OperationIdPropertyName),
            Kind = GetInt32(element, KindPropertyName),
            Result = GetOptionalObject(element, ResultPropertyName, ReadOperationSyncResultWireDelegate),
        };
    }

    /// <summary>Writes a snapshot operation disposition DTO.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The DTO.</param>
    private static void WriteSnapshotOperationDispositionWire(Utf8JsonWriter writer, SnapshotOperationDispositionWire value)
    {
        writer.WriteStartObject();
        writer.WriteString(OperationIdPropertyName, value.OperationId);
        writer.WriteNumber(KindPropertyName, value.Kind);
        WriteOptionalObject(writer, ResultPropertyName, value.Result, WriteOperationSyncResultWireDelegate);
        writer.WriteEndObject();
    }

    /// <summary>Describes a snapshot recovery request wire object.</summary>
    internal sealed class SnapshotRecoveryRequestWire
    {
        /// <summary>Gets or sets the stream identifier.</summary>
        public string StreamId { get; set; } = string.Empty;

        /// <summary>Gets or sets the subscription identifier.</summary>
        public Guid SubscriptionId { get; set; }

        /// <summary>Gets or sets the expired cursor.</summary>
        public string? ExpiredCursor { get; set; }

        /// <summary>Gets or sets the client-state contract identifier.</summary>
        public string ClientStateContractId { get; set; } = string.Empty;

        /// <summary>Gets or sets the client-state schema version.</summary>
        public int ClientStateSchemaVersion { get; set; }

        /// <summary>Gets or sets the snapshot format version.</summary>
        public int SnapshotFormatVersion { get; set; }

        /// <summary>Gets or sets the pending operations.</summary>
        public SyncOperationWire[] PendingOperations { get; init; } = [];

        /// <summary>Gets or sets the replay operations.</summary>
        public SyncOperationWire[] ReplayOperations { get; init; } = [];

        /// <summary>Gets or sets the maximum response bytes.</summary>
        public long MaximumResponseBytes { get; set; }
    }

    /// <summary>Describes a snapshot recovery response wire object.</summary>
    internal sealed class SnapshotRecoveryResponseWire
    {
        /// <summary>Gets or sets the recovery status.</summary>
        public int Status { get; set; }

        /// <summary>Gets or sets the recovered checkpoint.</summary>
        public RemoteSnapshotCheckpointWire? Checkpoint { get; set; }

        /// <summary>Gets or sets the operation dispositions.</summary>
        public SnapshotOperationDispositionWire[] OperationDispositions { get; init; } = [];

        /// <summary>Gets or sets the stable reason code.</summary>
        public string? ReasonCode { get; set; }
    }

    /// <summary>Describes a recovered checkpoint wire object.</summary>
    internal sealed class RemoteSnapshotCheckpointWire
    {
        /// <summary>Gets or sets the stream identifier.</summary>
        public string StreamId { get; set; } = string.Empty;

        /// <summary>Gets or sets the subscription identifier.</summary>
        public Guid SubscriptionId { get; set; }

        /// <summary>Gets or sets the frontier cursor.</summary>
        public string FrontierCursor { get; set; } = string.Empty;

        /// <summary>Gets or sets the server version.</summary>
        public string ServerVersion { get; set; } = string.Empty;

        /// <summary>Gets or sets the snapshot format version.</summary>
        public int SnapshotFormatVersion { get; set; }

        /// <summary>Gets or sets the client-state payload.</summary>
        public PayloadEnvelopeWire ClientState { get; init; } = new();

        /// <summary>Gets or sets the observed timestamp.</summary>
        public DateTimeOffset ObservedAtUtc { get; set; }
    }

    /// <summary>Describes one operation disposition wire object.</summary>
    internal sealed class SnapshotOperationDispositionWire
    {
        /// <summary>Gets or sets the operation identifier.</summary>
        public Guid OperationId { get; set; }

        /// <summary>Gets or sets the disposition kind.</summary>
        public int Kind { get; set; }

        /// <summary>Gets or sets the retained operation result.</summary>
        public OperationSyncResultWire? Result { get; set; }
    }
}
