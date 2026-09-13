// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Authored JSON metadata for the HTTP protocol DTO allowlist.</summary>
internal sealed partial class HttpProtocolJsonContext
{
    /// <summary>The serializer options used by the protocol metadata.</summary>
    internal static readonly JsonSerializerOptions Options =
        new() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull, PropertyNamingPolicy = JsonNamingPolicy.CamelCase, TypeInfoResolver = JsonTypeInfoResolver.Combine() };

    /// <summary>Gets the shared protocol JSON context.</summary>
    internal static HttpProtocolJsonContext Default { get; } = new();

    /// <summary>Gets the connect request metadata.</summary>
    internal JsonTypeInfo<ConnectRequestWire> ConnectRequestWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<ConnectRequestWire>(ReadConnectRequestWire, WriteConnectRequestWire));

    /// <summary>Gets the connect response metadata.</summary>
    internal JsonTypeInfo<ConnectResponseWire> ConnectResponseWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<ConnectResponseWire>(ReadConnectResponseWire, WriteConnectResponseWire));

    /// <summary>Gets the push request metadata.</summary>
    internal JsonTypeInfo<PushRequestWire> PushRequestWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<PushRequestWire>(ReadPushRequestWire, WritePushRequestWire));

    /// <summary>Gets the push response metadata.</summary>
    internal JsonTypeInfo<PushResponseWire> PushResponseWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<PushResponseWire>(ReadPushResponseWire, WritePushResponseWire));

    /// <summary>Gets the subscribe response metadata.</summary>
    internal JsonTypeInfo<SubscribeResponseWire> SubscribeResponseWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<SubscribeResponseWire>(ReadSubscribeResponseWire, WriteSubscribeResponseWire));

    /// <summary>Gets the acknowledgement request metadata.</summary>
    internal JsonTypeInfo<AcknowledgeRequestWire> AcknowledgeRequestWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<AcknowledgeRequestWire>(ReadAcknowledgeRequestWire, WriteAcknowledgeRequestWire));

    /// <summary>Gets the synchronization operation metadata.</summary>
    internal JsonTypeInfo<SyncOperationWire> SyncOperationWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<SyncOperationWire>(ReadSyncOperationWire, WriteSyncOperationWire));

    /// <summary>Gets the operation policy metadata.</summary>
    internal JsonTypeInfo<OperationPolicyWire> OperationPolicyWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<OperationPolicyWire>(ReadOperationPolicyWire, WriteOperationPolicyWire));

    /// <summary>Gets the payload envelope metadata.</summary>
    internal JsonTypeInfo<PayloadEnvelopeWire> PayloadEnvelopeWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<PayloadEnvelopeWire>(ReadPayloadEnvelopeWire, WritePayloadEnvelopeWire));

    /// <summary>Gets the operation synchronization result metadata.</summary>
    internal JsonTypeInfo<OperationSyncResultWire> OperationSyncResultWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<OperationSyncResultWire>(ReadOperationSyncResultWire, WriteOperationSyncResultWire));

    /// <summary>Gets the remote event batch metadata.</summary>
    internal JsonTypeInfo<RemoteEventBatchWire> RemoteEventBatchWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<RemoteEventBatchWire>(ReadRemoteEventBatchWire, WriteRemoteEventBatchWire));

    /// <summary>Gets the remote event metadata.</summary>
    internal JsonTypeInfo<RemoteEventWire> RemoteEventWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<RemoteEventWire>(ReadRemoteEventWire, WriteRemoteEventWire));

    /// <summary>Gets the remote event origin metadata.</summary>
    internal JsonTypeInfo<RemoteEventOriginWire> RemoteEventOriginWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<RemoteEventOriginWire>(ReadRemoteEventOriginWire, WriteRemoteEventOriginWire));

    /// <summary>Gets the remote operation completion metadata.</summary>
    internal JsonTypeInfo<RemoteOperationCompletionWire> RemoteOperationCompletionWireInfo { get; } =
        CreateTypeInfo(new ProtocolJsonConverter<RemoteOperationCompletionWire>(ReadRemoteOperationCompletionWire, WriteRemoteOperationCompletionWire));

    /// <summary>Creates converter-backed protocol metadata.</summary>
    /// <typeparam name="T">The protocol DTO type.</typeparam>
    /// <param name="converter">The typed converter.</param>
    /// <returns>The JSON metadata.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static JsonTypeInfo<T> CreateTypeInfo<T>(JsonConverter<T> converter) =>
        JsonMetadataServices.CreateValueInfo<T>(Options, converter);

    /// <summary>Describes an HTTP connect request.</summary>
    internal sealed class ConnectRequestWire
    {
        /// <summary>Gets or sets the minimum protocol version.</summary>
        public string MinimumProtocolVersion { get; set; } = string.Empty;

        /// <summary>Gets or sets the maximum protocol version.</summary>
        public string MaximumProtocolVersion { get; set; } = string.Empty;

        /// <summary>Gets or sets the client identity hint.</summary>
        public string ClientId { get; set; } = string.Empty;

        /// <summary>Gets or sets the optional tenant routing hint.</summary>
        public string? TenantHint { get; set; }

        /// <summary>Gets or sets the requested delivery guarantees.</summary>
        public int[] RequiredGuarantees { get; init; } = [];
    }

    /// <summary>Describes an HTTP connect response.</summary>
    internal sealed class ConnectResponseWire
    {
        /// <summary>Gets or sets the negotiated protocol version.</summary>
        public string ProtocolVersion { get; set; } = string.Empty;

        /// <summary>Gets or sets the negotiated feature flags.</summary>
        public int Features { get; set; }

        /// <summary>Gets or sets the maximum operation count.</summary>
        public int MaximumBatchOperations { get; set; }

        /// <summary>Gets or sets the maximum batch bytes.</summary>
        public long MaximumBatchBytes { get; set; }

        /// <summary>Gets or sets the server idempotency retention.</summary>
        public long? ServerIdempotencyRetentionMilliseconds { get; set; }

        /// <summary>Gets or sets the required client inbox retention.</summary>
        public long? ClientInboxRetentionRequiredMilliseconds { get; set; }
    }

    /// <summary>Describes an HTTP push request.</summary>
    internal sealed class PushRequestWire
    {
        /// <summary>Gets or sets the batch identifier.</summary>
        public Guid BatchId { get; set; }

        /// <summary>Gets or sets the operations in client sequence order.</summary>
        public SyncOperationWire[] Operations { get; init; } = [];
    }

    /// <summary>Describes an HTTP push response.</summary>
    internal sealed class PushResponseWire
    {
        /// <summary>Gets or sets the batch identifier.</summary>
        public Guid BatchId { get; set; }

        /// <summary>Gets or sets the per-operation results.</summary>
        public OperationSyncResultWire[] Operations { get; init; } = [];

        /// <summary>Gets or sets the optional server cursor.</summary>
        public string? ServerCursor { get; set; }
    }

    /// <summary>Describes an HTTP long-poll response.</summary>
    internal sealed class SubscribeResponseWire
    {
        /// <summary>Gets or sets the complete event batch groups.</summary>
        public RemoteEventBatchWire[] Batches { get; init; } = [];
    }

    /// <summary>Describes an HTTP acknowledgement request.</summary>
    internal sealed class AcknowledgeRequestWire
    {
        /// <summary>Gets or sets the subscription identifier.</summary>
        public Guid SubscriptionId { get; set; }

        /// <summary>Gets or sets the stream identifier.</summary>
        public string StreamId { get; set; } = string.Empty;

        /// <summary>Gets or sets the acknowledged cursor.</summary>
        public string Cursor { get; set; } = string.Empty;
    }

    /// <summary>Describes a pushed operation.</summary>
    internal sealed class SyncOperationWire
    {
        /// <summary>Gets or sets the operation identifier.</summary>
        public Guid OperationId { get; set; }

        /// <summary>Gets or sets the stream identifier.</summary>
        public string StreamId { get; set; } = string.Empty;

        /// <summary>Gets or sets the client sequence.</summary>
        public long ClientSequence { get; set; }

        /// <summary>Gets or sets the timestamp.</summary>
        public DateTimeOffset TimestampUtc { get; set; }

        /// <summary>Gets or sets the optional base version.</summary>
        public string? BaseVersion { get; set; }

        /// <summary>Gets or sets the operation type.</summary>
        public int Type { get; set; }

        /// <summary>Gets or sets the payload.</summary>
        public PayloadEnvelopeWire Payload { get; init; } = new();

        /// <summary>Gets or sets the policy.</summary>
        public OperationPolicyWire Policy { get; init; } = new();

        /// <summary>Gets or sets the metadata.</summary>
        public Dictionary<string, string> Metadata { get; init; } = [];
    }

    /// <summary>Describes an operation policy.</summary>
    internal sealed class OperationPolicyWire
    {
        /// <summary>Gets or sets the delivery guarantee.</summary>
        public int DeliveryGuarantee { get; set; }

        /// <summary>Gets or sets the durability.</summary>
        public int Durability { get; set; }

        /// <summary>Gets or sets the priority.</summary>
        public int Priority { get; set; }

        /// <summary>Gets or sets the conflict policy.</summary>
        public int ConflictPolicy { get; set; }
    }

    /// <summary>Describes a payload envelope.</summary>
    internal sealed class PayloadEnvelopeWire
    {
        /// <summary>Gets or sets the contract identifier.</summary>
        public string ContractId { get; set; } = string.Empty;

        /// <summary>Gets or sets the schema version.</summary>
        public int SchemaVersion { get; set; }

        /// <summary>Gets or sets the content type.</summary>
        public string ContentType { get; set; } = string.Empty;

        /// <summary>Gets or sets the base64 payload.</summary>
        public string Payload { get; set; } = string.Empty;

        /// <summary>Gets or sets the payload hash.</summary>
        public string PayloadHash { get; set; } = string.Empty;
    }

    /// <summary>Describes a remote operation result.</summary>
    internal sealed class OperationSyncResultWire
    {
        /// <summary>Gets or sets the operation identifier.</summary>
        public Guid OperationId { get; set; }

        /// <summary>Gets or sets the result kind.</summary>
        public int Kind { get; set; }

        /// <summary>Gets or sets the optional reason code.</summary>
        public string? ReasonCode { get; set; }

        /// <summary>Gets or sets the optional server version.</summary>
        public string? ServerVersion { get; set; }
    }

    /// <summary>Describes a remote event batch.</summary>
    internal sealed class RemoteEventBatchWire
    {
        /// <summary>Gets or sets the batch identifier.</summary>
        public Guid BatchId { get; set; }

        /// <summary>Gets or sets the stream identifier.</summary>
        public string StreamId { get; set; } = string.Empty;

        /// <summary>Gets or sets the previous cursor.</summary>
        public string? PreviousCursor { get; set; }

        /// <summary>Gets or sets the next cursor.</summary>
        public string NextCursor { get; set; } = string.Empty;

        /// <summary>Gets or sets the events.</summary>
        public RemoteEventWire[] Events { get; init; } = [];

        /// <summary>Gets or sets the completed operation groups.</summary>
        public RemoteOperationCompletionWire[] CompletedOperations { get; init; } = [];
    }

    /// <summary>Describes a remote event.</summary>
    internal sealed class RemoteEventWire
    {
        /// <summary>Gets or sets the event identifier.</summary>
        public Guid EventId { get; set; }

        /// <summary>Gets or sets the stream identifier.</summary>
        public string StreamId { get; set; } = string.Empty;

        /// <summary>Gets or sets the server cursor.</summary>
        public string ServerCursor { get; set; } = string.Empty;

        /// <summary>Gets or sets the commit timestamp.</summary>
        public DateTimeOffset CommittedAtUtc { get; set; }

        /// <summary>Gets or sets the optional causing operation identifier.</summary>
        public Guid? CausedByOperationId { get; set; }

        /// <summary>Gets or sets the optional origin.</summary>
        public RemoteEventOriginWire? Origin { get; set; }

        /// <summary>Gets or sets the payload.</summary>
        public PayloadEnvelopeWire Payload { get; init; } = new();

        /// <summary>Gets or sets the metadata.</summary>
        public Dictionary<string, string> Metadata { get; init; } = [];
    }

    /// <summary>Describes a remote event origin.</summary>
    internal sealed class RemoteEventOriginWire
    {
        /// <summary>Gets or sets the client identity.</summary>
        public string ClientId { get; set; } = string.Empty;

        /// <summary>Gets or sets the operation identifier.</summary>
        public Guid OperationId { get; set; }
    }

    /// <summary>Describes one complete operation effect group.</summary>
    internal sealed class RemoteOperationCompletionWire
    {
        /// <summary>Gets or sets the origin.</summary>
        public RemoteEventOriginWire Origin { get; init; } = new();

        /// <summary>Gets or sets the event identifiers.</summary>
        public Guid[] EventIds { get; init; } = [];
    }
}
