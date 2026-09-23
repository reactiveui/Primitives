// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpProtocolCodec"/>.</summary>
public sealed partial class HttpProtocolCodecTests
{
    /// <summary>Creates a server-side codec without requiring HTTP adapter options.</summary>
    /// <returns>The codec.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpProtocolCodec CreateServerCodec() => new(CreateServerLimits());

    /// <summary>Creates the empty JSON object payload bytes.</summary>
    /// <returns>The payload bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] CreateEmptyJsonPayloadBytes() => "{}"u8.ToArray();

    /// <summary>Creates protocol limits for server-side codec tests.</summary>
    /// <returns>The limits.</returns>
    private static HttpProtocolLimits CreateServerLimits() => new()
    {
        MaximumRequestBytes = TestMessageKilobytes * TestKilobyte,
        MaximumResponseBytes = TestMessageKilobytes * TestKilobyte,
        MaximumPayloadBytes = TestPayloadKilobytes * TestKilobyte,
        MaximumMetadataEntries = TestMetadataEntries,
        MaximumMetadataKeyBytes = TestMetadataKeyBytes,
        MaximumMetadataValueBytes = TestMetadataValueBytes,
        MaximumBatchOperations = TestMaximumCollectionCount,
        MaximumEventsPerBatch = TestMaximumCollectionCount,
        MaximumCompletedOperationsPerBatch = TestMaximumCollectionCount,
        MaximumJsonDepth = TestJsonDepth,
    };

    /// <summary>Creates a synchronization batch for server request tests.</summary>
    /// <param name="type">The operation type.</param>
    /// <returns>The batch.</returns>
    private static SyncBatch CreateServerBatch(SyncOperationType type) =>
        new(
            Guid.Parse(ServerBatchIdText),
            [
                new()
                {
                    OperationId = new(Guid.Parse(ServerOperationIdText)),
                    StreamId = new(ServerStreamName),
                    ClientSequence = 1,
                    TimestampUtc = DateTimeOffset.Parse(ServerTimestampText, CultureInfo.InvariantCulture),
                    BaseVersion = "v0",
                    Type = type,
                    Payload = new(ContractName, 1, PayloadContentType, CreateEmptyJsonPayloadBytes(), PayloadHash),
                    Policy = new(DeliveryGuarantee.AtLeastOnce, OperationDurability.Durable, 0, ConflictPolicy.Merge),
                    Metadata = new Dictionary<string, string> { [TraceMetadataKey] = type == SyncOperationType.Custom ? "custom" : "append" },
                },
            ]);

    /// <summary>Creates a synchronization batch from explicit operations.</summary>
    /// <param name="operations">The operations to include.</param>
    /// <returns>The synchronization batch.</returns>
    private static SyncBatch CreateServerBatchFromOperations(params SyncOperation[] operations) => new(Guid.Parse(ServerBatchIdText), operations);

    /// <summary>Creates a domain operation for caller validation tests.</summary>
    /// <param name="operationId">The operation identifier text.</param>
    /// <param name="streamId">The stream identifier text.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <param name="type">The operation type.</param>
    /// <param name="policy">The optional operation policy.</param>
    /// <param name="payload">The optional payload envelope.</param>
    /// <param name="metadata">The optional metadata entries.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateServerOperation(
        string operationId = ServerOperationIdText,
        string streamId = ServerStreamName,
        long sequence = SingleOperation,
        SyncOperationType type = SyncOperationType.Append,
        OperationPolicy? policy = null,
        PayloadEnvelope? payload = null,
        IReadOnlyDictionary<string, string>? metadata = null) =>
        new()
        {
            OperationId = new(Guid.Parse(operationId)),
            StreamId = new(streamId),
            ClientSequence = sequence,
            TimestampUtc = DateTimeOffset.Parse(ServerTimestampText, CultureInfo.InvariantCulture),
            Type = type,
            Payload = payload ?? new(ContractName, 1, PayloadContentType, CreateEmptyJsonPayloadBytes(), PayloadHash),
            Policy = policy ?? OperationPolicy.Default,
            Metadata = metadata ?? new Dictionary<string, string> { [TraceMetadataKey] = "1" },
        };

    /// <summary>Creates a wire operation from a domain operation.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The wire operation.</returns>
    private static HttpProtocolJsonContext.SyncOperationWire CreateOperationWire(SyncOperation operation) =>
        new()
        {
            OperationId = operation.OperationId.Value,
            StreamId = operation.StreamId.Value,
            ClientSequence = operation.ClientSequence,
            TimestampUtc = operation.TimestampUtc,
            BaseVersion = operation.BaseVersion,
            Type = (int)operation.Type,
            Payload = new()
            {
                ContractId = operation.Payload.ContractId,
                SchemaVersion = operation.Payload.SchemaVersion,
                ContentType = operation.Payload.ContentType,
                Payload = Convert.ToBase64String(operation.Payload.Payload.ToArray()),
                PayloadHash = operation.Payload.PayloadHash,
            },
            Policy = new()
            {
                DeliveryGuarantee = (int)operation.Policy.DeliveryGuarantee,
                Durability = (int)operation.Policy.Durability,
                Priority = operation.Policy.Priority,
                ConflictPolicy = (int)operation.Policy.ConflictPolicy,
            },
            Metadata = HttpProtocolCodecHelper.ToDictionary(operation.Metadata),
        };

    /// <summary>Creates a remote event for response serialization tests.</summary>
    /// <param name="eventId">The event identifier.</param>
    /// <param name="cursor">The cursor.</param>
    /// <param name="origin">The origin.</param>
    /// <returns>The event.</returns>
    private static RemoteEvent CreateServerEvent(string eventId, string cursor, RemoteEventOrigin origin) =>
        new(
            Guid.Parse(eventId),
            new(ServerStreamName),
            cursor,
            DateTimeOffset.Parse(ServerTimestampText, CultureInfo.InvariantCulture),
            origin.OperationId,
            new(ContractName, 1, PayloadContentType, CreateEmptyJsonPayloadBytes(), PayloadHash),
            new Dictionary<string, string> { [TraceMetadataKey] = cursor })
        { Origin = origin };

    /// <summary>Captures the query produced by the real client session subscribe path.</summary>
    /// <param name="request">The immutable subscription request.</param>
    /// <returns>The query text.</returns>
    private static async Task<string> CaptureSubscribeQueryAsync(RemoteSubscribeRequest request)
    {
        var capturedTask = QueryCaptureHandler.Prepare();
        var options = new HttpRemoteTransportOptions { HttpClient = QueryCaptureClient, BaseAddress = new("https://example.invalid/oc/") };
        var capabilities = new NegotiatedCapabilities(
            new(1, 0),
            RemoteTransportCapabilities.CursorResume,
            SingleOperation,
            TestKilobyte,
            null,
            null);
        await using var session = new HttpRemoteTransportSession(
            options,
            capabilities,
            new(new HttpRequestGate(1), new HttpRequestGate(1), new HttpRequestGate(1)),
            null,
            new(WireClientId),
            CancellationToken.None);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(QueryCaptureTimeoutSeconds));
        await using var enumerator = session.SubscribeAsync(request, cancellation.Token).GetAsyncEnumerator(cancellation.Token);
        var moveNext = enumerator.MoveNextAsync().AsTask();
        var captured = await capturedTask.WaitAsync(cancellation.Token);
        await cancellation.CancelAsync();
        try
        {
            _ = await moveNext;
        }
        catch (OperationCanceledException)
        {
        }

        return captured;
    }

    /// <summary>Creates a connect request JSON document.</summary>
    /// <param name="minimumVersion">The minimum protocol version text.</param>
    /// <param name="maximumVersion">The maximum protocol version text.</param>
    /// <param name="clientId">The client identifier text.</param>
    /// <param name="requiredGuarantees">The required guarantee JSON values.</param>
    /// <param name="tenantHintJson">The optional raw tenant hint JSON value.</param>
    /// <param name="extraJson">The optional extra JSON properties with a leading comma.</param>
    /// <returns>The JSON document.</returns>
    private static string ConnectRequestJson(
        string minimumVersion = "1.0",
        string maximumVersion = "1.1",
        string clientId = WireClientId,
        string requiredGuarantees = "0",
        string? tenantHintJson = null,
        string extraJson = "")
    {
        var tenantHint = tenantHintJson is null ? string.Empty : $",\"tenantHint\":{tenantHintJson}";
        return $"{{\"minimumProtocolVersion\":\"{minimumVersion}\""
            + $",\"maximumProtocolVersion\":\"{maximumVersion}\""
            + $",\"clientId\":\"{clientId}\""
            + $"{tenantHint},\"requiredGuarantees\":[{requiredGuarantees}]{extraJson}}}";
    }

    /// <summary>Creates a deferred malformed HTTP exception case.</summary>
    /// <param name="name">The display name.</param>
    /// <param name="act">The codec action.</param>
    /// <param name="expectedKind">The expected failure kind.</param>
    /// <param name="createCodec">The optional codec factory.</param>
    /// <returns>The deferred test case.</returns>
    private static Func<ServerHttpExceptionCase> ProtocolCase(
        string name,
        Action<HttpProtocolCodec> act,
        HttpTransportFailureKind expectedKind = HttpTransportFailureKind.ProtocolViolation,
        Func<HttpProtocolCodec>? createCodec = null)
    {
        var codecFactory = createCodec ?? CreateServerCodec;
        return () => new(
            name,
            () => CaptureHttpException(() => act(codecFactory())),
            expectedKind);
    }

    /// <summary>Creates a push request JSON document.</summary>
    /// <param name="batchId">The batch identifier text.</param>
    /// <param name="operations">The operation JSON entries.</param>
    /// <returns>The JSON document.</returns>
    private static string PushRequestJson(string batchId, params string[] operations) =>
        $"{{\"batchId\":\"{batchId}\",\"operations\":[{string.Join(',', operations)}]}}";

    /// <summary>Creates an operation JSON object.</summary>
    /// <param name="options">The optional operation fields.</param>
    /// <returns>The JSON object.</returns>
    private static string OperationJson(OperationJsonOptions? options = null)
    {
        options ??= new();
        var baseVersion = options.BaseVersion is null ? string.Empty : $",\"baseVersion\":\"{options.BaseVersion}\"";
        return $"{{\"operationId\":\"{options.OperationId}\",\"streamId\":\"{options.StreamId}\""
            + $",\"clientSequence\":{options.Sequence.ToString(CultureInfo.InvariantCulture)}"
            + $",\"timestampUtc\":\"{ServerTimestampText}\""
            + baseVersion
            + $",\"type\":{options.Type.ToString(CultureInfo.InvariantCulture)}"
            + $",\"payload\":{{\"contractId\":\"{options.ContractId}\""
            + $",\"schemaVersion\":{options.SchemaVersion.ToString(CultureInfo.InvariantCulture)}"
            + $",\"contentType\":\"{PayloadContentType}\",\"payload\":\"{options.Payload}\""
            + $",\"payloadHash\":\"{PayloadHash}\"}}"
            + $",\"policy\":{{\"deliveryGuarantee\":{options.DeliveryGuaranteeValue.ToString(CultureInfo.InvariantCulture)}"
            + $",\"durability\":{options.Durability.ToString(CultureInfo.InvariantCulture)}"
            + $",\"priority\":0,\"conflictPolicy\":{options.ConflictPolicyValue.ToString(CultureInfo.InvariantCulture)}}}"
            + $",\"metadata\":{options.Metadata}}}";
    }

    /// <summary>Creates an acknowledgement JSON document.</summary>
    /// <param name="subscriptionId">The subscription identifier text.</param>
    /// <param name="streamId">The stream identifier text.</param>
    /// <param name="cursor">The cursor text.</param>
    /// <returns>The JSON document.</returns>
    private static string AcknowledgementJson(
        string subscriptionId = "00000000-0000-0000-0000-000000000301",
        string streamId = ServerStreamName,
        string cursor = CursorOne) =>
        $"{{\"subscriptionId\":\"{subscriptionId}\",\"streamId\":\"{streamId}\",\"cursor\":\"{cursor}\"}}";

    /// <summary>Creates a subscribe response whose completion event references exceed the configured aggregate limit.</summary>
    /// <returns>The JSON document.</returns>
    private static string SubscribeResponseWithCompletedOperationsJson() =>
        $"{{\"batches\":[{{\"batchId\":\"{ServerBatchIdText}\",\"streamId\":\"{ServerStreamName}\","
        + $"\"nextCursor\":\"{CursorOne}\",\"events\":[],\"completedOperations\":["
        + $"{{\"origin\":{{\"clientId\":\"client-1\",\"operationId\":\"{ServerOperationIdText}\"}},"
        + $"\"eventIds\":[\"{ServerOperationIdText}\"]}},"
        + $"{{\"origin\":{{\"clientId\":\"client-2\",\"operationId\":\"{SecondOperationIdText}\"}},"
        + $"\"eventIds\":[\"{SecondOperationIdText}\"]}}]}}]}}";

    /// <summary>Creates a subscribe response JSON document with one event payload.</summary>
    /// <param name="payload">The base64 payload text to write.</param>
    /// <returns>The JSON document.</returns>
    private static string SubscribeResponseJson(string payload = EmptyPayloadBase64) =>
        $"{{\"batches\":[{{\"batchId\":\"{ServerBatchIdText}\",\"streamId\":\"{ServerStreamName}\","
        + $"\"nextCursor\":\"{CursorOne}\",\"events\":[{{\"eventId\":\"{ServerEventIdText}\","
        + $"\"streamId\":\"{ServerStreamName}\",\"serverCursor\":\"{CursorOne}\",\"committedAtUtc\":\"{ServerTimestampText}\","
        + $"\"payload\":{{\"contractId\":\"{ContractName}\",\"schemaVersion\":1,\"contentType\":\"{PayloadContentType}\","
        + $"\"payload\":\"{payload}\",\"payloadHash\":\"{PayloadHash}\"}},\"metadata\":{{}}}}],\"completedOperations\":[]}}]}}";

    /// <summary>Serializes a DTO with generated metadata.</summary>
    /// <typeparam name="T">The DTO type.</typeparam>
    /// <param name="value">The DTO value.</param>
    /// <param name="typeInfo">The type metadata.</param>
    /// <returns>The serialized bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] SerializeJson<T>(T value, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> typeInfo)
        where T : class =>
        Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value, typeInfo));

    /// <summary>Describes one malformed server codec HTTP exception case.</summary>
    [DebuggerDisplay("{Name}")]
    public sealed class ServerHttpExceptionCase
    {
        /// <summary>Initializes a new instance of the <see cref="ServerHttpExceptionCase"/> class.</summary>
        /// <param name="name">The display name.</param>
        /// <param name="act">The action that captures the thrown exception.</param>
        /// <param name="expectedKind">The expected failure kind.</param>
        public ServerHttpExceptionCase(string name, Func<HttpRemoteTransportException> act, HttpTransportFailureKind expectedKind)
        {
            Name = name;
            Act = act;
            ExpectedKind = expectedKind;
        }

        /// <summary>Gets the display name.</summary>
        public string Name { get; }

        /// <summary>Gets the action that captures the thrown exception.</summary>
        public Func<HttpRemoteTransportException> Act { get; }

        /// <summary>Gets the expected failure kind.</summary>
        public HttpTransportFailureKind ExpectedKind { get; }

        /// <inheritdoc/>
        public override string ToString() => Name;
    }

    /// <summary>Stores optional operation JSON fields.</summary>
    private sealed class OperationJsonOptions
    {
        /// <summary>Gets the operation identifier text.</summary>
        public string OperationId { get; init; } = ServerOperationIdText;

        /// <summary>Gets the stream identifier text.</summary>
        public string StreamId { get; init; } = ServerStreamName;

        /// <summary>Gets the optional base version text.</summary>
        public string? BaseVersion { get; init; }

        /// <summary>Gets the client sequence.</summary>
        public long Sequence { get; init; } = SingleOperation;

        /// <summary>Gets the operation type value.</summary>
        public int Type { get; init; } = (int)SyncOperationType.Append;

        /// <summary>Gets the contract identifier.</summary>
        public string ContractId { get; init; } = ContractName;

        /// <summary>Gets the schema version.</summary>
        public int SchemaVersion { get; init; } = SingleOperation;

        /// <summary>Gets the payload text.</summary>
        public string Payload { get; init; } = EmptyPayloadBase64;

        /// <summary>Gets the delivery guarantee value.</summary>
        public int DeliveryGuaranteeValue { get; init; } = (int)DeliveryGuarantee.AtLeastOnce;

        /// <summary>Gets the durability value.</summary>
        public int Durability { get; init; } = (int)OperationDurability.Durable;

        /// <summary>Gets the conflict policy value.</summary>
        public int ConflictPolicyValue { get; init; } = (int)ConflictPolicy.Merge;

        /// <summary>Gets the metadata JSON object.</summary>
        public string Metadata { get; init; } = "{\"trace\":\"1\"}";
    }

    /// <summary>Captures a single subscription request.</summary>
    private sealed class CapturingHandler : HttpMessageHandler
    {
        /// <summary>The captured query source.</summary>
        private TaskCompletionSource<string> _captured = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the captured query.</summary>
        internal string Query { get; private set; } = string.Empty;

        /// <summary>Prepares the handler for a new capture.</summary>
        /// <returns>The capture task.</returns>
        internal Task<string> Prepare()
        {
            Query = string.Empty;
            _captured = new(TaskCreationOptions.RunContinuationsAsynchronously);
            return _captured.Task;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Query = request.RequestUri?.Query ?? string.Empty;
            _ = _captured.TrySetResult(Query);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NoContent));
        }
    }
}
