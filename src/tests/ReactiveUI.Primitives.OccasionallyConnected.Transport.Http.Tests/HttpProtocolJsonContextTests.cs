// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests the authored HTTP protocol JSON metadata.</summary>
public sealed class HttpProtocolJsonContextTests
{
    /// <summary>The test base address.</summary>
    private const string BaseAddressText = "https://example.invalid/oc/";

    /// <summary>The common client identifier text.</summary>
    private const string ClientIdText = "client-1";

    /// <summary>The common content type text.</summary>
    private const string ContentTypeText = "application/json";

    /// <summary>The common current cursor text.</summary>
    private const string CursorText = "cursor-1";

    /// <summary>The common payload text.</summary>
    private const string EncodedPayloadText = "e30=";

    /// <summary>The common previous cursor text.</summary>
    private const string PreviousCursorText = "cursor-0";

    /// <summary>The common protocol version text.</summary>
    private const string ProtocolVersionText = "1.0";

    /// <summary>The common server cursor text.</summary>
    private const string ServerCursorText = "server-1";

    /// <summary>The common stream identifier text.</summary>
    private const string StreamIdText = "stream-1";

    /// <summary>The stream identifier JSON fragment.</summary>
    private const string StreamIdJsonFragment = "\",\"streamId\":\"";

    /// <summary>The common tenant hint text.</summary>
    private const string TenantHintText = "tenant-1";

    /// <summary>The common timestamp text.</summary>
    private const string TimestampText = "2026-09-13T00:00:07+00:00";

    /// <summary>The common maximum protocol version text.</summary>
    private const string MaximumProtocolVersionText = "1.1";

    /// <summary>The common server version text.</summary>
    private const string ServerVersionText = "v1";

    /// <summary>The common base version text.</summary>
    private const string BaseVersionText = "v0";

    /// <summary>The common payload contract identifier.</summary>
    private const string ContractIdText = "contract";

    /// <summary>The common payload hash text.</summary>
    private const string PayloadHashText = "sha256-test";

    /// <summary>The common metadata key text.</summary>
    private const string MetadataKeyText = "trace";

    /// <summary>The common metadata value text.</summary>
    private const string MetadataValueText = "1";

    /// <summary>The invalid base64 text.</summary>
    private const string InvalidBase64Text = "not-base64";

    /// <summary>The invalid timestamp text.</summary>
    private const string InvalidTimestampText = "not-a-date";

    /// <summary>The connect response feature flags value.</summary>
    private const int FeatureFlags = 15;

    /// <summary>The maximum batch operations value.</summary>
    private const int MaximumBatchOperationsValue = 10;

    /// <summary>The maximum batch bytes value.</summary>
    private const int MaximumBatchBytesValue = 1_024;

    /// <summary>The server idempotency retention value.</summary>
    private const int ServerIdempotencyRetentionMillisecondsValue = 60_000;

    /// <summary>The first required guarantee value.</summary>
    private const int FirstRequiredGuarantee = 1;

    /// <summary>The second required guarantee value.</summary>
    private const int SecondRequiredGuarantee = 2;

    /// <summary>The common client sequence value.</summary>
    private const long ClientSequenceValue = 7;

    /// <summary>The common operation type value.</summary>
    private const int OperationType = 1;

    /// <summary>The common schema version value.</summary>
    private const int SchemaVersionValue = 1;

    /// <summary>The common operation result kind.</summary>
    private const int OperationResultKind = 1;

    /// <summary>The common delivery guarantee value.</summary>
    private const int DeliveryGuaranteeValue = 1;

    /// <summary>The common durability value.</summary>
    private const int DurabilityValue = 1;

    /// <summary>The common priority value.</summary>
    private const int PriorityValue = 5;

    /// <summary>The common conflict policy value.</summary>
    private const int ConflictPolicyValue = 2;

    /// <summary>The deliberately restrictive JSON depth for hostile tests.</summary>
    private const int RestrictiveMaximumJsonDepth = 2;

    /// <summary>The common batch identifier text.</summary>
    private const string BatchIdText = "00000000-0000-0000-0000-000000000100";

    /// <summary>The common event identifier text.</summary>
    private const string EventIdText = "00000000-0000-0000-0000-000000000201";

    /// <summary>The common operation identifier text.</summary>
    private const string OperationIdText = "00000000-0000-0000-0000-000000000001";

    /// <summary>The common subscription identifier text.</summary>
    private const string SubscriptionIdText = "00000000-0000-0000-0000-000000000301";

    /// <summary>The shared HTTP client for codec tests.</summary>
    private static readonly HttpClient SharedHttpClient = new();

    /// <summary>Gets the golden acknowledgement request JSON.</summary>
    private static string AcknowledgeRequestJson =>
        $$"""{"subscriptionId":"{{SubscriptionIdText}}","streamId":"{{StreamIdText}}","cursor":"{{CursorText}}"}""";

    /// <summary>Gets the golden connect request JSON.</summary>
    private static string ConnectRequestJson =>
        "{\"minimumProtocolVersion\":\"" + ProtocolVersionText
        + "\",\"maximumProtocolVersion\":\"" + MaximumProtocolVersionText
        + "\",\"clientId\":\"" + ClientIdText
        + "\",\"tenantHint\":\"" + TenantHintText
        + "\",\"requiredGuarantees\":[1,2]}";

    /// <summary>Gets the golden connect response JSON.</summary>
    private static string ConnectResponseJson =>
        $$"""{"protocolVersion":"{{ProtocolVersionText}}","features":15,"maximumBatchOperations":10,"maximumBatchBytes":1024,"serverIdempotencyRetentionMilliseconds":60000}""";

    /// <summary>Gets the golden operation policy JSON.</summary>
    private static string OperationPolicyJson =>
        """{"deliveryGuarantee":1,"durability":1,"priority":5,"conflictPolicy":2}""";

    /// <summary>Gets the golden operation result JSON.</summary>
    private static string OperationSyncResultJson =>
        $$"""{"operationId":"{{OperationIdText}}","kind":1,"serverVersion":"{{ServerVersionText}}"}""";

    /// <summary>Gets the golden payload envelope JSON.</summary>
    private static string PayloadEnvelopeJson =>
        $$"""{"contractId":"{{ContractIdText}}","schemaVersion":1,"contentType":"{{ContentTypeText}}","payload":"{{EncodedPayloadText}}","payloadHash":"{{PayloadHashText}}"}""";

    /// <summary>Gets the golden push response JSON.</summary>
    private static string PushResponseJson =>
        $$"""{"batchId":"{{BatchIdText}}","operations":[{{OperationSyncResultJson}}],"serverCursor":"{{ServerCursorText}}"}""";

    /// <summary>Gets the golden remote event origin JSON.</summary>
    private static string RemoteEventOriginJson =>
        $$"""{"clientId":"{{ClientIdText}}","operationId":"{{OperationIdText}}"}""";

    /// <summary>Gets the golden remote operation completion JSON.</summary>
    private static string RemoteOperationCompletionJson =>
        $$"""{"origin":{{RemoteEventOriginJson}},"eventIds":["{{EventIdText}}"]}""";

    /// <summary>Verifies connect request JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectRequestWireUsesGoldenJson()
    {
        var value = new HttpProtocolJsonContext.ConnectRequestWire
        {
            MinimumProtocolVersion = ProtocolVersionText,
            MaximumProtocolVersion = MaximumProtocolVersionText,
            ClientId = ClientIdText,
            TenantHint = TenantHintText,
            RequiredGuarantees = [FirstRequiredGuarantee, SecondRequiredGuarantee],
        };

        await AssertJsonAsync(value, HttpProtocolJsonContext.Default.ConnectRequestWireInfo, ConnectRequestJson);
    }

    /// <summary>Verifies connect response JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConnectResponseWireUsesGoldenJson()
    {
        var value = new HttpProtocolJsonContext.ConnectResponseWire
        {
            ProtocolVersion = ProtocolVersionText,
            Features = FeatureFlags,
            MaximumBatchOperations = MaximumBatchOperationsValue,
            MaximumBatchBytes = MaximumBatchBytesValue,
            ServerIdempotencyRetentionMilliseconds = ServerIdempotencyRetentionMillisecondsValue,
        };

        await AssertJsonAsync(value, HttpProtocolJsonContext.Default.ConnectResponseWireInfo, ConnectResponseJson);
    }

    /// <summary>Verifies push request JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushRequestWireUsesGoldenJson()
    {
        var value = new HttpProtocolJsonContext.PushRequestWire { BatchId = ParseGuid(BatchIdText), Operations = [CreateOperation()] };

        await AssertJsonAsync(value, HttpProtocolJsonContext.Default.PushRequestWireInfo, PushRequestJson());
    }

    /// <summary>Verifies push response JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushResponseWireUsesGoldenJson()
    {
        var value = new HttpProtocolJsonContext.PushResponseWire { BatchId = ParseGuid(BatchIdText), Operations = [CreateOperationResult()], ServerCursor = ServerCursorText };

        await AssertJsonAsync(value, HttpProtocolJsonContext.Default.PushResponseWireInfo, PushResponseJson);
    }

    /// <summary>Verifies subscribe response JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeResponseWireUsesGoldenJson()
    {
        var value = new HttpProtocolJsonContext.SubscribeResponseWire { Batches = [CreateRemoteEventBatch()] };

        await AssertJsonAsync(value, HttpProtocolJsonContext.Default.SubscribeResponseWireInfo, SubscribeResponseJson());
    }

    /// <summary>Verifies acknowledgement request JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AcknowledgeRequestWireUsesGoldenJson()
    {
        var value = new HttpProtocolJsonContext.AcknowledgeRequestWire { SubscriptionId = ParseGuid(SubscriptionIdText), StreamId = StreamIdText, Cursor = CursorText };

        await AssertJsonAsync(value, HttpProtocolJsonContext.Default.AcknowledgeRequestWireInfo, AcknowledgeRequestJson);
    }

    /// <summary>Verifies synchronization operation JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SyncOperationWireUsesGoldenJson() =>
        AssertJsonAsync(CreateOperation(), HttpProtocolJsonContext.Default.SyncOperationWireInfo, SyncOperationJson());

    /// <summary>Verifies operation policy JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task OperationPolicyWireUsesGoldenJson() =>
        AssertJsonAsync(CreatePolicy(), HttpProtocolJsonContext.Default.OperationPolicyWireInfo, OperationPolicyJson);

    /// <summary>Verifies payload envelope JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task PayloadEnvelopeWireUsesGoldenJson() =>
        AssertJsonAsync(CreatePayload(), HttpProtocolJsonContext.Default.PayloadEnvelopeWireInfo, PayloadEnvelopeJson);

    /// <summary>Verifies operation result JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task OperationSyncResultWireUsesGoldenJson() =>
        AssertJsonAsync(CreateOperationResult(), HttpProtocolJsonContext.Default.OperationSyncResultWireInfo, OperationSyncResultJson);

    /// <summary>Verifies remote event batch JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task RemoteEventBatchWireUsesGoldenJson() =>
        AssertJsonAsync(CreateRemoteEventBatch(), HttpProtocolJsonContext.Default.RemoteEventBatchWireInfo, RemoteEventBatchJson());

    /// <summary>Verifies remote event JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task RemoteEventWireUsesGoldenJson() =>
        AssertJsonAsync(CreateRemoteEvent(), HttpProtocolJsonContext.Default.RemoteEventWireInfo, RemoteEventJson());

    /// <summary>Verifies remote event origin JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task RemoteEventOriginWireUsesGoldenJson() =>
        AssertJsonAsync(CreateOrigin(), HttpProtocolJsonContext.Default.RemoteEventOriginWireInfo, RemoteEventOriginJson);

    /// <summary>Verifies remote operation completion JSON is stable in both directions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task RemoteOperationCompletionWireUsesGoldenJson() =>
        AssertJsonAsync(CreateCompletion(), HttpProtocolJsonContext.Default.RemoteOperationCompletionWireInfo, RemoteOperationCompletionJson);

    /// <summary>Verifies duplicate protocol members are rejected instead of accepting a last-wins value.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DuplicateProtocolMemberThrowsJsonException()
    {
        const string json =
            """{"protocolVersion":"1.0","protocolVersion":"2.0","features":15,"maximumBatchOperations":10,"maximumBatchBytes":1024}""";

        await Assert.That(static () => JsonSerializer.Deserialize(json, HttpProtocolJsonContext.Default.ConnectResponseWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies duplicate nested metadata members are rejected before materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DuplicateNestedMemberThrowsJsonException()
    {
        var json = SyncOperationJson().Replace(MetadataJson(), DuplicateMetadataJson(), StringComparison.Ordinal);

        await Assert.That(() => JsonSerializer.Deserialize(json, HttpProtocolJsonContext.Default.SyncOperationWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies unknown non-duplicate members remain wire-compatible.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task UnknownMemberIsIgnored()
    {
        const string json =
            """{"protocolVersion":"1.0","features":15,"unknown":true,"maximumBatchOperations":10,"maximumBatchBytes":1024,"serverIdempotencyRetentionMilliseconds":60000}""";
        var value = JsonSerializer.Deserialize(json, HttpProtocolJsonContext.Default.ConnectResponseWireInfo);

        await Assert.That(JsonSerializer.Serialize(Require(value), HttpProtocolJsonContext.Default.ConnectResponseWireInfo)).IsEqualTo(ConnectResponseJson);
    }

    /// <summary>Verifies null for required protocol objects is rejected.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RequiredObjectNullThrowsJsonException()
    {
        var json = SyncOperationJson().Replace(PayloadEnvelopeJson, "null", StringComparison.Ordinal);

        await Assert.That(() => JsonSerializer.Deserialize(json, HttpProtocolJsonContext.Default.SyncOperationWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies number fields reject JSON strings.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task NumberStringThrowsJsonException()
    {
        const string json = """{"protocolVersion":"1.0","features":"15","maximumBatchOperations":10,"maximumBatchBytes":1024}""";

        await Assert.That(static () => JsonSerializer.Deserialize(json, HttpProtocolJsonContext.Default.ConnectResponseWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies malformed timestamps stay on the JSON exception path.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task InvalidTimestampThrowsJsonException()
    {
        var json = RemoteEventJson().Replace(TimestampText, InvalidTimestampText, StringComparison.Ordinal);

        await Assert.That(() => JsonSerializer.Deserialize(json, HttpProtocolJsonContext.Default.RemoteEventWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies malformed base64 payloads surface as protocol violations through the codec.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task InvalidBase64PayloadThrowsProtocolViolation()
    {
        var codec = new HttpProtocolCodec(CreateOptions(SharedHttpClient));
        var json = SubscribeResponseJson().Replace(EncodedPayloadText, InvalidBase64Text, StringComparison.Ordinal);

        var exception = CaptureHttpException(() => codec.DeserializeSubscribeResponse(Encoding.UTF8.GetBytes(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies codec depth bounds run before metadata materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ExcessiveJsonDepthThrowsProtocolViolation()
    {
        var codec = new HttpProtocolCodec(CreateOptions(SharedHttpClient) with { MaximumJsonDepth = RestrictiveMaximumJsonDepth });

        var exception = CaptureHttpException(() => codec.DeserializeSubscribeResponse(Encoding.UTF8.GetBytes(SubscribeResponseJson())));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies absent members use the same CLR defaults as the generated context.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AbsentMembersUseClosedMetadataDefaults()
    {
        const string EmptyObjectJson = "{}";
        var connect = Require(JsonSerializer.Deserialize(EmptyObjectJson, HttpProtocolJsonContext.Default.ConnectRequestWireInfo));
        var response = Require(JsonSerializer.Deserialize(EmptyObjectJson, HttpProtocolJsonContext.Default.ConnectResponseWireInfo));
        var operation = Require(JsonSerializer.Deserialize(EmptyObjectJson, HttpProtocolJsonContext.Default.SyncOperationWireInfo));
        var subscribe = Require(JsonSerializer.Deserialize(EmptyObjectJson, HttpProtocolJsonContext.Default.SubscribeResponseWireInfo));
        var completion = Require(JsonSerializer.Deserialize(EmptyObjectJson, HttpProtocolJsonContext.Default.RemoteOperationCompletionWireInfo));
        var remote = Require(JsonSerializer.Deserialize(EmptyObjectJson, HttpProtocolJsonContext.Default.RemoteEventWireInfo));

        await Assert.That(connect.ClientId).IsEqualTo(string.Empty);
        await Assert.That(connect.RequiredGuarantees.Length).IsEqualTo(0);
        await Assert.That(response.MaximumBatchOperations).IsEqualTo(0);
        await Assert.That(response.MaximumBatchBytes).IsEqualTo(0);
        await Assert.That(operation.OperationId).IsEqualTo(Guid.Empty);
        await Assert.That(operation.ClientSequence).IsEqualTo(0);
        await Assert.That(operation.TimestampUtc).IsEqualTo(default);
        await Assert.That(operation.Payload.ContractId).IsEqualTo(string.Empty);
        await Assert.That(operation.Policy.Priority).IsEqualTo(0);
        await Assert.That(operation.Metadata.Count).IsEqualTo(0);
        await Assert.That(subscribe.Batches.Length).IsEqualTo(0);
        await Assert.That(completion.EventIds.Length).IsEqualTo(0);
        await Assert.That(remote.Payload.ContractId).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies optional members are omitted when null.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task OptionalMembersAreOmittedWhenNull()
    {
        var value = CreateRemoteEvent();
        value.CausedByOperationId = null;
        value.Origin = null;

        var json = JsonSerializer.Serialize(value, HttpProtocolJsonContext.Default.RemoteEventWireInfo);

        await Assert.That(json.Contains("causedByOperationId", StringComparison.Ordinal)).IsFalse();
        await Assert.That(json.Contains("\"origin\"", StringComparison.Ordinal)).IsFalse();
    }

    /// <summary>Verifies empty JSON is rejected by the converter.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task EmptyJsonThrowsJsonException()
    {
        const string EmptyJson = "";

        await Assert.That(static () => JsonSerializer.Deserialize(EmptyJson, HttpProtocolJsonContext.Default.ConnectRequestWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies trailing JSON content is rejected by the converter.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task TrailingJsonThrowsJsonException()
    {
        const string Json = "{}{}";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.ConnectRequestWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies malformed object values are rejected by the duplicate scanner.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MissingPropertyValueThrowsJsonException()
    {
        const string Json = """{"protocolVersion":""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.ConnectResponseWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies unterminated objects are rejected by the duplicate scanner.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task UnterminatedObjectThrowsJsonException()
    {
        const string Json = "{\"protocolVersion\":\"1.0\"";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.ConnectResponseWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies root arrays are rejected for DTOs.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RootArrayThrowsJsonException()
    {
        const string Json = "[]";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.ConnectResponseWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies required string fields reject non-string JSON.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task StringNumberThrowsJsonException()
    {
        const string Json = """{"protocolVersion":1}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.ConnectResponseWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies GUID fields reject non-string JSON.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GuidNumberThrowsJsonException()
    {
        const string Json = """{"subscriptionId":1}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.AcknowledgeRequestWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies timestamp fields reject non-string JSON.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task TimestampNumberThrowsJsonException()
    {
        const string Json = """{"committedAtUtc":1}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.RemoteEventWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies DTO array fields reject non-array JSON.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ObjectArrayShapeThrowsJsonException()
    {
        const string Json = """{"batches":{}}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.SubscribeResponseWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies integer array fields reject non-array JSON.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task IntArrayShapeThrowsJsonException()
    {
        const string Json = """{"requiredGuarantees":{}}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.ConnectRequestWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies GUID array fields reject non-array JSON.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GuidArrayShapeThrowsJsonException()
    {
        const string Json = """{"eventIds":{}}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.RemoteOperationCompletionWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies metadata rejects non-object JSON.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MetadataArrayThrowsJsonException()
    {
        const string Json = """{"metadata":[]}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.RemoteEventWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies metadata values reject non-string JSON.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MetadataNumberValueThrowsJsonException()
    {
        const string Json = """{"metadata":{"trace":1}}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.RemoteEventWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies optional object members reject non-object JSON.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task OptionalObjectNumberThrowsJsonException()
    {
        const string Json = """{"origin":1}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.RemoteEventWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies root primitive values are rejected for DTOs.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RootNumberThrowsJsonException()
    {
        const string Json = "1";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.ConnectResponseWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies optional string fields reject non-string JSON.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task OptionalStringNumberThrowsJsonException()
    {
        const string Json = """{"serverCursor":1}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.PushResponseWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies required long fields reject JSON strings.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task Int64StringThrowsJsonException()
    {
        const string Json = """{"clientSequence":"7"}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.SyncOperationWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies optional long fields reject JSON strings.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task OptionalInt64StringThrowsJsonException()
    {
        const string Json = """{"serverIdempotencyRetentionMilliseconds":"60000"}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.ConnectResponseWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies optional GUID fields reject non-string JSON.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task OptionalGuidNumberThrowsJsonException()
    {
        const string Json = """{"causedByOperationId":1}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.RemoteEventWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies integer arrays reject non-number elements.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task IntArrayStringElementThrowsJsonException()
    {
        const string Json = """{"requiredGuarantees":["1"]}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.ConnectRequestWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Verifies GUID arrays reject non-string elements.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GuidArrayNumberElementThrowsJsonException()
    {
        const string Json = """{"eventIds":[1]}""";

        await Assert.That(static () => JsonSerializer.Deserialize(Json, HttpProtocolJsonContext.Default.RemoteOperationCompletionWireInfo))
            .ThrowsExactly<JsonException>();
    }

    /// <summary>Asserts bidirectional golden JSON behavior.</summary>
    /// <typeparam name="T">The DTO type.</typeparam>
    /// <param name="value">The DTO.</param>
    /// <param name="typeInfo">The metadata.</param>
    /// <param name="expectedJson">The expected JSON.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertJsonAsync<T>(T value, JsonTypeInfo<T> typeInfo, string expectedJson)
        where T : class
    {
        var serialized = JsonSerializer.Serialize(value, typeInfo);
        var deserialized = Require(JsonSerializer.Deserialize(expectedJson, typeInfo));

        await Assert.That(serialized).IsEqualTo(expectedJson);
        await Assert.That(JsonSerializer.Serialize(deserialized, typeInfo)).IsEqualTo(expectedJson);
    }

    /// <summary>Requires a deserialized value.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="value">The value.</param>
    /// <returns>The required value.</returns>
    /// <exception cref="InvalidOperationException">The value is null.</exception>
    private static T Require<T>(T? value)
        where T : class
    {
        if (value is not null)
        {
            return value;
        }

        throw new InvalidOperationException("Expected a deserialized value.");
    }

    /// <summary>Captures an HTTP transport exception.</summary>
    /// <param name="action">The action.</param>
    /// <returns>The captured exception.</returns>
    /// <exception cref="InvalidOperationException">The action did not throw the expected exception.</exception>
    private static HttpRemoteTransportException CaptureHttpException(Action action)
    {
        try
        {
            action();
        }
        catch (HttpRemoteTransportException exception)
        {
            return exception;
        }

        throw new InvalidOperationException("Expected an HTTP transport exception.");
    }

    /// <summary>Creates adapter options for codec tests.</summary>
    /// <param name="httpClient">The HTTP client.</param>
    /// <returns>The options.</returns>
    private static HttpRemoteTransportOptions CreateOptions(HttpClient httpClient) =>
        new() { HttpClient = httpClient, BaseAddress = new(BaseAddressText) };

    /// <summary>Creates a common synchronization operation DTO.</summary>
    /// <returns>The DTO.</returns>
    private static HttpProtocolJsonContext.SyncOperationWire CreateOperation() =>
        new()
        {
            OperationId = ParseGuid(OperationIdText),
            StreamId = StreamIdText,
            ClientSequence = ClientSequenceValue,
            TimestampUtc = ParseTimestamp(),
            BaseVersion = BaseVersionText,
            Type = OperationType,
            Payload = CreatePayload(),
            Policy = CreatePolicy(),
            Metadata = CreateMetadata(),
        };

    /// <summary>Creates a common operation policy DTO.</summary>
    /// <returns>The DTO.</returns>
    private static HttpProtocolJsonContext.OperationPolicyWire CreatePolicy() =>
        new() { DeliveryGuarantee = DeliveryGuaranteeValue, Durability = DurabilityValue, Priority = PriorityValue, ConflictPolicy = ConflictPolicyValue };

    /// <summary>Creates a common payload envelope DTO.</summary>
    /// <returns>The DTO.</returns>
    private static HttpProtocolJsonContext.PayloadEnvelopeWire CreatePayload() =>
        new() { ContractId = ContractIdText, SchemaVersion = SchemaVersionValue, ContentType = ContentTypeText, Payload = EncodedPayloadText, PayloadHash = PayloadHashText };

    /// <summary>Creates a common operation result DTO.</summary>
    /// <returns>The DTO.</returns>
    private static HttpProtocolJsonContext.OperationSyncResultWire CreateOperationResult() =>
        new() { OperationId = ParseGuid(OperationIdText), Kind = OperationResultKind, ServerVersion = ServerVersionText };

    /// <summary>Creates a common remote event batch DTO.</summary>
    /// <returns>The DTO.</returns>
    private static HttpProtocolJsonContext.RemoteEventBatchWire CreateRemoteEventBatch() =>
        new()
        {
            BatchId = ParseGuid(BatchIdText),
            StreamId = StreamIdText,
            PreviousCursor = PreviousCursorText,
            NextCursor = CursorText,
            Events = [CreateRemoteEvent()],
            CompletedOperations = [CreateCompletion()],
        };

    /// <summary>Creates a common remote event DTO.</summary>
    /// <returns>The DTO.</returns>
    private static HttpProtocolJsonContext.RemoteEventWire CreateRemoteEvent() =>
        new()
        {
            EventId = ParseGuid(EventIdText),
            StreamId = StreamIdText,
            ServerCursor = CursorText,
            CommittedAtUtc = ParseTimestamp(),
            CausedByOperationId = ParseGuid(OperationIdText),
            Origin = CreateOrigin(),
            Payload = CreatePayload(),
            Metadata = CreateMetadata(),
        };

    /// <summary>Creates a common remote event origin DTO.</summary>
    /// <returns>The DTO.</returns>
    private static HttpProtocolJsonContext.RemoteEventOriginWire CreateOrigin() =>
        new() { ClientId = ClientIdText, OperationId = ParseGuid(OperationIdText) };

    /// <summary>Creates a common remote operation completion DTO.</summary>
    /// <returns>The DTO.</returns>
    private static HttpProtocolJsonContext.RemoteOperationCompletionWire CreateCompletion() =>
        new() { Origin = CreateOrigin(), EventIds = [ParseGuid(EventIdText)] };

    /// <summary>Creates common metadata.</summary>
    /// <returns>The metadata.</returns>
    private static Dictionary<string, string> CreateMetadata()
    {
        Dictionary<string, string> metadata = [with(comparer: StringComparer.Ordinal)];
        metadata.Add(MetadataKeyText, MetadataValueText);
        return metadata;
    }

    /// <summary>Parses a stable GUID literal.</summary>
    /// <param name="value">The GUID text.</param>
    /// <returns>The GUID.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Guid ParseGuid(string value) => Guid.Parse(value);

    /// <summary>Parses the stable timestamp literal.</summary>
    /// <returns>The timestamp.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DateTimeOffset ParseTimestamp() => DateTimeOffset.Parse(TimestampText, CultureInfo.InvariantCulture);

    /// <summary>Creates the push request JSON.</summary>
    /// <returns>The JSON.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string PushRequestJson() =>
        $$"""{"batchId":"{{BatchIdText}}","operations":[{{SyncOperationJson()}}]}""";

    /// <summary>Creates the subscribe response JSON.</summary>
    /// <returns>The JSON.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string SubscribeResponseJson() =>
        $$"""{"batches":[{{RemoteEventBatchJson()}}]}""";

    /// <summary>Creates the synchronization operation JSON.</summary>
    /// <returns>The JSON.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string SyncOperationJson() =>
        "{\"operationId\":\"" + OperationIdText + StreamIdJsonFragment + StreamIdText
        + "\",\"clientSequence\":" + ClientSequenceValue
        + ",\"timestampUtc\":\"" + TimestampText
        + "\",\"baseVersion\":\"" + BaseVersionText
        + "\",\"type\":" + OperationType
        + ",\"payload\":" + PayloadEnvelopeJson
        + ",\"policy\":" + OperationPolicyJson
        + ",\"metadata\":" + MetadataJson() + '}';

    /// <summary>Creates the remote event batch JSON.</summary>
    /// <returns>The JSON.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string RemoteEventBatchJson() =>
        "{\"batchId\":\"" + BatchIdText + StreamIdJsonFragment + StreamIdText
        + "\",\"previousCursor\":\"" + PreviousCursorText
        + "\",\"nextCursor\":\"" + CursorText
        + "\",\"events\":[" + RemoteEventJson()
        + "],\"completedOperations\":[" + RemoteOperationCompletionJson + "]}";

    /// <summary>Creates the remote event JSON.</summary>
    /// <returns>The JSON.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string RemoteEventJson() =>
        "{\"eventId\":\"" + EventIdText + StreamIdJsonFragment + StreamIdText
        + "\",\"serverCursor\":\"" + CursorText
        + "\",\"committedAtUtc\":\"" + TimestampText
        + "\",\"causedByOperationId\":\"" + OperationIdText
        + "\",\"origin\":" + RemoteEventOriginJson
        + ",\"payload\":" + PayloadEnvelopeJson
        + ",\"metadata\":" + MetadataJson() + '}';

    /// <summary>Creates the metadata JSON.</summary>
    /// <returns>The JSON.</returns>
    private static string MetadataJson() => $$"""{"{{MetadataKeyText}}":"{{MetadataValueText}}"}""";

    /// <summary>Creates duplicate metadata JSON.</summary>
    /// <returns>The JSON.</returns>
    private static string DuplicateMetadataJson() => $$"""{"{{MetadataKeyText}}":"{{MetadataValueText}}","{{MetadataKeyText}}":"2"}""";
}
