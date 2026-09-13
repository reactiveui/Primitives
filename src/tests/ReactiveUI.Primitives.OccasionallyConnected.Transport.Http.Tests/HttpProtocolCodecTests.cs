// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpProtocolCodec"/>.</summary>
public sealed class HttpProtocolCodecTests
{
    /// <summary>The valid encoded empty JSON payload.</summary>
    private const string EmptyPayloadBase64 = "e30=";

    /// <summary>The invalid protocol version response.</summary>
    private const string InvalidProtocolVersionJson = """
        {"protocolVersion":"not-a-version","features":0,"maximumBatchOperations":1,"maximumBatchBytes":1}
        """;

    /// <summary>A base64 payload whose text cannot fit the small payload byte limit.</summary>
    private const string Base64TextBeyondSmallPayloadLimit = "AAAAAA==";

    /// <summary>The response with negative server retention.</summary>
    private const string NegativeServerRetentionJson = """
        {"protocolVersion":"1.0","features":0,"maximumBatchOperations":1,"maximumBatchBytes":1,"serverIdempotencyRetentionMilliseconds":-1}
        """;

    /// <summary>The response with negative inbox retention.</summary>
    private const string NegativeInboxRetentionJson = """
        {"protocolVersion":"1.0","features":0,"maximumBatchOperations":1,"maximumBatchBytes":1,"clientInboxRetentionRequiredMilliseconds":-1}
        """;

    /// <summary>The response with omitted optional retention values.</summary>
    private const string OmittedRetentionJson = """
        {"protocolVersion":"1.0","features":0,"maximumBatchOperations":1,"maximumBatchBytes":1}
        """;

    /// <summary>The JSON null document.</summary>
    private const string NullJson = "null";

    /// <summary>The JSON array document.</summary>
    private const string ArrayJson = "[]";

    /// <summary>The malformed JSON document.</summary>
    private const string MalformedJson = "{\"protocolVersion\":";

    /// <summary>The shared stream identifier.</summary>
    private const string StreamName = "stream-1";

    /// <summary>The contract identifier.</summary>
    private const string ContractName = "contract";

    /// <summary>The JSON payload content type.</summary>
    private const string PayloadContentType = "application/json";

    /// <summary>The test payload hash.</summary>
    private const string PayloadHash = "sha256-test";

    /// <summary>The small payload limit.</summary>
    private const int SmallPayloadBytes = 1;

    /// <summary>The small metadata entry limit.</summary>
    private const int SmallMetadataEntries = 1;

    /// <summary>The small metadata key byte limit.</summary>
    private const int SmallMetadataKeyBytes = 1;

    /// <summary>The small metadata value byte limit.</summary>
    private const int SmallMetadataValueBytes = 1;

    /// <summary>The small JSON depth limit.</summary>
    private const int SmallJsonDepth = 1;

    /// <summary>The maximum operation count for push response tests.</summary>
    private const int SingleOperation = 1;

    /// <summary>The default HTTPS base address.</summary>
    private static readonly Uri BaseAddress = new("https://example.invalid/oc/");

    /// <summary>The shared HTTP client required by options construction.</summary>
    private static readonly HttpClient SharedHttpClient = new();

    /// <summary>A valid operation id used by wire responses.</summary>
    private static readonly Guid OperationGuid = Guid.Parse("00000000-0000-0000-0000-000000000001");

    /// <summary>A valid event id used by wire responses.</summary>
    private static readonly Guid EventGuid = Guid.Parse("00000000-0000-0000-0000-000000000002");

    /// <summary>The valid batch id used by wire responses.</summary>
    private static readonly Guid BatchGuid = Guid.Parse("00000000-0000-0000-0000-000000000003");

    /// <summary>Provides invalid metadata cases.</summary>
    /// <returns>The metadata cases.</returns>
    public static IEnumerable<Func<MetadataLimitCase>> MetadataLimitCases()
    {
        yield return static () => new(
            SubscribeResponseJson(metadata: "\"a\":\"1\",\"b\":\"2\""),
            static options => options with { MaximumMetadataEntries = SmallMetadataEntries });
        yield return static () => new(
            SubscribeResponseJson(metadata: "\"aa\":\"1\""),
            static options => options with { MaximumMetadataKeyBytes = SmallMetadataKeyBytes });
        yield return static () => new(
            SubscribeResponseJson(metadata: "\"a\":\"22\""),
            static options => options with { MaximumMetadataValueBytes = SmallMetadataValueBytes });
    }

    /// <summary>Verifies invalid connect protocol versions are rejected.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeConnectResponseRejectsInvalidProtocolVersion()
    {
        var exception = CaptureConnectProtocolViolation(CreateCodec(), InvalidProtocolVersionJson);

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies negative optional retention values are rejected.</summary>
    /// <param name="json">The connect response body.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(NegativeServerRetentionJson)]
    [Arguments(NegativeInboxRetentionJson)]
    public async Task DeserializeConnectResponseRejectsNegativeRetention(string json)
    {
        var exception = CaptureConnectProtocolViolation(CreateCodec(), json);

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.IsTransient).IsFalse();
    }

    /// <summary>Verifies omitted optional retention values remain unset.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeConnectResponseAllowsOmittedRetention()
    {
        var capabilities = CreateCodec().DeserializeConnectResponse(Encode(OmittedRetentionJson));

        await Assert.That(capabilities.ServerIdempotencyRetention).IsNull();
        await Assert.That(capabilities.ClientInboxRetentionRequired).IsNull();
    }

    /// <summary>Verifies null and incompatible JSON documents are rejected before DTO use.</summary>
    /// <param name="json">The connect response body.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(NullJson)]
    [Arguments(ArrayJson)]
    [Arguments(MalformedJson)]
    public async Task DeserializeConnectResponseRejectsInvalidDocuments(string json)
    {
        var exception = CaptureConnectProtocolViolation(CreateCodec(), json);

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies subscribe responses may carry an empty body.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSubscribeResponseAllowsEmptyBody()
    {
        var batches = CreateCodec().DeserializeSubscribeResponse([]);

        await Assert.That(batches).IsEmpty();
    }

    /// <summary>Verifies JSON depth is bounded before materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSubscribeResponseRejectsExcessiveJsonDepth()
    {
        var codec = CreateCodec(static options => options with { MaximumJsonDepth = SmallJsonDepth });

        var exception = CaptureHttpException(() => codec.DeserializeSubscribeResponse(Encode("{\"batches\":[[]]}")));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies push responses cannot exceed the negotiated operation count.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializePushResponseRejectsTooManyOperationResults()
    {
        var codec = CreateCodec(static options => options with { MaximumBatchOperations = SingleOperation });
        var batch = CreateBatch();
        var json = "{\"batchId\":\"" + batch.BatchId.ToString("D", System.Globalization.CultureInfo.InvariantCulture)
            + "\",\"operations\":[{\"operationId\":\"" + OperationGuid.ToString("D", System.Globalization.CultureInfo.InvariantCulture)
            + "\",\"kind\":0},{\"operationId\":\"00000000-0000-0000-0000-000000000099\",\"kind\":0}]}";

        var exception = CaptureHttpException(() => codec.DeserializePushResponse(batch, Encode(json), retryAfter: null));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies receive payloads are rejected when the encoded payload cannot fit.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSubscribeResponseRejectsPayloadBase64BeyondLimit()
    {
        var codec = CreateCodec(static options => options with { MaximumPayloadBytes = SmallPayloadBytes });

        var exception = CaptureHttpException(() => codec.DeserializeSubscribeResponse(Encode(SubscribeResponseJson(payload: Base64TextBeyondSmallPayloadLimit))));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies receive payloads are rejected when base64 is malformed.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSubscribeResponseRejectsMalformedPayloadBase64()
    {
        var exception = CaptureHttpException(static () => CreateCodec().DeserializeSubscribeResponse(Encode(SubscribeResponseJson(payload: "not base64"))));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies decoded receive payloads are bounded.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSubscribeResponseRejectsDecodedPayloadBeyondLimit()
    {
        var codec = CreateCodec(static options => options with { MaximumPayloadBytes = SmallPayloadBytes });

        var exception = CaptureHttpException(() => codec.DeserializeSubscribeResponse(Encode(SubscribeResponseJson(payload: "AAA="))));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies receive metadata count and byte limits are enforced.</summary>
    /// <param name="testCase">The metadata limit case.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(MetadataLimitCases))]
    public async Task DeserializeSubscribeResponseRejectsInvalidMetadata(MetadataLimitCase testCase)
    {
        var codec = CreateCodec(testCase.Configure);

        var exception = CaptureHttpException(() => codec.DeserializeSubscribeResponse(Encode(testCase.Json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies null receive metadata values are treated as protocol violations.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSubscribeResponseRejectsNullMetadataValue()
    {
        var exception = CaptureHttpException(static () => CreateCodec().DeserializeSubscribeResponse(Encode(SubscribeResponseJson(metadata: "\"trace\":null"))));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies receive events without optional operation and origin fields round-trip as remote events.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSubscribeResponseAllowsServerOriginatedEventsWithoutOrigin()
    {
        var batches = CreateCodec().DeserializeSubscribeResponse(Encode(SubscribeResponseJson(includeCausedByOperationId: false, includeOrigin: false)));

        await Assert.That(batches).Count().IsEqualTo(SingleOperation);
        await Assert.That(batches[0].Events).Count().IsEqualTo(SingleOperation);
        await Assert.That(batches[0].Events[0].CausedByOperationId).IsNull();
        await Assert.That(batches[0].Events[0].Origin).IsNull();
    }

    /// <summary>Creates a codec for default options.</summary>
    /// <returns>The codec.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpProtocolCodec CreateCodec() => CreateCodec(static options => options);

    /// <summary>Creates a codec for customized options.</summary>
    /// <param name="configure">The options customizer.</param>
    /// <returns>The codec.</returns>
    private static HttpProtocolCodec CreateCodec(Func<HttpRemoteTransportOptions, HttpRemoteTransportOptions> configure)
    {
        var options = new HttpRemoteTransportOptions { HttpClient = SharedHttpClient, BaseAddress = BaseAddress };
        return new(configure(options));
    }

    /// <summary>Creates the pushed batch used for response validation.</summary>
    /// <returns>The sync batch.</returns>
    private static SyncBatch CreateBatch()
    {
        SyncOperation operation = new()
        {
            OperationId = new(OperationGuid),
            StreamId = new(StreamName),
            ClientSequence = SingleOperation,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Append,
            Payload = new(ContractName, SingleOperation, PayloadContentType, "{}"u8.ToArray(), PayloadHash),
            Metadata = new Dictionary<string, string> { ["trace"] = "1" },
        };
        return new(BatchGuid, [operation]);
    }

    /// <summary>Creates a subscribe response body.</summary>
    /// <param name="payload">The encoded payload.</param>
    /// <param name="metadata">The metadata JSON entries.</param>
    /// <param name="includeCausedByOperationId">Whether to include the caused-by operation id.</param>
    /// <param name="includeOrigin">Whether to include the origin.</param>
    /// <returns>The JSON body.</returns>
    private static string SubscribeResponseJson(
        string payload = EmptyPayloadBase64,
        string metadata = "\"trace\":\"1\"",
        bool includeCausedByOperationId = true,
        bool includeOrigin = true)
    {
        var causedBy = includeCausedByOperationId ? $",\"causedByOperationId\":\"{OperationGuid:D}\"" : string.Empty;
        var origin = includeOrigin ? $",\"origin\":{{\"clientId\":\"client-1\",\"operationId\":\"{OperationGuid:D}\"}}" : string.Empty;
        return "{\"batches\":[{\"batchId\":\"" + BatchGuid.ToString("D", System.Globalization.CultureInfo.InvariantCulture)
            + "\",\"streamId\":\"stream-1\",\"nextCursor\":\"cursor-1\",\"events\":[{\"eventId\":\""
            + EventGuid.ToString("D", System.Globalization.CultureInfo.InvariantCulture)
            + "\",\"streamId\":\"stream-1\",\"serverCursor\":\"cursor-1\",\"committedAtUtc\":\"1970-01-01T00:00:00+00:00\""
            + causedBy
            + origin
            + ",\"payload\":{\"contractId\":\"contract\",\"schemaVersion\":1,\"contentType\":\"application/json\",\"payload\":\""
            + payload
            + "\",\"payloadHash\":\"sha256-test\"},\"metadata\":{"
            + metadata
            + "}}],\"completedOperations\":[]}]}";
    }

    /// <summary>Encodes a JSON string as UTF-8 bytes.</summary>
    /// <param name="json">The JSON string.</param>
    /// <returns>The encoded bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] Encode(string json) => Encoding.UTF8.GetBytes(json);

    /// <summary>Captures a protocol violation while deserializing a connect response.</summary>
    /// <param name="codec">The codec.</param>
    /// <param name="json">The response JSON.</param>
    /// <returns>The captured exception.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpRemoteTransportException CaptureConnectProtocolViolation(HttpProtocolCodec codec, string json) =>
        CaptureHttpException(() => codec.DeserializeConnectResponse(Encode(json)));

    /// <summary>Captures an HTTP transport exception from a synchronous action.</summary>
    /// <param name="action">The action under test.</param>
    /// <returns>The captured exception.</returns>
    /// <exception cref="InvalidOperationException">The action completed successfully.</exception>
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

    /// <summary>Describes one invalid metadata case.</summary>
    /// <param name="Json">The response JSON.</param>
    /// <param name="Configure">The codec configuration.</param>
    [DebuggerDisplay("{Json,nq}")]
    public sealed record MetadataLimitCase(string Json, Func<HttpRemoteTransportOptions, HttpRemoteTransportOptions> Configure);
}
