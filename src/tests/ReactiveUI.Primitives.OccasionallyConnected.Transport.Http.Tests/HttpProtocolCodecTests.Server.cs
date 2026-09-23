// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Globalization;
using System.Net.Http;
namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpProtocolCodec"/>.</summary>
public sealed partial class HttpProtocolCodecTests
{
    /// <summary>The test kilobyte size.</summary>
    private const int TestKilobyte = 1024;

    /// <summary>The second test sequence number.</summary>
    private const int SecondSequence = 2;

    /// <summary>The invalid enum test value.</summary>
    private const int InvalidEnumValue = 99;

    /// <summary>The sequence start position query text.</summary>
    private const string QuerySequenceText = "42";

    /// <summary>The invalid capabilities bit field value.</summary>
    private const int InvalidFeatureValue = 1_073_741_824;

    /// <summary>The expected receive batch count.</summary>
    private const int ExpectedReceiveBatchCount = 2;

    /// <summary>The query capture timeout in seconds.</summary>
    private const int QueryCaptureTimeoutSeconds = 10;

    /// <summary>The advertised test batch operation count.</summary>
    private const int TestCapabilityBatchOperations = 10;

    /// <summary>The test request and response kilobytes.</summary>
    private const int TestMessageKilobytes = 32;

    /// <summary>The test payload kilobytes.</summary>
    private const int TestPayloadKilobytes = 8;

    /// <summary>The test metadata entry count.</summary>
    private const int TestMetadataEntries = 8;

    /// <summary>The test metadata key byte count.</summary>
    private const int TestMetadataKeyBytes = 64;

    /// <summary>The test metadata value byte count.</summary>
    private const int TestMetadataValueBytes = 256;

    /// <summary>The shared maximum collection count.</summary>
    private const int TestMaximumCollectionCount = 8;

    /// <summary>The shared protocol JSON depth.</summary>
    private const int TestJsonDepth = 32;

    /// <summary>The shared test stream name.</summary>
    private const string ServerStreamName = "stream-1";

    /// <summary>The alternate test stream name.</summary>
    private const string AlternateServerStreamName = "stream-2";

    /// <summary>The wire client identifier used in request JSON.</summary>
    private const string WireClientId = "client";

    /// <summary>The server cursor used in response JSON.</summary>
    private const string ServerCursor = "server-1";

    /// <summary>The event identifier text used in response JSON.</summary>
    private const string ServerEventIdText = "00000000-0000-0000-0000-000000000201";

    /// <summary>The sentinel payload value that must not leak through sanitized protocol failures.</summary>
    private const string PayloadLeakSentinel = "credential-token";

    /// <summary>The malformed identifier text used by negative tests.</summary>
    private const string InvalidWireText = "x";

    /// <summary>The malformed stream identifier text used by negative tests.</summary>
    private const string InvalidStreamName = "bad stream";

    /// <summary>The shared server version returned by result tests.</summary>
    private const string ServerVersion = "v1";

    /// <summary>The empty JSON object payload.</summary>
    private const string EmptyJsonPayload = "{}";

    /// <summary>The first byte in the oversized payload test data.</summary>
    private const byte FirstPayloadByte = 1;

    /// <summary>The second byte in the oversized payload test data.</summary>
    private const byte SecondPayloadByte = 2;

    /// <summary>The shared test timestamp text.</summary>
    private const string ServerTimestampText = "2026-09-13T00:00:00+00:00";

    /// <summary>The shared trace metadata key.</summary>
    private const string TraceMetadataKey = "trace";

    /// <summary>The server batch identifier text.</summary>
    private const string ServerBatchIdText = "00000000-0000-0000-0000-000000000100";

    /// <summary>The server operation identifier text.</summary>
    private const string ServerOperationIdText = "00000000-0000-0000-0000-000000000001";

    /// <summary>The second server operation identifier text.</summary>
    private const string SecondOperationIdText = "00000000-0000-0000-0000-000000000002";

    /// <summary>The third server operation identifier text.</summary>
    private const string ThirdOperationIdText = "00000000-0000-0000-0000-000000000003";

    /// <summary>The empty GUID text.</summary>
    private const string EmptyGuidText = "00000000-0000-0000-0000-000000000000";

    /// <summary>The first test cursor.</summary>
    private const string CursorOne = "cursor-1";

    /// <summary>The second test cursor.</summary>
    private const string CursorTwo = "cursor-2";

    /// <summary>The third test cursor.</summary>
    private const string CursorThree = "cursor-3";

    /// <summary>The resume cursor test value.</summary>
    private const string ResumeCursor = "resume/1";

    /// <summary>The anchor cursor test value.</summary>
    private const string AnchorCursor = "anchor/1";

    /// <summary>The subscribe query required prefix.</summary>
    private const string SubscribeQueryPrefix =
        "streamId=stream-1&subscriptionId=00000000-0000-0000-0000-000000000301&positionKind=0";

    /// <summary>The subscribe query prefix without a position kind.</summary>
    private const string SubscribeMissingPositionQuery =
        "streamId=stream-1&subscriptionId=00000000-0000-0000-0000-000000000301";

    /// <summary>The subscribe query prefix with a leading question mark and without a position kind.</summary>
    private const string SubscribeMissingPositionQueryWithPrefix = $"?{SubscribeMissingPositionQuery}";

    /// <summary>The smallest payload byte limit whose base64 character budget overflows an integer multiplication.</summary>
    private const int Base64MultiplicationOverflowPayloadBytes = 1_610_612_734;

    /// <summary>The largest whole-millisecond duration representable by <see cref="TimeSpan"/>.</summary>
    private static readonly long MaximumWholeTimeSpanMilliseconds = TimeSpan.MaxValue.Ticks / TimeSpan.TicksPerMillisecond;

    /// <summary>The shared query capture handler.</summary>
    private static readonly CapturingHandler QueryCaptureHandler = new();

    /// <summary>The shared query capture client.</summary>
    private static readonly HttpClient QueryCaptureClient = new(QueryCaptureHandler);

    /// <summary>The shared subscription identifier.</summary>
    private static readonly SubscriptionId ServerSubscriptionId = new(Guid.Parse("00000000-0000-0000-0000-000000000301"));

    /// <summary>The oversized payload test data.</summary>
    private static readonly byte[] LargePayloadBytes = [FirstPayloadByte, SecondPayloadByte];

    /// <summary>Verifies a client connect request decodes on the server path.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeConnectRequestReadsClientWireClaim()
    {
        var codec = CreateServerCodec();
        var request = new TransportConnectRequest(
            new(new(1, 0), new(1, 1)),
            new("wire-client", "wire-tenant"),
            [DeliveryGuarantee.AtLeastOnce, DeliveryGuarantee.ExactlyOnce]);
        var bytes = codec.SerializeConnectRequest(request);
        var decoded = codec.DeserializeConnectRequest(bytes);
        await Assert.That(decoded.SupportedProtocolVersions.Minimum).IsEqualTo(new(1, 0));
        await Assert.That(decoded.SupportedProtocolVersions.Maximum).IsEqualTo(new(1, 1));
        await Assert.That(decoded.Client).IsEqualTo(new("wire-client", "wire-tenant"));
        await Assert.That(decoded.RequiredGuarantees.Count).IsEqualTo(ExpectedReceiveBatchCount);
        await Assert.That(decoded.RequiredGuarantees).Contains(DeliveryGuarantee.AtLeastOnce);
        await Assert.That(decoded.RequiredGuarantees).Contains(DeliveryGuarantee.ExactlyOnce);
    }

    /// <summary>Verifies a receive acknowledgement request decodes on the server path.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeAcknowledgementReadsClientWireCursor()
    {
        var decoded = CreateServerCodec().DeserializeAcknowledgement(Encode(AcknowledgementJson()));
        await Assert.That(decoded.StreamId.Value).IsEqualTo(ServerStreamName);
        await Assert.That(decoded.SubscriptionId.Value).IsEqualTo(ServerSubscriptionId.Value);
        await Assert.That(decoded.Cursor).IsEqualTo(CursorOne);
    }

    /// <summary>Verifies declared custom operations survive HTTP request decoding.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializePushRequestAllowsDeclaredCustomOperationType()
    {
        var codec = CreateServerCodec();
        var batch = CreateServerBatch(SyncOperationType.Custom);
        var bytes = SerializeJson(
            new HttpProtocolJsonContext.PushRequestWire { BatchId = batch.BatchId, Operations = [CreateOperationWire(batch.Operations[0])] },
            HttpProtocolJsonContext.Default.PushRequestWireInfo);
        var decoded = codec.DeserializePushRequest(bytes);
        await Assert.That(decoded.Operations[0].Type).IsEqualTo(SyncOperationType.Custom);
        await Assert.That(decoded.Operations[0].BaseVersion).IsEqualTo("v0");
        await Assert.That(decoded.Operations[0].Metadata[TraceMetadataKey]).IsEqualTo("custom");
    }

    /// <summary>Verifies large valid payload limits do not overflow the decoded base64 bound.</summary>
    /// <param name="maximumPayloadBytes">The configured decoded payload byte limit.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(int.MaxValue)]
    [Arguments(Base64MultiplicationOverflowPayloadBytes)]
    public async Task DeserializePushRequestAllowsTinyPayloadUnderLargeConfiguredPayloadLimits(int maximumPayloadBytes)
    {
        var codec = new HttpProtocolCodec(CreateServerLimits() with { MaximumPayloadBytes = maximumPayloadBytes });
        var decoded = codec.DeserializePushRequest(Encode(PushRequestJson(ServerBatchIdText, OperationJson())));
        await Assert.That(decoded.Operations[0].Payload.Payload.Length).IsEqualTo(CreateEmptyJsonPayloadBytes().Length);
    }

    /// <summary>Verifies malformed push schemas cannot default into valid domain values.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializePushRequestRejectsMissingRequiredMembers()
    {
        const string Json = """
            {"batchId":"00000000-0000-0000-0000-000000000100","operations":[{"streamId":"stream-1"}]}
            """;
        var exception = CaptureHttpException(static () => CreateServerCodec().DeserializePushRequest(Encode(Json)));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies JSON depth is rejected by the real codec preflight before DTO conversion.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializePushRequestRejectsBodiesPastConfiguredJsonDepth()
    {
        var codec = new HttpProtocolCodec(CreateServerLimits() with { MaximumJsonDepth = SecondSequence });
        var json = PushRequestJson(ServerBatchIdText, OperationJson());
        var exception = CaptureHttpException(() => codec.DeserializePushRequest(Encode(json)));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.InnerException).IsNull();
    }

    /// <summary>Verifies current subscribe query output parses into the server request contract.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ParseSubscribeRequestReadsCurrentClientQueryShape()
    {
        var request = new RemoteSubscribeRequest(
            new(ServerStreamName),
            ServerSubscriptionId,
            ResumeCursor,
            StartPosition.FromCursor(AnchorCursor));
        var query = await CaptureSubscribeQueryAsync(request);
        var decoded = CreateServerCodec().ParseSubscribeRequest(query);
        await Assert.That(decoded.StreamId).IsEqualTo(request.StreamId);
        await Assert.That(decoded.SubscriptionId).IsEqualTo(request.SubscriptionId);
        await Assert.That(decoded.Cursor).IsEqualTo(ResumeCursor);
        await Assert.That(decoded.InitialPosition.Kind).IsEqualTo(StartPositionKind.FromCursor);
        await Assert.That(decoded.InitialPosition.Cursor).IsEqualTo(AnchorCursor);
    }

    /// <summary>Verifies query parsing rejects duplicate, unknown, conflicting, and missing protocol keys.</summary>
    /// <param name="query">The malformed query.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments($"{SubscribeQueryPrefix}&streamId=stream-2")]
    [Arguments($"{SubscribeQueryPrefix}&tenantHint=tenant")]
    [Arguments($"{SubscribeQueryPrefix}&timestamp=2026-09-13T00%3A00%3A00.0000000%2B00%3A00")]
    [Arguments(SubscribeMissingPositionQuery)]
    [Arguments("streamId=stream-1&subscriptionId=00000000-0000-0000-0000-000000000000&positionKind=0")]
    [Arguments("streamId=stream-1&subscriptionId=x&positionKind=0")]
    [Arguments($"{SubscribeMissingPositionQuery}&positionKind=x")]
    [Arguments($"{SubscribeMissingPositionQuery}&positionKind=3&initialCursor=")]
    [Arguments($"{SubscribeMissingPositionQuery}&positionKind=3")]
    [Arguments($"{SubscribeMissingPositionQuery}&positionKind=3&sequence=1&initialCursor=anchor")]
    [Arguments($"{SubscribeQueryPrefix}&cursor=%")]
    [Arguments($"{SubscribeQueryPrefix}&")]
    public async Task ParseSubscribeRequestRejectsMalformedQuery(string query)
    {
        var exception = CaptureHttpException(() => CreateServerCodec().ParseSubscribeRequest(query));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies server connect responses use the client response decoder's wire shape.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeConnectResponseWritesClientReadableCapabilities()
    {
        var expected = new NegotiatedCapabilities(
            new(1, 0),
            RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.CursorResume,
            TestCapabilityBatchOperations,
            TestKilobyte,
            TimeSpan.FromMinutes(1),
            null);
        var bytes = CreateServerCodec().SerializeConnectResponse(expected);
        var decoded = CreateServerCodec().DeserializeConnectResponse(bytes);
        await Assert.That(decoded).IsEqualTo(expected);
    }

    /// <summary>Verifies server push responses validate and serialize exact operation results.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializePushResponseWritesClientReadableResult()
    {
        var codec = CreateServerCodec();
        var batch = CreateServerBatch(SyncOperationType.Custom);
        var result = new RemoteSyncResult(
            batch.BatchId,
            [new(batch.Operations[0].OperationId, OperationResultKind.Accepted, null, ServerVersion)],
            ServerCursor,
            null);
        var decoded = codec.DeserializePushResponse(batch, codec.SerializePushResponse(batch, result), null);
        await Assert.That(decoded.BatchId).IsEqualTo(result.BatchId);
        await Assert.That(decoded.ServerCursor).IsEqualTo(result.ServerCursor);
        await Assert.That(decoded.RetryAfter).IsEqualTo(result.RetryAfter);
        await Assert.That(decoded.Operations.Count).IsEqualTo(result.Operations.Count);
        await Assert.That(decoded.Operations[0]).IsEqualTo(result.Operations[0]);
    }

    /// <summary>Verifies ordered receive batches, zero-event completions, and multi-event completions are encoded intact.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSubscribeResponseWritesOrderedCompleteBatches()
    {
        var codec = CreateServerCodec();
        var origin = new RemoteEventOrigin("client-1", new(Guid.Parse(ServerOperationIdText)));
        var firstEvent = CreateServerEvent("00000000-0000-0000-0000-000000000201", CursorOne, origin);
        var secondEvent = CreateServerEvent("00000000-0000-0000-0000-000000000202", CursorTwo, origin);
        var first = new RemoteEventBatch(Guid.Parse("00000000-0000-0000-0000-000000000401"), new(ServerStreamName), null, CursorOne, [firstEvent])
        {
            CompletedOperations = [new(origin, [firstEvent.EventId])],
        };
        var second = new RemoteEventBatch(Guid.Parse("00000000-0000-0000-0000-000000000402"), new(ServerStreamName), CursorOne, CursorTwo, [secondEvent])
        {
            CompletedOperations =
            [
                new(origin, [secondEvent.EventId]),
                new(new("client-2", new(Guid.Parse(SecondOperationIdText))), []),
            ],
        };
        var decoded = codec.DeserializeSubscribeResponse(codec.SerializeSubscribeResponse([first, second]), new StreamId(ServerStreamName));
        await Assert.That(decoded).Count().IsEqualTo(ExpectedReceiveBatchCount);
        await Assert.That(decoded[0].BatchId).IsEqualTo(first.BatchId);
        await Assert.That(decoded[1].BatchId).IsEqualTo(second.BatchId);
        await Assert.That(decoded[1].CompletedOperations[0].EventIds).Count().IsEqualTo(SingleOperation);
        await Assert.That(decoded[1].CompletedOperations[0].EventIds[0]).IsEqualTo(secondEvent.EventId);
        await Assert.That(decoded[1].CompletedOperations[1].EventIds).IsEmpty();
    }

    /// <summary>Verifies receive batch serialization preserves events without optional origin references.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSubscribeResponseWritesEventWithoutOptionalOrigin()
    {
        var codec = CreateServerCodec();
        var remoteEvent = new RemoteEvent(
            Guid.Parse("00000000-0000-0000-0000-000000000203"),
            new(ServerStreamName),
            CursorOne,
            DateTimeOffset.Parse(ServerTimestampText, CultureInfo.InvariantCulture),
            null,
            new(ContractName, 1, PayloadContentType, CreateEmptyJsonPayloadBytes(), PayloadHash),
            new Dictionary<string, string> { [TraceMetadataKey] = CursorOne });
        var batch = new RemoteEventBatch(Guid.Parse(ServerBatchIdText), new(ServerStreamName), null, CursorOne, [remoteEvent]);
        var decoded = codec.DeserializeSubscribeResponse(codec.SerializeSubscribeResponse([batch]), new StreamId(ServerStreamName));
        await Assert.That(decoded[0].Events[0].CausedByOperationId).IsNull();
        await Assert.That(decoded[0].Events[0].Origin).IsNull();
    }

    /// <summary>Verifies server response serialization rejects hostile caller counts before use.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSubscribeResponseRejectsTooManyBatches()
    {
        var codec = new HttpProtocolCodec(CreateServerLimits() with { MaximumBatchOperations = 1 });
        var batch = new RemoteEventBatch(Guid.NewGuid(), new(ServerStreamName), null, CursorOne, []);
        var exception = CaptureHttpException(() => codec.SerializeSubscribeResponse([batch, batch]));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies malformed server codec wire inputs fail before domain defaults can be created.</summary>
    /// <param name="testCase">The malformed protocol case.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(ServerHttpExceptionCases))]
    public async Task ServerCodecRejectsMalformedProtocolInputs(ServerHttpExceptionCase testCase)
    {
        var exception = testCase.Act();
        await Assert.That(exception.Kind).IsEqualTo(testCase.ExpectedKind);
    }

    /// <summary>Verifies subscribe parsing returns exact decoded query field text for replay canonicalization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ParseSubscribeRequestWithQueryFieldsPreservesDecodedWireValues()
    {
        const string Query = "?streamId=stream-1&subscriptionId=00000000-0000-0000-0000-000000000301&positionKind=02&sequence=0042&cursor=resume-%F0%9F%A7%AA";
        const long ExpectedSequence = 42;
        var parsed = CreateServerCodec().ParseSubscribeRequestWithQueryFields(Query);
        await Assert.That(parsed.Request.InitialPosition.Kind).IsEqualTo(StartPositionKind.FromSequence);
        await Assert.That(parsed.Request.InitialPosition.Sequence).IsEqualTo(ExpectedSequence);
        await Assert.That(parsed.QueryFields).Contains(new KeyValuePair<string, string>("positionKind", "02"));
        await Assert.That(parsed.QueryFields).Contains(new KeyValuePair<string, string>("sequence", "0042"));
        await Assert.That(parsed.QueryFields).Contains(new KeyValuePair<string, string>("cursor", "resume-🧪"));
    }

    /// <summary>Verifies raw supplementary Unicode and percent-encoded UTF-8 query values decode identically.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ParseSubscribeRequestPreservesRawAndPercentEncodedUnicodeScalars()
    {
        const string Scalar = "🧪";
        const string RawQuery = $"{SubscribeMissingPositionQueryWithPrefix}&cursor=resume-🧪&positionKind=3&initialCursor=anchor-🧪";
        const string EscapedQuery =
            $"{SubscribeMissingPositionQueryWithPrefix}&cursor=resume-%F0%9F%A7%AA&positionKind=3&initialCursor=anchor-%F0%9F%A7%AA";
        var raw = CreateServerCodec().ParseSubscribeRequest(RawQuery);
        var escaped = CreateServerCodec().ParseSubscribeRequest(EscapedQuery);
        await Assert.That(raw.Cursor).IsEqualTo($"resume-{Scalar}");
        await Assert.That(raw.InitialPosition.Cursor).IsEqualTo($"anchor-{Scalar}");
        await Assert.That(escaped.Cursor).IsEqualTo(raw.Cursor);
        await Assert.That(escaped.InitialPosition.Cursor).IsEqualTo(raw.InitialPosition.Cursor);
    }

    /// <summary>Verifies lone surrogate query values are rejected before reaching domain identifiers.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ParseSubscribeRequestRejectsLoneSurrogateQueryValues()
    {
        var surrogate = new string(['\ud800']);
        var query = $"{SubscribeQueryPrefix}&cursor={surrogate}";
        var exception = CaptureHttpException(() => CreateServerCodec().ParseSubscribeRequest(query));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.InnerException).IsNull();
    }

    /// <summary>Verifies subscribe query parsing handles every start position mode and malformed separators.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ParseSubscribeRequestHandlesQueryModesAndSeparatorFailures()
    {
        const string TimestampQuery =
            $"{SubscribeMissingPositionQueryWithPrefix}&positionKind=1&timestamp=2026-09-13T00%3A00%3A00.0000000%2B00%3A00";
        const string SequenceQuery = $"?streamId=stream-1&subscriptionId=00000000-0000-0000-0000-000000000301&positionKind=2&sequence={QuerySequenceText}";
        const string LowerHexQuery = $"{SubscribeMissingPositionQueryWithPrefix}&cursor=%f0%9f%a7%aa&positionKind=3&initialCursor=anchor";
        var timestamp = CreateServerCodec().ParseSubscribeRequest(TimestampQuery);
        var sequence = CreateServerCodec().ParseSubscribeRequest(SequenceQuery);
        var lowerHex = CreateServerCodec().ParseSubscribeRequest(LowerHexQuery);
        var empty = CaptureHttpException(static () => CreateServerCodec().ParseSubscribeRequest("?"));
        var missingEquals = CaptureHttpException(static () => CreateServerCodec().ParseSubscribeRequest("streamId"));
        var invalidHex = CaptureHttpException(
            static () => CreateServerCodec().ParseSubscribeRequest($"{SubscribeQueryPrefix}&cursor=%G0"));
        var invalidUtf8 = CaptureHttpException(
            static () => CreateServerCodec().ParseSubscribeRequest($"{SubscribeQueryPrefix}&cursor=%F0%9F"));
        var invalidKind = CaptureHttpException(
            static () => CreateServerCodec().ParseSubscribeRequest($"{SubscribeMissingPositionQuery}&positionKind=99"));
        var tooManyKeys = CaptureHttpException(static () => new HttpProtocolCodec(CreateServerLimits() with { MaximumQueryKeys = 1 })
            .ParseSubscribeRequest(SubscribeQueryPrefix));
        await Assert.That(timestamp.InitialPosition.Kind).IsEqualTo(StartPositionKind.FromTimestamp);
        await Assert.That(sequence.InitialPosition.Kind).IsEqualTo(StartPositionKind.FromSequence);
        await Assert.That(lowerHex.Cursor).IsEqualTo("🧪");
        await Assert.That(empty.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(missingEquals.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(invalidHex.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(invalidUtf8.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(invalidKind.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(tooManyKeys.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies additional subscribe query value failures stay protocol violations.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ParseSubscribeRequestRejectsInvalidPositionValues()
    {
        var latest = CreateServerCodec().ParseSubscribeRequest($"?{SubscribeQueryPrefix}");
        var timestampMissing = CaptureHttpException(
            static () => CreateServerCodec().ParseSubscribeRequest($"{SubscribeMissingPositionQuery}&positionKind=1"));
        var sequenceMissing = CaptureHttpException(
            static () => CreateServerCodec().ParseSubscribeRequest($"{SubscribeMissingPositionQuery}&positionKind=2"));
        var sequenceInvalid = CaptureHttpException(
            static () => CreateServerCodec().ParseSubscribeRequest($"{SubscribeMissingPositionQuery}&positionKind=2&sequence=x"));
        var duplicateSeparator = CaptureHttpException(
            static () => CreateServerCodec().ParseSubscribeRequest($"{SubscribeQueryPrefix}&&cursor=after"));
        await Assert.That(latest.InitialPosition.Kind).IsEqualTo(StartPositionKind.Latest);
        await Assert.That(timestampMissing.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(sequenceMissing.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(sequenceInvalid.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(duplicateSeparator.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies additional enum values are accepted from wire where declared by the domain contracts.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializePushRequestReadsAllDeclaredOperationEnums()
    {
        var request = PushRequestJson(
            ServerBatchIdText,
            OperationJson(new()
            {
                BaseVersion = "v0",
                Type = (int)SyncOperationType.Update,
                DeliveryGuaranteeValue = (int)DeliveryGuarantee.AtMostOnce,
                Durability = (int)OperationDurability.Volatile,
                ConflictPolicyValue = (int)ConflictPolicy.LastWriterWins,
            }),
            OperationJson(new() { OperationId = SecondOperationIdText, Sequence = SecondSequence, Type = (int)SyncOperationType.Delete, ConflictPolicyValue = (int)ConflictPolicy.Custom }));
        var decoded = CreateServerCodec().DeserializePushRequest(Encode(request));
        await Assert.That(decoded.Operations[0].Type).IsEqualTo(SyncOperationType.Update);
        await Assert.That(decoded.Operations[1].Type).IsEqualTo(SyncOperationType.Delete);
        await Assert.That(decoded.Operations[0].Policy.DeliveryGuarantee).IsEqualTo(DeliveryGuarantee.AtMostOnce);
        await Assert.That(decoded.Operations[0].Policy.Durability).IsEqualTo(OperationDurability.Volatile);
        await Assert.That(decoded.Operations[0].Policy.ConflictPolicy).IsEqualTo(ConflictPolicy.LastWriterWins);
        await Assert.That(decoded.Operations[0].BaseVersion).IsEqualTo("v0");
        await Assert.That(decoded.Operations[1].Policy.ConflictPolicy).IsEqualTo(ConflictPolicy.Custom);
    }

    /// <summary>Verifies preflight rejects malformed scalar and array value kinds before DTO use.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ServerCodecRejectsMalformedPreflightValueKinds()
    {
        const string NumberAsStringJson = """
            {"protocolVersion":"1.0","features":"0","maximumBatchOperations":1,"maximumBatchBytes":1}
            """;
        const string OptionalNumberAsStringJson = """
            {"protocolVersion":"1.0","features":0,"maximumBatchOperations":1,"maximumBatchBytes":1,"serverIdempotencyRetentionMilliseconds":"1"}
            """;
        const string RequiredGuaranteesObjectJson = """
            {"minimumProtocolVersion":"1.0","maximumProtocolVersion":"1.1","clientId":"client","requiredGuarantees":{}}
            """;
        const string CompletionIdNumberJson = """
            {
              "batches": [
                {
                  "batchId": "00000000-0000-0000-0000-000000000100",
                  "streamId": "stream-1",
                  "nextCursor": "cursor-1",
                  "events": [],
                  "completedOperations": [
                    {
                      "origin": {
                        "clientId": "client-1",
                        "operationId": "00000000-0000-0000-0000-000000000001"
                      },
                      "eventIds": [1]
                    }
                  ]
                }
              ]
            }

            """;
        var number = CaptureHttpException(static () => CreateServerCodec().DeserializeConnectResponse(Encode(NumberAsStringJson)));
        var optionalNumber = CaptureHttpException(static () => CreateServerCodec().DeserializeConnectResponse(Encode(OptionalNumberAsStringJson)));
        var requiredGuarantees = CaptureHttpException(static () => CreateServerCodec().DeserializeConnectRequest(Encode(RequiredGuaranteesObjectJson)));
        var completionId = CaptureHttpException(static () => CreateServerCodec().DeserializeSubscribeResponse(Encode(CompletionIdNumberJson)));
        await Assert.That(number.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(optionalNumber.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(requiredGuarantees.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(completionId.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies all declared push result kinds serialize in server responses.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializePushResponseWritesAllDeclaredResultKinds()
    {
        var codec = CreateServerCodec();
        var batch = CreateServerBatch(SyncOperationType.Append);
        var kinds = new[]
        {
            OperationResultKind.Accepted,
            OperationResultKind.Conflict,
            OperationResultKind.Rejected,
            OperationResultKind.Retryable,
        };
        foreach (var kind in kinds)
        {
            var result = new RemoteSyncResult(
                batch.BatchId,
                [new(batch.Operations[0].OperationId, kind, "reason", ServerVersion)],
                ServerCursor,
                null);
            var decoded = codec.DeserializePushResponse(batch, codec.SerializePushResponse(batch, result), null);
            await Assert.That(decoded.Operations[0].Kind).IsEqualTo(kind);
        }
    }

    /// <summary>Verifies invalid feature flags and completion event aggregates fail before domain use.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeResponsesRejectInvalidFeatureAndCompletionAggregates()
    {
        const string FeaturesJson = """
            {"protocolVersion":"1.0","features":1073741824,"maximumBatchOperations":1,"maximumBatchBytes":1}
            """;
        const string DurationJson = """
            {"protocolVersion":"1.0","features":0,"maximumBatchOperations":1,"maximumBatchBytes":1,"serverIdempotencyRetentionMilliseconds":9223372036854775807}
            """;
        var completionJson = SubscribeResponseWithCompletedOperationsJson();
        var codec = new HttpProtocolCodec(CreateServerLimits() with { MaximumEventsPerBatch = SingleOperation });
        var features = CaptureHttpException(static () => CreateServerCodec().DeserializeConnectResponse(Encode(FeaturesJson)));
        var duration = CaptureHttpException(static () => CreateServerCodec().DeserializeConnectResponse(Encode(DurationJson)));
        var completions = CaptureHttpException(() => codec.DeserializeSubscribeResponse(Encode(completionJson)));
        await Assert.That(features.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(duration.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(completions.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies maximum whole-millisecond retention values decode without double rounding.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeConnectResponsePreservesMaximumWholeMillisecondRetention()
    {
        var expected = TimeSpan.FromTicks(MaximumWholeTimeSpanMilliseconds * TimeSpan.TicksPerMillisecond);
        var json = "{\"protocolVersion\":\"1.0\",\"features\":0,\"maximumBatchOperations\":1,\"maximumBatchBytes\":1,"
            + $"\"serverIdempotencyRetentionMilliseconds\":{MaximumWholeTimeSpanMilliseconds.ToString(CultureInfo.InvariantCulture)}}}";
        var decoded = CreateServerCodec().DeserializeConnectResponse(Encode(json));
        await Assert.That(decoded.ServerIdempotencyRetention).IsEqualTo(expected);
    }

    /// <summary>Verifies server serialization rejects malformed caller supplied domain values.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ServerSerializationRejectsInvalidCallerValues()
    {
        var codec = CreateServerCodec();
        var smallCodec = new HttpProtocolCodec(CreateServerLimits() with { MaximumBatchOperations = SingleOperation });
        var request = new TransportConnectRequest(
            new(new(2, 0), new(1, 0)),
            new(WireClientId, null),
            [DeliveryGuarantee.AtLeastOnce]);
        var guarantees = new TransportConnectRequest(
            new(new(1, 0), new(1, 1)),
            new(WireClientId, null),
            [DeliveryGuarantee.AtMostOnce, DeliveryGuarantee.AtLeastOnce]);
        var invalidOperation = CreateServerOperation(operationId: EmptyGuidText);
        var invalidPolicy = CreateServerOperation(policy: new(
            DeliveryGuarantee.AtLeastOnce,
            OperationDurability.Durable,
            OperationPolicy.MaximumPriority + SingleOperation,
            ConflictPolicy.Merge));
        var version = CaptureHttpException(() => codec.SerializeConnectResponse(new(new(2, 0), 0, 1, 1, null, null)));
        var features = CaptureHttpException(() => codec.SerializeConnectResponse(new(
            new(1, 0),
            (RemoteTransportCapabilities)InvalidFeatureValue,
            1,
            1,
            null,
            null)));
        var operations = CaptureHttpException(() => codec.SerializeConnectResponse(new(new(1, 0), 0, 0, 1, null, null)));
        var bytes = CaptureHttpException(() => codec.SerializeConnectResponse(new(new(1, 0), 0, 1, 0, null, null)));
        var retention = CaptureHttpException(() => codec.SerializeConnectResponse(new(
            new(1, 0),
            0,
            1,
            1,
            TimeSpan.FromMilliseconds(-SingleOperation),
            null)));
        var exactlyOnce = CaptureHttpException(() => codec.SerializeConnectResponse(new(
            new(1, 0),
            0,
            1,
            1,
            null,
            null)
        { EffectiveExactlyOnceWindow = TimeSpan.FromMilliseconds(-SingleOperation) }));
        var range = CaptureHttpException(() => codec.SerializeConnectRequest(request));
        var tooManyGuarantees = CaptureHttpException(() => smallCodec.SerializeConnectRequest(guarantees));
        var operation = CaptureHttpException(() => codec.SerializePushRequest(CreateServerBatchFromOperations(invalidOperation)));
        var policy = CaptureHttpException(() => codec.SerializePushRequest(CreateServerBatchFromOperations(invalidPolicy)));
        await Assert.That(version.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(features.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(operations.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(bytes.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(retention.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exactlyOnce.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(range.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(tooManyGuarantees.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
        await Assert.That(operation.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(policy.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies push serialization rejects invalid payload envelope and metadata values.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializePushRequestRejectsInvalidPayloadEnvelopeAndMetadata()
    {
        var codec = CreateServerCodec();
        var metadataCodec = new HttpProtocolCodec(CreateServerLimits() with { MaximumMetadataEntries = SingleOperation });
        var invalidPayload = new PayloadEnvelope(ContractName, 0, PayloadContentType, CreateEmptyJsonPayloadBytes(), PayloadHash);
        var invalidMetadata = new Dictionary<string, string> { ["first"] = "1", ["second"] = "2" };
        var payload = CaptureHttpException(() => codec.SerializePushRequest(CreateServerBatchFromOperations(CreateServerOperation(payload: invalidPayload))));
        var metadata = CaptureHttpException(() => metadataCodec.SerializePushRequest(CreateServerBatchFromOperations(CreateServerOperation(metadata: invalidMetadata))));
        await Assert.That(payload.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(metadata.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies push serialization rejects invalid outgoing operation enum and stream limit values.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializePushRequestRejectsInvalidOperationTypeAndStreamLimit()
    {
        var codec = CreateServerCodec();
        var streamCodec = new HttpProtocolCodec(CreateServerLimits() with { MaximumProtocolStringBytes = SingleOperation });
        var invalidType = CreateServerOperation(type: (SyncOperationType)InvalidEnumValue);
        var streamOverLimit = CreateServerOperation(streamId: AlternateServerStreamName);
        var operationType = CaptureHttpException(() => codec.SerializePushRequest(CreateServerBatchFromOperations(invalidType)));
        var stream = CaptureHttpException(() => streamCodec.SerializePushRequest(CreateServerBatchFromOperations(streamOverLimit)));
        await Assert.That(operationType.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(stream.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies receive batch serialization rejects invalid server-supplied batch shapes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSubscribeResponseRejectsInvalidBatchShapes()
    {
        var codec = CreateServerCodec();
        var smallEventCodec = new HttpProtocolCodec(CreateServerLimits() with { MaximumEventsPerBatch = SingleOperation });
        var origin = new RemoteEventOrigin("client-1", new(Guid.Parse(ServerOperationIdText)));
        var firstEvent = CreateServerEvent("00000000-0000-0000-0000-000000000201", CursorOne, origin);
        var secondEvent = CreateServerEvent("00000000-0000-0000-0000-000000000202", CursorTwo, origin);
        var nullBatches = new RemoteEventBatch[SingleOperation];
        var nullCompletions = new RemoteOperationCompletion[SingleOperation];
        var overflowingEvents = new RemoteEventBatch(Guid.NewGuid(), new(ServerStreamName), null, CursorThree, [firstEvent, secondEvent]);
        var missingNextCursor = new RemoteEventBatch(Guid.NewGuid(), new(ServerStreamName), null, string.Empty, []);
        var overflowingCompletions = new RemoteEventBatch(Guid.NewGuid(), new(ServerStreamName), null, CursorOne, [])
        {
            CompletedOperations = [new(origin, [Guid.Parse(ServerOperationIdText), Guid.Parse(SecondOperationIdText)])],
        };
        var nullCompletionBatch = new RemoteEventBatch(Guid.NewGuid(), new(ServerStreamName), null, CursorOne, [])
            { CompletedOperations = nullCompletions };
        var emptyEventId = new RemoteEvent(
            Guid.Empty,
            new(ServerStreamName),
            CursorOne,
            DateTimeOffset.Parse(ServerTimestampText, CultureInfo.InvariantCulture),
            null,
            new(ContractName, 1, PayloadContentType, CreateEmptyJsonPayloadBytes(), PayloadHash),
            new Dictionary<string, string> { [TraceMetadataKey] = CursorOne });
        var badEventBatch = new RemoteEventBatch(Guid.NewGuid(), new(ServerStreamName), null, CursorOne, [emptyEventId]);
        var missingCompletionEvent = new RemoteEventBatch(Guid.NewGuid(), new(ServerStreamName), null, CursorOne, [])
            { CompletedOperations = [new(origin, [Guid.Parse(ThirdOperationIdText)])] };
        var largePayload = new PayloadEnvelope(ContractName, 1, PayloadContentType, LargePayloadBytes, PayloadHash);
        var payloadEvent = new RemoteEvent(
            Guid.Parse("00000000-0000-0000-0000-000000000203"),
            new(ServerStreamName),
            CursorThree,
            DateTimeOffset.Parse(ServerTimestampText, CultureInfo.InvariantCulture),
            origin.OperationId,
            largePayload,
            new Dictionary<string, string> { [TraceMetadataKey] = CursorThree })
        { Origin = origin };
        var payloadBatch = new RemoteEventBatch(Guid.NewGuid(), new(ServerStreamName), null, CursorThree, [payloadEvent])
            { CompletedOperations = [new(origin, [payloadEvent.EventId])] };
        var smallPayloadCodec = new HttpProtocolCodec(CreateServerLimits() with { MaximumPayloadBytes = SingleOperation });
        var nullBatch = CaptureHttpException(() => codec.SerializeSubscribeResponse(nullBatches));
        var tooManyEvents = CaptureHttpException(() => smallEventCodec.SerializeSubscribeResponse([overflowingEvents]));
        var tooManyCompletionIds = CaptureHttpException(() => smallEventCodec.SerializeSubscribeResponse([overflowingCompletions]));
        var cursor = CaptureHttpException(() => codec.SerializeSubscribeResponse([missingNextCursor]));
        var nullCompletion = CaptureHttpException(() => codec.SerializeSubscribeResponse([nullCompletionBatch]));
        var eventId = CaptureHttpException(() => codec.SerializeSubscribeResponse([badEventBatch]));
        var completion = CaptureHttpException(() => codec.SerializeSubscribeResponse([missingCompletionEvent]));
        var payload = CaptureHttpException(() => smallPayloadCodec.SerializeSubscribeResponse([payloadBatch]));
        await Assert.That(nullBatch.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(tooManyEvents.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
        await Assert.That(tooManyCompletionIds.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
        await Assert.That(cursor.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(nullCompletion.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(eventId.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(completion.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(payload.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies server push result serialization rejects mismatched result sets.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializePushResponseRejectsInvalidResultSets()
    {
        var codec = CreateServerCodec();
        var smallCodec = new HttpProtocolCodec(CreateServerLimits() with { MaximumBatchOperations = SingleOperation });
        var batch = CreateServerBatch(SyncOperationType.Append);
        var operation = batch.Operations[0].OperationId;
        var other = new OperationId(Guid.Parse(SecondOperationIdText));
        var retry = CaptureHttpException(() => codec.SerializePushResponse(batch, new(
            batch.BatchId,
            [new(operation, OperationResultKind.Accepted, null, ServerVersion)],
            ServerCursor,
            TimeSpan.FromMilliseconds(-SingleOperation))));
        var tooMany = CaptureHttpException(() => smallCodec.SerializePushResponse(batch, new(
            batch.BatchId,
            [new(operation, OperationResultKind.Accepted, null, ServerVersion), new(other, OperationResultKind.Accepted, null, ServerVersion)],
            ServerCursor,
            null)));
        var emptyOperation = CaptureHttpException(() => codec.SerializePushResponse(batch, new(
            batch.BatchId,
            [new(new(Guid.Empty), OperationResultKind.Accepted, null, null)],
            ServerCursor,
            null)));
        var invalidKind = CaptureHttpException(() => codec.SerializePushResponse(batch, new(
            batch.BatchId,
            [new(operation, (OperationResultKind)InvalidEnumValue, null, null)],
            ServerCursor,
            null)));
        await Assert.That(retry.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(tooMany.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
        await Assert.That(emptyOperation.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(invalidKind.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(() => codec.SerializePushResponse(batch, new(Guid.NewGuid(), [], ServerCursor, null)))
            .ThrowsExactly<SyncBatchValidationException>();
        await Assert.That(() => codec.SerializePushResponse(batch, new(batch.BatchId, [new(operation, 0, null, null), new(operation, 0, null, null)], null, null)))
            .ThrowsExactly<SyncBatchValidationException>();
        await Assert.That(() => codec.SerializePushResponse(batch, new(batch.BatchId, [new(other, 0, null, null)], null, null)))
            .ThrowsExactly<SyncBatchValidationException>();
        await Assert.That(() => codec.SerializePushResponse(batch, new(batch.BatchId, [], null, null)))
            .ThrowsExactly<SyncBatchValidationException>();
    }

    /// <summary>Verifies subscribe response deserialization rejects stream identifiers that fail domain grammar.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSubscribeResponseRejectsInvalidDomainStreamIdentifier()
    {
        const string ResponseJson = """
            {
              "batches": [
                {
                  "batchId": "00000000-0000-0000-0000-000000000100",
                  "streamId": "bad stream",
                  "nextCursor": "cursor-1",
                  "events": [],
                  "completedOperations": []
                }
              ]
            }

            """;
        var exception = CaptureHttpException(static () => CreateServerCodec().DeserializeSubscribeResponse(Encode(ResponseJson)));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.InnerException).IsNull();
        await Assert.That(exception.Data.Count).IsEqualTo(0);
    }

    /// <summary>Verifies subscribe response deserialization sanitizes mismatched event origin correlations.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSubscribeResponseRejectsMismatchedEventOrigin()
    {
        const string ResponseJson = """
            {
              "batches": [
                {
                  "batchId": "00000000-0000-0000-0000-000000000100",
                  "streamId": "stream-1",
                  "nextCursor": "cursor-1",
                  "events": [
                    {
                      "eventId": "00000000-0000-0000-0000-000000000201",
                      "streamId": "stream-1",
                      "serverCursor": "cursor-1",
                      "committedAtUtc": "2026-09-13T00:00:00+00:00",
                      "causedByOperationId": "00000000-0000-0000-0000-000000000001",
                      "origin": {
                        "clientId": "client-1",
                        "operationId": "00000000-0000-0000-0000-000000000002"
                      },
                      "payload": {
                        "contractId": "contract",
                        "schemaVersion": 1,
                        "contentType": "application/json",
                        "payload": "e30=",
                        "payloadHash": "sha256-test"
                      },
                      "metadata": {}
                    }
                  ],
                  "completedOperations": []
                }
              ]
            }

            """;
        var exception = CaptureHttpException(static () => CreateServerCodec().DeserializeSubscribeResponse(Encode(ResponseJson)));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.InnerException).IsNull();
        await Assert.That(exception.Data.Count).IsEqualTo(0);
    }

    /// <summary>Verifies malformed protocol failures do not retain parser exception details.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MalformedProtocolExceptionsAreSanitized()
    {
        var exception = CaptureHttpException(static () => CreateServerCodec().DeserializePushRequest(Encode(PushRequestJson(
            ServerBatchIdText,
            OperationJson(new() { Payload = PayloadLeakSentinel })))));
        var subscribeException = CaptureHttpException(static () =>
            CreateServerCodec().DeserializeSubscribeResponse(Encode(SubscribeResponseJson(PayloadLeakSentinel))));
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.InnerException).IsNull();
        await Assert.That(exception.Message.Contains(PayloadLeakSentinel, StringComparison.Ordinal)).IsFalse();
        await Assert.That(exception.Data.Count).IsEqualTo(0);
        await Assert.That(subscribeException.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(subscribeException.InnerException).IsNull();
        await Assert.That(subscribeException.Message.Contains(PayloadLeakSentinel, StringComparison.Ordinal)).IsFalse();
        await Assert.That(subscribeException.Data.Count).IsEqualTo(0);
    }
}
