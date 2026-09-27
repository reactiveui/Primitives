// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using System.Text.Json;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpProtocolCodec"/> against the retained protocol-v1 golden fixtures.</summary>
public sealed partial class HttpProtocolCodecTests
{
    /// <summary>The unknown member injected into golden fixtures.</summary>
    private const string GoldenUnknownMember = "\"futureMinorField\":{\"nested\":[1,2,3]},";

    /// <summary>The message used when a test names a fixture without a codec mapping.</summary>
    private const string GoldenUnknownFixtureMessage = "Unknown golden fixture.";

    /// <summary>The byte offset after the root object and the first member quote.</summary>
    private const int GoldenFirstValueSearchStart = 2;

    /// <summary>The length of the <c>:{</c> token that opens a nested object.</summary>
    private const int GoldenNestedObjectTokenLength = 2;

    /// <summary>The later v1 minor version used by negotiation tests.</summary>
    private const int GoldenLaterMinorVersion = 7;

    /// <summary>The key that serializes tests using the shared subscribe query capture client.</summary>
    private const string SubscribeQueryCaptureKey = "SubscribeQueryCapture";

    /// <summary>Provides every JSON wire fixture name.</summary>
    /// <returns>The fixture names.</returns>
    public static IEnumerable<string> GoldenJsonFixtures()
    {
        yield return GoldenConnectRequestFile;
        yield return GoldenConnectResponseFile;
        yield return GoldenPushRequestFile;
        yield return GoldenPushResponseFile;
        yield return GoldenSubscribeResponseFile;
        yield return GoldenAcknowledgeRequestFile;
        yield return GoldenSnapshotRecoveryRequestFile;
        yield return GoldenSnapshotRecoveredResponseFile;
        yield return GoldenSnapshotRetentionExpiredResponseFile;
    }

    /// <summary>Verifies the current encoder writes every golden fixture byte for byte.</summary>
    /// <param name="fixture">The fixture name.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(GoldenJsonFixtures))]
    public async Task GoldenFixtureMatchesCurrentEncoder(string fixture)
    {
        var encoded = ProtocolGoldenFixtures.ToText(EncodeGolden(fixture));

        await Assert.That(encoded).IsEqualTo(ProtocolGoldenFixtures.ReadText(fixture));
    }

    /// <summary>Verifies every golden fixture decodes with the current decoder and re-encodes byte for byte.</summary>
    /// <param name="fixture">The fixture name.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(GoldenJsonFixtures))]
    public async Task GoldenFixtureRoundTripsThroughCurrentDecoder(string fixture)
    {
        var reencoded = ProtocolGoldenFixtures.ToText(ReencodeGolden(fixture, ProtocolGoldenFixtures.ReadBytes(fixture)));

        await Assert.That(reencoded).IsEqualTo(ProtocolGoldenFixtures.ReadText(fixture));
    }

    /// <summary>Verifies the golden connect request decodes to the expected request.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GoldenConnectRequestDecodesToExpectedRequest()
    {
        var decoded = CreateGoldenCodec().DeserializeConnectRequest(ProtocolGoldenFixtures.ReadBytes(GoldenConnectRequestFile));
        var expected = CreateGoldenConnectRequest();

        await Assert.That(decoded.SupportedProtocolVersions).IsEqualTo(expected.SupportedProtocolVersions);
        await Assert.That(decoded.Client).IsEqualTo(expected.Client);
        await AssertGoldenSequenceAsync(decoded.RequiredGuarantees, expected.RequiredGuarantees);
    }

    /// <summary>Verifies the golden connect response decodes to the expected capabilities.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GoldenConnectResponseDecodesToExpectedCapabilities()
    {
        var decoded = CreateGoldenCodec().DeserializeConnectResponse(ProtocolGoldenFixtures.ReadBytes(GoldenConnectResponseFile));

        await Assert.That(decoded).IsEqualTo(CreateGoldenCapabilities());
    }

    /// <summary>Verifies the golden push request decodes to the expected operations.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GoldenPushRequestDecodesToExpectedOperations()
    {
        var decoded = CreateGoldenCodec().DeserializePushRequest(ProtocolGoldenFixtures.ReadBytes(GoldenPushRequestFile));
        var expected = CreateGoldenBatch();

        await Assert.That(decoded.BatchId).IsEqualTo(expected.BatchId);
        await Assert.That(decoded.Operations.Count).IsEqualTo(expected.Operations.Count);
        for (var index = 0; index < expected.Operations.Count; index++)
        {
            await AssertGoldenOperationAsync(decoded.Operations[index], expected.Operations[index]);
        }
    }

    /// <summary>Verifies the golden push response decodes every operation result kind.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GoldenPushResponseDecodesEveryOperationResultKind()
    {
        var decoded = CreateGoldenCodec().DeserializePushResponse(CreateGoldenBatch(), ProtocolGoldenFixtures.ReadBytes(GoldenPushResponseFile), null);
        var expected = CreateGoldenSyncResult();

        await Assert.That(decoded.BatchId).IsEqualTo(expected.BatchId);
        await Assert.That(decoded.ServerCursor).IsEqualTo(expected.ServerCursor);
        await AssertGoldenSequenceAsync(decoded.Operations, expected.Operations);
        await AssertGoldenSequenceAsync(
            decoded.Operations.Select(static result => result.Kind),
            [OperationResultKind.Accepted, OperationResultKind.Conflict, OperationResultKind.Rejected, OperationResultKind.Retryable]);
    }

    /// <summary>Verifies the golden subscribe response decodes to the expected cursor chain.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GoldenSubscribeResponseDecodesToExpectedBatches()
    {
        var decoded = CreateGoldenCodec().DeserializeSubscribeResponse(ProtocolGoldenFixtures.ReadBytes(GoldenSubscribeResponseFile), new StreamId(GoldenStreamId));
        var expected = CreateGoldenReceiveBatches();

        await Assert.That(decoded.Length).IsEqualTo(expected.Length);
        for (var index = 0; index < expected.Length; index++)
        {
            await Assert.That(decoded[index].BatchId).IsEqualTo(expected[index].BatchId);
            await Assert.That(decoded[index].PreviousCursor).IsEqualTo(expected[index].PreviousCursor);
            await Assert.That(decoded[index].NextCursor).IsEqualTo(expected[index].NextCursor);
            await AssertGoldenSequenceAsync(
                decoded[index].Events.Select(static item => item.ServerCursor),
                expected[index].Events.Select(static item => item.ServerCursor));
            await Assert.That(decoded[index].CompletedOperations.Count).IsEqualTo(expected[index].CompletedOperations.Count);
        }

        await Assert.That(decoded[0].Events[0].Origin).IsEqualTo(expected[0].Events[0].Origin);
        await Assert.That(decoded[0].Events[0].Metadata[GoldenTraceKey]).IsEqualTo("evt-1");
        await AssertGoldenSequenceAsync(decoded[0].CompletedOperations[0].EventIds, expected[0].CompletedOperations[0].EventIds);
    }

    /// <summary>Verifies the golden acknowledgement decodes to the expected acknowledgement.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GoldenAcknowledgementDecodesToExpectedAcknowledgement()
    {
        var decoded = CreateGoldenCodec().DeserializeAcknowledgement(ProtocolGoldenFixtures.ReadBytes(GoldenAcknowledgeRequestFile));

        await Assert.That(decoded).IsEqualTo(CreateGoldenAcknowledgement());
    }

    /// <summary>Verifies the golden recovered snapshot decodes to the expected checkpoint and dispositions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The decoded result has no checkpoint.</exception>
    [Test]
    public async Task GoldenRecoveredSnapshotDecodesToExpectedCheckpoint()
    {
        var request = CreateGoldenSnapshotRequest();
        var decoded = CreateGoldenCodec().DeserializeSnapshotRecoveryResponse(request, ProtocolGoldenFixtures.ReadBytes(GoldenSnapshotRecoveredResponseFile));
        var expected = CreateGoldenRecoveredSnapshot(request);
        var checkpoint = decoded.Checkpoint ?? throw new InvalidOperationException("Expected a recovered checkpoint.");

        await Assert.That(decoded.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(checkpoint.FrontierCursor).IsEqualTo(GoldenFrontierCursor);
        await Assert.That(checkpoint.ObservedAtUtc).IsEqualTo(ParseGoldenTimestamp(GoldenCommitTimestampText));
        await Assert.That(Encoding.UTF8.GetString(checkpoint.ClientState.Payload.Span)).IsEqualTo(GoldenStateJson);
        await Assert.That(checkpoint.ClientState.PayloadHash).IsEqualTo(GoldenStateHash);
        await AssertGoldenSequenceAsync(decoded.OperationDispositions, expected.OperationDispositions);
    }

    /// <summary>Verifies the golden retention-gap snapshot decodes as a retention-expired recovery.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GoldenRetentionExpiredSnapshotDecodesAsRetentionGap()
    {
        var decoded = CreateGoldenCodec().DeserializeSnapshotRecoveryResponse(
            CreateGoldenSnapshotRequest(),
            ProtocolGoldenFixtures.ReadBytes(GoldenSnapshotRetentionExpiredResponseFile));

        await Assert.That(decoded.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.RetentionExpired);
        await Assert.That(decoded.Checkpoint).IsNull();
        await Assert.That(decoded.ReasonCode).IsEqualTo("retention-expired");
    }

    /// <summary>Verifies the protocol DTO layer ignores unknown members added by a future minor version.</summary>
    /// <param name="fixture">The fixture name.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(GoldenJsonFixtures))]
    public async Task GoldenFixtureWithUnknownMemberIsIgnoredByProtocolMetadata(string fixture)
    {
        var json = InjectGoldenUnknownMember(ProtocolGoldenFixtures.ReadText(fixture));

        var reserialized = ReserializeGoldenDto(fixture, json);

        await Assert.That(reserialized).IsEqualTo(ProtocolGoldenFixtures.ReadText(fixture));
    }

    /// <summary>Verifies the strict v1 codec fails closed on unknown members instead of guessing their meaning.</summary>
    /// <param name="fixture">The fixture name.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(GoldenJsonFixtures))]
    public async Task GoldenFixtureWithUnknownMemberIsRejectedByCodec(string fixture)
    {
        var bytes = Encoding.UTF8.GetBytes(InjectGoldenUnknownMember(ProtocolGoldenFixtures.ReadText(fixture)));

        var exception = CaptureHttpException(() => _ = ReencodeGolden(fixture, bytes));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies duplicate members in a golden envelope are rejected instead of last-wins.</summary>
    /// <param name="fixture">The fixture name.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(GoldenJsonFixtures))]
    public async Task GoldenFixtureWithDuplicateMemberIsRejected(string fixture)
    {
        var json = ProtocolGoldenFixtures.ReadText(fixture);
        var firstMemberEnd = json.IndexOf(',', StringComparison.Ordinal);
        var duplicated = string.Concat(json.AsSpan(0, firstMemberEnd + 1), json.AsSpan(1, firstMemberEnd), json.AsSpan(firstMemberEnd + 1));

        var exception = CaptureHttpException(() => _ = ReencodeGolden(fixture, Encoding.UTF8.GetBytes(duplicated)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies truncated golden envelopes are rejected.</summary>
    /// <param name="fixture">The fixture name.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(GoldenJsonFixtures))]
    public async Task GoldenFixtureTruncatedIsRejected(string fixture)
    {
        var bytes = ProtocolGoldenFixtures.ReadBytes(fixture);
        var truncated = bytes.AsSpan(0, bytes.Length - 1).ToArray();

        var exception = CaptureHttpException(() => _ = ReencodeGolden(fixture, truncated));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies golden envelopes with invalid UTF-8 are rejected.</summary>
    /// <param name="fixture">The fixture name.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(GoldenJsonFixtures))]
    public async Task GoldenFixtureWithInvalidUtf8IsRejected(string fixture)
    {
        const byte InvalidUtf8Byte = 0xFF;
        var bytes = ProtocolGoldenFixtures.ReadBytes(fixture);
        var quote = Array.IndexOf(bytes, (byte)'"', GoldenFirstValueSearchStart);
        bytes[quote + 1] = InvalidUtf8Byte;

        var exception = CaptureHttpException(() => _ = ReencodeGolden(fixture, bytes));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies a golden connect response from a different protocol major is rejected.</summary>
    /// <param name="version">The foreign protocol version.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("2.0")]
    [Arguments("0.9")]
    public async Task GoldenConnectResponseWithForeignMajorIsRejected(string version)
    {
        var json = ProtocolGoldenFixtures.ReadText(GoldenConnectResponseFile)
            .Replace("\"protocolVersion\":\"1.0\"", $"\"protocolVersion\":\"{version}\"", StringComparison.Ordinal);

        var exception = CaptureHttpException(() => CreateGoldenCodec().DeserializeConnectResponse(Encoding.UTF8.GetBytes(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies a golden connect response at a later v1 minor still decodes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GoldenConnectResponseWithLaterMinorDecodes()
    {
        var json = ProtocolGoldenFixtures.ReadText(GoldenConnectResponseFile)
            .Replace("\"protocolVersion\":\"1.0\"", "\"protocolVersion\":\"1.7\"", StringComparison.Ordinal);
        Version expected = new(1, GoldenLaterMinorVersion);

        var decoded = CreateGoldenCodec().DeserializeConnectResponse(Encoding.UTF8.GetBytes(json));

        await Assert.That(decoded.ProtocolVersion).IsEqualTo(expected);
    }

    /// <summary>Verifies the client writes every canonical subscribe cursor form exactly as retained.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [NotInParallel(SubscribeQueryCaptureKey)]
    public async Task GoldenSubscribeQueriesMatchCurrentClientEncoder()
    {
        var expected = ProtocolGoldenFixtures.ReadLines(GoldenSubscribeQueriesFile);
        var requests = CreateGoldenSubscribeRequests();
        await Assert.That(expected.Length).IsEqualTo(requests.Length);

        for (var index = 0; index < requests.Length; index++)
        {
            await Assert.That(await CaptureSubscribeQueryAsync(requests[index])).IsEqualTo(expected[index]);
        }
    }

    /// <summary>Verifies every canonical subscribe cursor form decodes to the retained request.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task GoldenSubscribeQueriesDecodeToExpectedRequests()
    {
        var queries = ProtocolGoldenFixtures.ReadLines(GoldenSubscribeQueriesFile);
        var requests = CreateGoldenSubscribeRequests();
        var codec = CreateServerCodec();

        for (var index = 0; index < requests.Length; index++)
        {
            var decoded = codec.ParseSubscribeRequest(queries[index]);
            await Assert.That(decoded.StreamId).IsEqualTo(requests[index].StreamId);
            await Assert.That(decoded.SubscriptionId).IsEqualTo(requests[index].SubscriptionId);
            await Assert.That(decoded.Cursor).IsEqualTo(requests[index].Cursor);
            await Assert.That(decoded.InitialPosition).IsEqualTo(requests[index].InitialPosition);
        }
    }

    /// <summary>Encodes the golden object for one fixture.</summary>
    /// <param name="fixture">The fixture name.</param>
    /// <returns>The encoded bytes.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="fixture"/> has no codec mapping.</exception>
    private static byte[] EncodeGolden(string fixture)
    {
        var codec = CreateGoldenCodec();
        return fixture switch
        {
            GoldenConnectRequestFile => codec.SerializeConnectRequest(CreateGoldenConnectRequest()),
            GoldenConnectResponseFile => codec.SerializeConnectResponse(CreateGoldenCapabilities()),
            GoldenPushRequestFile => codec.SerializePushRequest(CreateGoldenBatch()),
            GoldenPushResponseFile => codec.SerializePushResponse(CreateGoldenBatch(), CreateGoldenSyncResult()),
            GoldenSubscribeResponseFile => codec.SerializeSubscribeResponse(CreateGoldenReceiveBatches()),
            GoldenAcknowledgeRequestFile => codec.SerializeAcknowledgement(CreateGoldenAcknowledgement()),
            GoldenSnapshotRecoveryRequestFile => codec.SerializeSnapshotRecoveryRequest(CreateGoldenSnapshotRequest()),
            GoldenSnapshotRecoveredResponseFile => codec.SerializeSnapshotRecoveryResponse(
                CreateGoldenSnapshotRequest(),
                CreateGoldenRecoveredSnapshot(CreateGoldenSnapshotRequest())),
            GoldenSnapshotRetentionExpiredResponseFile => codec.SerializeSnapshotRecoveryResponse(
                CreateGoldenSnapshotRequest(),
                CreateGoldenRetentionExpiredSnapshot()),
            _ => throw new ArgumentOutOfRangeException(nameof(fixture), fixture, GoldenUnknownFixtureMessage),
        };
    }

    /// <summary>Decodes fixture bytes with the current decoder and re-encodes the decoded object.</summary>
    /// <param name="fixture">The fixture name.</param>
    /// <param name="bytes">The fixture bytes.</param>
    /// <returns>The re-encoded bytes.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="fixture"/> has no codec mapping.</exception>
    private static byte[] ReencodeGolden(string fixture, byte[] bytes)
    {
        var codec = CreateGoldenCodec();
        var request = CreateGoldenSnapshotRequest();
        return fixture switch
        {
            GoldenConnectRequestFile => codec.SerializeConnectRequest(codec.DeserializeConnectRequest(bytes)),
            GoldenConnectResponseFile => codec.SerializeConnectResponse(codec.DeserializeConnectResponse(bytes)),
            GoldenPushRequestFile => codec.SerializePushRequest(codec.DeserializePushRequest(bytes)),
            GoldenPushResponseFile => codec.SerializePushResponse(CreateGoldenBatch(), codec.DeserializePushResponse(CreateGoldenBatch(), bytes, null)),
            GoldenSubscribeResponseFile => codec.SerializeSubscribeResponse(codec.DeserializeSubscribeResponse(bytes)),
            GoldenAcknowledgeRequestFile => codec.SerializeAcknowledgement(codec.DeserializeAcknowledgement(bytes)),
            GoldenSnapshotRecoveryRequestFile => codec.SerializeSnapshotRecoveryRequest(codec.DeserializeSnapshotRecoveryRequest(bytes)),
            GoldenSnapshotRecoveredResponseFile or GoldenSnapshotRetentionExpiredResponseFile =>
                codec.SerializeSnapshotRecoveryResponse(request, codec.DeserializeSnapshotRecoveryResponse(request, bytes)),
            _ => throw new ArgumentOutOfRangeException(nameof(fixture), fixture, GoldenUnknownFixtureMessage),
        };
    }

    /// <summary>Reads and writes fixture JSON through the protocol DTO metadata only.</summary>
    /// <param name="fixture">The fixture name.</param>
    /// <param name="json">The JSON text.</param>
    /// <returns>The reserialized JSON text.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="fixture"/> has no DTO mapping.</exception>
    private static string ReserializeGoldenDto(string fixture, string json) => fixture switch
    {
        GoldenConnectRequestFile => ReserializeGoldenDto(json, HttpProtocolJsonContext.Default.ConnectRequestWireInfo),
        GoldenConnectResponseFile => ReserializeGoldenDto(json, HttpProtocolJsonContext.Default.ConnectResponseWireInfo),
        GoldenPushRequestFile => ReserializeGoldenDto(json, HttpProtocolJsonContext.Default.PushRequestWireInfo),
        GoldenPushResponseFile => ReserializeGoldenDto(json, HttpProtocolJsonContext.Default.PushResponseWireInfo),
        GoldenSubscribeResponseFile => ReserializeGoldenDto(json, HttpProtocolJsonContext.Default.SubscribeResponseWireInfo),
        GoldenAcknowledgeRequestFile => ReserializeGoldenDto(json, HttpProtocolJsonContext.Default.AcknowledgeRequestWireInfo),
        GoldenSnapshotRecoveryRequestFile => ReserializeGoldenDto(json, HttpProtocolJsonContext.Default.SnapshotRecoveryRequestWireInfo),
        GoldenSnapshotRecoveredResponseFile or GoldenSnapshotRetentionExpiredResponseFile =>
            ReserializeGoldenDto(json, HttpProtocolJsonContext.Default.SnapshotRecoveryResponseWireInfo),
        _ => throw new ArgumentOutOfRangeException(nameof(fixture), fixture, GoldenUnknownFixtureMessage),
    };

    /// <summary>Reads and writes JSON through one protocol DTO metadata instance.</summary>
    /// <typeparam name="T">The DTO type.</typeparam>
    /// <param name="json">The JSON text.</param>
    /// <param name="typeInfo">The DTO metadata.</param>
    /// <returns>The reserialized JSON text.</returns>
    /// <exception cref="InvalidOperationException">The JSON is the null literal.</exception>
    private static string ReserializeGoldenDto<T>(string json, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> typeInfo)
        where T : class
    {
        var value = JsonSerializer.Deserialize(json, typeInfo) ?? throw new InvalidOperationException("Expected a protocol DTO.");
        return JsonSerializer.Serialize(value, typeInfo);
    }

    /// <summary>Injects an unknown member at the start of the root object and the first nested object.</summary>
    /// <param name="json">The fixture JSON.</param>
    /// <returns>The JSON with unknown members.</returns>
    private static string InjectGoldenUnknownMember(string json)
    {
        var withRoot = string.Concat("{".AsSpan(), GoldenUnknownMember.AsSpan(), json.AsSpan(1));
        var nested = withRoot.IndexOf(":{\"", GoldenUnknownMember.Length + 1, StringComparison.Ordinal);
        return nested < 0 ? withRoot : withRoot.Insert(nested + GoldenNestedObjectTokenLength, GoldenUnknownMember);
    }

    /// <summary>Asserts a decoded operation matches the golden operation field by field.</summary>
    /// <param name="actual">The decoded operation.</param>
    /// <param name="expected">The golden operation.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertGoldenOperationAsync(SyncOperation actual, SyncOperation expected)
    {
        await Assert.That(actual.OperationId).IsEqualTo(expected.OperationId);
        await Assert.That(actual.StreamId).IsEqualTo(expected.StreamId);
        await Assert.That(actual.ClientSequence).IsEqualTo(expected.ClientSequence);
        await Assert.That(actual.TimestampUtc).IsEqualTo(expected.TimestampUtc);
        await Assert.That(actual.BaseVersion).IsEqualTo(expected.BaseVersion);
        await Assert.That(actual.Type).IsEqualTo(expected.Type);
        await Assert.That(actual.Policy).IsEqualTo(expected.Policy);
        await Assert.That(actual.Payload.ContractId).IsEqualTo(expected.Payload.ContractId);
        await Assert.That(actual.Payload.SchemaVersion).IsEqualTo(expected.Payload.SchemaVersion);
        await Assert.That(actual.Payload.PayloadHash).IsEqualTo(expected.Payload.PayloadHash);
        await Assert.That(actual.Payload.Payload.Span.SequenceEqual(expected.Payload.Payload.Span)).IsTrue();
        await AssertGoldenSequenceAsync(
            actual.Metadata.OrderBy(static pair => pair.Key, StringComparer.Ordinal),
            expected.Metadata.OrderBy(static pair => pair.Key, StringComparer.Ordinal));
    }

    /// <summary>Asserts two sequences contain equal items in the same order.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="actual">The decoded items.</param>
    /// <param name="expected">The golden items.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertGoldenSequenceAsync<T>(IEnumerable<T> actual, IEnumerable<T> expected)
    {
        T[] actualItems = [.. actual];
        T[] expectedItems = [.. expected];
        await Assert.That(actualItems.Length).IsEqualTo(expectedItems.Length);
        for (var index = 0; index < expectedItems.Length; index++)
        {
            await Assert.That(actualItems[index]).IsEqualTo(expectedItems[index]);
        }
    }
}
