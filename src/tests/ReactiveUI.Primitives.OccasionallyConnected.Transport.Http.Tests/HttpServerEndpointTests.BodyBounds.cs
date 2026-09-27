// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Text;
namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests hostile request bodies and stream identifiers on the portable server endpoint.</summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>The nesting depth used by deeply nested JSON bodies.</summary>
    private const int HostileJsonNestingDepth = 100_000;

    /// <summary>The metadata entry limit configured for cardinality tests.</summary>
    private const int CardinalityMetadataLimit = 4;

    /// <summary>The uncompressed size of the decompression bomb body.</summary>
    private const int DecompressionBombBytes = 8 * BytesPerKibibyte * BytesPerKibibyte;

    /// <summary>The serialized stream identifier property for the shared push fixture.</summary>
    private const string SerializedStreamIdProperty = "\"streamId\":\"stream-1\"";

    /// <summary>The traversal stream identifier used by subscribe query tests.</summary>
    private const string TraversalSubscribeQuery =
        "?streamId=..%2Fsecret&subscriptionId=00000000-0000-0000-0000-000000000301&positionKind=0";

    /// <summary>Verifies unsafe wire stream identifiers are rejected by the push route before hub effects.</summary>
    /// <param name="jsonStreamId">The JSON-escaped stream identifier text placed in the request body.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("../stream-1")]
    [Arguments("stream-1/../../etc")]
    [Arguments("/stream-1")]
    [Arguments("stream-1/")]
    [Arguments("stream//1")]
    [Arguments("..\\\\stream-1")]
    [Arguments("stream\\u0000-1")]
    [Arguments("stream\\u202E-1")]
    [Arguments("C:stream-1")]
    [Arguments("")]
    public async Task HandleAsyncPushRejectsUnsafeWireStreamIdBeforeHub(string jsonStreamId)
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        var body = ReplaceStreamId(CreateCodec().SerializePushRequest(CreateBatch()), jsonStreamId);
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies a decomposed Unicode wire stream identifier reaches the hub in canonical composed form.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushNormalizesDecomposedWireStreamIdBeforeHub()
    {
        var observed = new List<SyncBatch>();
        var hub = new RecordingHub
        {
            ApplyHandler = (incoming, client, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                observed.Add(incoming);
                return ValueTask.FromResult(CreateServerResult(incoming, client));
            },
        };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        var session = await ConnectReplaySessionAsync(endpoint);
        var body = ReplaceStreamId(CreateCodec().SerializePushRequest(CreateBatch()), "cafe\\u0301");
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
        AddSessionReplayHeaders(request, HttpReplayOperationKind.Push, "POST", "push", [], body, session);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(observed[0].Operations[0].StreamId.Value).IsEqualTo("café");
    }

    /// <summary>Verifies an acknowledgement carrying a traversal stream identifier is rejected before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAcknowledgeRejectsTraversalStreamIdBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        var body = ReplaceStreamId(CreateCodec().SerializeAcknowledgement(CreateAcknowledgement()), "../stream-1");
        using var request = CreateProtocolRequest(HttpMethod.Post, AcknowledgeUri, body);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.AcknowledgeClient).IsNull();
    }

    /// <summary>Verifies a subscribe query carrying a traversal stream identifier is rejected before replay authorization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeRejectsTraversalStreamIdQueryBeforeReplayAuthorization()
    {
        var authorizer = new RecordingReplayAuthorizer();
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, authorizer));
        using var request = await CreateSignedSubscribeRequestAsync(endpoint);
        var authorizationCallsAfterConnect = authorizer.Calls;
        request.RequestUri = new($"{SubscribeUri}{TraversalSubscribeQuery}");

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(authorizer.Calls).IsEqualTo(authorizationCallsAfterConnect);
        await Assert.That(hub.SubscribeClient).IsNull();
    }

    /// <summary>Verifies a deeply nested JSON body is rejected with bounded work before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsDeeplyNestedJsonBodyBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        var json = new StringBuilder("{\"batchId\":\"")
            .Append(BatchIdText)
            .Append("\",\"operations\":")
            .Append('[', HostileJsonNestingDepth)
            .Append(']', HostileJsonNestingDepth)
            .Append('}')
            .ToString();
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, Encoding.UTF8.GetBytes(json));

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(response.Content.Headers.ContentLength.GetValueOrDefault()).IsEqualTo(0);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies operation metadata above the configured cardinality is rejected before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsMetadataCardinalityAboveLimitBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub) with { MaximumMetadataEntries = CardinalityMetadataLimit });
        var metadata = new Dictionary<string, string>();
        for (var index = 0; index <= CardinalityMetadataLimit; index++)
        {
            metadata[$"key-{index}"] = "value";
        }

        var operation = CreateOperation() with { Metadata = metadata };
        var body = CreateCodec().SerializePushRequest(new(Guid.Parse(BatchIdText), [operation]));
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.RequestEntityTooLarge);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies compressed bodies, including a decompression bomb, are rejected on every body route before decoding.</summary>
    /// <param name="target">The route target to exercise.</param>
    /// <param name="encoding">The declared content encoding.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(ConnectBodyReadTarget, "gzip")]
    [Arguments(PushBodyReadTarget, "gzip")]
    [Arguments(PushBodyReadTarget, "deflate")]
    [Arguments(PushBodyReadTarget, "br")]
    [Arguments(AcknowledgeBodyReadTarget, "gzip")]
    public async Task HandleAsyncRejectsCompressedDecompressionBombBeforeDecoding(int target, string encoding)
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        var bomb = CreateGzipBomb();
        await Assert.That(bomb.Length < BytesPerKibibyte * BytesPerKibibyte).IsTrue();
        using var content = CreateProtocolContent(bomb);
        content.Headers.ContentEncoding.Add(encoding);
        using var request = CreateBodyReadRequest(target, content);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.UnsupportedMediaType);
        await Assert.That(hub.ApplyClient).IsNull();
        await Assert.That(hub.AcknowledgeClient).IsNull();
    }

    /// <summary>Replaces the serialized stream identifier in a protocol body.</summary>
    /// <param name="body">The serialized protocol body.</param>
    /// <param name="jsonStreamId">The JSON-escaped replacement identifier text.</param>
    /// <returns>The rewritten body.</returns>
    /// <exception cref="InvalidOperationException">The fixture body does not contain the expected stream identifier.</exception>
    private static byte[] ReplaceStreamId(byte[] body, string jsonStreamId)
    {
        var json = Encoding.UTF8.GetString(body);
        if (!json.Contains(SerializedStreamIdProperty, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The fixture body does not contain the expected stream identifier.");
        }

        return Encoding.UTF8.GetBytes(json.Replace(SerializedStreamIdProperty, $"\"streamId\":\"{jsonStreamId}\"", StringComparison.Ordinal));
    }

    /// <summary>Creates a small gzip body that expands to several mebibytes of JSON whitespace.</summary>
    /// <returns>The compressed bytes.</returns>
    private static byte[] CreateGzipBomb()
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.SmallestSize, leaveOpen: true))
        {
            var block = new byte[BytesPerKibibyte];
            Array.Fill(block, (byte)' ');
            for (var written = 0; written < DecompressionBombBytes; written += block.Length)
            {
                gzip.Write(block, 0, block.Length);
            }
        }

        return output.ToArray();
    }
}
