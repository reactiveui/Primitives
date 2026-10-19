// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>
/// Deterministic, seeded and bounded fuzz tests for <see cref="HttpServerEndpoint"/> request handling. Every case
/// derives its own seed from a fixed base seed and the case index, so a failure message is enough to replay it.
/// </summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>The fixed base seed for request envelope fuzzing.</summary>
    private const ulong EndpointEnvelopeFuzzSeed = 0x5EED_E0D0_0000_0001UL;

    /// <summary>The fixed base seed for signed body fuzzing.</summary>
    private const ulong EndpointSignedFuzzSeed = 0x5EED_E0D0_0000_0002UL;

    /// <summary>The request envelope fuzz case count.</summary>
    private const int EndpointEnvelopeFuzzCases = 3000;

    /// <summary>The signed body fuzz case count.</summary>
    private const int EndpointSignedFuzzCases = 2000;

    /// <summary>The number of cases that share one endpoint before a fresh endpoint resets replay state.</summary>
    private const int EndpointFuzzCasesPerEndpoint = 250;

    /// <summary>The request body byte limit configured for fuzzed endpoints.</summary>
    private const int EndpointFuzzBodyLimit = 4096;

    /// <summary>The maximum number of failures collected before a fuzz test stops.</summary>
    private const int EndpointFuzzFailureLimit = 8;

    /// <summary>The long-poll timeout configured for fuzzed endpoints.</summary>
    private const int EndpointFuzzLongPollMilliseconds = 20;

    /// <summary>The largest random body, as a multiple of the body limit.</summary>
    private const int EndpointFuzzBodyFactor = 3;

    /// <summary>The number of request content shapes.</summary>
    private const int EndpointFuzzContentShapes = 4;

    /// <summary>The content shape whose declared length matches the body.</summary>
    private const int KnownLengthContentShape = 1;

    /// <summary>The content shape that declares no length.</summary>
    private const int UnknownLengthContentShape = 2;

    /// <summary>The number of request body sources.</summary>
    private const int EndpointFuzzBodySources = 3;

    /// <summary>The number of random headers added at most.</summary>
    private const int EndpointFuzzMaximumHeaders = 4;

    /// <summary>The divisor that sets the minimum share of envelope cases that must reach body reading.</summary>
    private const int EndpointFuzzMinimumBodyReadShare = 20;

    /// <summary>The odds that a biased pick returns its preferred value.</summary>
    private const int EndpointFuzzPreferredOdds = 2;

    /// <summary>The odds that a request target gets extra query text.</summary>
    private const int EndpointFuzzQueryOdds = 4;

    /// <summary>The first successful HTTP status code.</summary>
    private const int FirstSuccessStatusCode = 200;

    /// <summary>The first redirection HTTP status code.</summary>
    private const int FirstRedirectionStatusCode = 300;

    /// <summary>The first client error HTTP status code.</summary>
    private const int FirstClientErrorStatusCode = 400;

    /// <summary>The first status code after the server error range.</summary>
    private const int ServerErrorStatusCodeLimit = 600;

    /// <summary>The golden push request fixture used as fuzz seed input.</summary>
    private const string FuzzPushRequestFixture = "push-request.json";

    /// <summary>The golden acknowledgement request fixture used as fuzz seed input.</summary>
    private const string FuzzAcknowledgeRequestFixture = "acknowledge-request.json";

    /// <summary>The golden connect request fixture used as fuzz seed input.</summary>
    private const string FuzzConnectRequestFixture = "connect-request.json";

    /// <summary>The signed push target.</summary>
    private const int SignedPushTarget = 0;

    /// <summary>The signed acknowledgement target.</summary>
    private const int SignedAcknowledgeTarget = 1;

    /// <summary>The per-case timeout that turns a hang into a reported failure.</summary>
    private static readonly TimeSpan EndpointFuzzCaseTimeout = TimeSpan.FromSeconds(5);

    /// <summary>HTTP methods sent by the envelope fuzzer.</summary>
    private static readonly string[] EndpointFuzzMethods = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "TRACE", "FUZZ", "post", "get"];

    /// <summary>Request targets sent by the envelope fuzzer.</summary>
    private static readonly string[] EndpointFuzzTargets =
    [
        ConnectUri, PushUri, AcknowledgeUri, SubscribeUri + SubscribeQuery, "https://example.invalid/snapshot-recovery", RelativeConnectUri,
        RelativeSubscribeUri + SubscribeQuery, EscapedPushUri, EscapedPushSeparatorUri, "https://example.invalid/../push", RootUri,
        MalformedPercentRouteUri, MalformedHexRouteUri, "https://example.invalid/push/", "https://example.invalid//push", "https://example.invalid/PUSH",
        $"https://example.invalid/{new string('a', EndpointFuzzBodyLimit)}", "https://example.invalid/subscribe?", "/subscribe?streamId=%ZZ",
    ];

    /// <summary>Body routes preferred by the envelope fuzzer so most cases reach body reading.</summary>
    private static readonly string[] EndpointFuzzBodyRoutes = [ConnectUri, PushUri, AcknowledgeUri, "https://example.invalid/snapshot-recovery"];

    /// <summary>Query suffixes appended by the envelope fuzzer.</summary>
    private static readonly string[] EndpointFuzzQuerySuffixes = ["?x=1", "&&", "&cursor=%00", "#fragment", "?%", "&positionKind=99", "&sequence=-1"];

    /// <summary>Content types sent by the envelope fuzzer.</summary>
    private static readonly string[] EndpointFuzzContentTypes =
    [
        ProtocolMediaType, "application/json", "application/vnd.reactiveui.occasionally-connected+json;v=2", DuplicateProtocolVersionMediaType,
        "application/vnd.reactiveui.occasionally-connected+json", "APPLICATION/VND.REACTIVEUI.OCCASIONALLY-CONNECTED+JSON;V=1", "text/plain",
        "application/vnd.reactiveui.occasionally-connected+json; v=\"1\"", "multipart/form-data; boundary=x", ";;;", string.Empty,
    ];

    /// <summary>Content encodings sent by the envelope fuzzer.</summary>
    private static readonly string[] EndpointFuzzContentEncodings = [string.Empty, string.Empty, "gzip", "br", "identity", "gzip, deflate", "x-unknown"];

    /// <summary>Header names sent by the envelope fuzzer.</summary>
    private static readonly string[] EndpointFuzzHeaderNames =
    [
        ReplayMessageIdHeader, ReplayNonceHeader, ReplaySentAtHeader, ReplaySessionIdHeader, ReplayMacHeader, "Accept", "Transfer-Encoding",
        "Expect", "X-Unknown", "traceparent", "tracestate",
    ];

    /// <summary>Header values sent by the envelope fuzzer.</summary>
    private static readonly string[] EndpointFuzzHeaderValues =
    [
        string.Empty, ReplayMessageId, "a b", "2026-09-17T22:00:00+00:00", "not-a-date", new string('x', EndpointFuzzBodyLimit), "é", "%00",
        InvalidHeaderReplaySessionId, InvalidHeaderReplayMac, "chunked", "100-continue", "00-00000000000000000000000000000000-0000000000000000-00",
    ];

    /// <summary>
    /// Verifies random methods, targets, headers, content types, content encodings and body sizes always produce a
    /// success, client error or non-500 server error status, never hang, never throw, and never read more than one
    /// byte past the configured body limit.
    /// </summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task FuzzedRequestEnvelopeReturnsBoundedStatusWithoutOverreading()
    {
        byte[][] seeds = [.. ReadEndpointFuzzSeeds()];
        List<string> failures = [];
        var bodyReads = 0;
        for (var batch = 0; batch < EndpointEnvelopeFuzzCases / EndpointFuzzCasesPerEndpoint; batch++)
        {
            await using var endpoint = new HttpServerEndpoint(CreateFuzzEndpointOptions());
            for (var offset = 0; offset < EndpointFuzzCasesPerEndpoint && failures.Count < EndpointFuzzFailureLimit; offset++)
            {
                var caseIndex = (batch * EndpointFuzzCasesPerEndpoint) + offset;
                var random = new ProtocolFuzzRandom(ProtocolFuzzRandom.DeriveSeed(EndpointEnvelopeFuzzSeed, nameof(HttpServerEndpoint), caseIndex));
                var (request, body) = CreateFuzzEnvelopeRequest(random, seeds);
                using (request)
                {
                    var outcome = await SendFuzzRequestAsync(endpoint, request, body);
                    bodyReads += body is { BytesRead: > 0 } ? 1 : 0;
                    AddEndpointFuzzFailure(failures, outcome.Failure, ProtocolFuzzRandom.Describe(EndpointEnvelopeFuzzSeed, nameof(HttpServerEndpoint), caseIndex));
                }
            }
        }

        await Assert.That(string.Join(Environment.NewLine, failures)).IsEmpty();
        await Assert.That(bodyReads).IsGreaterThan(EndpointEnvelopeFuzzCases / EndpointFuzzMinimumBodyReadShare);
    }

    /// <summary>
    /// Verifies correctly signed push, acknowledgement and connect requests with mutated bodies reach the protocol
    /// codec and always produce a success, client error or non-500 server error status without hanging or throwing.
    /// </summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task FuzzedSignedBodyIsDecodedOrRejectedWithBoundedStatus()
    {
        byte[][] seeds = [.. ReadEndpointFuzzSeeds()];
        List<string> failures = [];
        var successes = 0;
        for (var batch = 0; batch < EndpointSignedFuzzCases / EndpointFuzzCasesPerEndpoint; batch++)
        {
            await using var endpoint = new HttpServerEndpoint(CreateFuzzEndpointOptions());
            var session = await ConnectReplaySessionAsync(endpoint);
            for (var offset = 0; offset < EndpointFuzzCasesPerEndpoint && failures.Count < EndpointFuzzFailureLimit; offset++)
            {
                var caseIndex = (batch * EndpointFuzzCasesPerEndpoint) + offset;
                var random = new ProtocolFuzzRandom(ProtocolFuzzRandom.DeriveSeed(EndpointSignedFuzzSeed, nameof(HttpServerEndpoint), caseIndex));
                var target = random.Next(seeds.Length);
                var body = ProtocolFuzzMutator.Mutate(seeds[target], random);
                using var request = CreateSignedFuzzRequest(target, body, session, caseIndex);
                var outcome = await SendFuzzRequestAsync(endpoint, request, null);
                successes += outcome.StatusCode == HttpStatusCode.OK ? 1 : 0;
                AddEndpointFuzzFailure(failures, outcome.Failure, ProtocolFuzzRandom.Describe(EndpointSignedFuzzSeed, nameof(HttpServerEndpoint), caseIndex));
            }
        }

        await Assert.That(string.Join(Environment.NewLine, failures)).IsEmpty();
        await Assert.That(successes).IsGreaterThan(0);
    }

    /// <summary>Reads the golden request bodies used as fuzz seed input.</summary>
    /// <returns>The push, acknowledgement and connect request bodies.</returns>
    private static IEnumerable<byte[]> ReadEndpointFuzzSeeds()
    {
        yield return ProtocolGoldenFixtures.ReadBytes(FuzzPushRequestFixture);
        yield return ProtocolGoldenFixtures.ReadBytes(FuzzAcknowledgeRequestFixture);
        yield return ProtocolGoldenFixtures.ReadBytes(FuzzConnectRequestFixture);
    }

    /// <summary>Creates endpoint options for fuzzing with a small body limit and a short long-poll window.</summary>
    /// <returns>The endpoint options.</returns>
    private static HttpServerEndpointOptions CreateFuzzEndpointOptions() =>
        CreateReplayOptions(new AcceptingFuzzHub()) with
        {
            MaximumRequestBytes = EndpointFuzzBodyLimit,
            LongPollTimeout = TimeSpan.FromMilliseconds(EndpointFuzzLongPollMilliseconds),
        };

    /// <summary>Records a failed case with its reproduction coordinates.</summary>
    /// <param name="failures">The collected failures.</param>
    /// <param name="failure">The case failure, or <see langword="null"/>.</param>
    /// <param name="reproduction">The reproduction coordinates.</param>
    private static void AddEndpointFuzzFailure(List<string> failures, string? failure, string reproduction)
    {
        if (failure is not null)
        {
            failures.Add($"{reproduction}: {failure}");
        }
    }

    /// <summary>Sends one fuzzed request and checks the endpoint invariants.</summary>
    /// <param name="endpoint">The endpoint.</param>
    /// <param name="request">The request.</param>
    /// <param name="body">The counting body stream, when the request has one.</param>
    /// <returns>The case outcome.</returns>
    private static async Task<EndpointFuzzOutcome> SendFuzzRequestAsync(HttpServerEndpoint endpoint, HttpRequestMessage request, CountingReadStream? body)
    {
        HttpResponseMessage response;
        try
        {
            response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask().WaitAsync(EndpointFuzzCaseTimeout);
        }
        catch (TimeoutException)
        {
            return new(null, "the request exceeded the per-case timeout");
        }
        catch (Exception exception)
        {
            return new(null, $"HandleAsync threw {exception.GetType().FullName}: {exception.Message}");
        }

        using (response)
        {
            return new(response.StatusCode, CheckFuzzResponse(request, response.StatusCode, body));
        }
    }

    /// <summary>Checks the status code range and the body read bound of one response.</summary>
    /// <param name="request">The request.</param>
    /// <param name="statusCode">The response status code.</param>
    /// <param name="body">The counting body stream, when the request has one.</param>
    /// <returns>The failure text, or <see langword="null"/> when the case holds.</returns>
    private static string? CheckFuzzResponse(HttpRequestMessage request, HttpStatusCode statusCode, CountingReadStream? body)
    {
        var numeric = (int)statusCode;
        var bounded = numeric is >= FirstSuccessStatusCode and < FirstRedirectionStatusCode
            || (numeric is >= FirstClientErrorStatusCode and < ServerErrorStatusCodeLimit && statusCode != HttpStatusCode.InternalServerError);
        if (!bounded)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{request.Method} {request.RequestUri} returned {numeric}");
        }

        return body is not null && body.BytesRead > EndpointFuzzBodyLimit + 1
            ? string.Create(CultureInfo.InvariantCulture, $"{request.Method} {request.RequestUri} read {body.BytesRead} body bytes past a {EndpointFuzzBodyLimit}-byte limit")
            : null;
    }

    /// <summary>Creates one fuzzed request envelope.</summary>
    /// <param name="random">The case generator.</param>
    /// <param name="seeds">The golden request bodies.</param>
    /// <returns>The request and its counting body stream, when it has one.</returns>
    private static (HttpRequestMessage Request, CountingReadStream? Body) CreateFuzzEnvelopeRequest(ProtocolFuzzRandom random, byte[][] seeds)
    {
        var request = new HttpRequestMessage(new HttpMethod(PickBiased(random, PostMethodName, EndpointFuzzMethods)), CreateFuzzUri(random));
        var headerCount = random.Next(EndpointFuzzMaximumHeaders + 1);
        for (var index = 0; index < headerCount; index++)
        {
            _ = request.Headers.TryAddWithoutValidation(random.Pick(EndpointFuzzHeaderNames), random.Pick(EndpointFuzzHeaderValues));
        }

        var shape = random.Next(EndpointFuzzContentShapes);
        if (shape == 0)
        {
            return (request, null);
        }

        var body = new CountingReadStream(CreateFuzzBody(random, seeds));
        var content = new StreamContent(body);
        content.Headers.ContentLength = shape switch
        {
            KnownLengthContentShape => body.Length,
            UnknownLengthContentShape => null,
            _ => random.Next(EndpointFuzzBodyLimit * EndpointFuzzBodyFactor),
        };
        AddFuzzContentHeader(content, "Content-Type", PickBiased(random, ProtocolMediaType, EndpointFuzzContentTypes));
        AddFuzzContentHeader(content, "Content-Encoding", PickBiased(random, string.Empty, EndpointFuzzContentEncodings));
        request.Content = content;
        return (request, body);
    }

    /// <summary>Returns a preferred value half of the time and a random alternative otherwise.</summary>
    /// <param name="random">The case generator.</param>
    /// <param name="preferred">The preferred value that lets a case reach deeper request handling.</param>
    /// <param name="alternatives">The hostile alternatives.</param>
    /// <returns>The picked value.</returns>
    private static string PickBiased(ProtocolFuzzRandom random, string preferred, string[] alternatives) =>
        random.OneIn(EndpointFuzzPreferredOdds) ? preferred : random.Pick(alternatives);

    /// <summary>Adds a content header without validation when its value is not empty.</summary>
    /// <param name="content">The content.</param>
    /// <param name="name">The header name.</param>
    /// <param name="value">The header value.</param>
    private static void AddFuzzContentHeader(HttpContent content, string name, string value)
    {
        if (value.Length > 0)
        {
            _ = content.Headers.TryAddWithoutValidation(name, value);
        }
    }

    /// <summary>Creates a fuzzed request target.</summary>
    /// <param name="random">The case generator.</param>
    /// <returns>The request URI.</returns>
    private static Uri CreateFuzzUri(ProtocolFuzzRandom random)
    {
        var target = PickBiased(random, random.Pick(EndpointFuzzBodyRoutes), EndpointFuzzTargets);
        if (random.OneIn(EndpointFuzzQueryOdds))
        {
            target += random.Pick(EndpointFuzzQuerySuffixes);
        }

        return Uri.TryCreate(target, UriKind.RelativeOrAbsolute, out var uri) ? uri : new(PushUri);
    }

    /// <summary>Creates a fuzzed request body.</summary>
    /// <param name="random">The case generator.</param>
    /// <param name="seeds">The golden request bodies.</param>
    /// <returns>The body bytes.</returns>
    private static byte[] CreateFuzzBody(ProtocolFuzzRandom random, byte[][] seeds) => random.Next(EndpointFuzzBodySources) switch
    {
        0 => ProtocolFuzzMutator.Mutate(random.Pick(seeds), random),
        1 => random.NextBytes(random.Next(EndpointFuzzBodyLimit * EndpointFuzzBodyFactor)),
        _ => Encoding.ASCII.GetBytes(new string('[', random.Next(EndpointFuzzBodyLimit, EndpointFuzzBodyLimit * EndpointFuzzBodyFactor))),
    };

    /// <summary>Creates a correctly signed request for a mutated body.</summary>
    /// <param name="target">The signed target.</param>
    /// <param name="body">The mutated body.</param>
    /// <param name="session">The replay session.</param>
    /// <param name="caseIndex">The case index used for unique replay identifiers.</param>
    /// <returns>The signed request.</returns>
    private static HttpRequestMessage CreateSignedFuzzRequest(int target, byte[] body, ReplaySession session, int caseIndex)
    {
        var messageId = string.Create(CultureInfo.InvariantCulture, $"fuzz-message-{caseIndex}");
        var nonce = string.Create(CultureInfo.InvariantCulture, $"fuzz-nonce-{caseIndex}");
        if (target == SignedPushTarget)
        {
            var push = CreateProtocolRequest(HttpMethod.Post, PushUri, body);
            AddFuzzSessionReplayHeaders(push, new(HttpReplayOperationKind.Push, "push", body, session, messageId, nonce));
            return push;
        }

        if (target == SignedAcknowledgeTarget)
        {
            var acknowledge = CreateProtocolRequest(HttpMethod.Post, AcknowledgeUri, body);
            AddFuzzSessionReplayHeaders(acknowledge, new(HttpReplayOperationKind.Acknowledge, "ack", body, session, messageId, nonce));
            return acknowledge;
        }

        var connect = CreateProtocolRequest(HttpMethod.Post, ConnectUri, body);
        AddConnectReplayHeaders(connect, body, ReplaySentAtUtc, messageId, nonce);
        return connect;
    }

    /// <summary>Adds signed replay headers with unique replay identifiers for one fuzz case.</summary>
    /// <param name="request">The request.</param>
    /// <param name="signing">The signing inputs.</param>
    private static void AddFuzzSessionReplayHeaders(HttpRequestMessage request, FuzzSigning signing)
    {
        AddFreshnessHeaders(request, signing.MessageId, signing.Nonce, ReplaySentAtUtc);
        request.Headers.Add(ReplaySessionIdHeader, signing.Session.SessionId);
        var canonical = CreateCanonicalRequest(signing.Operation, "POST", signing.Path, [], signing.Body, ReplaySentAtUtc);
        var replayRequest = new HttpReplayRequest
        {
            Operation = signing.Operation,
            Principal = new(TenantId, ClientId),
            MessageId = signing.MessageId,
            Nonce = signing.Nonce,
            SentAtUtc = ReplaySentAtUtc,
            ReplaySessionId = signing.Session.SessionId,
            ReplayMac = "placeholder",
            CanonicalRequest = canonical,
        };
        var hasher = new HttpReplayEnvelopeHasher();
        var envelope = hasher.Create(replayRequest);
        request.Headers.Add(ReplayMacHeader, hasher.ComputeMac(Encoding.UTF8.GetBytes(signing.Session.SessionSecret), envelope.MacInput));
    }

    /// <summary>Describes the outcome of one endpoint fuzz case.</summary>
    /// <param name="StatusCode">The response status, when a response was produced.</param>
    /// <param name="Failure">The invariant violation, or <see langword="null"/>.</param>
    private readonly record struct EndpointFuzzOutcome(HttpStatusCode? StatusCode, string? Failure);

    /// <summary>A hub that accepts every operation, acknowledgement and subscription without side effects.</summary>
    private sealed class AcceptingFuzzHub : IServerStreamHub
    {
        /// <inheritdoc/>
        public ValueTask<ServerSyncResult> ApplyOperationsAsync(SyncBatch batch, ServerAuthenticatedClient client, CancellationToken cancellationToken)
        {
            var results = new OperationSyncResult[batch.Operations.Count];
            for (var index = 0; index < results.Length; index++)
            {
                results[index] = new(batch.Operations[index].OperationId, OperationResultKind.Accepted, null, ServerCursor);
            }

            return ValueTask.FromResult(new ServerSyncResult(new(batch.BatchId, results, ServerCursor, null), []));
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, ServerAuthenticatedClient client, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(RemoteSubscribeRequest request, ServerAuthenticatedClient client, CancellationToken cancellationToken) =>
            YieldBatches([], cancellationToken);
    }

    /// <summary>A non-seekable request body that counts every byte the endpoint reads.</summary>
    /// <param name="body">The body bytes.</param>
    private sealed class CountingReadStream(byte[] body) : Stream
    {
        /// <summary>The underlying body.</summary>
        private readonly MemoryStream _inner = new(body, writable: false);

        /// <summary>Gets the number of bytes read by the endpoint.</summary>
        public long BytesRead { get; private set; }

        /// <inheritdoc/>
        public override bool CanRead => true;

        /// <inheritdoc/>
        public override bool CanSeek => false;

        /// <inheritdoc/>
        public override bool CanWrite => false;

        /// <inheritdoc/>
        public override long Length => _inner.Length;

        /// <inheritdoc/>
        public override long Position
        {
            get => _inner.Position;
            set => throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override void Flush()
        {
        }

        /// <inheritdoc/>
        public override int Read(byte[] buffer, int offset, int count)
        {
            var read = _inner.Read(buffer, offset, count);
            BytesRead += read;
            return read;
        }

        /// <inheritdoc/>
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        /// <inheritdoc/>
        public override void SetLength(long value) => throw new NotSupportedException();

        /// <inheritdoc/>
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _inner.Dispose();
            }

            base.Dispose(disposing);
        }
    }

    /// <summary>Groups the inputs that sign one fuzzed session request.</summary>
    /// <param name="Operation">The replay operation kind.</param>
    /// <param name="Path">The canonical route path.</param>
    /// <param name="Body">The exact request body.</param>
    /// <param name="Session">The replay session.</param>
    /// <param name="MessageId">The unique replay message identifier.</param>
    /// <param name="Nonce">The unique replay nonce.</param>
    private sealed record FuzzSigning(HttpReplayOperationKind Operation, string Path, byte[] Body, ReplaySession Session, string MessageId, string Nonce);
}
