// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Tests for <see cref="AspNetHttpRequestBridge"/>.</summary>
public sealed class AspNetHttpRequestBridgeTests
{
    /// <summary>The bearer token accepted by the development credential store.</summary>
    private const string Token = "token-a";

    /// <summary>The authenticated tenant identifier associated with <see cref="Token"/>.</summary>
    private const string Tenant = "tenant-a";

    /// <summary>The authenticated client identifier associated with <see cref="Token"/>.</summary>
    private const string Client = "client-a";

    /// <summary>The JSON request body used by bridge tests.</summary>
    private const string BodyText = """{"value":42}""";

    /// <summary>The JSON response body returned by the fake endpoint.</summary>
    private const string ReplyText = """{"accepted":true}""";

    /// <summary>The response header name used by bridge tests.</summary>
    private const string ReplyHeaderName = "X-Reply-Id";

    /// <summary>The first response header value used by bridge tests.</summary>
    private const string ReplyHeaderFirstValue = "reply-1";

    /// <summary>The second response header value used by bridge tests.</summary>
    private const string ReplyHeaderSecondValue = "reply-2";

    /// <summary>The expected multi-value response header count.</summary>
    private const int ExpectedReplyHeaderValueCount = 2;

    /// <summary>The exception message used when the bridge fails to provide request content.</summary>
    private const string MissingRequestContentMessage = "The request content should be available.";

    /// <summary>The content-length header name used by body detection tests.</summary>
    private const string ContentLengthHeaderName = "Content-Length";

    /// <summary>The transfer-encoding header name used by body detection tests.</summary>
    private const string TransferEncodingHeaderName = "Transfer-Encoding";

    /// <summary>The loopback port used when constructing the test request host.</summary>
    private const int RequestPort = 5088;

    /// <summary>The endpoint request byte limit used by bounded streaming tests.</summary>
    private const int MaximumEndpointRequestBytes = 64;

    /// <summary>The stream member read buffer size used by bridge tests.</summary>
    private const int StreamMemberReadBufferSize = 4;

    /// <summary>The seek position used by stream member tests.</summary>
    private const int StreamMemberSeekPosition = 1;

    /// <summary>The synthetic chunked request body size used by bounded streaming tests.</summary>
    private const int ByteSequenceSize = 256 * 1024;

    /// <summary>The response header values used by bridge tests.</summary>
    private static readonly string[] ReplyHeaderValues = [ReplyHeaderFirstValue, ReplyHeaderSecondValue];

    /// <summary>Verifies the bridge preserves request body, headers, status and response body around host authentication.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncPreservesPortableRequestAndResponse()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();
        PortableHttpEndpointDispatch dispatch = static async (request, client, cancellationToken) =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Assert.That(client).IsEqualTo(new(Tenant, Client));
            await Assert.That(request.Method).IsEqualTo(HttpMethod.Post);
            await Assert.That(request.RequestUri?.AbsolutePath).IsEqualTo("/oc/push");
            await Assert.That(request.RequestUri?.Query).IsEqualTo("?a=1");
            await Assert.That(request.Headers.Contains("X-Trace-Id")).IsTrue();
            await Assert.That(request.Content).IsNotNull();
            var content = request.Content ?? throw new InvalidOperationException(MissingRequestContentMessage);
            var body = await content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            await Assert.That(body).IsEqualTo(BodyText);

            var response = new HttpResponseMessage(HttpStatusCode.Accepted) { Content = new StringContent(ReplyText, Encoding.UTF8, "application/json") };
            _ = response.Headers.TryAddWithoutValidation(ReplyHeaderName, ReplyHeaderFirstValue);
            return response;
        };

        await bridge.InvokeAsync(context, new([new(Token, Tenant, Client)]), dispatch);

        await Assert.That(context.Response.StatusCode).IsEqualTo((int)HttpStatusCode.Accepted);
        await Assert.That(context.Response.Headers[ReplyHeaderName].ToString()).IsEqualTo(ReplyHeaderFirstValue);
        await Assert.That(await ReadBodyAsync(context.Response).ConfigureAwait(false)).IsEqualTo(ReplyText);
    }

    /// <summary>Verifies unauthenticated calls fail before dispatch.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncRejectsMissingTokenBeforeDispatch()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();
        _ = context.Request.Headers.Remove(DevelopmentCredentialStore.TokenHeaderName);
        var dispatched = false;

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            (_, _, _) =>
            {
                dispatched = true;
                return ValueTask.FromResult<HttpResponseMessage>(new(HttpStatusCode.OK));
            });

        await Assert.That(context.Response.StatusCode).IsEqualTo((int)HttpStatusCode.Unauthorized);
        await Assert.That(dispatched).IsFalse();
    }

    /// <summary>Verifies request cancellation is passed through rather than translated into a success response.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncPreservesCancellation()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        context.RequestAborted = cancellation.Token;

        _ = await Assert.ThrowsExactlyAsync<OperationCanceledException>(() =>
            bridge.InvokeAsync(
                context,
                new([new(Token, Tenant, Client)]),
                static (_, _, cancellationToken) => throw new OperationCanceledException(cancellationToken)));
    }

    /// <summary>Verifies HTTP/2 requests with a detectable body do not require content-length or transfer-encoding headers.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncUsesBodyDetectionFeatureForHttp2Requests()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();
        context.Request.Protocol = "HTTP/2";
        context.Request.ContentLength = null;
        _ = context.Request.Headers.Remove(TransferEncodingHeaderName);
        context.Features.Set<IHttpRequestBodyDetectionFeature>(new RequestBodyDetectionFeature(true));

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            static async (request, _, cancellationToken) =>
            {
                await Assert.That(request.Content).IsNotNull();
                var content = request.Content ?? throw new InvalidOperationException(MissingRequestContentMessage);
                var body = await content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                await Assert.That(body).IsEqualTo(BodyText);
                return new(HttpStatusCode.OK);
            });
    }

    /// <summary>Verifies disposing the portable request wrapper does not close the ASP.NET-owned request body stream.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncLeavesHostOwnedRequestBodyOpen()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            static (_, _, _) => ValueTask.FromResult<HttpResponseMessage>(new(HttpStatusCode.OK)));

        context.Request.Body.Position = 0;
        using var reader = new StreamReader(context.Request.Body, Encoding.UTF8, leaveOpen: true);
        var body = await reader.ReadToEndAsync().ConfigureAwait(false);
        await Assert.That(body).IsEqualTo(BodyText);
    }

    /// <summary>Verifies chunked request bodies are not buffered before the endpoint can enforce request limits.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncLetsEndpointBoundChunkedBodyReadsWithoutBuffering()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();
        var body = new CountingNonSeekableReadStream(ByteSequenceSize);
        context.Request.Body = body;
        context.Request.ContentLength = null;
        _ = context.Request.Headers.Remove(ContentLengthHeaderName);
        context.Request.Headers.TransferEncoding = "chunked";

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            static async (request, _, cancellationToken) =>
            {
                var content = request.Content ?? throw new InvalidOperationException(MissingRequestContentMessage);
                var stream = await content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                var buffer = new byte[MaximumEndpointRequestBytes + 1];
                var read = await stream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
                return new(read > MaximumEndpointRequestBytes ? HttpStatusCode.RequestEntityTooLarge : HttpStatusCode.OK);
            });

        await Assert.That(context.Response.StatusCode).IsEqualTo((int)HttpStatusCode.RequestEntityTooLarge);
        await Assert.That(body.BytesRead).IsLessThanOrEqualTo(MaximumEndpointRequestBytes + 1L);
        await Assert.That(body.CanRead).IsTrue();
    }

    /// <summary>Verifies ambiguous token headers fail before dispatch.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncRejectsMultipleTokensBeforeDispatch()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();
        context.Request.Headers[DevelopmentCredentialStore.TokenHeaderName] = new[] { Token, "other-token" };
        var dispatched = false;

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            (_, _, _) =>
            {
                dispatched = true;
                return ValueTask.FromResult<HttpResponseMessage>(new(HttpStatusCode.OK));
            });

        await Assert.That(context.Response.StatusCode).IsEqualTo((int)HttpStatusCode.Unauthorized);
        await Assert.That(dispatched).IsFalse();
    }

    /// <summary>Verifies an unknown single token fails after the header shape is accepted.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncRejectsUnknownSingleTokenBeforeDispatch()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();
        context.Request.Headers[DevelopmentCredentialStore.TokenHeaderName] = "unknown-token";
        var dispatched = false;

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            (_, _, _) =>
            {
                dispatched = true;
                return ValueTask.FromResult<HttpResponseMessage>(new(HttpStatusCode.OK));
            });

        await Assert.That(context.Response.StatusCode).IsEqualTo((int)HttpStatusCode.Unauthorized);
        await Assert.That(dispatched).IsFalse();
    }

    /// <summary>Verifies a present token header with a null value is treated as unauthenticated.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncRejectsNullTokenValueBeforeDispatch()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();
        context.Request.Headers[DevelopmentCredentialStore.TokenHeaderName] = new(new string[1]);
        var dispatched = false;

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            (_, _, _) =>
            {
                dispatched = true;
                return ValueTask.FromResult<HttpResponseMessage>(new(HttpStatusCode.OK));
            });

        await Assert.That(context.Response.StatusCode).IsEqualTo((int)HttpStatusCode.Unauthorized);
        await Assert.That(dispatched).IsFalse();
    }

    /// <summary>Verifies requests without a detectable body do not allocate portable content.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncDoesNotCreateContentWhenBodyDetectionForbidsBody()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();
        context.Request.Method = HttpMethods.Get;
        context.Request.ContentLength = null;
        _ = context.Request.Headers.Remove(ContentLengthHeaderName);
        _ = context.Request.Headers.Remove(TransferEncodingHeaderName);
        context.Features.Set<IHttpRequestBodyDetectionFeature>(new RequestBodyDetectionFeature(false));

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            static (request, _, _) =>
                ValueTask.FromResult<HttpResponseMessage>(request.Content is null ? new(HttpStatusCode.OK) : new(HttpStatusCode.BadRequest)));

        await Assert.That(context.Response.StatusCode).IsEqualTo((int)HttpStatusCode.OK);
    }

    /// <summary>Verifies content-only headers do not create portable content when ASP.NET says the request has no body.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncDropsContentHeadersWhenNoPortableContentExists()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();
        context.Request.Method = HttpMethods.Get;
        context.Request.ContentLength = null;
        _ = context.Request.Headers.Remove(ContentLengthHeaderName);
        _ = context.Request.Headers.Remove(TransferEncodingHeaderName);
        context.Features.Set<IHttpRequestBodyDetectionFeature>(new RequestBodyDetectionFeature(false));

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            static async (request, _, _) =>
            {
                await Assert.That(request.Content).IsNull();
                return new(HttpStatusCode.OK);
            });

        await Assert.That(context.Response.StatusCode).IsEqualTo((int)HttpStatusCode.OK);
    }

    /// <summary>Verifies responses without portable content preserve status and leave the body empty.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncPreservesNoContentResponseWithoutBody()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            static (_, _, _) => ValueTask.FromResult<HttpResponseMessage>(new(HttpStatusCode.NoContent)));

        await Assert.That(context.Response.StatusCode).IsEqualTo((int)HttpStatusCode.NoContent);
        await Assert.That(await ReadBodyAsync(context.Response).ConfigureAwait(false)).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies response headers are copied when the portable response exposes default empty content.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncCopiesResponseHeadersWithDefaultEmptyContent()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            static (_, _, _) =>
            {
                var response = new HttpResponseMessage(HttpStatusCode.NoContent) { Content = null };
                _ = response.Headers.TryAddWithoutValidation(ReplyHeaderName, ReplyHeaderValues);
                return ValueTask.FromResult(response);
            });

        await Assert.That(context.Response.StatusCode).IsEqualTo((int)HttpStatusCode.NoContent);
        var replyHeaders = context.Response.Headers[ReplyHeaderName].ToArray();
        await Assert.That(replyHeaders).Count().IsEqualTo(ExpectedReplyHeaderValueCount);
        await Assert.That(replyHeaders[0]).IsEqualTo(ReplyHeaderFirstValue);
        await Assert.That(replyHeaders[1]).IsEqualTo(ReplyHeaderSecondValue);
        await Assert.That(await ReadBodyAsync(context.Response).ConfigureAwait(false)).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies a response whose content is explicitly removed exposes default empty content and leaves the ASP.NET body empty.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncPreservesExplicitlyRemovedResponseContentAsEmptyBody()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            static (_, _, _) =>
            {
                var response = new HttpResponseMessage(HttpStatusCode.NotModified) { Content = null };
                return ValueTask.FromResult(response);
            });

        await Assert.That(context.Response.StatusCode).IsEqualTo((int)HttpStatusCode.NotModified);
        await Assert.That(await ReadBodyAsync(context.Response).ConfigureAwait(false)).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies request content exposes the seekable ASP.NET body stream without taking ownership.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncForwardsSeekableRequestStreamMembers()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            static async (request, _, cancellationToken) =>
            {
                var content = request.Content ?? throw new InvalidOperationException(MissingRequestContentMessage);
                var stream = await content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                await Assert.That(stream.CanRead).IsTrue();
                await Assert.That(stream.CanWrite).IsFalse();
                await Assert.That(stream.CanSeek).IsTrue();
                await Assert.That(stream.Length).IsEqualTo(BodyText.Length);
                await Assert.That(stream.Position).IsEqualTo(0);

                stream.Position = StreamMemberSeekPosition;
                await Assert.That(stream.Position).IsEqualTo(StreamMemberSeekPosition);
                stream.Position = 0;
                var buffer = new byte[StreamMemberReadBufferSize];
                var synchronousRead = ReadWithSynchronousMembers(stream, buffer);
                await Assert.That(synchronousRead).IsGreaterThan(0);
                var asyncRead = await stream.ReadAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
                await Assert.That(asyncRead).IsGreaterThanOrEqualTo(0);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                await Assert.That(() => stream.SetLength(0)).ThrowsExactly<NotSupportedException>();
                await Assert.That(() => stream.Write(buffer, 0, buffer.Length)).ThrowsExactly<NotSupportedException>();
                return new(HttpStatusCode.OK);
            });

        context.Request.Body.Position = 0;
        await Assert.That(context.Request.Body.CanRead).IsTrue();
    }

    /// <summary>Verifies forwarded stream members keep the ASP.NET-owned body readable without enabling writes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvokeAsyncForwardsBorrowedStreamMembersWithoutClosingHostBody()
    {
        var bridge = new AspNetHttpRequestBridge();
        var context = CreateContext();
        var body = CreateStream("""{"value":42}"""u8.ToArray());
        context.Request.Body = body;
        context.Request.ContentLength = body.Length;

        await bridge.InvokeAsync(
            context,
            new([new(Token, Tenant, Client)]),
            static async (request, _, cancellationToken) =>
            {
                var content = request.Content ?? throw new InvalidOperationException(MissingRequestContentMessage);
                var stream = await content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                await Assert.That(stream.CanWrite).IsFalse();
                FlushForwardedStreamSynchronously(stream);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                await Assert.That(() => stream.SetLength(0)).ThrowsExactly<NotSupportedException>();
                await Assert.That(() => stream.Write([], 0, 0)).ThrowsExactly<NotSupportedException>();
                return new(HttpStatusCode.OK);
            });

        await Assert.That(body.CanRead).IsTrue();
        await Assert.That(body.Position).IsGreaterThanOrEqualTo(0);
    }

    /// <summary>Flushes the forwarded request stream outside the async assertion flow.</summary>
    /// <param name="stream">The forwarded request stream.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void FlushForwardedStreamSynchronously(Stream stream) =>
        stream.Flush();

    /// <summary>Exercises the request stream synchronous members outside the async test flow.</summary>
    /// <param name="stream">The forwarded request stream.</param>
    /// <param name="buffer">The read buffer.</param>
    /// <returns>The number of bytes read synchronously.</returns>
    private static int ReadWithSynchronousMembers(Stream stream, byte[] buffer)
    {
        _ = stream.Seek(0, SeekOrigin.Begin);
        var read = stream.Read(buffer, 0, buffer.Length);
        _ = stream.Read(buffer.AsSpan());
        stream.Flush();
        return read;
    }

    /// <summary>Creates an ASP.NET HTTP context populated with a POST request and response stream.</summary>
    /// <returns>The populated HTTP context.</returns>
    private static DefaultHttpContext CreateContext()
    {
        var context = new DefaultHttpContext();
        context.Request.Method = HttpMethods.Post;
        context.Request.Scheme = "http";
        context.Request.Host = new("127.0.0.1", RequestPort);
        context.Request.Path = "/oc/push";
        context.Request.QueryString = new("?a=1");
        context.Request.Headers[DevelopmentCredentialStore.TokenHeaderName] = Token;
        context.Request.Headers["X-Trace-Id"] = "trace-1";
        context.Request.ContentType = "application/json";
        var body = """{"value":42}"""u8.ToArray();
        context.Request.ContentLength = body.Length;
        context.Request.Body = CreateStream(body);
        context.Response.Body = CreateWritableStream();
        return context;
    }

    /// <summary>Creates a seekable stream for an HTTP body.</summary>
    /// <param name="body">The body bytes.</param>
    /// <returns>The created stream.</returns>
    private static MemoryStream CreateStream(byte[] body) => new(body);

    /// <summary>Creates an expandable stream for the ASP.NET response body.</summary>
    /// <returns>The created stream.</returns>
    private static MemoryStream CreateWritableStream() => new();

    /// <summary>Reads the buffered response body as UTF-8 text.</summary>
    /// <param name="response">The response to inspect.</param>
    /// <returns>The response body text.</returns>
    private static async Task<string> ReadBodyAsync(HttpResponse response)
    {
        response.Body.Position = 0;
        using var reader = new StreamReader(response.Body, Encoding.UTF8, leaveOpen: true);
        return await reader.ReadToEndAsync().ConfigureAwait(false);
    }

    /// <summary>Non-seekable request stream that records how many source bytes are consumed.</summary>
    /// <param name="length">The number of readable bytes.</param>
    private sealed class CountingNonSeekableReadStream(long length) : Stream
    {
        /// <summary>The next byte position to read.</summary>
        private long _position;

        /// <inheritdoc />
        public override bool CanRead => !Disposed;

        /// <inheritdoc />
        public override bool CanSeek => false;

        /// <inheritdoc />
        public override bool CanWrite => false;

        /// <inheritdoc />
        public override long Length => length;

        /// <inheritdoc />
        public override long Position
        {
            get => _position;
            set => throw new NotSupportedException();
        }

        /// <summary>Gets the number of bytes read from the source stream.</summary>
        internal long BytesRead { get; private set; }

        /// <summary>Gets or sets whether the stream was disposed.</summary>
        private bool Disposed { get; set; }

        /// <inheritdoc />
        public override void Flush()
        {
        }

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count)
        {
            ObjectDisposedException.ThrowIf(Disposed, this);
            var read = (int)Math.Min(count, length - _position);
            if (read <= 0)
            {
                return 0;
            }

            Array.Fill(buffer, (byte)'a', offset, read);
            _position += read;
            BytesRead += read;
            return read;
        }

        /// <inheritdoc />
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var read = (int)Math.Min(buffer.Length, length - _position);
            if (read <= 0)
            {
                return ValueTask.FromResult(0);
            }

            buffer.Span[..read].Fill((byte)'a');
            _position += read;
            BytesRead += read;
            return ValueTask.FromResult(read);
        }

        /// <inheritdoc />
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        /// <inheritdoc />
        public override void SetLength(long value) => throw new NotSupportedException();

        /// <inheritdoc />
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            Disposed = true;
            base.Dispose(disposing);
        }
    }

    /// <summary>Test implementation of <see cref="IHttpRequestBodyDetectionFeature"/>.</summary>
    /// <param name="canHaveBody">The value returned by <see cref="CanHaveBody"/>.</param>
    private sealed class RequestBodyDetectionFeature(bool canHaveBody) : IHttpRequestBodyDetectionFeature
    {
        /// <inheritdoc />
        public bool CanHaveBody { get; } = canHaveBody;
    }
}
