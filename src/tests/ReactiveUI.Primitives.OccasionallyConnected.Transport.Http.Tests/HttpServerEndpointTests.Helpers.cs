// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Globalization;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;
namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Test helpers for <see cref="HttpServerEndpointTests"/>.</summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>The connect request target for body-read lifecycle tests.</summary>
    private const int ConnectBodyReadTarget = 0;

    /// <summary>The push request target for body-read lifecycle tests.</summary>
    private const int PushBodyReadTarget = 1;

    /// <summary>The acknowledgement request target for body-read lifecycle tests.</summary>
    private const int AcknowledgeBodyReadTarget = 2;

    /// <summary>The object name used when simulating disposed request body reads.</summary>
    private const string RequestBodyObjectName = "request-body";

    /// <summary>Creates endpoint options for tests.</summary>
    /// <param name="hub">The borrowed hub.</param>
    /// <returns>The endpoint options.</returns>
    private static HttpServerEndpointOptions CreateOptions(IServerStreamHub hub) =>
        new()
        {
            Hub = hub,
            DeclaredCapabilities = CreateCapabilities(),
            ReplayAuthorizer = AllowReplayAuthorizer.Instance,
            ReplayProtection = new HttpReplayProtectionOptions { TimeProvider = new ReplayTimeProvider(ReplaySentAtUtc) },
        };

    /// <summary>Creates endpoint capabilities.</summary>
    /// <param name="features">The optional feature override.</param>
    /// <returns>The capabilities.</returns>
    private static NegotiatedCapabilities CreateCapabilities(
        RemoteTransportCapabilities features = RemoteTransportCapabilities.BatchPush
            | RemoteTransportCapabilities.CursorResume
            | RemoteTransportCapabilities.ReceiveAcknowledgements
            | RemoteTransportCapabilities.ServerIdempotency
            | RemoteTransportCapabilities.AtomicApplyAndAcknowledge) =>
        new(
            new(ProtocolMajorVersion, ProtocolMinorVersion),
            features,
            DeclaredMaximumBatchOperations,
            DeclaredMaximumBatchBytes,
            TimeSpan.FromMinutes(EffectiveExactlyOnceWindowMinutes),
            TimeSpan.FromMinutes(RetryTtlMinutes));

    /// <summary>Creates a protocol codec for endpoint tests.</summary>
    /// <returns>The codec.</returns>
    private static HttpProtocolCodec CreateCodec() => new(new HttpProtocolLimits
    {
        MaximumRequestBytes = TestBodyLimitKibibytes * BytesPerKibibyte,
        MaximumResponseBytes = TestBodyLimitKibibytes * BytesPerKibibyte,
        MaximumPayloadBytes = TestPayloadLimitKibibytes * BytesPerKibibyte,
        MaximumMetadataEntries = SmallCollectionLimit,
        MaximumMetadataKeyBytes = MetadataKeyByteLimit,
        MaximumMetadataValueBytes = MetadataValueByteLimit,
        MaximumBatchOperations = SmallCollectionLimit,
        MaximumEventsPerBatch = SmallCollectionLimit,
        MaximumCompletedOperationsPerBatch = SmallCollectionLimit,
        MaximumJsonDepth = JsonDepthLimit,
    });

    /// <summary>Creates an authenticated client principal.</summary>
    /// <returns>The authenticated client.</returns>
    private static ServerAuthenticatedClient CreateAuthenticatedClient() => new(TenantId, ClientId);

    /// <summary>Creates an acknowledgement request fixture.</summary>
    /// <returns>The acknowledgement.</returns>
    private static ReceiveAcknowledgement CreateAcknowledgement() => new(new(Guid.Parse(SubscriptionIdText)), new(StreamName), CursorOne);

    /// <summary>Creates a connect request for the supplied wire client.</summary>
    /// <param name="clientId">The client identifier.</param>
    /// <returns>The connect request.</returns>
    private static TransportConnectRequest CreateConnectRequest(string clientId) => new(
        new(new(1, 0), new(1, 0)),
        new(clientId, "forged-tenant"),
        [DeliveryGuarantee.AtLeastOnce]);

    /// <summary>Creates one valid synchronization batch.</summary>
    /// <returns>The batch.</returns>
    private static SyncBatch CreateBatch() => new(Guid.Parse(BatchIdText), [CreateOperation()]);

    /// <summary>Creates one valid operation.</summary>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation() => new()
    {
        OperationId = new(Guid.Parse(OperationIdText)),
        StreamId = new(StreamName),
        ClientSequence = 1,
        TimestampUtc = DateTimeOffset.Parse("2026-09-13T00:00:00+00:00", CultureInfo.InvariantCulture),
        Type = SyncOperationType.Append,
        Payload = new(ContractName, 1, PayloadContentType, "{}"u8.ToArray(), PayloadHash),
        Metadata = new Dictionary<string, string> { ["trace"] = "1" },
    };

    /// <summary>Creates one successful server result for a decoded batch.</summary>
    /// <param name="batch">The decoded batch.</param>
    /// <param name="client">The trusted principal.</param>
    /// <returns>The server result.</returns>
    private static ServerSyncResult CreateServerResult(SyncBatch batch, ServerAuthenticatedClient client)
    {
        var result = new RemoteSyncResult(
            batch.BatchId,
            [new(batch.Operations[0].OperationId, OperationResultKind.Accepted, null, client.TenantId)],
            ServerCursor,
            null);
        return new(result, []);
    }

    /// <summary>Creates one complete receive batch.</summary>
    /// <returns>The receive batch.</returns>
    private static RemoteEventBatch CreateReceiveBatch()
    {
        var operationId = new OperationId(Guid.Parse(OperationIdText));
        var origin = new RemoteEventOrigin(ClientId, operationId);
        var remoteEvent = new RemoteEvent(
            Guid.Parse("00000000-0000-0000-0000-000000000201"),
            new(StreamName),
            CursorTwo,
            DateTimeOffset.Parse("2026-09-13T00:00:01+00:00", CultureInfo.InvariantCulture),
            operationId,
            new(ContractName, 1, PayloadContentType, "{}"u8.ToArray(), PayloadHash),
            new Dictionary<string, string> { ["trace"] = "receive" })
        { Origin = origin };
        var batchId = Guid.Parse("00000000-0000-0000-0000-000000000200");
        return new(batchId, new(StreamName), CursorOne, CursorTwo, [remoteEvent]) { CompletedOperations = [new(origin, [remoteEvent.EventId])] };
    }

    /// <summary>Creates a protocol request with a bounded JSON body.</summary>
    /// <param name="method">The HTTP method.</param>
    /// <param name="uri">The request URI.</param>
    /// <param name="body">The encoded protocol body.</param>
    /// <returns>The HTTP request.</returns>
    private static HttpRequestMessage CreateProtocolRequest(HttpMethod method, string uri, byte[] body) => new(method, uri) { Content = CreateProtocolContent(body) };

    /// <summary>Creates protocol content.</summary>
    /// <param name="body">The encoded body.</param>
    /// <returns>The HTTP content.</returns>
    private static ByteArrayContent CreateProtocolContent(byte[] body)
    {
        var content = new ByteArrayContent(body);
        content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse(ProtocolMediaType);
        return content;
    }

    /// <summary>Creates protocol stream content without a declared content length.</summary>
    /// <param name="body">The encoded body.</param>
    /// <returns>The HTTP content.</returns>
    private static StreamContent CreateProtocolNonSeekableStreamContent(byte[] body)
    {
        var content = new StreamContent(new NonSeekableReadStream(body));
        content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse(ProtocolMediaType);
        return content;
    }

    /// <summary>Creates protocol content whose read remains pending until request cancellation.</summary>
    /// <param name="readStarted">The signal completed when the endpoint reads the stream.</param>
    /// <returns>The HTTP content.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ProtocolReadStreamContent CreateBlockingProtocolContent(TaskCompletionSource<object?> readStarted) =>
        CreateProtocolStreamContent(new BlockingReadStream(readStarted));

    /// <summary>Creates protocol content whose read fails with object disposal.</summary>
    /// <returns>The HTTP content.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ProtocolReadStreamContent CreateDisposedReadProtocolContent() => CreateProtocolStreamContent(new DisposedReadStream());

    /// <summary>Creates protocol content whose read fails with a typed transport failure.</summary>
    /// <param name="failure">The typed transport failure thrown by the request body stream.</param>
    /// <returns>The HTTP content.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ProtocolReadStreamContent CreateThrowingTransportFailureProtocolContent(HttpRemoteTransportException failure) =>
        CreateProtocolStreamContent(new TransportFailureReadStream(failure));

    /// <summary>Creates protocol content backed by a caller-supplied stream.</summary>
    /// <param name="stream">The stream exposed to the endpoint.</param>
    /// <returns>The HTTP content.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ProtocolReadStreamContent CreateProtocolStreamContent(Stream stream)
    {
        var content = new ProtocolReadStreamContent(stream);
        content.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse(ProtocolMediaType);
        return content;
    }

    /// <summary>Creates a body-carrying endpoint request for the selected route.</summary>
    /// <param name="target">The route target.</param>
    /// <param name="content">The request body content.</param>
    /// <returns>The HTTP request.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="target"/> is unknown.</exception>
    private static HttpRequestMessage CreateBodyReadRequest(int target, HttpContent content)
    {
        var uri = target switch
        {
            ConnectBodyReadTarget => ConnectUri,
            PushBodyReadTarget => PushUri,
            AcknowledgeBodyReadTarget => AcknowledgeUri,
            _ => throw new ArgumentOutOfRangeException(nameof(target), target, "The body-read target is unknown."),
        };
        return new(HttpMethod.Post, uri) { Content = content };
    }

    /// <summary>Reads the full response body.</summary>
    /// <param name="response">The response.</param>
    /// <returns>The response body bytes.</returns>
    private static async Task<byte[]> ReadResponseBodyAsync(HttpResponseMessage response)
    {
        await Assert.That(response.Content).IsNotNull();
        return Encoding.UTF8.GetBytes(await response.Content.ReadAsStringAsync().ConfigureAwait(false));
    }

    /// <summary>Creates a deterministic asynchronous receive sequence.</summary>
    /// <param name="batches">The batches to yield.</param>
    /// <param name="cancellationToken">The enumeration cancellation token.</param>
    /// <returns>The async batch sequence.</returns>
    private static async IAsyncEnumerable<RemoteEventBatch> YieldBatches(
        IReadOnlyList<RemoteEventBatch> batches,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (var index = 0; index < batches.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return batches[index];
        }
    }

    /// <summary>Creates an asynchronous coordination signal.</summary>
    /// <returns>The signal.</returns>
    private static TaskCompletionSource<object?> CreateSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Creates a bounded no-batch subscription that remains active until released.</summary>
    /// <param name="entered">The signal completed after enumeration starts.</param>
    /// <param name="releaseTask">The task that releases the subscription.</param>
    /// <param name="cancellationToken">The enumeration cancellation token.</param>
    /// <returns>The async batch sequence.</returns>
    private static async IAsyncEnumerable<RemoteEventBatch> BlockSubscriptionAsync(
        TaskCompletionSource<object?> entered,
        Task releaseTask,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _ = entered.TrySetResult(null);
        await releaseTask.WaitAsync(cancellationToken).ConfigureAwait(false);
        yield break;
    }

    /// <summary>Creates a subscription that throws from token registration when the poll deadline cancels it.</summary>
    /// <param name="entered">The signal completed after the throwing registration is installed.</param>
    /// <param name="cancellationToken">The enumeration cancellation token.</param>
    /// <returns>The async batch sequence.</returns>
    private static async IAsyncEnumerable<RemoteEventBatch> BlockWithThrowingCancellationRegistrationAsync(
        TaskCompletionSource<object?> entered,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var registration = cancellationToken.Register(static () => throw new InvalidOperationException("Cancellation registration failed."));
        _ = entered.TrySetResult(null);
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        yield break;
    }

    /// <summary>Creates a subscription that completes only when its token is canceled.</summary>
    /// <param name="cancellationToken">The enumeration cancellation token.</param>
    /// <returns>The async batch sequence.</returns>
    private static async IAsyncEnumerable<RemoteEventBatch> BlockUntilCancelledAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        yield break;
    }

    /// <summary>Creates a subscription that signals entry and completes only when its token is canceled.</summary>
    /// <param name="entered">The signal completed after enumeration starts.</param>
    /// <param name="cancellationToken">The enumeration cancellation token.</param>
    /// <returns>The async batch sequence.</returns>
    private static async IAsyncEnumerable<RemoteEventBatch> BlockUntilCancelledAsync(
        TaskCompletionSource<object?> entered,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _ = entered.TrySetResult(null);
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        yield break;
    }

    /// <summary>Creates a subscription that throws the supplied cancellation token after shutdown cancels it.</summary>
    /// <param name="entered">The signal completed after enumeration starts.</param>
    /// <param name="cancellationToken">The enumeration cancellation token.</param>
    /// <returns>The async batch sequence.</returns>
    /// <exception cref="OperationCanceledException">The subscription throws the supplied canceled token.</exception>
    private static async IAsyncEnumerable<RemoteEventBatch> ThrowWhenCancelledAsync(
        TaskCompletionSource<object?> entered,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        _ = entered.TrySetResult(null);
        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }

        yield break;
    }

    /// <summary>Creates a subscription that fails after enumeration starts.</summary>
    /// <param name="cancellationToken">The enumeration cancellation token.</param>
    /// <returns>The async batch sequence.</returns>
    /// <exception cref="InvalidOperationException">The subscription fails after enumeration starts.</exception>
    private static async IAsyncEnumerable<RemoteEventBatch> ThrowSubscriptionAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        if (cancellationToken.IsCancellationRequested)
        {
            yield break;
        }

        throw new InvalidOperationException("Subscription failed after possible effects.");
    }

    /// <summary>Creates an apply operation that signals hub entry and completes only when endpoint shutdown cancels it.</summary>
    /// <param name="entered">The signal completed after apply starts.</param>
    /// <param name="batch">The decoded batch.</param>
    /// <param name="client">The trusted authenticated client.</param>
    /// <param name="cancellationToken">The apply cancellation token.</param>
    /// <returns>The server sync result if cancellation does not occur.</returns>
    private static async ValueTask<ServerSyncResult> BlockApplyUntilCancelledAsync(
        TaskCompletionSource<object?> entered,
        SyncBatch batch,
        ServerAuthenticatedClient client,
        CancellationToken cancellationToken)
    {
        _ = entered.TrySetResult(null);
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        return CreateServerResult(batch, client);
    }

    /// <summary>Verifies a task completes within the bounded test timeout.</summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="task">The task.</param>
    /// <returns>The task result.</returns>
    private static async Task<T> AwaitResultAsync<T>(Task<T> task)
    {
        await AssertCompletesAsync(task).ConfigureAwait(false);
        return await task.ConfigureAwait(false);
    }

    /// <summary>Verifies a task completes within the bounded test timeout without observing its result.</summary>
    /// <param name="task">The task.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertCompletesAsync(Task task)
    {
        var timeout = Task.Delay(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds), CancellationToken.None);
        var completed = await Task.WhenAny(task, timeout).ConfigureAwait(false);
        await Assert.That(completed).IsEqualTo(task);
    }

    /// <summary>Creates a text view of exception diagnostic metadata.</summary>
    /// <param name="exception">The exception carrying diagnostic metadata.</param>
    /// <returns>The diagnostic metadata text.</returns>
    private static string CreateDiagnosticMetadataText(Exception exception)
    {
        var builder = new StringBuilder();
        foreach (var key in exception.Data.Keys)
        {
            _ = builder.Append(key);
            _ = builder.Append('=');
            _ = builder.Append(exception.Data[key]);
            _ = builder.Append(';');
        }

        return builder.ToString();
    }

    /// <summary>Provides a readable body stream whose length cannot be computed by HTTP content.</summary>
    /// <param name="body">The stream bytes.</param>
    private sealed class NonSeekableReadStream(byte[] body) : Stream
    {
        /// <summary>The backing readable stream.</summary>
        private readonly MemoryStream _inner = new(body, writable: false);

        /// <inheritdoc/>
        public override bool CanRead => true;

        /// <inheritdoc/>
        public override bool CanSeek => false;

        /// <inheritdoc/>
        public override bool CanWrite => false;

        /// <inheritdoc/>
        public override long Length => throw new NotSupportedException();

        /// <inheritdoc/>
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override void Flush() => _inner.Flush();

        /// <inheritdoc/>
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

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

    /// <summary>Provides HTTP content that exposes a configured stream without declaring a content length.</summary>
    /// <param name="readStream">The stream returned to HTTP content readers.</param>
    private sealed class ProtocolReadStreamContent(Stream readStream) : HttpContent
    {
        /// <summary>The stream returned to the endpoint.</summary>
        private readonly Stream _readStream = readStream;

        /// <inheritdoc/>
        protected override Task<Stream> CreateContentReadStreamAsync() => Task.FromResult(_readStream);

        /// <inheritdoc/>
        protected override Task<Stream> CreateContentReadStreamAsync(CancellationToken cancellationToken) => Task.FromResult(_readStream);

        /// <inheritdoc/>
        protected override Task SerializeToStreamAsync(Stream stream, System.Net.TransportContext? context) =>
            _readStream.CopyToAsync(stream);

        /// <inheritdoc/>
        protected override Task SerializeToStreamAsync(
            Stream stream,
            System.Net.TransportContext? context,
            CancellationToken cancellationToken) =>
            _readStream.CopyToAsync(stream, cancellationToken);

        /// <inheritdoc/>
        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _readStream.Dispose();
            }

            base.Dispose(disposing);
        }
    }

    /// <summary>Provides a read stream that remains pending until canceled.</summary>
    /// <param name="readStarted">The signal completed when a read is attempted.</param>
    private sealed class BlockingReadStream(TaskCompletionSource<object?> readStarted) : Stream
    {
        /// <summary>The signal completed when a read is attempted.</summary>
        private readonly TaskCompletionSource<object?> _readStarted = readStarted;

        /// <inheritdoc/>
        public override bool CanRead => true;

        /// <inheritdoc/>
        public override bool CanSeek => false;

        /// <inheritdoc/>
        public override bool CanWrite => false;

        /// <inheritdoc/>
        public override long Length => throw new NotSupportedException();

        /// <inheritdoc/>
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override void Flush()
        {
        }

        /// <inheritdoc/>
        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException("Synchronous body reads are not used by endpoint tests.");

        /// <inheritdoc/>
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            _ = _readStarted.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            return 0;
        }

        /// <inheritdoc/>
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _ = _readStarted.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            return 0;
        }

        /// <inheritdoc/>
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        /// <inheritdoc/>
        public override void SetLength(long value) => throw new NotSupportedException();

        /// <inheritdoc/>
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>Provides a request body stream that fails because the request body was disposed by the host.</summary>
    private sealed class DisposedReadStream : Stream
    {
        /// <inheritdoc/>
        public override bool CanRead => true;

        /// <inheritdoc/>
        public override bool CanSeek => false;

        /// <inheritdoc/>
        public override bool CanWrite => false;

        /// <inheritdoc/>
        public override long Length => throw new NotSupportedException();

        /// <inheritdoc/>
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override void Flush()
        {
        }

        /// <inheritdoc/>
        public override int Read(byte[] buffer, int offset, int count) => throw new ObjectDisposedException(RequestBodyObjectName);

        /// <inheritdoc/>
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            Task.FromException<int>(new ObjectDisposedException(RequestBodyObjectName));

        /// <inheritdoc/>
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            ValueTask.FromException<int>(new ObjectDisposedException(RequestBodyObjectName));

        /// <inheritdoc/>
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        /// <inheritdoc/>
        public override void SetLength(long value) => throw new NotSupportedException();

        /// <inheritdoc/>
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>Provides a request body stream that throws a typed transport failure while reading.</summary>
    /// <param name="failure">The typed transport failure.</param>
    private sealed class TransportFailureReadStream(HttpRemoteTransportException failure) : Stream
    {
        /// <summary>The typed transport failure.</summary>
        private readonly HttpRemoteTransportException _failure = failure;

        /// <inheritdoc/>
        public override bool CanRead => true;

        /// <inheritdoc/>
        public override bool CanSeek => false;

        /// <inheritdoc/>
        public override bool CanWrite => false;

        /// <inheritdoc/>
        public override long Length => throw new NotSupportedException();

        /// <inheritdoc/>
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        /// <inheritdoc/>
        public override void Flush()
        {
        }

        /// <inheritdoc/>
        public override int Read(byte[] buffer, int offset, int count) => throw _failure;

        /// <inheritdoc/>
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            Task.FromException<int>(_failure);

        /// <inheritdoc/>
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            ValueTask.FromException<int>(_failure);

        /// <inheritdoc/>
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        /// <inheritdoc/>
        public override void SetLength(long value) => throw new NotSupportedException();

        /// <inheritdoc/>
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>Records hub calls made by the endpoint.</summary>
    private sealed class RecordingHub : IServerStreamHub
    {
        /// <summary>Gets or sets the apply handler.</summary>
        public Func<SyncBatch, ServerAuthenticatedClient, CancellationToken, ValueTask<ServerSyncResult>>? ApplyHandler { get; init; }

        /// <summary>Gets or sets the subscription handler.</summary>
        public Func<RemoteSubscribeRequest, ServerAuthenticatedClient, CancellationToken, IAsyncEnumerable<RemoteEventBatch>>? SubscribeHandler { get; init; }

        /// <summary>Gets or sets the acknowledgement handler.</summary>
        public Func<ReceiveAcknowledgement, ServerAuthenticatedClient, CancellationToken, ValueTask>? AcknowledgeHandler { get; init; }

        /// <summary>Gets the last trusted apply principal.</summary>
        public ServerAuthenticatedClient? ApplyClient { get; private set; }

        /// <summary>Gets the last trusted acknowledgement principal.</summary>
        public ServerAuthenticatedClient? AcknowledgeClient { get; private set; }

        /// <summary>Gets the last trusted subscription principal.</summary>
        public ServerAuthenticatedClient? SubscribeClient { get; private set; }

        /// <summary>Gets the last acknowledgement request.</summary>
        public ReceiveAcknowledgement? Acknowledgement { get; private set; }

        /// <inheritdoc/>
        public ValueTask<ServerSyncResult> ApplyOperationsAsync(
            SyncBatch batch,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            ApplyClient = client;
            return ApplyHandler is null ? ValueTask.FromResult(CreateServerResult(batch, client)) : ApplyHandler(batch, client, cancellationToken);
        }

        /// <inheritdoc/>
        public ValueTask AcknowledgeAsync(
            ReceiveAcknowledgement acknowledgement,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Acknowledgement = acknowledgement;
            AcknowledgeClient = client;
            return AcknowledgeHandler is null ? ValueTask.CompletedTask : AcknowledgeHandler(acknowledgement, client, cancellationToken);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(
            RemoteSubscribeRequest request,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            SubscribeClient = client;
            return SubscribeHandler is null ? YieldBatches([], cancellationToken) : SubscribeHandler(request, client, cancellationToken);
        }
    }

    /// <summary>Provides a timer whose asynchronous disposal is released by the test.</summary>
    /// <param name="returnsFalse">A value indicating whether timer arming returns <see langword="false"/>.</param>
    /// <param name="changeFailure">The optional timer arming failure.</param>
    private sealed class ControlledDisposeTimeProvider(bool returnsFalse = false, Exception? changeFailure = null) : TimeProvider
    {
        /// <summary>A value indicating whether timer arming returns <see langword="false"/>.</summary>
        private readonly bool _returnsFalse = returnsFalse;

        /// <summary>The optional timer arming failure.</summary>
        private readonly Exception? _changeFailure = changeFailure;

        /// <summary>The signal completed when the endpoint creates a timer.</summary>
        private readonly TaskCompletionSource<ControlledDisposeTimer> _created = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ControlledDisposeTimer(_returnsFalse, _changeFailure);
            _ = _created.TrySetResult(timer);
            return timer;
        }

        /// <summary>Waits until the endpoint creates its poll deadline timer.</summary>
        /// <returns>The created timer.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<ControlledDisposeTimer> WaitForTimerAsync() =>
            _created.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds));
    }

    /// <summary>Records timer arming and holds asynchronous disposal until released.</summary>
    /// <param name="returnsFalse">A value indicating whether timer arming returns <see langword="false"/>.</param>
    /// <param name="changeFailure">The optional timer arming failure.</param>
    private sealed class ControlledDisposeTimer(bool returnsFalse, Exception? changeFailure) : ITimer
    {
        /// <summary>A value indicating whether timer arming returns <see langword="false"/>.</summary>
        private readonly bool _returnsFalse = returnsFalse;

        /// <summary>The optional timer arming failure.</summary>
        private readonly Exception? _changeFailure = changeFailure;

        /// <summary>The signal that allows asynchronous disposal to complete.</summary>
        private readonly TaskCompletionSource<object?> _disposeCanComplete = CreateSignal();

        /// <summary>The signal completed when asynchronous disposal starts.</summary>
        private readonly TaskCompletionSource<object?> _disposeAsyncStarted = CreateSignal();

        /// <summary>Gets a task completed when asynchronous disposal starts.</summary>
        public Task DisposeAsyncStarted => _disposeAsyncStarted.Task;

        /// <inheritdoc/>
        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            if (_changeFailure is not null)
            {
                throw _changeFailure;
            }

            return !_returnsFalse;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _ = ReleaseDispose();

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => new(WaitForReleaseAsync());

        /// <summary>Releases the held asynchronous disposal operation.</summary>
        /// <returns><see langword="true"/> when disposal was released by this call.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ReleaseDispose() => _disposeCanComplete.TrySetResult(null);

        /// <summary>Signals asynchronous disposal and waits until the test releases it.</summary>
        /// <returns>The asynchronous wait operation.</returns>
        private async Task WaitForReleaseAsync()
        {
            _ = _disposeAsyncStarted.TrySetResult(null);
            await _disposeCanComplete.Task.ConfigureAwait(false);
        }
    }

    /// <summary>Provides a timer that fails when the endpoint arms the poll deadline.</summary>
    /// <param name="returnsFalse">A value indicating whether timer arming returns <see langword="false"/> instead of throwing.</param>
    /// <param name="disposeFailureMessage">The optional timer disposal failure message.</param>
    /// <param name="changeFailure">The optional timer arming failure.</param>
    private sealed class ArmFailureTimeProvider(
        bool returnsFalse,
        string disposeFailureMessage = "",
        Exception? changeFailure = null) : TimeProvider
    {
        /// <summary>A value indicating whether timer arming returns <see langword="false"/> instead of throwing.</summary>
        private readonly bool _returnsFalse = returnsFalse;

        /// <summary>The optional timer disposal failure message.</summary>
        private readonly string _disposeFailureMessage = disposeFailureMessage;

        /// <summary>The optional timer arming failure.</summary>
        private readonly Exception? _changeFailure = changeFailure;

        /// <summary>The signal completed when the endpoint creates a timer.</summary>
        private readonly TaskCompletionSource<ArmFailureTimer> _created = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ArmFailureTimer(_returnsFalse, _disposeFailureMessage, _changeFailure);
            _ = _created.TrySetResult(timer);
            return timer;
        }

        /// <summary>Waits until the endpoint creates its poll deadline timer.</summary>
        /// <returns>The created timer.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<ArmFailureTimer> WaitForTimerAsync() =>
            _created.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds));
    }

    /// <summary>Provides a timer factory that throws before returning a timer.</summary>
    /// <param name="createFailure">The timer creation failure.</param>
    private sealed class CreateTimerFailureTimeProvider(Exception createFailure) : TimeProvider
    {
        /// <summary>The timer creation failure.</summary>
        private readonly Exception _createFailure = createFailure;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            throw _createFailure;
    }

    /// <summary>Fails timer arming and records asynchronous disposal.</summary>
    /// <param name="returnsFalse">A value indicating whether timer arming returns <see langword="false"/> instead of throwing.</param>
    /// <param name="disposeFailureMessage">The optional timer disposal failure message.</param>
    /// <param name="changeFailure">The optional timer arming failure.</param>
    private sealed class ArmFailureTimer(bool returnsFalse, string disposeFailureMessage, Exception? changeFailure) : ITimer
    {
        /// <summary>A value indicating whether timer arming returns <see langword="false"/> instead of throwing.</summary>
        private readonly bool _returnsFalse = returnsFalse;

        /// <summary>The optional timer disposal failure message.</summary>
        private readonly string _disposeFailureMessage = disposeFailureMessage;

        /// <summary>The optional timer arming failure.</summary>
        private readonly Exception? _changeFailure = changeFailure;

        /// <summary>The signal completed when asynchronous disposal starts.</summary>
        private readonly TaskCompletionSource<object?> _disposeAsyncStarted = CreateSignal();

        /// <summary>Gets a task completed when asynchronous disposal starts.</summary>
        public Task DisposeAsyncStarted => _disposeAsyncStarted.Task;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Change(TimeSpan dueTime, TimeSpan period) =>
            _returnsFalse ? false : throw (_changeFailure ?? new InvalidOperationException("Timer arm failed."));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _disposeAsyncStarted.TrySetResult(null);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync()
        {
            Dispose();
            return _disposeFailureMessage.Length == 0
                ? ValueTask.CompletedTask
                : new(Task.FromException(new InvalidOperationException(_disposeFailureMessage)));
        }
    }

    /// <summary>Provides a startup exception whose diagnostic metadata store cannot be accessed.</summary>
    private sealed class InaccessibleDataException : InvalidOperationException
    {
        /// <summary>Initializes a new instance of the <see cref="InaccessibleDataException"/> class.</summary>
        public InaccessibleDataException()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="InaccessibleDataException"/> class.</summary>
        /// <param name="message">The exception message.</param>
        public InaccessibleDataException(string message)
            : base(message)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="InaccessibleDataException"/> class.</summary>
        /// <param name="message">The exception message.</param>
        /// <param name="innerException">The inner exception.</param>
        public InaccessibleDataException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <inheritdoc/>
        public override System.Collections.IDictionary Data =>
            throw new InvalidOperationException("Diagnostic metadata is not available.");
    }
}
