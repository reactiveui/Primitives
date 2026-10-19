// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>
/// Audits <see cref="HttpRemoteTransportAdapter"/> for hidden retries. The sync engine owns retry policy, so every
/// engine request must cause exactly one network attempt under every failure class, and any retry hint must reach the
/// engine instead of being acted on inside the adapter.
/// </summary>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>A 500 response failure.</summary>
    private const string InternalServerErrorFailure = "500";

    /// <summary>A 502 response failure.</summary>
    private const string BadGatewayFailure = "502";

    /// <summary>A 503 response failure with a retry hint.</summary>
    private const string ServiceUnavailableRetryAfterFailure = "503+Retry-After";

    /// <summary>A 429 response failure with a retry hint.</summary>
    private const string TooManyRequestsRetryAfterFailure = "429+Retry-After";

    /// <summary>A 408 response failure.</summary>
    private const string RequestTimeoutFailure = "408";

    /// <summary>A connection reset failure.</summary>
    private const string ConnectionResetFailure = "connection-reset";

    /// <summary>A stream I/O failure.</summary>
    private const string StreamFailure = "io";

    /// <summary>An HTTP client timeout failure.</summary>
    private const string ClientTimeoutFailure = "client-timeout";

    /// <summary>A caller cancellation while the request is in flight.</summary>
    private const string CallerCancellationFailure = "caller-cancel";

    /// <summary>The Retry-After delta, in seconds, returned by retry-hint failures.</summary>
    private const int SingleAttemptRetryAfterSeconds = 120;

    /// <summary>The time allowed for a hidden background retry to show up after the call fails.</summary>
    private const int BackgroundRetryGraceMilliseconds = 100;

    /// <summary>The connect route suffix used by the snapshot-capable adapter.</summary>
    private const string SingleAttemptConnectSuffix = $"/{ReplayEndpointConnectPath}";

    /// <summary>The number of empty long polls served before a failure in the poll continuation test.</summary>
    private const int EmptyLongPollsBeforeFailure = 2;

    /// <summary>The Windows socket error code for a connection reset by the peer.</summary>
    private const int ConnectionResetSocketError = 10_054;

    /// <summary>The bound for a call that must fail fast instead of honoring a retry hint internally.</summary>
    private static readonly TimeSpan SingleAttemptFailFastBound = TimeSpan.FromSeconds(30);

    /// <summary>Provides every failure class for the single-attempt audit.</summary>
    /// <returns>The failure class names.</returns>
    public static IEnumerable<string> SingleAttemptFailures()
    {
        yield return InternalServerErrorFailure;
        yield return BadGatewayFailure;
        yield return ServiceUnavailableRetryAfterFailure;
        yield return TooManyRequestsRetryAfterFailure;
        yield return RequestTimeoutFailure;
        yield return ConnectionResetFailure;
        yield return StreamFailure;
        yield return ClientTimeoutFailure;
        yield return CallerCancellationFailure;
    }

    /// <summary>Verifies connect makes exactly one network attempt and surfaces the failure.</summary>
    /// <param name="failure">The failure class.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(SingleAttemptFailures))]
    public async Task ConnectAsyncMakesExactlyOneAttemptPerFailureClass(string failure)
    {
        var handler = new SingleAttemptHandler(failure, failConnect: true);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, TimeProvider.System);

        await AssertSingleAttemptAsync(
            handler,
            failure,
            async token => _ = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), token));
    }

    /// <summary>Verifies push makes exactly one network attempt and surfaces the failure.</summary>
    /// <param name="failure">The failure class.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(SingleAttemptFailures))]
    public async Task PushAsyncMakesExactlyOneAttemptPerFailureClass(string failure)
    {
        var handler = new SingleAttemptHandler(failure, failConnect: false);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, TimeProvider.System);
        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);

        await AssertSingleAttemptAsync(handler, failure, async token => _ = await session.PushAsync(CreateBatch(), token));
    }

    /// <summary>Verifies one subscribe poll makes exactly one network attempt and surfaces the failure.</summary>
    /// <param name="failure">The failure class.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(SingleAttemptFailures))]
    public async Task SubscribeAsyncMakesExactlyOneAttemptPerFailureClass(string failure)
    {
        var handler = new SingleAttemptHandler(failure, failConnect: false);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, TimeProvider.System);
        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);

        await AssertSingleAttemptAsync(handler, failure, token => MoveFirstSubscriptionBatchAsync(session, token));
    }

    /// <summary>Verifies acknowledge makes exactly one network attempt and surfaces the failure.</summary>
    /// <param name="failure">The failure class.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(SingleAttemptFailures))]
    public async Task AcknowledgeAsyncMakesExactlyOneAttemptPerFailureClass(string failure)
    {
        var handler = new SingleAttemptHandler(failure, failConnect: false);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, TimeProvider.System);
        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);
        ReceiveAcknowledgement acknowledgement = new(new(Guid.Parse("00000000-0000-0000-0000-000000000301")), CreateStreamId(), ReplayCursorOne);

        await AssertSingleAttemptAsync(handler, failure, async token => await session.AcknowledgeAsync(acknowledgement, token));
    }

    /// <summary>Verifies snapshot recovery makes exactly one network attempt and surfaces the failure.</summary>
    /// <param name="failure">The failure class.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(SingleAttemptFailures))]
    public async Task GetSnapshotAsyncMakesExactlyOneAttemptPerFailureClass(string failure)
    {
        var handler = new SingleAttemptHandler(failure, failConnect: false);
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, TimeProvider.System);
        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);
        var recovery = (IRemoteSnapshotRecoverySession)session;

        await AssertSingleAttemptAsync(handler, failure, async token => _ = await recovery.GetSnapshotAsync(CreateSnapshotRecoveryRequest(), token));
    }

    /// <summary>
    /// Verifies a successful push that carries a Retry-After hint returns the hint on the result after one attempt,
    /// so the engine decides when to send the next batch.
    /// </summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncSurfacesSuccessRetryAfterHintWithoutWaiting()
    {
        var batch = CreateBatch();
        var pushes = 0;
        var handler = new RecordingHttpHandler(request =>
        {
            if (request.RequestUri?.AbsolutePath.EndsWith(SingleAttemptConnectSuffix, StringComparison.Ordinal) == true)
            {
                return CreateReplayConnectResponseWithSnapshotRecovery();
            }

            _ = Interlocked.Increment(ref pushes);
            var response = CreateJsonResponse(HttpStatusCode.OK, PushResponseJson(batch), ProtocolMediaType);
            _ = response.Headers.TryAddWithoutValidation("Retry-After", SingleAttemptRetryAfterSeconds.ToString(CultureInfo.InvariantCulture));
            return response;
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, TimeProvider.System);
        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);
        var started = Stopwatch.GetTimestamp();

        var result = await session.PushAsync(batch, CancellationToken.None);

        await Assert.That(result.RetryAfter).IsEqualTo(TimeSpan.FromSeconds(SingleAttemptRetryAfterSeconds));
        await Assert.That(Stopwatch.GetElapsedTime(started)).IsLessThan(SingleAttemptFailFastBound);
        await Assert.That(Volatile.Read(ref pushes)).IsEqualTo(1);
    }

    /// <summary>
    /// Verifies the subscription continues long polling only after empty responses. The first failure ends the
    /// subscription after exactly one failed attempt, so the long-poll continuation is not a retry loop.
    /// </summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsyncStopsPollingAfterFirstFailure()
    {
        var polls = 0;
        var handler = new RecordingHttpHandler(request =>
        {
            if (request.RequestUri?.AbsolutePath.EndsWith(SingleAttemptConnectSuffix, StringComparison.Ordinal) == true)
            {
                return CreateReplayConnectResponseWithSnapshotRecovery();
            }

            return Interlocked.Increment(ref polls) <= EmptyLongPollsBeforeFailure
                ? new HttpResponseMessage(HttpStatusCode.NoContent)
                : new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateSnapshotRecoveryAdapter(httpClient, TimeProvider.System);
        await using var session = await adapter.ConnectAsync(CreateSnapshotRecoveryConnectRequest(), CancellationToken.None);

        var exception = await CaptureHttpExceptionAsync(() => MoveFirstSubscriptionBatchAsync(session, CancellationToken.None));
        await Task.Delay(BackgroundRetryGraceMilliseconds, CancellationToken.None);

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.Transient);
        await Assert.That(Volatile.Read(ref polls)).IsEqualTo(EmptyLongPollsBeforeFailure + 1);
    }

    /// <summary>Runs one failing engine request and asserts the single-attempt invariants.</summary>
    /// <param name="handler">The counting handler.</param>
    /// <param name="failure">The failure class.</param>
    /// <param name="action">The engine request.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertSingleAttemptAsync(SingleAttemptHandler handler, string failure, Func<CancellationToken, Task> action)
    {
        using var cancellation = new CancellationTokenSource();
        var started = Stopwatch.GetTimestamp();
        var exception = await CaptureSingleAttemptFailureAsync(handler, failure, cancellation, action);
        var elapsed = Stopwatch.GetElapsedTime(started);
        await Task.Delay(BackgroundRetryGraceMilliseconds, CancellationToken.None);

        await Assert.That(handler.FailedSends).IsEqualTo(1);
        await Assert.That(exception is HttpRemoteTransportException or OperationCanceledException).IsTrue();
        await Assert.That(elapsed).IsLessThan(SingleAttemptFailFastBound);
        if (failure is ServiceUnavailableRetryAfterFailure or TooManyRequestsRetryAfterFailure)
        {
            var transport = (HttpRemoteTransportException)exception;
            await Assert.That(transport.Kind).IsEqualTo(HttpTransportFailureKind.Transient);
            await Assert.That(transport.RetryAfter).IsEqualTo(TimeSpan.FromSeconds(SingleAttemptRetryAfterSeconds));
            await Assert.That(transport.RetryFailure.RetryAfter).IsEqualTo(TimeSpan.FromSeconds(SingleAttemptRetryAfterSeconds));
        }

        if (failure == CallerCancellationFailure)
        {
            await Assert.That(exception is OperationCanceledException).IsTrue();
        }
    }

    /// <summary>Starts a failing engine request, cancels it when the failure class requires it, and captures its exception.</summary>
    /// <param name="handler">The counting handler.</param>
    /// <param name="failure">The failure class.</param>
    /// <param name="cancellation">The caller cancellation source.</param>
    /// <param name="action">The engine request.</param>
    /// <returns>The captured exception.</returns>
    /// <exception cref="InvalidOperationException">The request completed successfully.</exception>
    private static async Task<Exception> CaptureSingleAttemptFailureAsync(
        SingleAttemptHandler handler,
        string failure,
        CancellationTokenSource cancellation,
        Func<CancellationToken, Task> action)
    {
        var task = action(cancellation.Token);
        if (failure == CallerCancellationFailure)
        {
            await handler.FailureStarted.Task.WaitAsync(TimeSpan.FromSeconds(AwaitTimeoutSeconds));
            await cancellation.CancelAsync();
        }

        try
        {
            await task.WaitAsync(TimeSpan.FromSeconds(AwaitTimeoutSeconds));
        }
        catch (Exception exception) when (exception is not TimeoutException)
        {
            return exception;
        }

        throw new InvalidOperationException("Expected the engine request to fail.");
    }

    /// <summary>
    /// Opens a subscription and waits for its first batch. A canceled subscription ends without an exception, so this
    /// helper reports that end as the caller's cancellation.
    /// </summary>
    /// <param name="session">The session.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>The asynchronous operation.</returns>
    private static async Task MoveFirstSubscriptionBatchAsync(IRemoteTransportSession session, CancellationToken cancellationToken)
    {
        RemoteSubscribeRequest request = new(CreateStreamId(), new(Guid.Parse("00000000-0000-0000-0000-000000000301")), null, StartPosition.Latest);
        await using var enumerator = session.SubscribeAsync(request, cancellationToken).GetAsyncEnumerator(cancellationToken);
        if (!await enumerator.MoveNextAsync())
        {
            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    /// <summary>Creates the response or exception for one failure class.</summary>
    /// <param name="failure">The failure class.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The failure response.</returns>
    /// <exception cref="HttpRequestException">The failure class simulates a connection reset.</exception>
    /// <exception cref="IOException">The failure class simulates a stream failure.</exception>
    /// <exception cref="TaskCanceledException">The failure class simulates an HTTP client timeout.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="failure"/> is unknown.</exception>
    private static async Task<HttpResponseMessage> CreateSingleAttemptFailureAsync(string failure, CancellationToken cancellationToken) => failure switch
    {
        InternalServerErrorFailure => new HttpResponseMessage(HttpStatusCode.InternalServerError),
        BadGatewayFailure => new HttpResponseMessage(HttpStatusCode.BadGateway),
        ServiceUnavailableRetryAfterFailure => CreateRetryAfterResponse(HttpStatusCode.ServiceUnavailable),
        TooManyRequestsRetryAfterFailure => CreateRetryAfterResponse(HttpTransportStatus.TooManyRequests),
        RequestTimeoutFailure => new HttpResponseMessage(HttpStatusCode.RequestTimeout),
        ConnectionResetFailure => throw new HttpRequestException("reset", new IOException("reset", new SocketException(ConnectionResetSocketError))),
        StreamFailure => throw new IOException("stream failed"),
        ClientTimeoutFailure => throw new TaskCanceledException("timeout", new TimeoutException()),
        CallerCancellationFailure => await WaitForCancellationAsync(cancellationToken),
        _ => throw new ArgumentOutOfRangeException(nameof(failure), failure, "Unknown failure class."),
    };

    /// <summary>Creates a failure response with a Retry-After delta.</summary>
    /// <param name="statusCode">The status code.</param>
    /// <returns>The response.</returns>
    private static HttpResponseMessage CreateRetryAfterResponse(HttpStatusCode statusCode)
    {
        var response = new HttpResponseMessage(statusCode);
        _ = response.Headers.TryAddWithoutValidation("Retry-After", SingleAttemptRetryAfterSeconds.ToString(CultureInfo.InvariantCulture));
        return response;
    }

    /// <summary>Waits until the caller cancels the in-flight request.</summary>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>A response that is never produced.</returns>
    /// <exception cref="OperationCanceledException">The caller canceled the request.</exception>
    private static async Task<HttpResponseMessage> WaitForCancellationAsync(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        throw new OperationCanceledException(cancellationToken);
    }

    /// <summary>Counts network attempts and fails every attempt on the audited route.</summary>
    /// <param name="failure">The failure class.</param>
    /// <param name="failConnect">A value indicating whether connect is the audited route.</param>
    private sealed class SingleAttemptHandler(string failure, bool failConnect) : HttpMessageHandler
    {
        /// <summary>The number of attempts on the audited route.</summary>
        private int _failedSends;

        /// <summary>Gets the number of attempts on the audited route.</summary>
        public int FailedSends => Volatile.Read(ref _failedSends);

        /// <summary>Gets a signal that completes when the audited route receives an attempt.</summary>
        public TaskCompletionSource<object?> FailureStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var isConnect = request.RequestUri?.AbsolutePath.EndsWith(SingleAttemptConnectSuffix, StringComparison.Ordinal) == true;
            if (isConnect && !failConnect)
            {
                return Task.FromResult(CreateReplayConnectResponseWithSnapshotRecovery());
            }

            _ = Interlocked.Increment(ref _failedSends);
            _ = FailureStarted.TrySetResult(null);
            return CreateSingleAttemptFailureAsync(failure, cancellationToken);
        }
    }
}
