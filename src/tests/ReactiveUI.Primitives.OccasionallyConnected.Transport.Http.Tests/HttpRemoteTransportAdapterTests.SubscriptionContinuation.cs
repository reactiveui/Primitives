// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests continuation and stream binding across HTTP receive polls.</summary>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>The expected subscribe poll count after a paused subscription is released.</summary>
    private const int ReleasedPausedSubscriptionPolls = 2;

    /// <summary>Disposes the enumerator in the cancellation failure test.</summary>
    private const int EnumeratorDisposalTarget = 0;

    /// <summary>Disposes the session in the cancellation failure test.</summary>
    private const int SessionDisposalTarget = 1;

    /// <summary>Disposes the adapter in the cancellation failure test.</summary>
    private const int AdapterDisposalTarget = 2;

    /// <summary>Verifies a failed cancellation callback cannot strand repeated disposal callers.</summary>
    /// <param name="target">The owner whose disposal is exercised.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(EnumeratorDisposalTarget)]
    [Arguments(SessionDisposalTarget)]
    [Arguments(AdapterDisposalTarget)]
    public async Task SubscribeAsyncDisposalFailureIsSharedAfterCancellationCallbackThrows(int target)
    {
        var entered = CreateCompletionSource();
        var handler = new RecordingHttpHandler(async (request, cancellationToken) =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            await using var registration = cancellationToken.Register(static () => throw new InvalidOperationException("Cancellation callback failed."));
            _ = entered.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return CreateProtocolResponse(HttpStatusCode.OK, SubscribeResponseJson());
        });
        using var httpClient = CreateHttpClient(handler);
        var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(PriorCursor, StartPosition.Latest), CancellationToken.None).GetAsyncEnumerator();
        var move = enumerator.MoveNextAsync().AsTask();
        await AwaitWithTimeoutAsync(entered.Task);

        async Task DisposeAsync()
        {
            var task = target switch
            {
                SessionDisposalTarget => session.DisposeAsync().AsTask(),
                AdapterDisposalTarget => adapter.DisposeAsync().AsTask(),
                _ => enumerator.DisposeAsync().AsTask(),
            };
            await task.WaitAsync(TimeSpan.FromSeconds(AwaitTimeoutSeconds));
        }

        await Assert.That(DisposeAsync).ThrowsExactly<AggregateException>();
        await AssertCompletesAsync(move);
        await Assert.That(DisposeAsync).ThrowsExactly<AggregateException>();
        if (target == AdapterDisposalTarget)
        {
            return;
        }

        await adapter.DisposeAsync();
    }

    /// <summary>Verifies disposal observes a move before the transport invokes synchronous callbacks.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeAsyncDisposalDrainsMoveStartedInsideHttpCallback()
    {
        var release = CreateCompletionSource();
        IAsyncEnumerator<RemoteEventBatch>? subscription = null;
        Task? disposal = null;
        var handler = new RecordingHttpHandler(async (request, _) =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            disposal = (subscription ?? throw new InvalidOperationException("Subscription was not assigned.")).DisposeAsync().AsTask();
            await release.Task;
            return CreateProtocolResponse(HttpStatusCode.OK, SubscribeResponseJson());
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        subscription = session.SubscribeAsync(CreateSubscribeRequest(PriorCursor, StartPosition.Latest), CancellationToken.None).GetAsyncEnumerator();
        var move = subscription.MoveNextAsync().AsTask();
        try
        {
            await Assert.That(disposal).IsNotNull();
            await Assert.That(disposal?.IsCompleted).IsFalse();
        }
        finally
        {
            _ = release.TrySetResult(null);
            await AssertCompletesAsync(move);
            await subscription.DisposeAsync();
        }
    }

    /// <summary>Verifies the next poll resumes after the batch already yielded.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeAsyncAdvancesCursorBetweenPolls()
    {
        var polls = 0;
        var handler = new RecordingHttpHandler(request =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            polls++;
            var batch = polls == 1
                ? SubscribeBatchJson("00000000-0000-0000-0000-000000000401", CurrentCursor, NextCursor)
                : SubscribeBatchJson("00000000-0000-0000-0000-000000000402", NextCursor, SkippedPreviousCursor);
            return CreateProtocolResponse(HttpStatusCode.OK, SubscribeResponseJson(batch));
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var request = CreateSubscribeRequest(CurrentCursor, StartPosition.Latest);
        await using var enumerator = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();

        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        await Assert.That(enumerator.Current.NextCursor).IsEqualTo(NextCursor);
        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        await Assert.That(enumerator.Current.NextCursor).IsEqualTo(SkippedPreviousCursor);
        await Assert.That(handler.Requests[^1].RequestUri?.Query).Contains($"cursor={NextCursor}");
        await Assert.That(request.Cursor).IsEqualTo(CurrentCursor);
    }

    /// <summary>Rejects a valid batch belonging to a stream other than the requested stream.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeAsyncRejectsForeignStreamBeforeYield()
    {
        var json = SubscribeResponseJson().Replace(StreamName, "foreign-stream", StringComparison.Ordinal);
        var handler = new RecordingHttpHandler(request => CreateProtocolResponse(
            HttpStatusCode.OK,
            request.RequestUri?.AbsolutePath == ConnectRoute ? ConnectResponseJson : json));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var request = CreateSubscribeRequest(PriorCursor, StartPosition.Latest);
        await using var enumerator = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();

        var exception = await CaptureHttpExceptionAsync(async () => _ = await enumerator.MoveNextAsync());
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Rejects an event whose stream does not match its containing subscription.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeAsyncRejectsForeignEventStreamBeforeYield()
    {
        var json = SubscribeResponseJson().Replace(
            "\"eventId\":\"00000000-0000-0000-0000-000000000201\",\"streamId\":\"stream-1\"",
            "\"eventId\":\"00000000-0000-0000-0000-000000000201\",\"streamId\":\"foreign-stream\"",
            StringComparison.Ordinal);
        var handler = new RecordingHttpHandler(request => CreateProtocolResponse(
            HttpStatusCode.OK,
            request.RequestUri?.AbsolutePath == ConnectRoute ? ConnectResponseJson : json));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var request = CreateSubscribeRequest(PriorCursor, StartPosition.Latest);
        await using var enumerator = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();

        var exception = await CaptureHttpExceptionAsync(async () => _ = await enumerator.MoveNextAsync());
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies paused subscriptions hold bounded capacity until their owning session is disposed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeAsyncReleasesPausedSubscriptionCapacityWhenSessionDisposes()
    {
        var subscribePolls = 0;
        var handler = new RecordingHttpHandler(request =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            subscribePolls++;
            var batch = SubscribeBatchJson($"00000000-0000-0000-0000-0000000004{subscribePolls:00}", PriorCursor, CurrentCursor);
            return CreateProtocolResponse(HttpStatusCode.OK, SubscribeResponseJson(batch));
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(
            httpClient,
            CreateBaseAddress(),
            static options => options with { MaximumConcurrentSubscriptions = 1 });
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var request = CreateSubscribeRequest(PriorCursor, StartPosition.Latest);
        await using var first = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();

        await Assert.That(await first.MoveNextAsync()).IsTrue();
        var exception = await CaptureHttpExceptionAsync(async () =>
        {
            await using var blocked = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();
            _ = await blocked.MoveNextAsync();
        });
        await session.DisposeAsync();
        var nextSession = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await using var admitted = nextSession.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.Transient);
        await Assert.That(await admitted.MoveNextAsync()).IsTrue();
        await Assert.That(subscribePolls).IsEqualTo(ReleasedPausedSubscriptionPolls);
    }

    /// <summary>Verifies disposing before the first move releases subscription admission without sending HTTP.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeAsyncDisposeBeforeFirstMoveSendsNoSubscribeRequest()
    {
        var subscribePolls = 0;
        var handler = new RecordingHttpHandler(request =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            subscribePolls++;
            return CreateProtocolResponse(HttpStatusCode.OK, SubscribeResponseJson());
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(
            httpClient,
            CreateBaseAddress(),
            static options => options with { MaximumConcurrentSubscriptions = 1 });
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var request = CreateSubscribeRequest(PriorCursor, StartPosition.Latest);
        var first = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();

        await Assert.That(() => first.Current).ThrowsExactly<InvalidOperationException>();
        await first.DisposeAsync();
        await Assert.That(() => first.Current).ThrowsExactly<InvalidOperationException>();
        await using var second = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();

        await Assert.That(await second.MoveNextAsync()).IsTrue();
        await Assert.That(subscribePolls).IsEqualTo(1);
    }

    /// <summary>Verifies disposing an active subscription enumerator cancels its poll and releases admission.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeAsyncDisposeCancelsActiveMoveAndReleasesAdmission()
    {
        var subscribeEntered = CreateCompletionSource();
        var subscribePolls = 0;
        var handler = new RecordingHttpHandler(async (request, cancellationToken) =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            subscribePolls++;
            if (subscribePolls == FirstSubscribeAttempt)
            {
                _ = subscribeEntered.TrySetResult(null);
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }

            return CreateProtocolResponse(HttpStatusCode.OK, SubscribeResponseJson());
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(
            httpClient,
            CreateBaseAddress(),
            static options => options with { MaximumConcurrentSubscriptions = 1 });
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var request = CreateSubscribeRequest(PriorCursor, StartPosition.Latest);
        var first = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();
        var firstMove = first.MoveNextAsync().AsTask();
        await AwaitWithTimeoutAsync(subscribeEntered.Task);

        await first.DisposeAsync();
        await using var second = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();

        await AssertCompletesAsync(firstMove);
        await Assert.That(await second.MoveNextAsync()).IsTrue();
    }

    /// <summary>Verifies overlapping consumer moves are rejected before issuing another poll.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeAsyncRejectsOverlappingMoveNext()
    {
        var subscribeEntered = CreateCompletionSource();
        var handler = new RecordingHttpHandler(async (request, cancellationToken) =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            _ = subscribeEntered.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return CreateProtocolResponse(HttpStatusCode.OK, SubscribeResponseJson());
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var request = CreateSubscribeRequest(PriorCursor, StartPosition.Latest);
        await using var enumerator = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();
        var firstMove = enumerator.MoveNextAsync().AsTask();
        await AwaitWithTimeoutAsync(subscribeEntered.Task);

        async Task MoveAgainAsync() => _ = await enumerator.MoveNextAsync();

        await Assert.That(MoveAgainAsync).ThrowsExactly<InvalidOperationException>();
        await session.DisposeAsync();
        await AssertCompletesAsync(firstMove);
    }
}
