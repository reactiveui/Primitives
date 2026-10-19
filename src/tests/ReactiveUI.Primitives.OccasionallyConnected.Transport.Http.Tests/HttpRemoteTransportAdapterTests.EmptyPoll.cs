// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using Microsoft.Extensions.Time.Testing;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests idle subscription pacing and cancellation.</summary>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>The minimum idle poll interval in milliseconds.</summary>
    private const int EmptyPollIntervalMilliseconds = 1000;

    /// <summary>The expected poll count after an idle response and a data response.</summary>
    private const int EmptyThenDataPollCount = 2;

    /// <summary>Verifies both empty response forms wait only for the unelapsed idle interval.</summary>
    /// <param name="noContent">Whether the empty response uses HTTP 204.</param>
    /// <param name="elapsedMilliseconds">The time already spent waiting for the response.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(true, 0)]
    [Arguments(false, 0)]
    [Arguments(true, 400)]
    [Arguments(false, 400)]
    [Arguments(true, 30_000)]
    [Arguments(false, 30_000)]
    public async Task SubscribeAsyncPacesEmptyPolls(bool noContent, int elapsedMilliseconds)
    {
        var clock = new FakeTimeProvider();
        var polls = 0;
        var handler = new RecordingHttpHandler(request =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            polls++;
            if (polls != 1)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, SubscribeResponseJson());
            }

            clock.Advance(TimeSpan.FromMilliseconds(elapsedMilliseconds));
            return noContent
                ? new HttpResponseMessage(HttpStatusCode.NoContent)
                : CreateProtocolResponse(HttpStatusCode.OK, "{\"batches\":[]}");
        });
        using var client = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(client, CreateBaseAddress(), options => options with { TimeProvider = clock });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await using var enumerator = session.SubscribeAsync(CreateSubscribeRequest(PriorCursor, StartPosition.Latest), CancellationToken.None)
            .GetAsyncEnumerator();
        var move = enumerator.MoveNextAsync().AsTask();

        if (elapsedMilliseconds < EmptyPollIntervalMilliseconds)
        {
            await Assert.That(polls).IsEqualTo(1);
            await Assert.That(move.IsCompleted).IsFalse();
            clock.Advance(TimeSpan.FromMilliseconds(EmptyPollIntervalMilliseconds - 1 - elapsedMilliseconds));
            await Assert.That(polls).IsEqualTo(1);
            clock.Advance(TimeSpan.FromMilliseconds(1));
        }

        await Assert.That(await move.WaitAsync(TimeSpan.FromSeconds(AwaitTimeoutSeconds))).IsTrue();
        await Assert.That(polls).IsEqualTo(EmptyThenDataPollCount);
        await Assert.That(enumerator.Current.NextCursor).IsEqualTo(CurrentCursor);
    }

    /// <summary>Verifies all subscription lifetimes cancel a pending idle wait.</summary>
    /// <param name="target">The lifetime to cancel.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("caller")]
    [Arguments("enumerator")]
    [Arguments("session")]
    [Arguments("adapter")]
    public async Task SubscribeAsyncCancelsEmptyPollWait(string target)
    {
        var clock = new FakeTimeProvider();
        var polls = 0;
        var handler = new RecordingHttpHandler(request =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            polls++;
            return new HttpResponseMessage(HttpStatusCode.NoContent);
        });
        using var client = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(client, CreateBaseAddress(), options => options with { TimeProvider = clock });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        await using var enumerator = session.SubscribeAsync(CreateSubscribeRequest(PriorCursor, StartPosition.Latest), cancellation.Token)
            .GetAsyncEnumerator();
        var move = enumerator.MoveNextAsync().AsTask();
        await Assert.That(polls).IsEqualTo(1);
        await Assert.That(move.IsCompleted).IsFalse();
        switch (target)
        {
            case "caller":
            {
                await cancellation.CancelAsync();
                break;
            }

            case "enumerator":
            {
                await enumerator.DisposeAsync();
                break;
            }

            case "session":
            {
                await session.DisposeAsync();
                break;
            }

            default:
            {
                await adapter.DisposeAsync();
                break;
            }
        }

        await Assert.That(await move.WaitAsync(TimeSpan.FromSeconds(AwaitTimeoutSeconds))).IsFalse();
        clock.Advance(TimeSpan.FromMinutes(1));
        await Assert.That(polls).IsEqualTo(1);
    }

    /// <summary>Verifies a data response starts the next poll without advancing the clock.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeAsyncDoesNotPaceDataResponses()
    {
        var clock = new FakeTimeProvider();
        var polls = 0;
        var handler = new RecordingHttpHandler(request =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            polls++;
            var batch = polls == 1
                ? SubscribeBatchJson("00000000-0000-0000-0000-000000000401", PriorCursor, CurrentCursor)
                : SubscribeBatchJson("00000000-0000-0000-0000-000000000402", CurrentCursor, NextCursor);
            return CreateProtocolResponse(HttpStatusCode.OK, SubscribeResponseJson(batch));
        });
        using var client = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(client, CreateBaseAddress(), options => options with { TimeProvider = clock });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await using var enumerator = session.SubscribeAsync(CreateSubscribeRequest(PriorCursor, StartPosition.Latest), CancellationToken.None)
            .GetAsyncEnumerator();

        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        var secondMove = enumerator.MoveNextAsync().AsTask();
        await Assert.That(secondMove.IsCompletedSuccessfully).IsTrue();
        await Assert.That(await secondMove).IsTrue();
        await Assert.That(polls).IsEqualTo(EmptyThenDataPollCount);
        await Assert.That(enumerator.Current.NextCursor).IsEqualTo(NextCursor);
    }
}
