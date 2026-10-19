// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests disposal behavior for <see cref="HttpRemoteTransportAdapter"/> and its sessions.</summary>
/// <content>Contains disposal tests for <see cref="HttpRemoteTransportAdapter"/> and its sessions.</content>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>Verifies disposing a clean adapter repeatedly remains stable.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposeAsyncCanBeRepeatedAfterCleanAdapterDisposal()
    {
        using var httpClient = CreateHttpClient(new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson)));
        var adapter = CreateAdapter(httpClient);

        await adapter.DisposeAsync();
        await adapter.DisposeAsync();
    }

    /// <summary>Verifies a second adapter disposal waits for the in-progress disposal result.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposeAsyncCanBeRepeatedWhileAdapterDisposalIsActive()
    {
        var connectEntered = CreateCompletionSource();
        var handler = new RecordingHttpHandler(async (request, cancellationToken) =>
        {
            _ = connectEntered.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
        });
        using var httpClient = CreateHttpClient(handler);
        var adapter = CreateAdapter(httpClient);
        var connect = adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None).AsTask();
        await AwaitWithTimeoutAsync(connectEntered.Task);

        var firstDispose = adapter.DisposeAsync().AsTask();
        var secondDispose = adapter.DisposeAsync().AsTask();

        await AwaitWithTimeoutAsync(firstDispose);
        await AwaitWithTimeoutAsync(secondDispose);
        await Assert.That(async () => await connect).Throws<OperationCanceledException>();
    }

    /// <summary>Verifies disposing a clean session repeatedly remains stable.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SessionDisposeAsyncCanBeRepeatedAfterCleanDisposal()
    {
        var handler = new RecordingHttpHandler(static request => CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson));
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        await session.DisposeAsync();
        await session.DisposeAsync();
    }

    /// <summary>Verifies a second session disposal waits for the in-progress disposal result.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SessionDisposeAsyncCanBeRepeatedWhileDisposalIsActive()
    {
        var pushEntered = CreateCompletionSource();
        var handler = new RecordingHttpHandler(async (request, cancellationToken) =>
        {
            if (request.RequestUri?.AbsolutePath == ConnectRoute)
            {
                return CreateProtocolResponse(HttpStatusCode.OK, ConnectResponseJson);
            }

            _ = pushEntered.TrySetResult(null);
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return CreateProtocolResponse(HttpStatusCode.OK, "{}");
        });
        using var httpClient = CreateHttpClient(handler);
        await using var adapter = CreateAdapter(httpClient);
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var push = session.PushAsync(CreateBatch(), CancellationToken.None).AsTask();
        await AwaitWithTimeoutAsync(pushEntered.Task);

        var firstDispose = session.DisposeAsync().AsTask();
        var secondDispose = session.DisposeAsync().AsTask();

        await AwaitWithTimeoutAsync(firstDispose);
        await AwaitWithTimeoutAsync(secondDispose);
        await Assert.That(async () => await push).Throws<OperationCanceledException>();
    }
}
