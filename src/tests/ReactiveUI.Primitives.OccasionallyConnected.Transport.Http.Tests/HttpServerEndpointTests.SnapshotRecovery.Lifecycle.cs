// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Snapshot recovery lifecycle tests for <see cref="HttpServerEndpoint"/>.</summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>Verifies snapshot recovery shutdown during body reads maps to a transient response before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSnapshotRecoveryShutdownDuringBodyReadReturnsServiceUnavailableBeforeHub()
    {
        var readStarted = CreateSignal();
        var hub = new RecordingSnapshotRecoveryHub();
        var endpoint = new HttpServerEndpoint(CreateSnapshotRecoveryOptions(hub));
        HttpResponseMessage? response = null;
        Task<HttpResponseMessage>? responseTask = null;
        Task? disposeTask = null;
        try
        {
            using var content = CreateBlockingProtocolContent(readStarted);
            using var request = new HttpRequestMessage(HttpMethod.Post, SnapshotRecoveryUri) { Content = content };
            responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
            await readStarted.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            disposeTask = endpoint.DisposeAsync().AsTask();
            response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
            await Assert.That(hub.RecoveryClient).IsNull();
            await AssertCompletesAsync(disposeTask).ConfigureAwait(false);
        }
        finally
        {
            disposeTask ??= endpoint.DisposeAsync().AsTask();
            try
            {
                if (responseTask is not null && response is null)
                {
                    response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
                }
            }
            finally
            {
                response?.Dispose();
                await AssertCompletesAsync(disposeTask).ConfigureAwait(false);
            }
        }
    }
}
