// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if NET8_0 && !NET9_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>Checks real SignalR peer failures at the RPC-to-protocol boundary.</summary>
public sealed class SignalRRpcHandlerTests
{
    /// <summary>Checks a malicious peer cannot invent a valid protocol response.</summary>
    /// <param name="failure">The peer fault.</param>
    /// <returns>The test task.</returns>
    [Test]
    [Arguments(SignalRPeerFailure.EmptyResponse)]
    [Arguments(SignalRPeerFailure.HubFailure)]
    [Arguments(SignalRPeerFailure.MalformedBody)]
    [Arguments(SignalRPeerFailure.InvalidStatus)]
    [Arguments(SignalRPeerFailure.RequestAsResponse)]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task InvalidPeerResponseIsRejected(SignalRPeerFailure failure)
    {
        await using var server = await SignalRRemoteTransportAdapterTests.TestServer.StartAsync(peerFailure: failure);
        await using var connection = new HubConnectionBuilder()
            .WithUrl(server.Endpoint, static options => options.Headers.Add("Authorization", "Bearer test"))
            .AddJsonProtocol(static options => options.PayloadSerializerOptions.TypeInfoResolver = SignalRCarrierJsonContext.Default)
            .Build();
        await connection.StartAsync();
        using var client = new HttpMessageInvoker(new SignalRRpcHandler(connection));
        using var request = new HttpRequestMessage(HttpMethod.Get, "https://signalr.invalid/unknown");
        var exception = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(() => client.SendAsync(request, CancellationToken.None));
        await Assert.That(exception!.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Checks stopped connections fail without a hidden start or reconnect.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task UnstartedConnectionFailsImmediately()
    {
        await using var connection = new HubConnectionBuilder().WithUrl("https://example.com/sync").Build();
        using var client = new HttpMessageInvoker(new SignalRRpcHandler(connection));
        using var request = new HttpRequestMessage(HttpMethod.Get, "https://signalr.invalid/unknown");
        _ = await Assert.ThrowsExactlyAsync<HttpRequestException>(() => client.SendAsync(request, CancellationToken.None));
        await Assert.That(connection.State).IsEqualTo(HubConnectionState.Disconnected);
    }
}
