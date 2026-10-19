// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.WebSockets;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets.Tests;

/// <content>Verifies host-owned socket configuration before connection.</content>
public sealed partial class WebSocketRemoteTransportAdapterTests
{
    /// <summary>The keep-alive interval supplied by the test host.</summary>
    private static readonly TimeSpan ConfiguredKeepAlive = TimeSpan.FromSeconds(15);

    /// <summary>Verifies authentication and keep-alive configuration reaches the connected socket.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConnectAsyncUsesHostSocketConfigurationBeforeHandshake()
    {
        await using var peer = await TestPeer.StartAsync(static async (socket, request) =>
            await SendAsync(socket, ConnectResponseMessageType, request.MessageId, CreateCapabilities()));
        ClientWebSocketOptions? configured = null;
        var calls = 0;
        var options = CreateOptions(peer.Endpoint) with
        {
            ConfigureSocket = socketOptions =>
            {
                configured = socketOptions;
                calls++;
                socketOptions.SetRequestHeader("Authorization", "Bearer test-token");
                socketOptions.KeepAliveInterval = ConfiguredKeepAlive;
            },
        };
        await using var adapter = new WebSocketRemoteTransportAdapter(options);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        await Assert.That(calls).IsEqualTo(1);
        await Assert.That(configured!.KeepAliveInterval).IsEqualTo(ConfiguredKeepAlive);
        await Assert.That(peer.AuthorizationHeader).IsEqualTo("Bearer test-token");
        await Assert.That(session.NegotiatedCapabilities.ProtocolVersion).IsEqualTo(new(1, 0));
    }

    /// <summary>Verifies host configuration failures propagate before any connection is attempted.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConnectAsyncPropagatesSocketConfigurationFailure()
    {
        var options = CreateOptions(new(UnreachableEndpoint)) with
        {
            ConfigureSocket = static _ => throw new InvalidOperationException("Socket configuration failed."),
        };
        await using var adapter = new WebSocketRemoteTransportAdapter(options);

        var error = await Assert.That(async () => await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(error!.Message).IsEqualTo("Socket configuration failed.");
    }
}
