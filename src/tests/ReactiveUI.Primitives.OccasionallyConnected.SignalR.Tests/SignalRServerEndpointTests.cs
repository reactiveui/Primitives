// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if NET8_0 && !NET9_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif
using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Claims;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>Checks protocol admission before the borrowed real server hub can have an effect.</summary>
public sealed class SignalRServerEndpointTests
{
    /// <summary>The declared logical body bound for admission tests.</summary>
    private const int LogicalBodyBytes = 1024;

    /// <summary>Checks malformed and oversized carrier classification.</summary>
    /// <param name="oversized">Whether the carrier exceeds the input limit.</param>
    /// <returns>The test task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task InvalidCarrierIsClassifiedBeforeHubWork(bool oversized)
    {
        await using var server = await SignalRRemoteTransportAdapterTests.TestServer.StartAsync();
        var request = oversized ? new byte[SignalRCarrier.MaximumBytes + 1] : "{"u8.ToArray();
        var bytes = await server.ProtocolEndpoint.ExchangeAsync(request, Principal(), CancellationToken.None);
        var response = SignalRCarrier.Decode(bytes);
        var expected = oversized ? HttpStatusCode.RequestEntityTooLarge : HttpStatusCode.BadRequest;
        await Assert.That(response.StatusCode).IsEqualTo((int)expected);
        await Assert.That(server.Domain.Calls).IsEqualTo(0);
    }

    /// <summary>Checks authority-changing routes and unsupported verbs.</summary>
    /// <param name="method">The verb.</param>
    /// <param name="path">The path.</param>
    /// <param name="status">The invalid request status.</param>
    /// <returns>The test task.</returns>
    [Test]
    [Arguments("PUT", "/push", 0)]
    [Arguments("POST", null, 0)]
    [Arguments("POST", "push", 0)]
    [Arguments("POST", "//other.example/push", 0)]
    [Arguments("POST", "/push#fragment", 0)]
    [Arguments("POST", "/push\\segment", 0)]
    [Arguments("POST", "/push", 1)]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task InvalidRouteCannotReachCore(string method, string? path, int status)
    {
        await using var server = await SignalRRemoteTransportAdapterTests.TestServer.StartAsync();
        var request = SignalRCarrier.Encode(new(method, path, status, [], []));
        var response = SignalRCarrier.Decode(await server.ProtocolEndpoint.ExchangeAsync(request, Principal(), CancellationToken.None));
        await Assert.That(response.StatusCode).IsEqualTo((int)HttpStatusCode.BadRequest);
        await Assert.That(server.Domain.Calls).IsEqualTo(0);
    }

    /// <summary>Checks host principals are required before parsing untrusted bodies.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task AnonymousOrMissingPrincipalIsRejectedBeforeParsing()
    {
        await using var server = await SignalRRemoteTransportAdapterTests.TestServer.StartAsync();
        var absent = await server.ProtocolEndpoint.ExchangeAsync([], null, CancellationToken.None);
        var anonymous = await server.ProtocolEndpoint.ExchangeAsync([], new(), CancellationToken.None);
        var identity = await server.ProtocolEndpoint.ExchangeAsync([], new(new ClaimsIdentity()), CancellationToken.None);
        await Assert.That(SignalRCarrier.Decode(absent).StatusCode).IsEqualTo((int)HttpStatusCode.Unauthorized);
        await Assert.That(SignalRCarrier.Decode(anonymous).StatusCode).IsEqualTo((int)HttpStatusCode.Unauthorized);
        await Assert.That(SignalRCarrier.Decode(identity).StatusCode).IsEqualTo((int)HttpStatusCode.Unauthorized);
    }

    /// <summary>Checks a host can refuse to map an authenticated connection to a trusted tenant/client.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task AuthenticatedPrincipalWithoutMappingIsDenied()
    {
        await using var server = await SignalRRemoteTransportAdapterTests.TestServer.StartAsync();
        await using var endpoint = new SignalRServerEndpoint(Options(server.CoreHub), static _ => null);
        var bytes = await endpoint.ExchangeAsync([], Principal(), CancellationToken.None);
        await Assert.That(SignalRCarrier.Decode(bytes).StatusCode).IsEqualTo((int)HttpStatusCode.Unauthorized);
        await endpoint.DisposeAsync();
        await using var adapter = server.Adapter();
        var request = new TransportConnectRequest(new(new(1, 0), new(1, 0)), new("client"), []);
        await using var session = await adapter.ConnectAsync(request, CancellationToken.None);
        await Assert.That(session.NegotiatedCapabilities.ProtocolVersion).IsEqualTo(new(1, 0));
    }

    /// <summary>Checks endpoint composition cannot exceed the independent carrier bounds.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task InvalidCompositionAndOversizedLimitsAreRejected()
    {
        await using var server = await SignalRRemoteTransportAdapterTests.TestServer.StartAsync();
        var options = Options(server.CoreHub);
        _ = Assert.ThrowsExactly<ArgumentNullException>(static () => { _ = new SignalRServerEndpoint(null!, static _ => null); });
        _ = Assert.ThrowsExactly<ArgumentNullException>(() => { _ = new SignalRServerEndpoint(options, null!); });
        _ = Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
        {
            _ = new SignalRServerEndpoint(options with { MaximumRequestBytes = SignalRCarrier.MaximumBodyBytes + 1 }, static _ => null);
        });
        _ = Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
        {
            _ = new SignalRServerEndpoint(options with { MaximumResponseBytes = SignalRCarrier.MaximumBodyBytes + 1 }, static _ => null);
        });
        await Assert.That(server.Domain.Calls).IsEqualTo(0);
    }

    /// <summary>Creates a valid composition for authentication and capacity admission tests.</summary>
    /// <param name="hub">The borrowed real core hub.</param>
    /// <returns>The endpoint options.</returns>
    private static HttpServerEndpointOptions Options(IServerStreamHub hub) => new()
    {
        Hub = hub,
        DeclaredCapabilities = new(new(1, 0), RemoteTransportCapabilities.None, 1, LogicalBodyBytes, null, null),
        ReplayAuthorizer = new ReplayAuthorizer(),
    };

    /// <summary>Creates the trusted test principal supplied by the host.</summary>
    /// <returns>The principal.</returns>
    private static ClaimsPrincipal Principal() => new(new ClaimsIdentity([new(ClaimTypes.NameIdentifier, "client")], "test"));

    /// <summary>Refuses protected replay admission in tests that must stop before that stage.</summary>
    private sealed class ReplayAuthorizer : IHttpReplayAuthorizer
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);
    }
}
