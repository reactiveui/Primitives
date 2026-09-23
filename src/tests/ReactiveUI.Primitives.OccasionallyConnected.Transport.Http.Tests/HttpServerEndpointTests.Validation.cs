// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using ReactiveUI.Primitives.OccasionallyConnected;
namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Additional public request validation tests for <see cref="HttpServerEndpoint"/>.</summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>Verifies exactly-once retention is accepted when atomic apply-and-acknowledge support is declared.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConstructorAcceptsExactlyOnceWindowWhenAtomicApplyAndAcknowledgeIsDeclared()
    {
        var capabilities = CreateCapabilities(RemoteTransportCapabilities.AtomicApplyAndAcknowledge) with
        {
            EffectiveExactlyOnceWindow = TimeSpan.FromMinutes(EffectiveExactlyOnceWindowMinutes),
        };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()) with { DeclaredCapabilities = capabilities });
        await Assert.That(endpoint.DeclaredCapabilities).IsSameReferenceAs(capabilities);
    }

    /// <summary>Verifies unsafe percent-encoded routes are rejected before endpoint route matching.</summary>
    /// <param name="requestTarget">The unsafe relative request target.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("/%FF")]
    [Arguments("/%C3%28")]
    [Arguments("/%2E")]
    [Arguments("/%2E%2E")]
    public async Task HandleAsyncRejectsUnsafeEscapedRouteBeforeHub(string requestTarget)
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(requestTarget, UriKind.Relative));
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies body-defined routes reject query text even when route normalization matches.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncRejectsRelativeBodyRouteQueryBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = CreateProtocolRequest(HttpMethod.Post, "/%70ush?ignored=1#fragment", CreateCodec().SerializePushRequest(CreateBatch()));
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies relative route matching stops at fragment separators before dispatch.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncRoutesRelativeKnownPathBeforeFragment()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedPushRequestAsync(endpoint, CreateBatch(), "/push#fragment", "push");
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies invalid Unicode in a relative route is rejected by endpoint route parsing.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncRejectsInvalidUnicodeRelativeRouteBeforeHub()
    {
        var requestTarget = $"/{'\uD800'}";
        var uri = new Uri(requestTarget, UriKind.Relative);
        await Assert.That(uri.OriginalString).IsEqualTo(requestTarget);
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = new HttpRequestMessage(HttpMethod.Post, uri);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies impossible-to-buffer declared HTTP content lengths are rejected before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsDeclaredContentLengthOverInt32BeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var content = CreateProtocolContent(CreateCodec().SerializePushRequest(CreateBatch()));
        content.Headers.ContentLength = (long)int.MaxValue + 1;
        using var request = new HttpRequestMessage(HttpMethod.Post, PushUri) { Content = content };
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.RequestEntityTooLarge);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies a protocol version parameter without a value is rejected before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsProtocolVersionParameterWithoutValueBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var content = new ByteArrayContent(CreateCodec().SerializePushRequest(CreateBatch()));
        var contentType = new MediaTypeHeaderValue("application/vnd.reactiveui.occasionally-connected+json");
        contentType.Parameters.Add(new("v"));
        content.Headers.ContentType = contentType;
        using var request = new HttpRequestMessage(HttpMethod.Post, PushUri) { Content = content };
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.UnsupportedMediaType);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies subscribe requests require a bounded protocol query before reaching the hub.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeRejectsMissingQueryBeforeHub()
    {
        var hub = new RecordingHub { SubscribeHandler = static (_, _, cancellationToken) => YieldBatches([CreateReceiveBatch()], cancellationToken) };
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(RelativeSubscribeUri, UriKind.Relative));
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.SubscribeClient).IsNull();
    }

    /// <summary>Verifies relative subscribe queries stop before URI fragments.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeAcceptsRelativeQueryBeforeFragment()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var request = await CreateSignedRelativeSubscribeRequestAsync(endpoint);
        request.RequestUri = new($"{RelativeSubscribeUri}{SubscribeQuery}#fragment", UriKind.Relative);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.NoContent);
        await Assert.That(hub.SubscribeClient).IsEqualTo(CreateAuthenticatedClient());
    }

    /// <summary>Verifies malformed protocol request bodies are rejected before endpoint effects.</summary>
    /// <param name="target">The route target to exercise.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(ConnectBodyReadTarget)]
    [Arguments(AcknowledgeBodyReadTarget)]
    public async Task HandleAsyncRejectsMalformedBodyBeforeEffects(int target)
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var content = CreateProtocolContent("{"u8.ToArray());
        using var request = CreateBodyReadRequest(target, content);
        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(hub.ApplyClient).IsNull();
        await Assert.That(hub.AcknowledgeClient).IsNull();
    }
}
