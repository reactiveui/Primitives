// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Net;
using System.Net.Http;
using System.Text;
namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests stream-scoped authorization and redacted failures on the portable server endpoint.</summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>The stream the stream-scoped authorizer denies.</summary>
    private const string ForbiddenStreamName = "forbidden-stream";

    /// <summary>The signed subscribe query for the forbidden stream.</summary>
    private const string ForbiddenSubscribeQuery =
        "?streamId=forbidden-stream&subscriptionId=00000000-0000-0000-0000-000000000301&positionKind=0";

    /// <summary>A sentinel secret that must never cross the HTTP boundary.</summary>
    private const string SentinelSecret = "sentinel-bearer-token-7f3a";

    /// <summary>A sentinel tenant identifier that must never cross the HTTP boundary.</summary>
    private const string SentinelTenant = "sentinel-tenant-91c2";

    /// <summary>A sentinel payload text that must never cross the HTTP boundary.</summary>
    private const string SentinelPayload = "sentinel-payload-44d0";

    /// <summary>The hub failure message that carries every sentinel value.</summary>
    private const string SentinelFailureMessage = $"{SentinelSecret} {SentinelTenant} {SentinelPayload} {StreamName}";

    /// <summary>Verifies a push that targets an unauthorized stream is forbidden before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushRejectsUnauthorizedStreamBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, new StreamScopedReplayAuthorizer()));
        var batch = new SyncBatch(Guid.Parse(BatchIdText), [CreateOperation() with { StreamId = new(ForbiddenStreamName) }]);
        using var request = await CreateSignedPushRequestAsync(endpoint, batch);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Forbidden);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies an acknowledgement for an unauthorized stream is forbidden before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAcknowledgeRejectsUnauthorizedStreamBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, new StreamScopedReplayAuthorizer()));
        var acknowledgement = CreateAcknowledgement() with { StreamId = new(ForbiddenStreamName) };
        using var request = await CreateSignedAcknowledgeRequestAsync(endpoint, acknowledgement);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Forbidden);
        await Assert.That(hub.AcknowledgeClient).IsNull();
    }

    /// <summary>Verifies a subscription to an unauthorized stream is forbidden before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeRejectsUnauthorizedStreamBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub, new StreamScopedReplayAuthorizer()));
        var session = await ConnectReplaySessionAsync(endpoint);
        using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{ForbiddenSubscribeQuery}");
        AddSessionReplayHeaders(
            request,
            HttpReplayOperationKind.Subscribe,
            "GET",
            CanonicalSubscribePath,
            [new("streamId", ForbiddenStreamName), new("subscriptionId", SubscriptionIdText), new("positionKind", "0")],
            [],
            session);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Forbidden);
        await Assert.That(hub.SubscribeClient).IsNull();
    }

    /// <summary>Verifies a hub failure carrying secrets crosses the HTTP boundary only as a bodyless status.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushHubFailureDoesNotExposeSecretsOrIdentifiers()
    {
        var hub = new RecordingHub { ApplyHandler = static (_, _, _) => throw new UnauthorizedAccessException(SentinelFailureMessage) };
        await using var endpoint = new HttpServerEndpoint(CreateReplayOptions(hub));
        using var request = await CreateSignedPushRequestAsync(endpoint, CreateBatch());

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        var text = await ReadResponseTextAsync(response);
        await Assert.That(response.StatusCode).IsNotEqualTo(HttpStatusCode.OK);
        await Assert.That(text).DoesNotContain(SentinelSecret);
        await Assert.That(text).DoesNotContain(SentinelTenant);
        await Assert.That(text).DoesNotContain(SentinelPayload);
        await Assert.That(text).DoesNotContain(StreamName);
    }

    /// <summary>Verifies a malformed body carrying secrets is rejected without echoing any of its text.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushMalformedBodyRejectionDoesNotEchoRequestText()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        var body = "{\"batchId\":\"sentinel-bearer-token-7f3a\",\"operations\":[{\"streamId\":\"sentinel-payload-44d0\""u8.ToArray();
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, body);

        using var response = await endpoint.HandleAsync(request, new(SentinelTenant, ClientId), CancellationToken.None);

        var text = await ReadResponseTextAsync(response);
        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
        await Assert.That(text).DoesNotContain(SentinelSecret);
        await Assert.That(text).DoesNotContain(SentinelPayload);
        await Assert.That(text).DoesNotContain(SentinelTenant);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Reads every response header and the response body as one text block.</summary>
    /// <param name="response">The response.</param>
    /// <returns>The combined response text.</returns>
    private static async Task<string> ReadResponseTextAsync(HttpResponseMessage response)
    {
        var builder = new StringBuilder();
        foreach (var header in response.Headers)
        {
            _ = builder.Append(header.Key).Append(':').AppendJoin(',', header.Value).Append('\n');
        }

        if (response.Content is not null)
        {
            foreach (var header in response.Content.Headers)
            {
                _ = builder.Append(header.Key).Append(':').AppendJoin(',', header.Value).Append('\n');
            }

            _ = builder.Append(await response.Content.ReadAsStringAsync().ConfigureAwait(false));
        }

        return builder.ToString();
    }

    /// <summary>Denies replay authorization for requests that name the forbidden stream.</summary>
    private sealed class StreamScopedReplayAuthorizer : IHttpReplayAuthorizer
    {
        /// <inheritdoc/>
        public ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            for (var index = 0; index < context.StreamIds.Count; index++)
            {
                if (string.Equals(context.StreamIds[index].Value, ForbiddenStreamName, StringComparison.Ordinal))
                {
                    return ValueTask.FromResult(false);
                }
            }

            return ValueTask.FromResult(true);
        }
    }
}
