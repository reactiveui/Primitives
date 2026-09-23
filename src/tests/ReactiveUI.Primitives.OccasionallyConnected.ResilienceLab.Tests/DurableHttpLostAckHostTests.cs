// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Exercises the real HTTP bridge and invalid wire inputs for the lost-ACK host.</summary>
public sealed partial class DurableHttpLostAckHostTests
{
    /// <summary>The push endpoint path.</summary>
    private const string PushPath = "/push";

    /// <summary>The JSON content type sent to the HTTP endpoint.</summary>
    private const string JsonContentType = "application/json";

    /// <summary>The server database filename within each scoped host directory.</summary>
    private const string ServerDatabaseFileName = "server.sqlite";

    /// <summary>A request body well beyond the bounded host limit.</summary>
    private const int OversizedBodyBytes = 1_000_000;

    /// <summary>The count at which a push is a retry for the host proof.</summary>
    private const int RejectedPushRequests = 2;

    /// <summary>The shared HTTP client for live host requests.</summary>
    private static readonly HttpClient Client = new();

    /// <summary>Validates operation identifiers are read only from well-formed push arrays.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PushOperationIdParsingRejectsMalformedRequests()
    {
        var valid = "{\"operations\":[{\"operationId\":\"0c9e9f98-72f5-4a7b-b8d9-0890de91c7f6\"}]}"u8.ToArray();
        var validId = DurableHttpLostAckScenario.DurableHttpLostAckHost.TryReadFirstPushOperationId(valid);
        await Assert.That(validId.HasValue).IsTrue();
        await Assert.That(validId?.Value).IsEqualTo(Guid.Parse("0c9e9f98-72f5-4a7b-b8d9-0890de91c7f6"));

        foreach (var json in new[]
        {
            "{}",
            "{\"operations\":{}}",
            "{\"operations\":[]}",
            "{\"operations\":[{}]}",
            "{\"operations\":[{\"operationId\":12}]}",
            "{\"operations\":[{\"operationId\":\"not-a-guid\"}]}",
            "{",
        })
        {
            var parsed = DurableHttpLostAckScenario.DurableHttpLostAckHost.TryReadFirstPushOperationId(Encoding.UTF8.GetBytes(json));
            await Assert.That(parsed.HasValue).IsFalse();
        }
    }

    /// <summary>Enforces the body bound from both declared and streamed lengths.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RequestBodyRejectsOversizedDeclaredAndStreamedPayloads()
    {
        var declared = CreateRequest(HttpMethods.Post, PushPath, new byte[1]);
        declared.ContentLength = OversizedBodyBytes;
        await Assert.That(async () => await DurableHttpLostAckScenario.DurableHttpLostAckHost.ReadBoundedRequestBodyAsync(declared, CancellationToken.None)).Throws<InvalidOperationException>();

        var streamed = CreateRequest(HttpMethods.Post, PushPath, new byte[OversizedBodyBytes]);
        await Assert.That(async () => await DurableHttpLostAckScenario.DurableHttpLostAckHost.ReadBoundedRequestBodyAsync(streamed, CancellationToken.None)).Throws<InvalidOperationException>();
    }

    /// <summary>Preserves GET without a content body and POST with its content metadata.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PortableRequestPreservesMethodBodyAndContentType()
    {
        var get = CreateRequest(HttpMethods.Get, "/subscribe", []);
        using var portableGet = await DurableHttpLostAckScenario.DurableHttpLostAckHost.CreatePortableRequestAsync(get, isPush: false, CancellationToken.None);
        await Assert.That(portableGet.Message.Method).IsEqualTo(HttpMethod.Get);
        await Assert.That(portableGet.Message.Content is null).IsTrue();
        await Assert.That(DurableHttpLostAckScenario.DurableHttpLostAckHost.IsSubscribe(get)).IsTrue();
        await Assert.That(DurableHttpLostAckScenario.DurableHttpLostAckHost.IsPush(get)).IsFalse();

        var post = CreateRequest(HttpMethods.Post, PushPath, "{}"u8.ToArray());
        post.ContentType = JsonContentType;
        using var portablePost = await DurableHttpLostAckScenario.DurableHttpLostAckHost.CreatePortableRequestAsync(post, isPush: true, CancellationToken.None);
        await Assert.That(portablePost.Message.Method).IsEqualTo(HttpMethod.Post);
        await Assert.That(portablePost.Message.Content is not null).IsTrue();
        await Assert.That(portablePost.Message.Content?.Headers.ContentType?.MediaType).IsEqualTo(JsonContentType);
        await Assert.That(DurableHttpLostAckScenario.DurableHttpLostAckHost.IsPush(post)).IsTrue();
        await Assert.That(DurableHttpLostAckScenario.DurableHttpLostAckHost.IsSubscribe(post)).IsFalse();
    }

    /// <summary>Rejects an unauthenticated request through a live Kestrel listener.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task LiveHostRejectsUnauthenticatedRequest()
    {
        var root = CreateTemporaryHostDirectory();
        try
        {
            await using var host = await DurableHttpLostAckScenario.DurableHttpLostAckHost.StartAsync(Path.Combine(root, ServerDatabaseFileName), TimeProvider.System, CancellationToken.None);
            using var response = await Client.GetAsync(new Uri(host.BaseAddress, "negotiate"));
            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>Rejects unknown credentials before they reach the durable endpoint.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CredentialMappingRejectsUnknownCaller()
    {
        var request = CreateRequest(HttpMethods.Get, "/negotiate", []);
        request.Headers["X-Resilience-Lab-Credential"] = "unknown";
        var authenticated = DurableHttpLostAckScenario.DurableHttpLostAckHost.TryResolveAuthenticatedClient(request, out var client, out var credential);
        await Assert.That(authenticated).IsFalse();
        await Assert.That(client.ClientId).IsEqualTo(string.Empty);
        await Assert.That(credential).IsEqualTo(string.Empty);

        var mapped = DurableHttpLostAckScenario.DurableHttpLostAckHost.TryMapCredential("unknown", out client, out credential);
        await Assert.That(mapped).IsFalse();
        await Assert.That(client.ClientId).IsEqualTo(string.Empty);
        await Assert.That(credential).IsEqualTo(string.Empty);
    }

    /// <summary>Canceled host startup releases its partially constructed SQLite and HTTP resources.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CanceledStartupReleasesPartialHost()
    {
        var root = CreateTemporaryHostDirectory();
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        try
        {
            await Assert.That(async () =>
            {
                await using var host = await DurableHttpLostAckScenario.DurableHttpLostAckHost.StartAsync(Path.Combine(root, ServerDatabaseFileName), TimeProvider.System, cancellation.Token);
            }).Throws<OperationCanceledException>();
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>Two actual rejected push requests surface both initial and retry HTTP failures.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task LiveHostSurfacesRejectedFirstAndRetryPush()
    {
        var root = CreateTemporaryHostDirectory();
        try
        {
            await using var host = await DurableHttpLostAckScenario.DurableHttpLostAckHost.StartAsync(Path.Combine(root, ServerDatabaseFileName), TimeProvider.System, CancellationToken.None);
            for (var attempt = 0; attempt < RejectedPushRequests; attempt++)
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(host.BaseAddress, "push")) { Content = new StringContent("{", Encoding.UTF8, JsonContentType) };
                _ = request.Headers.TryAddWithoutValidation("X-Resilience-Lab-Credential", "lost-ack-first-context");
                using var response = await Client.SendAsync(request);
                await Assert.That(response.IsSuccessStatusCode).IsFalse();
            }

            await Assert.That(async () => await host.WaitForFirstPushCommittedAsync(CancellationToken.None)).Throws<InvalidOperationException>();
            await Assert.That(async () => await host.WaitForRetryPushResponseAsync(CancellationToken.None)).Throws<InvalidOperationException>();
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>Response copying handles a status-only response without constructing content.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResponseHeaderCopyAcceptsContentlessResponse()
    {
        using var source = new HttpResponseMessage(HttpStatusCode.BadRequest);
        _ = source.Headers.TryAddWithoutValidation("X-Diagnostic", "rejected");
        var target = new DefaultHttpContext().Response;
        DurableHttpLostAckScenario.DurableHttpLostAckHost.CopyResponseHeaders(source, target);
        await Assert.That(target.Headers["X-Diagnostic"].ToString()).IsEqualTo("rejected");
        await Assert.That(target.Headers.ContainsKey("Content-Type")).IsFalse();
    }

    /// <summary>An unstarted Kestrel application has no bound address to return to clients.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UnstartedApplicationHasNoBoundBaseAddress()
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = [] });
        await using var application = builder.Build();
        await Assert.That(() => DurableHttpLostAckScenario.DurableHttpLostAckHost.ResolveBaseAddress(application)).Throws<InvalidOperationException>();
    }

    /// <summary>Creates a scoped directory for a real Kestrel host and its SQLite sidecars.</summary>
    /// <returns>The directory path.</returns>
    private static string CreateTemporaryHostDirectory()
    {
        var root = Path.Combine(Path.GetTempPath(), $"oc-lost-ack-host-{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(root);
        return root;
    }

    /// <summary>Creates an ASP.NET request with a real readable body.</summary>
    /// <param name="method">The HTTP method.</param>
    /// <param name="path">The request path.</param>
    /// <param name="body">The request body.</param>
    /// <returns>The request.</returns>
    private static HttpRequest CreateRequest(string method, string path, byte[] body)
    {
        var context = new DefaultHttpContext();
        context.Request.Scheme = "http";
        context.Request.Host = new("localhost");
        context.Request.Method = method;
        context.Request.Path = path;
        context.Request.Body = new MemoryStream(body, writable: false);
        return context.Request;
    }
}
