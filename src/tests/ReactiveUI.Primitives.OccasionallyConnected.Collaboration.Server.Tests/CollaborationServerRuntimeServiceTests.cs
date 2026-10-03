// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Tests for <see cref="CollaborationServerRuntimeService"/>.</summary>
public sealed class CollaborationServerRuntimeServiceTests
{
    /// <summary>The bearer token accepted by runtime-service options.</summary>
    private const string Token = "token-a";

    /// <summary>The tenant configured for runtime-service options.</summary>
    private const string Tenant = "tenant-a";

    /// <summary>The client configured for runtime-service options.</summary>
    private const string Client = "client-a";

    /// <summary>Verifies credentials cannot be read before hosted startup creates the runtime.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CredentialsRejectsAccessBeforeStartup()
    {
        var service = new CollaborationServerRuntimeService(CreateOptions());

        await Assert.That(() => service.Credentials).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies request handling cannot run before hosted startup creates the runtime.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HandleAsyncRejectsAccessBeforeStartup()
    {
        var service = new CollaborationServerRuntimeService(CreateOptions());
        using var request = new HttpRequestMessage(HttpMethod.Get, "http://127.0.0.1/oc/capabilities");

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            service.HandleAsync(request, new(Tenant, Client), CancellationToken.None).AsTask());
    }

    /// <summary>Creates valid runtime-service options without starting the runtime.</summary>
    /// <returns>The configured options.</returns>
    private static CollaborationServerOptions CreateOptions() =>
        new() { ListenUri = new(CollaborationServerOptions.DefaultUrl), DatabasePath = OwnedTempDirectory.CreateDatabasePath("rxui-oc-runtime-service-"), Credentials = [new(Token, Tenant, Client)] };
}
