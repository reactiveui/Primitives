// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for the trusted server-authenticated client contract.</summary>
public sealed class ServerAuthenticatedClientTests
{
    /// <summary>The trusted tenant identifier.</summary>
    private const string TenantId = "tenant-a";

    /// <summary>The trusted client identifier.</summary>
    private const string ClientId = "client-a";

    /// <summary>Verifies the public principal carries only trusted tenant and client identity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConstructorRetainsOnlyTrustedTenantAndClient()
    {
        var client = new ServerAuthenticatedClient(TenantId, ClientId);

        await Assert.That(client.TenantId).IsEqualTo(TenantId);
        await Assert.That(client.ClientId).IsEqualTo(ClientId);
        await Assert.That(typeof(ServerAuthenticatedClient).GetProperty("TenantHint")).IsNull();
        await Assert.That(typeof(ServerAuthenticatedClient).GetMethod("ToClientIdentity", Type.EmptyTypes)).IsNull();
    }

    /// <summary>Verifies the public authorization scope carries only trusted tenant and client identity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ScopeConstructorRetainsTrustedTenantAndClient()
    {
        var scope = new ServerStreamAuthorizationScope(TenantId, ClientId);

        await Assert.That(scope.TenantId).IsEqualTo(TenantId);
        await Assert.That(scope.ClientId).IsEqualTo(ClientId);
    }
}
