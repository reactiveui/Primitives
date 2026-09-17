// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Tests for <see cref="DevelopmentCredential"/>.</summary>
public sealed class DevelopmentCredentialTests
{
    /// <summary>The first test token used by credential tests.</summary>
    private const string TokenA = "token-a";

    /// <summary>The second test token used by credential tests.</summary>
    private const string TokenB = "token-b";

    /// <summary>The first tenant used by credential tests.</summary>
    private const string TenantA = "tenant-a";

    /// <summary>The second tenant used by credential tests.</summary>
    private const string TenantB = "tenant-b";

    /// <summary>The first client used by credential tests.</summary>
    private const string ClientA = "client-a";

    /// <summary>The second client used by credential tests.</summary>
    private const string ClientB = "client-b";

    /// <summary>The serialized credentials used by parser tests.</summary>
    private const string SerializedCredentials = $"{TokenA}:{TenantA}:{ClientA};{TokenB}:{TenantB}:{ClientB}";

    /// <summary>A malformed serialized credential missing the client segment.</summary>
    private const string MalformedCredential = $"{TokenA}:{TenantA}";

    /// <summary>A serialized credential with a blank token segment.</summary>
    private const string BlankTokenCredential = $" :{TenantA}:{ClientA}";

    /// <summary>Verifies configured tokens map to trusted tenant and client identities.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ParseManyMapsTokensToTrustedClients()
    {
        var credentials = DevelopmentCredential.ParseMany(SerializedCredentials);
        var store = new DevelopmentCredentialStore(credentials);

        var authenticated = store.TryAuthenticate(TokenB, out var client);

        await Assert.That(authenticated).IsTrue();
        await Assert.That(client.TenantId).IsEqualTo(TenantB);
        await Assert.That(client.ClientId).IsEqualTo(ClientB);
    }

    /// <summary>Verifies empty credential configuration parses to an empty list instead of a default token.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ParseManyReturnsEmptyCredentialsForWhitespace()
    {
        var credentials = DevelopmentCredential.ParseMany(" ");

        await Assert.That(credentials).Count().IsEqualTo(0);
    }

    /// <summary>Verifies a credential creates the trusted server client bound to its configured identity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ToAuthenticatedClientUsesConfiguredTenantAndClient()
    {
        var client = new DevelopmentCredential(TokenA, TenantA, ClientA).ToAuthenticatedClient();

        await Assert.That(client.TenantId).IsEqualTo(TenantA);
        await Assert.That(client.ClientId).IsEqualTo(ClientA);
    }

    /// <summary>Verifies unknown tokens are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TryAuthenticateRejectsUnknownTokens()
    {
        var store = new DevelopmentCredentialStore([new(TokenA, TenantA, ClientA)]);

        var authenticated = store.TryAuthenticate(TokenB, out var client);

        await Assert.That(authenticated).IsFalse();
        await Assert.That(client.TenantId).IsEqualTo(string.Empty);
        await Assert.That(client.ClientId).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies malformed serialized credentials are rejected before the store is created.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ParseManyRejectsMalformedCredentialEntry() =>
        await Assert.That(static () => DevelopmentCredential.ParseMany(MalformedCredential)).ThrowsExactly<ArgumentException>();

    /// <summary>Verifies blank credential segments are rejected by parser validation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ParseManyRejectsBlankCredentialSegment() =>
        await Assert.That(static () => DevelopmentCredential.ParseMany(BlankTokenCredential)).ThrowsExactly<ArgumentException>();

    /// <summary>Verifies duplicate development tokens are rejected before request handling.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StoreRejectsDuplicateCredentialTokens()
    {
        var credentials = new[]
        {
            new DevelopmentCredential(TokenA, TenantA, ClientA),
            new DevelopmentCredential(TokenA, TenantB, ClientB),
        };

        await Assert.That(() => new DevelopmentCredentialStore(credentials)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies manually constructed credentials reject blank trusted identity fields.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StoreRejectsBlankCredentialIdentity()
    {
        var credentials = new[] { new DevelopmentCredential(TokenA, " ", ClientA) };

        await Assert.That(() => new DevelopmentCredentialStore(credentials)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies manually built credential lists reject null entries during shared validation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ValidateAllRejectsNullCredentialEntries()
    {
        var credentials = CreateCredentialsWithNullEntry();

        await Assert.That(() => DevelopmentCredential.ValidateAll(credentials)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies the development store rejects null entries while building its owned token map.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StoreRejectsNullCredentialEntries()
    {
        var credentials = CreateCredentialsWithNullEntry();

        await Assert.That(() => new DevelopmentCredentialStore(credentials)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Creates a credential array containing a default null reference.</summary>
    /// <returns>The credential array.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DevelopmentCredential[] CreateCredentialsWithNullEntry() =>
        new DevelopmentCredential[1];
}
