// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LocalStoreInitialization"/>.</summary>
public sealed class LocalStoreInitializationTests
{
    /// <summary>The store identity used by tests.</summary>
    private const string StoreIdentity = "store-1";

    /// <summary>The client identity used by tests.</summary>
    private const string ClientId = "client-a";

    /// <summary>Verifies encryption configuration is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsEncryptionConfiguration()
    {
        var initialization = new LocalStoreInitialization(StoreIdentity, 1, true);

        await Assert.That(initialization.StoreIdentity).IsEqualTo(StoreIdentity);
        await Assert.That(initialization.RequiredSchemaVersion).IsEqualTo(1);
        await Assert.That(initialization.RequireAuthenticatedEncryptionAtRest).IsTrue();
        await Assert.That(initialization.ClientId).IsNull();
    }

    /// <summary>Verifies client identity binding configuration is retained without changing the positional constructor.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task InitPropertyRetainsClientIdentityBinding()
    {
        var initialization = new LocalStoreInitialization(StoreIdentity, 1, false) { ClientId = ClientId };

        await Assert.That(initialization.StoreIdentity).IsEqualTo(StoreIdentity);
        await Assert.That(initialization.RequiredSchemaVersion).IsEqualTo(1);
        await Assert.That(initialization.RequireAuthenticatedEncryptionAtRest).IsFalse();
        await Assert.That(initialization.ClientId).IsEqualTo(ClientId);
    }
}
