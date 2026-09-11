// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LocalStoreInitialization"/>.</summary>
public sealed class LocalStoreInitializationTests
{
    /// <summary>Verifies encryption configuration is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsEncryptionConfiguration()
    {
        var initialization = new LocalStoreInitialization("store-1", 1, true);

        await Assert.That(initialization.StoreIdentity).IsEqualTo("store-1");
        await Assert.That(initialization.RequiredSchemaVersion).IsEqualTo(1);
        await Assert.That(initialization.RequireAuthenticatedEncryptionAtRest).IsTrue();
    }
}
