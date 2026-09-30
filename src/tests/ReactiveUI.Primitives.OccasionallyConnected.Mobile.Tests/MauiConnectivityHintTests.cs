// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Maui.Networking;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests;

/// <summary>Tests the Essentials connectivity adapter's hints and subscriptions.</summary>
public sealed class MauiConnectivityHintTests
{
    /// <summary>Checks only internet access is an availability hint and disposal removes the platform subscription.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task EssentialsNetworkAccessRemainsAHintAndDisposalUnsubscribes()
    {
        var platform = new MobileTestConnectivity();
        const int ExpectedChanges = 2;
        var hints = new MauiConnectivityHint(platform);
        var changes = 0;
        hints.Changed += (_, _) => changes++;
        platform.SetAccess(NetworkAccess.ConstrainedInternet);
        await Assert.That(hints.NetworkAvailable).IsFalse();
        platform.SetAccess(NetworkAccess.Internet);
        await Assert.That(hints.NetworkAvailable).IsTrue();
        await Assert.That(changes).IsEqualTo(ExpectedChanges);
        hints.Dispose();
        hints.Dispose();
        platform.SetAccess(NetworkAccess.None);
        await Assert.That(changes).IsEqualTo(ExpectedChanges);
    }
}
