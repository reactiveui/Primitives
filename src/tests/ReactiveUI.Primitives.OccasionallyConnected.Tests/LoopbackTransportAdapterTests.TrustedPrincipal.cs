// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Trusted-principal API tests for <see cref="LoopbackTransportAdapter"/>.</summary>
public sealed partial class LoopbackTransportAdapterTests
{
    /// <summary>Verifies loopback options expose only a host-authenticated principal for hub calls.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OptionsExposeAuthenticatedClientInsteadOfDeclarativeClient()
    {
        var options = CreateOptions(new RecordingHub());

        await Assert.That(options.AuthenticatedClient).IsEqualTo(new(TrustedTenant, TrustedClientId));
        await Assert.That(typeof(LoopbackTransportAdapterOptions).GetProperty("Client")).IsNull();
    }
}
