// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>Checks secure transport composition before opening a physical connection.</summary>
public sealed class SignalRRemoteTransportOptionsTests
{
    /// <summary>Checks URI and insecure-loopback policy.</summary>
    /// <param name="address">The endpoint.</param>
    /// <param name="allowLoopback">Whether loopback HTTP is allowed.</param>
    /// <param name="valid">Whether the endpoint is valid.</param>
    /// <returns>The test task.</returns>
    [Test]
    [Arguments("https://example.com/sync", false, true)]
    [Arguments("http://127.0.0.1/sync", true, true)]
    [Arguments("http://127.0.0.1/sync", false, false)]
    [Arguments("http://example.com/sync", true, false)]
    [Arguments("ftp://example.com/sync", true, false)]
    [Arguments("https://user:password@example.com/sync", false, false)]
    [Arguments("https://example.com/sync#fragment", false, false)]
    [Arguments("relative", false, false)]
    public async Task EndpointPolicyRejectsUntrustedUris(string address, bool allowLoopback, bool valid)
    {
        var options = new SignalRRemoteTransportOptions
        { Endpoint = new(address, UriKind.RelativeOrAbsolute), AllowInsecureLoopbackHttp = allowLoopback };
        if (valid)
        {
            options.Validate();
        }
        else
        {
            _ = Assert.ThrowsExactly<ArgumentException>(options.Validate);
        }

        await Assert.That(options.Endpoint.OriginalString).IsEqualTo(address);
    }

    /// <summary>Checks physical connection capacity and required options.</summary>
    /// <returns>The test task.</returns>
    [Test]
    public async Task MissingOptionsAndNonpositiveCapacityAreRejected()
    {
        _ = Assert.ThrowsExactly<ArgumentNullException>(static () => { _ = new SignalRRemoteTransportAdapter(null!); });
        var options = new SignalRRemoteTransportOptions { Endpoint = new("https://example.com/sync"), MaximumSessions = 0 };
        _ = Assert.ThrowsExactly<ArgumentOutOfRangeException>(options.Validate);
        _ = Assert.ThrowsExactly<ArgumentNullException>(() => (options with { Endpoint = null!, MaximumSessions = 1 }).Validate());
        await Assert.That(options.MaximumSessions).IsEqualTo(0);
    }
}
