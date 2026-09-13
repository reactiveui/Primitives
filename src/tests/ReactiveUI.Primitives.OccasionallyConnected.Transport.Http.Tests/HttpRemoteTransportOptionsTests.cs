// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpRemoteTransportOptions"/>.</summary>
public sealed class HttpRemoteTransportOptionsTests
{
    /// <summary>The trusted base address text.</summary>
    private const string BaseAddressText = "https://example.invalid/oc/";

    /// <summary>A shared HTTP client for option validation tests.</summary>
    private static readonly HttpClient SharedHttpClient = new();

    /// <summary>Verifies relative base addresses are rejected.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ValidateRejectsRelativeBaseAddress()
    {
        var options = new HttpRemoteTransportOptions { HttpClient = SharedHttpClient, BaseAddress = new("oc/", UriKind.Relative) };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies invalid route paths are rejected.</summary>
    /// <param name="path">The route path.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("")]
    [Arguments(" \t")]
    [Arguments("/rooted")]
    [Arguments("https://example.invalid/absolute")]
    public async Task ValidateRelativePathRejectsUnsafeRoutes(string path)
    {
        await Assert.That(() => HttpRemoteTransportOptionsValidation.ValidateRelativePath(path, nameof(path))).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies positive integer options reject zero and negative values.</summary>
    /// <param name="value">The option value.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task ValidatePositiveRejectsNonPositiveValues(int value)
    {
        await Assert.That(() => HttpRemoteTransportOptionsValidation.ValidatePositive(value, nameof(value))).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies the instance validator checks every positive limit.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ValidateRejectsInvalidInstanceLimit()
    {
        var options = CreateOptions(SharedHttpClient) with { MaximumResponseBytes = 0 };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies invalid subscription capacity is rejected.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ValidateRejectsInvalidMaximumConcurrentSubscriptions()
    {
        var options = CreateOptions(SharedHttpClient) with { MaximumConcurrentSubscriptions = 0 };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Creates valid options.</summary>
    /// <param name="httpClient">The HTTP client.</param>
    /// <returns>The options.</returns>
    private static HttpRemoteTransportOptions CreateOptions(HttpClient httpClient) => new() { HttpClient = httpClient, BaseAddress = new(BaseAddressText) };
}
