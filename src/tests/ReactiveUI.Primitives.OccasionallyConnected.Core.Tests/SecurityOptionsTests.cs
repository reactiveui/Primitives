// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Threading.Tasks;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests bounded security configuration.</summary>
public sealed class SecurityOptionsTests
{
    /// <summary>The number of bytes in one mebibyte.</summary>
    private const int BytesPerMebibyte = 1024 * 1024;

    /// <summary>The documented default payload limit.</summary>
    private const int DefaultMaximumPayloadBytes = BytesPerMebibyte;

    /// <summary>The documented default message limit.</summary>
    private const int DefaultMaximumMessageBytes = 2 * BytesPerMebibyte;

    /// <summary>The documented default decompressed-message limit.</summary>
    private const int DefaultMaximumDecompressedBytes = 4 * BytesPerMebibyte;

    /// <summary>The documented default metadata entry limit.</summary>
    private const int DefaultMaximumMetadataEntries = 32;

    /// <summary>The documented default metadata value limit.</summary>
    private const int DefaultMaximumMetadataValueBytes = 4096;

    /// <summary>The documented default JSON depth limit.</summary>
    private const int DefaultMaximumJsonDepth = 64;

    /// <summary>The documented default replay window in minutes.</summary>
    private const int DefaultReplayWindowMinutes = 5;

    /// <summary>The documented default nonce retention in minutes.</summary>
    private const int DefaultNonceRetentionMinutes = 10;

    /// <summary>The smaller limit in ordered-boundary tests.</summary>
    private const int SmallerSecurityLimit = 1;

    /// <summary>The larger limit in ordered-boundary tests.</summary>
    private const int LargerSecurityLimit = 2;

    /// <summary>Verifies default limits are bounded and encryption at rest remains opt-in.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    public async Task DefaultsUseDocumentedBounds()
    {
        var options = new SecurityOptions();

        options.Validate();

        await Assert.That(options.RequireAuthenticatedEncryptionAtRest).IsFalse();
        await Assert.That(options.MaximumPayloadBytes).IsEqualTo(DefaultMaximumPayloadBytes);
        await Assert.That(options.MaximumMessageBytes).IsEqualTo(DefaultMaximumMessageBytes);
        await Assert.That(options.MaximumDecompressedBytes).IsEqualTo(DefaultMaximumDecompressedBytes);
        await Assert.That(options.MaximumMetadataEntries).IsEqualTo(DefaultMaximumMetadataEntries);
        await Assert.That(options.MaximumMetadataValueBytes).IsEqualTo(DefaultMaximumMetadataValueBytes);
        await Assert.That(options.MaximumJsonDepth).IsEqualTo(DefaultMaximumJsonDepth);
        await Assert.That(options.ReplayWindow).IsEqualTo(TimeSpan.FromMinutes(DefaultReplayWindowMinutes));
        await Assert.That(options.NonceRetention).IsEqualTo(TimeSpan.FromMinutes(DefaultNonceRetentionMinutes));
    }

    /// <summary>Verifies every integral security limit must remain positive.</summary>
    /// <param name="value">The invalid limit.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task NonPositiveIntegralLimitsAreRejected(int value)
    {
        await AssertSecurityFailure(
            new SecurityOptions { MaximumPayloadBytes = value },
            nameof(SecurityOptions.MaximumPayloadBytes),
            value);
        await AssertSecurityFailure(
            new SecurityOptions { MaximumMessageBytes = value },
            nameof(SecurityOptions.MaximumMessageBytes),
            value);
        await AssertSecurityFailure(
            new SecurityOptions { MaximumDecompressedBytes = value },
            nameof(SecurityOptions.MaximumDecompressedBytes),
            value);
        await AssertSecurityFailure(
            new SecurityOptions { MaximumMetadataEntries = value },
            nameof(SecurityOptions.MaximumMetadataEntries),
            value);
        await AssertSecurityFailure(
            new SecurityOptions { MaximumMetadataValueBytes = value },
            nameof(SecurityOptions.MaximumMetadataValueBytes),
            value);
        await AssertSecurityFailure(
            new SecurityOptions { MaximumJsonDepth = value },
            nameof(SecurityOptions.MaximumJsonDepth),
            value);
    }

    /// <summary>Verifies finite positive replay limits are required.</summary>
    /// <param name="ticks">The invalid interval.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    [Arguments(0L)]
    [Arguments(-1L)]
    [Arguments(long.MaxValue)]
    public async Task InvalidReplayIntervalsAreRejected(long ticks)
    {
        var value = TimeSpan.FromTicks(ticks);

        await AssertSecurityFailure(
            new SecurityOptions { ReplayWindow = value },
            nameof(SecurityOptions.ReplayWindow),
            value);
        await AssertSecurityFailure(
            new SecurityOptions { NonceRetention = value },
            nameof(SecurityOptions.NonceRetention),
            value);
    }

    /// <summary>Verifies each nested security bound is ordered.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    public async Task ContradictorySecurityBoundsAreRejected()
    {
        await AssertSecurityFailure(
            new SecurityOptions { MaximumPayloadBytes = LargerSecurityLimit, MaximumMessageBytes = SmallerSecurityLimit },
            nameof(SecurityOptions.MaximumPayloadBytes),
            LargerSecurityLimit);
        var messageFailure = new SecurityOptions { MaximumPayloadBytes = SmallerSecurityLimit, MaximumMessageBytes = LargerSecurityLimit, MaximumDecompressedBytes = SmallerSecurityLimit };

        await AssertSecurityFailure(messageFailure, nameof(SecurityOptions.MaximumMessageBytes), LargerSecurityLimit);
        var replayFailure = new SecurityOptions { ReplayWindow = TimeSpan.FromMinutes(LargerSecurityLimit), NonceRetention = TimeSpan.FromMinutes(SmallerSecurityLimit) };

        await AssertSecurityFailure(
            replayFailure,
            nameof(SecurityOptions.NonceRetention),
            TimeSpan.FromMinutes(SmallerSecurityLimit));
    }

    /// <summary>Verifies a valid copied configuration retains the source value.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    public async Task CopiedSecurityOptionsDoNotMutateTheSource()
    {
        var source = new SecurityOptions();
        var copy = source with { RequireAuthenticatedEncryptionAtRest = true };

        copy.Validate();

        await Assert.That(source.RequireAuthenticatedEncryptionAtRest).IsFalse();
        await Assert.That(copy.RequireAuthenticatedEncryptionAtRest).IsTrue();
    }

    /// <summary>Asserts a security option failure.</summary>
    /// <param name="options">The options to validate.</param>
    /// <param name="parameterName">The expected parameter name.</param>
    /// <param name="actualValue">The expected actual value.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    private static async Task AssertSecurityFailure(
        SecurityOptions options,
        string parameterName,
        object actualValue)
    {
        var exception = Assert.ThrowsExactly<ArgumentOutOfRangeException>(options.Validate);

        await Assert.That(exception.ParamName).IsEqualTo(parameterName);
        await Assert.That(exception.ActualValue).IsEqualTo(actualValue);
    }
}
