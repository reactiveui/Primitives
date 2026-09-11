// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Threading.Tasks;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests privacy-preserving diagnostic configuration.</summary>
public sealed class DiagnosticsOptionsTests
{
    /// <summary>The documented default activity sampling ratio.</summary>
    private const double DefaultActivitySamplingRatio = 0.1;

    /// <summary>The documented default queued fault limit.</summary>
    private const int DefaultMaximumQueuedFaults = 256;

    /// <summary>The documented default queued fault byte limit.</summary>
    private const int DefaultMaximumQueuedFaultBytes = 64 * 1024;

    /// <summary>The documented default high-water mark.</summary>
    private const double DefaultHighWaterMark = 0.8;

    /// <summary>The invalid ratio below the lower boundary.</summary>
    private const double BelowMinimumSamplingRatio = -0.1;

    /// <summary>The invalid ratio above the upper boundary.</summary>
    private const double AboveMaximumSamplingRatio = 1.1;

    /// <summary>The accepted minimum sampling ratio.</summary>
    private const double MinimumSamplingRatio = 0;

    /// <summary>The accepted maximum sampling ratio.</summary>
    private const double MaximumSamplingRatio = 1;

    /// <summary>The invalid high-water mark below the lower boundary.</summary>
    private const double BelowMinimumHighWaterMark = -0.1;

    /// <summary>Verifies documented defaults remain bounded and identifier-safe.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    public async Task DefaultsUseDocumentedLimits()
    {
        var options = new DiagnosticsOptions();

        options.Validate();

        await Assert.That(options.Enabled).IsTrue();
        await Assert.That(options.IncludeHashedIdentifiers).IsFalse();
        await Assert.That(options.ActivitySamplingRatio).IsEqualTo(DefaultActivitySamplingRatio);
        await Assert.That(options.MaximumQueuedFaults).IsEqualTo(DefaultMaximumQueuedFaults);
        await Assert.That(options.MaximumQueuedFaultBytes).IsEqualTo(DefaultMaximumQueuedFaultBytes);
        await Assert.That(options.HighWaterMark).IsEqualTo(DefaultHighWaterMark);
    }

    /// <summary>Verifies sampling is finite and inclusive between zero and one.</summary>
    /// <param name="ratio">The invalid ratio.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    [Arguments(BelowMinimumSamplingRatio)]
    [Arguments(AboveMaximumSamplingRatio)]
    [Arguments(double.NaN)]
    [Arguments(double.PositiveInfinity)]
    public async Task InvalidSamplingRatiosAreRejected(double ratio)
    {
        var exception = Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => (new DiagnosticsOptions { ActivitySamplingRatio = ratio }).Validate());

        await Assert.That(exception.ParamName).IsEqualTo(nameof(DiagnosticsOptions.ActivitySamplingRatio));
        await Assert.That(exception.ActualValue).IsEqualTo(ratio);
    }

    /// <summary>Verifies sampling accepts both inclusive boundaries.</summary>
    [Test]
    public void SamplingBoundariesAreAccepted()
    {
        (new DiagnosticsOptions { ActivitySamplingRatio = MinimumSamplingRatio }).Validate();
        (new DiagnosticsOptions { ActivitySamplingRatio = MaximumSamplingRatio }).Validate();
    }

    /// <summary>Verifies the queued fault limit remains positive.</summary>
    /// <param name="value">The invalid limit.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task NonPositiveQueuedFaultLimitsAreRejected(int value)
    {
        var exception = Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => (new DiagnosticsOptions { MaximumQueuedFaults = value }).Validate());

        await Assert.That(exception.ParamName).IsEqualTo(nameof(DiagnosticsOptions.MaximumQueuedFaults));
        await Assert.That(exception.ActualValue).IsEqualTo(value);
    }

    /// <summary>Verifies the queued fault byte limit remains positive.</summary>
    /// <param name="value">The invalid limit.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task NonPositiveQueuedFaultByteLimitsAreRejected(int value)
    {
        var exception = Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => (new DiagnosticsOptions { MaximumQueuedFaultBytes = value }).Validate());

        await Assert.That(exception.ParamName).IsEqualTo(nameof(DiagnosticsOptions.MaximumQueuedFaultBytes));
        await Assert.That(exception.ActualValue).IsEqualTo(value);
    }

    /// <summary>Verifies high-water marks leave a usable noncritical interval.</summary>
    /// <param name="value">The invalid high-water mark.</param>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    [Arguments(0.0)]
    [Arguments(BelowMinimumHighWaterMark)]
    [Arguments(1.0)]
    [Arguments(double.NaN)]
    [Arguments(double.PositiveInfinity)]
    public async Task InvalidHighWaterMarksAreRejected(double value)
    {
        var exception = Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => (new DiagnosticsOptions { HighWaterMark = value }).Validate());

        await Assert.That(exception.ParamName).IsEqualTo(nameof(DiagnosticsOptions.HighWaterMark));
        await Assert.That(exception.ActualValue).IsEqualTo(value);
    }

    /// <summary>Verifies records can be copied without changing the source configuration.</summary>
    /// <returns>A task that represents the asynchronous assertion work.</returns>
    [Test]
    public async Task CopiedDiagnosticOptionsDoNotMutateTheSource()
    {
        var source = new DiagnosticsOptions();
        var copy = source with { IncludeHashedIdentifiers = true };

        copy.Validate();

        await Assert.That(source.IncludeHashedIdentifiers).IsFalse();
        await Assert.That(copy.IncludeHashedIdentifiers).IsTrue();
    }
}
