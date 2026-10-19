// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests configuration of endpoint circuit breakers.</summary>
public sealed class CircuitBreakerOptionsTests
{
    /// <summary>The documented default failure threshold.</summary>
    private const int DefaultFailureThreshold = 5;

    /// <summary>The documented default probe interval in seconds.</summary>
    private const int DefaultProbeSeconds = 30;

    /// <summary>Verifies the design defaults remain valid.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DefaultsValidate()
    {
        var options = new CircuitBreakerOptions();
        options.Validate();

        await Assert.That(options.FailureThreshold).IsEqualTo(DefaultFailureThreshold);
        await Assert.That(options.OpenDuration).IsEqualTo(TimeSpan.FromSeconds(DefaultProbeSeconds));
    }

    /// <summary>Verifies non-positive thresholds are rejected.</summary>
    /// <param name="threshold">The invalid threshold.</param>
    /// <returns>A task representing the assertion.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task NonPositiveThresholdsAreRejected(int threshold)
    {
        var options = new CircuitBreakerOptions { FailureThreshold = threshold };
        await Assert.That(options.Validate).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies invalid probe intervals cannot disable recovery indefinitely.</summary>
    /// <param name="ticks">The invalid interval in ticks.</param>
    /// <returns>A task representing the assertion.</returns>
    [Test]
    [Arguments(0L)]
    [Arguments(-1L)]
    [Arguments(long.MaxValue)]
    public async Task InvalidOpenDurationsAreRejected(long ticks)
    {
        var options = new CircuitBreakerOptions { OpenDuration = TimeSpan.FromTicks(ticks) };
        await Assert.That(options.Validate).ThrowsExactly<InvalidOperationException>();
    }
}
