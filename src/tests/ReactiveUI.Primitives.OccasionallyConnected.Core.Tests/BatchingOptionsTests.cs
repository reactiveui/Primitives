// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests count, encoded-byte and dwell-time batching limits.</summary>
public sealed class BatchingOptionsTests
{
    /// <summary>The documented default batch operation limit.</summary>
    private const int DefaultMaximumOperations = 100;

    /// <summary>The documented default batch byte limit.</summary>
    private const long DefaultMaximumBytes = 1_048_576;

    /// <summary>The documented default dwell time in milliseconds.</summary>
    private const int DefaultDwellMilliseconds = 50;

    /// <summary>Checks that the defaults bound every batching dimension.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task DefaultsBoundOperationsBytesAndDwellTime()
    {
        var options = new BatchingOptions();

        options.Validate();

        await Assert.That(options.MaximumOperations).IsEqualTo(DefaultMaximumOperations);
        await Assert.That(options.MaximumBytes).IsEqualTo(DefaultMaximumBytes);
        await Assert.That(options.MaximumDwellTime).IsEqualTo(TimeSpan.FromMilliseconds(DefaultDwellMilliseconds));
        await Assert.That(options.MaxInFlightBatchesPerStream).IsEqualTo(1);
    }

    /// <summary>Checks that invalid operation limits cannot disable admission bounds.</summary>
    /// <param name="maximum">The invalid limit.</param>
    /// <returns>The asynchronous assertion.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task NonPositiveOperationLimitsAreRejected(int maximum)
    {
        var options = new BatchingOptions { MaximumOperations = maximum };

        await Assert.That(options.Validate).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks that invalid byte limits cannot disable admission bounds.</summary>
    /// <param name="maximum">The invalid limit.</param>
    /// <returns>The asynchronous assertion.</returns>
    [Test]
    [Arguments(0L)]
    [Arguments(-1L)]
    public async Task NonPositiveByteLimitsAreRejected(long maximum)
    {
        var options = new BatchingOptions { MaximumBytes = maximum };

        await Assert.That(options.Validate).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks that a batch cannot remain buffered with an invalid deadline.</summary>
    /// <param name="ticks">The invalid deadline duration.</param>
    /// <returns>The asynchronous assertion.</returns>
    [Test]
    [Arguments(0L)]
    [Arguments(-1L)]
    public async Task NonPositiveDwellTimesAreRejected(long ticks)
    {
        var options = new BatchingOptions { MaximumDwellTime = TimeSpan.FromTicks(ticks) };

        await Assert.That(options.Validate).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks that per-stream concurrency remains bounded and enabled.</summary>
    /// <param name="maximum">The invalid concurrency.</param>
    /// <returns>The asynchronous assertion.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task NonPositiveInFlightLimitsAreRejected(int maximum)
    {
        var options = new BatchingOptions { MaxInFlightBatchesPerStream = maximum };

        await Assert.That(options.Validate).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks the smallest useful limits and immutable reconfiguration.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task MinimumLimitsCanBeConfiguredWithoutMutatingDefaults()
    {
        var defaults = new BatchingOptions();
        var options = defaults with { MaximumOperations = 1, MaximumBytes = 1, MaximumDwellTime = TimeSpan.FromTicks(1) };

        options.Validate();

        await Assert.That(options.MaximumOperations).IsEqualTo(1);
        await Assert.That(options.MaximumBytes).IsEqualTo(1L);
        await Assert.That(defaults.MaximumOperations).IsEqualTo(DefaultMaximumOperations);
    }
}
