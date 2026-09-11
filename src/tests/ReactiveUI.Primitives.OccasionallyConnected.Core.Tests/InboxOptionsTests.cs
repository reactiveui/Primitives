// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests durable incoming event capacity limits.</summary>
public sealed class InboxOptionsTests
{
    /// <summary>The implementation default event capacity.</summary>
    private const int DefaultMaxEvents = 10_000;

    /// <summary>The implementation default encoded byte capacity.</summary>
    private const long DefaultMaxBytes = 64L * 1024 * 1024;

    /// <summary>Verifies that each default keeps durable incoming events bounded.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task DefaultsUseFiniteCapacityLimits()
    {
        var options = new InboxOptions();

        options.Validate();

        await Assert.That(options.MaxEvents).IsEqualTo(DefaultMaxEvents);
        await Assert.That(options.MaxBytes).IsEqualTo(DefaultMaxBytes);
    }

    /// <summary>Verifies that each durable event capacity must be positive.</summary>
    /// <param name="maxEvents">The invalid event capacity.</param>
    /// <returns>The asynchronous assertion.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task NonPositiveMaxEventsAreRejected(int maxEvents)
    {
        var options = new InboxOptions { MaxEvents = maxEvents };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies that each durable encoded byte capacity must be positive.</summary>
    /// <param name="maxBytes">The invalid byte capacity.</param>
    /// <returns>The asynchronous assertion.</returns>
    [Test]
    [Arguments(0L)]
    [Arguments(-1L)]
    public async Task NonPositiveMaxBytesAreRejected(long maxBytes)
    {
        var options = new InboxOptions { MaxBytes = maxBytes };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies copied capacity settings can use each smallest positive value without mutating the source.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task MinimumPositiveLimitsCanBeCopiedWithoutChangingDefaults()
    {
        var defaults = new InboxOptions();
        var options = defaults with { MaxEvents = 1, MaxBytes = 1 };

        options.Validate();

        await Assert.That(options.MaxEvents).IsEqualTo(1);
        await Assert.That(options.MaxBytes).IsEqualTo(1L);
        await Assert.That(defaults.MaxEvents).IsEqualTo(DefaultMaxEvents);
        await Assert.That(defaults.MaxBytes).IsEqualTo(DefaultMaxBytes);
    }
}
