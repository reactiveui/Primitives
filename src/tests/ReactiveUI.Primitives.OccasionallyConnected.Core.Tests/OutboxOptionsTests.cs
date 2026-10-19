// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests durable outgoing operation capacity limits.</summary>
public sealed class OutboxOptionsTests
{
    /// <summary>The documented default operation capacity.</summary>
    private const int DefaultMaxOperations = 10_000;

    /// <summary>The documented default encoded byte capacity.</summary>
    private const long DefaultMaxBytes = 64L * 1024 * 1024;

    /// <summary>The implementation default concurrent blocked publisher capacity.</summary>
    private const int DefaultMaximumBlockedPublishers = 1_000;

    /// <summary>Verifies that each default keeps durable publishing bounded.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task DefaultsUseDocumentedFiniteCapacityLimits()
    {
        var options = new OutboxOptions();

        options.Validate();

        await Assert.That(options.MaxOperations).IsEqualTo(DefaultMaxOperations);
        await Assert.That(options.MaxBytes).IsEqualTo(DefaultMaxBytes);
        await Assert.That(options.MaximumBlockedPublishers).IsEqualTo(DefaultMaximumBlockedPublishers);
    }

    /// <summary>Verifies that each durable operation capacity must be positive.</summary>
    /// <param name="maxOperations">The invalid operation capacity.</param>
    /// <returns>The asynchronous assertion.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task NonPositiveMaxOperationsAreRejected(int maxOperations)
    {
        var options = new OutboxOptions { MaxOperations = maxOperations };

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
        var options = new OutboxOptions { MaxBytes = maxBytes };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies that the number of waiting publishers remains bounded.</summary>
    /// <param name="maximumBlockedPublishers">The invalid waiting publisher capacity.</param>
    /// <returns>The asynchronous assertion.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task NonPositiveMaximumBlockedPublishersAreRejected(int maximumBlockedPublishers)
    {
        var options = new OutboxOptions { MaximumBlockedPublishers = maximumBlockedPublishers };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies copied capacity settings can use each smallest positive value without mutating the source.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task MinimumPositiveLimitsCanBeCopiedWithoutChangingDefaults()
    {
        var defaults = new OutboxOptions();
        var options = defaults with { MaxOperations = 1, MaxBytes = 1, MaximumBlockedPublishers = 1 };

        options.Validate();

        await Assert.That(options.MaxOperations).IsEqualTo(1);
        await Assert.That(options.MaxBytes).IsEqualTo(1L);
        await Assert.That(options.MaximumBlockedPublishers).IsEqualTo(1);
        await Assert.That(defaults.MaxOperations).IsEqualTo(DefaultMaxOperations);
        await Assert.That(defaults.MaxBytes).IsEqualTo(DefaultMaxBytes);
        await Assert.That(defaults.MaximumBlockedPublishers).IsEqualTo(DefaultMaximumBlockedPublishers);
    }
}
