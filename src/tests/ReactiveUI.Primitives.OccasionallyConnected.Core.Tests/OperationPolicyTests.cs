// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OperationPolicy"/>.</summary>
public sealed class OperationPolicyTests
{
    /// <summary>An invalid conflict policy value.</summary>
    private const ConflictPolicy InvalidConflictPolicy = (ConflictPolicy)99;

    /// <summary>An invalid delivery guarantee value.</summary>
    private const DeliveryGuarantee InvalidDeliveryGuarantee = (DeliveryGuarantee)99;

    /// <summary>An invalid durability value.</summary>
    private const OperationDurability InvalidOperationDurability = (OperationDurability)99;

    /// <summary>Verifies invalid policy combinations are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsUnsupportedCombinations()
    {
        var nonDurableExactlyOnce = new OperationPolicy(DeliveryGuarantee.ExactlyOnce, OperationDurability.Volatile, 0, ConflictPolicy.Merge);
        var excessivePriority = new OperationPolicy(DeliveryGuarantee.AtLeastOnce, OperationDurability.Durable, OperationPolicy.MaximumPriority + 1, ConflictPolicy.Merge);
        var configuredPriority = new OperationPolicy(DeliveryGuarantee.AtLeastOnce, OperationDurability.Durable, OperationPolicy.MaximumPriority + 1, ConflictPolicy.Merge);
        await Assert.That(nonDurableExactlyOnce.Validate).ThrowsExactly<ArgumentException>();
        await Assert.That(excessivePriority.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => configuredPriority.Validate(OperationPolicy.MinimumPriority, OperationPolicy.MaximumPriority + 1)).ThrowsNothing();
    }

    /// <summary>Verifies malformed values and reversed ranges are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsMalformedValues()
    {
        await Assert.That((OperationPolicy.Default with { DeliveryGuarantee = InvalidDeliveryGuarantee }).Validate).ThrowsExactly<ArgumentException>();
        await Assert.That((OperationPolicy.Default with { Durability = InvalidOperationDurability }).Validate).ThrowsExactly<ArgumentException>();
        await Assert.That((OperationPolicy.Default with { ConflictPolicy = InvalidConflictPolicy }).Validate).ThrowsExactly<ArgumentException>();
        await Assert.That(static () => OperationPolicy.Default.Validate(OperationPolicy.MaximumPriority, OperationPolicy.MinimumPriority)).ThrowsExactly<ArgumentOutOfRangeException>();
    }
}
