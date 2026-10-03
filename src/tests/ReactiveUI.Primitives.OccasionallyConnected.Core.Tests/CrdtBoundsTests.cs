// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="CrdtBounds"/>.</summary>
public sealed class CrdtBoundsTests
{
    /// <summary>Verifies ownership bounds cannot exceed the fixed allocation ceilings.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OwnershipLimitsRejectValuesAboveTheirDefaults()
    {
        var defaults = CrdtBounds.Default;
        Action[] invalidValidations =
        [
            () => (defaults with { MaximumCounterComponents = defaults.MaximumCounterComponents + 1 }).Validate(),
            () => (defaults with { MaximumDotBindings = defaults.MaximumDotBindings + 1 }).Validate(),
            () => (defaults with { MaximumTombstones = defaults.MaximumTombstones + 1 }).Validate(),
            () => (defaults with { MaximumElements = defaults.MaximumElements + 1 }).Validate(),
            () => (defaults with { MaximumElementBytes = defaults.MaximumElementBytes + 1 }).Validate(),
            () => (defaults with { MaximumRegisterBytes = defaults.MaximumRegisterBytes + 1 }).Validate(),
        ];

        foreach (var validate in invalidValidations)
        {
            await Assert.That(validate).ThrowsExactly<ArgumentOutOfRangeException>();
        }
    }

    /// <summary>Verifies defaults and reduced ownership limits remain valid configuration.</summary>
    [Test]
    public void OwnershipLimitsAcceptDefaultsAndReductions()
    {
        CrdtBounds.Default.Validate();
        (CrdtBounds.Default with
        {
            MaximumCounterComponents = 1,
            MaximumDotBindings = 1,
            MaximumTombstones = 1,
            MaximumElements = 1,
            MaximumElementBytes = 1,
            MaximumRegisterBytes = 1,
        }).Validate();
    }
}
