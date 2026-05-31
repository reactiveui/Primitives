// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>
/// Compatibility helpers for tests migrated from Microsoft.Reactive.Testing.
/// </summary>
internal static class VirtualClockTestExtensions
{
    /// <summary>
    /// Advances a virtual clock by ticks.
    /// </summary>
    /// <param name="clock">The virtual clock.</param>
    /// <param name="ticks">The number of ticks to advance.</param>
    public static void AdvanceBy(this VirtualClock clock, long ticks) =>
        clock.AdvanceBy(TimeSpan.FromTicks(ticks));

    /// <summary>
    /// Advances a virtual clock to an absolute tick value.
    /// </summary>
    /// <param name="clock">The virtual clock.</param>
    /// <param name="ticks">The absolute tick value.</param>
    public static void AdvanceTo(this VirtualClock clock, long ticks) =>
        clock.AdvanceTo(new DateTimeOffset(ticks, TimeSpan.Zero));
}
