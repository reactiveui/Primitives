// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Compatibility helpers for tests migrated from Microsoft.Reactive.Testing.</summary>
internal static class VirtualClockTestExtensions
{
    /// <summary>Tick-based advancement helpers for a virtual clock.</summary>
    /// <param name="clock">The virtual clock.</param>
    extension(VirtualClock clock)
    {
        /// <summary>Advances a virtual clock by ticks.</summary>
        /// <param name="ticks">The number of ticks to advance.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void AdvanceBy(long ticks) =>
            clock.AdvanceBy(TimeSpan.FromTicks(ticks));

        /// <summary>Advances a virtual clock to an absolute tick value.</summary>
        /// <param name="ticks">The absolute tick value.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void AdvanceTo(long ticks) =>
            clock.AdvanceTo(new(ticks, TimeSpan.Zero));
    }
}
