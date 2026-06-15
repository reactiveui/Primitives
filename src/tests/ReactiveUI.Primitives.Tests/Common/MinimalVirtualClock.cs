// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Creates a minimal tick-based <see cref="VirtualTimeSequencer{TAbsolute, TRelative}"/> used to exercise scheduling edge branches.</summary>
internal static class MinimalVirtualClock
{
    /// <summary>Adds a tick offset to an absolute tick value.</summary>
    private static readonly Func<long, long, long> Adder = static (absolute, relative) => absolute + relative;

    /// <summary>Converts an absolute tick value to a <see cref="DateTimeOffset"/>.</summary>
    private static readonly Func<long, DateTimeOffset> ToDateTimeOffset = static absolute => DateTimeOffset.UnixEpoch.AddTicks(absolute);

    /// <summary>Converts a <see cref="TimeSpan"/> to a tick count.</summary>
    private static readonly Func<TimeSpan, long> ToRelative = static timeSpan => timeSpan.Ticks;

    /// <summary>Creates a tick-based virtual sequencer ordered by the default comparer.</summary>
    /// <returns>The virtual sequencer.</returns>
    public static VirtualTimeSequencer<long, long> Create() => Create(Comparer<long>.Default);

    /// <summary>Creates a tick-based virtual sequencer ordered by the supplied comparer.</summary>
    /// <param name="comparer">The comparer used to order scheduled times.</param>
    /// <returns>The virtual sequencer.</returns>
    public static VirtualTimeSequencer<long, long> Create(IComparer<long> comparer) =>
        new(0L, comparer, Adder, ToDateTimeOffset, ToRelative);
}
