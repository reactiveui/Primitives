// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if !NET8_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Provides built-in sequencers for scheduling work over time.</summary>
public static partial class Sequencer
{
    /// <summary>Gets a sequencer that schedules work as soon as possible on the current thread.</summary>
    public static CurrentThreadSequencer CurrentThread => CurrentThreadSequencer.Instance;

    /// <summary>Gets a sequencer that schedules work immediately on the current thread.</summary>
    public static ImmediateSequencer Immediate => ImmediateSequencer.Instance;

    /// <summary>Gets the default queueing sequencer for background work.</summary>
    public static ISequencer Default => TaskPoolSequencer.Default;

#if NET8_0_OR_GREATER
    /// <summary>Gets the shared wall-clock time used by real-time sequencers.</summary>
    internal static DateTimeOffset Now => TimeProvider.System.GetUtcNow();
#else
    /// <summary>Gets the shared wall-clock time used by real-time sequencers.</summary>
    [SuppressMessage(
        "Major Code Smell",
        "S6354:Use a testable date/time provider",
        Justification = "Not available all platforms")]
    internal static DateTimeOffset Now => DateTimeOffset.UtcNow;
#endif

    /// <summary>Gets the current monotonic timestamp used by real-time sequencers.</summary>
    internal static long Timestamp => System.Diagnostics.Stopwatch.GetTimestamp();

    /// <summary>Normalizes the specified <see cref="TimeSpan"/> value to a positive value.</summary>
    /// <param name="timeSpan">The <see cref="TimeSpan"/> value to normalize.</param>
    /// <returns>The specified TimeSpan value if it is zero or positive; otherwise, <see cref="TimeSpan.Zero"/>.</returns>
    public static TimeSpan Normalize(TimeSpan timeSpan) => timeSpan.Ticks < 0 ? TimeSpan.Zero : timeSpan;

    /// <summary>Adds a relative duration to an absolute monotonic timestamp.</summary>
    /// <param name="timestamp">Absolute monotonic timestamp.</param>
    /// <param name="dueTime">Relative duration.</param>
    /// <returns>The absolute timestamp after the duration.</returns>
    internal static long AddTimestamp(long timestamp, TimeSpan dueTime)
    {
        var delta = ToTimestampDelta(dueTime);
        if (delta == 0)
        {
            return timestamp;
        }

        return long.MaxValue - timestamp < delta ? long.MaxValue : timestamp + delta;
    }

    /// <summary>Calculates the remaining wall time until a monotonic timestamp.</summary>
    /// <param name="dueTimestamp">Absolute monotonic timestamp.</param>
    /// <returns>The remaining time until <paramref name="dueTimestamp"/>.</returns>
    internal static TimeSpan TimeUntil(long dueTimestamp)
    {
        var delta = dueTimestamp - Timestamp;
        return delta <= 0
            ? TimeSpan.Zero
            : TimeSpan.FromSeconds(delta / (double)System.Diagnostics.Stopwatch.Frequency);
    }

    /// <summary>Converts a monotonic timestamp delta to a relative duration.</summary>
    /// <param name="timestampDelta">Monotonic timestamp delta.</param>
    /// <returns>The duration represented by <paramref name="timestampDelta"/>.</returns>
    internal static TimeSpan ToTimeSpanDelta(long timestampDelta)
    {
        if (timestampDelta <= 0)
        {
            return TimeSpan.Zero;
        }

        var ticks = timestampDelta * (double)TimeSpan.TicksPerSecond / System.Diagnostics.Stopwatch.Frequency;
        return ticks >= TimeSpan.MaxValue.Ticks
            ? TimeSpan.MaxValue
            : TimeSpan.FromTicks(Math.Max(1, (long)Math.Ceiling(ticks)));
    }

    /// <summary>Converts a relative duration to monotonic timestamp ticks.</summary>
    /// <param name="dueTime">Relative duration.</param>
    /// <returns>Timestamp ticks representing <paramref name="dueTime"/>.</returns>
    internal static long ToTimestampDelta(TimeSpan dueTime)
    {
        var normalized = Normalize(dueTime);
        if (normalized == TimeSpan.Zero)
        {
            return 0;
        }

        var ticks = normalized.TotalSeconds * System.Diagnostics.Stopwatch.Frequency;
        return ticks >= long.MaxValue ? long.MaxValue : Math.Max(1, (long)Math.Ceiling(ticks));
    }
}
