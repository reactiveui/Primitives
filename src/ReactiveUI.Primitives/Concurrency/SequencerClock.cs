// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Reads time, blocks and creates timers through a <see cref="TimeProvider"/> while reporting timestamps at <see cref="Stopwatch.Frequency"/>.</summary>
internal sealed class SequencerClock
{
    /// <summary>Timer used by a blocking wait to release its waiter.</summary>
    private static readonly TimerCallback ReleaseWaiter = static state => ((ManualResetEventSlim)state!).Set();

    /// <summary>The provider supplying the current time, timestamps and timers.</summary>
    private readonly TimeProvider _provider;

    /// <summary>The provider's timestamp ticks per second.</summary>
    private readonly long _frequency;

    /// <summary>The provider timestamp treated as zero when rescaling; zero when the provider already counts at <see cref="Stopwatch.Frequency"/>.</summary>
    private readonly long _origin;

    /// <summary>Whether the provider is <see cref="TimeProvider.System"/>, which waits on the calling thread.</summary>
    private readonly bool _isSystem;

    /// <summary>Initializes a new instance of the <see cref="SequencerClock"/> class.</summary>
    /// <param name="timeProvider">The provider supplying time, timestamps and timers.</param>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider"/> is <see langword="null"/>.</exception>
    internal SequencerClock(TimeProvider timeProvider)
    {
        ArgumentExceptionHelper.ThrowIfNull(timeProvider);

        _provider = timeProvider;
        _frequency = timeProvider.TimestampFrequency;
        _origin = _frequency == Stopwatch.Frequency ? 0 : timeProvider.GetTimestamp();
        _isSystem = ReferenceEquals(timeProvider, TimeProvider.System);
    }

    /// <summary>Gets the clock backed by <see cref="TimeProvider.System"/>.</summary>
    internal static SequencerClock Default { get; } = new(TimeProvider.System);

    /// <summary>Gets the current UTC time.</summary>
    /// <returns>The provider's current UTC time.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal DateTimeOffset GetUtcNow() => _provider.GetUtcNow();

    /// <summary>Gets the monotonic timestamp in <see cref="Stopwatch.Frequency"/> ticks per second.</summary>
    /// <returns>The provider's timestamp rescaled to <see cref="Stopwatch.Frequency"/> units.</returns>
    internal long GetTimestamp()
    {
        var timestamp = _provider.GetTimestamp();
        return _frequency == Stopwatch.Frequency ? timestamp : Rescale(timestamp - _origin, _frequency);
    }

    /// <summary>Calculates the remaining time until a monotonic timestamp.</summary>
    /// <param name="dueTimestamp">Absolute timestamp in <see cref="Stopwatch.Frequency"/> units.</param>
    /// <returns>The nonnegative remaining time.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal TimeSpan TimeUntil(long dueTimestamp) => Sequencer.TimeUntil(dueTimestamp, GetTimestamp());

    /// <summary>Blocks the calling thread for a delay measured by the provider.</summary>
    /// <param name="dueTime">The delay.</param>
    internal void Wait(TimeSpan dueTime)
    {
        if (_isSystem)
        {
            SleepOnSystemClock(dueTime);
            return;
        }

        using ManualResetEventSlim released = new(false);
        using var timer = _provider.CreateTimer(ReleaseWaiter, released, dueTime, Timeout.InfiniteTimeSpan);
        released.Wait();
    }

    /// <summary>Creates a timer through the provider.</summary>
    /// <param name="callback">The callback invoked when the timer fires.</param>
    /// <param name="state">The callback state.</param>
    /// <param name="dueTime">The delay before the first firing.</param>
    /// <param name="period">The interval between later firings.</param>
    /// <returns>The timer.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
        _provider.CreateTimer(callback, state, dueTime, period);

    /// <summary>Converts a timestamp delta counted at <paramref name="frequency"/> ticks per second to <see cref="Stopwatch.Frequency"/> units.</summary>
    /// <param name="delta">The timestamp delta.</param>
    /// <param name="frequency">Ticks per second of <paramref name="delta"/>.</param>
    /// <returns>The delta in <see cref="Stopwatch.Frequency"/> units.</returns>
    private static long Rescale(long delta, long frequency) =>
        (delta / frequency * Stopwatch.Frequency) + (long)((decimal)(delta % frequency) * Stopwatch.Frequency / frequency);

    /// <summary>Blocks the calling thread on the system clock.</summary>
    /// <param name="dueTime">The delay.</param>
    [ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SleepOnSystemClock(TimeSpan dueTime) => Thread.Sleep(dueTime);
}
