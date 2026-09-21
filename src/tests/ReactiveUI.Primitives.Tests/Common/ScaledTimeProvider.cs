// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>A time provider over a <see cref="VirtualClock"/> that counts timestamps at a chosen frequency and reports the timers it creates.</summary>
internal sealed class ScaledTimeProvider : TimeProvider
{
    /// <summary>The virtual clock supplying time and timers.</summary>
    private readonly VirtualClock _clock;

    /// <summary>The number of virtual clock ticks in one timestamp tick.</summary>
    private readonly long _ticksPerTimestamp;

    /// <summary>Completes when the first timer is created.</summary>
    private readonly TaskCompletionSource _timerCreated = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Records the due time of each timer created.</summary>
    private readonly ConcurrentQueue<TimeSpan> _timerDueTimes = new();

    /// <summary>Initializes a new instance of the <see cref="ScaledTimeProvider"/> class.</summary>
    /// <param name="clock">The virtual clock supplying time and timers.</param>
    /// <param name="frequency">The timestamp ticks per second, a divisor of <see cref="TimeSpan.TicksPerSecond"/>.</param>
    internal ScaledTimeProvider(VirtualClock clock, long frequency)
    {
        _clock = clock;
        TimestampFrequency = frequency;
        _ticksPerTimestamp = TimeSpan.TicksPerSecond / frequency;
    }

    /// <inheritdoc/>
    public override long TimestampFrequency { get; }

    /// <summary>Gets a task that completes when the first timer is created.</summary>
    internal Task TimerCreated => _timerCreated.Task;

    /// <summary>Gets the due times of the timers created so far, in creation order.</summary>
    internal TimeSpan[] TimerDueTimes => [.. _timerDueTimes];

    /// <inheritdoc/>
    public override DateTimeOffset GetUtcNow() => _clock.Now;

    /// <inheritdoc/>
    public override long GetTimestamp() => _clock.Now.UtcTicks / _ticksPerTimestamp;

    /// <inheritdoc/>
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = _clock.CreateTimer(callback, state, dueTime, period);
        _timerDueTimes.Enqueue(dueTime);
        _ = _timerCreated.TrySetResult();
        return timer;
    }
}
