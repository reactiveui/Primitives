// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Time provider whose timers fire only when the caller steps the clock, so async timer pipelines run without real waits.</summary>
[DebuggerDisplay("SteppedTimeProvider: Armed = {_armed.Count}")]
internal sealed class SteppedTimeProvider : TimeProvider
{
    /// <summary>The timers currently waiting for a step.</summary>
    private readonly ConcurrentDictionary<SteppedTimer, byte> _armed = new();

    /// <summary>Completes when a timer is armed after the most recent step.</summary>
    private TaskCompletionSource _armedSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <inheritdoc/>
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        SteppedTimer timer = new(this, callback, state);
        _ = timer.Change(dueTime, period);
        return timer;
    }

    /// <summary>Returns a task that completes once at least one timer is waiting for a step.</summary>
    /// <returns>A task that completes when a timer is armed.</returns>
    internal Task WhenTimerArmedAsync()
    {
        var signal = Volatile.Read(ref _armedSignal);
        return _armed.IsEmpty ? signal.Task : Task.CompletedTask;
    }

    /// <summary>Fires every armed timer once; one-shot timers are disarmed, periodic timers stay armed.</summary>
    /// <returns>The number of timers fired.</returns>
    internal int Step()
    {
        List<SteppedTimer> due = [with(capacity: _armed.Count)];
        foreach (var entry in _armed)
        {
            due.Add(entry.Key);
            if (entry.Key.IsOneShot)
            {
                _ = _armed.TryRemove(entry.Key, out _);
            }
        }

        _ = Interlocked.Exchange(ref _armedSignal, new(TaskCreationOptions.RunContinuationsAsynchronously));

        foreach (var timer in due)
        {
            timer.Fire();
        }

        return due.Count;
    }

    /// <summary>Arms a timer and wakes anyone waiting for one.</summary>
    /// <param name="timer">The timer to arm.</param>
    private void Arm(SteppedTimer timer)
    {
        _armed[timer] = 0;
        _ = Volatile.Read(ref _armedSignal).TrySetResult();
    }

    /// <summary>Disarms a timer.</summary>
    /// <param name="timer">The timer to disarm.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Disarm(SteppedTimer timer) => _ = _armed.TryRemove(timer, out _);

    /// <summary>A timer owned by a <see cref="SteppedTimeProvider"/>.</summary>
    /// <param name="owner">The provider that fires the timer.</param>
    /// <param name="callback">The callback invoked on each firing.</param>
    /// <param name="state">The state passed to <paramref name="callback"/>.</param>
    [DebuggerDisplay("SteppedTimer: OneShot = {IsOneShot}")]
    private sealed class SteppedTimer(SteppedTimeProvider owner, TimerCallback callback, object? state) : ITimer
    {
        /// <summary>Gets a value indicating whether the timer disarms after firing.</summary>
        internal bool IsOneShot { get; private set; } = true;

        /// <inheritdoc/>
        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            IsOneShot = period == Timeout.InfiniteTimeSpan || period == TimeSpan.Zero;
            if (dueTime == Timeout.InfiniteTimeSpan)
            {
                owner.Disarm(this);
                return true;
            }

            owner.Arm(this);
            return true;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => owner.Disarm(this);

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            owner.Disarm(this);
            return default;
        }

        /// <summary>Invokes the timer callback.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Fire() => callback(state);
    }
}
