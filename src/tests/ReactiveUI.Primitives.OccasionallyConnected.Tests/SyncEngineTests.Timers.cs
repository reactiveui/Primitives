// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Timer test doubles for <see cref="SyncEngineTests"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Manual clock with timer support for upload pump tests.</summary>
    /// <param name="timestamp">The starting timestamp.</param>
    private sealed class ManualTimerTimeProvider(DateTimeOffset timestamp) : TimeProvider
    {
        /// <summary>The timestamp frequency used by this provider.</summary>
        private const long Frequency = TimeSpan.TicksPerSecond;

        /// <summary>Protects timestamp and timers.</summary>
        private readonly Lock _gate = new();

        /// <summary>Stores active timers.</summary>
        private readonly List<ManualTimer> _timers = [];

        /// <summary>The current timestamp.</summary>
        private DateTimeOffset _timestamp = timestamp;

        /// <summary>The current monotonic timestamp.</summary>
        private long _monotonicTimestamp;

        /// <summary>Gets active timer count.</summary>
        public int TimerCount
        {
            get
            {
                lock (_gate)
                {
                    return _timers.Count;
                }
            }
        }

        /// <inheritdoc/>
        public override long TimestampFrequency => Frequency;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => _timestamp;

        /// <inheritdoc/>
        public override long GetTimestamp() => _monotonicTimestamp;

        /// <summary>Adjusts wall-clock time without changing the monotonic clock.</summary>
        /// <param name="timestamp">The replacement UTC timestamp.</param>
        public void SetUtcNow(DateTimeOffset timestamp)
        {
            lock (_gate)
            {
                _timestamp = timestamp;
            }
        }

        /// <summary>Checks whether an active timer has the requested remaining due time.</summary>
        /// <param name="dueTime">The expected remaining due time.</param>
        /// <returns>Whether a timer with the expected due time exists.</returns>
        public bool HasTimerDueIn(TimeSpan dueTime)
        {
            lock (_gate)
            {
                var dueUtc = _timestamp.Add(dueTime);
                return _timers.Exists(timer => timer.IsScheduledFor(dueUtc));
            }
        }

        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ManualTimer(this, callback, state, dueTime, period);
            lock (_gate)
            {
                _timers.Add(timer);
            }

            return timer;
        }

        /// <summary>Advances the current timestamp and fires due timers.</summary>
        /// <param name="duration">The duration.</param>
        public void Advance(TimeSpan duration)
        {
            ManualTimer[] due;
            lock (_gate)
            {
                _timestamp = _timestamp.Add(duration);
                _monotonicTimestamp += duration.Ticks;
                due = _timers.Where(static timer => timer.IsDue).ToArray();
            }

            for (var i = 0; i < due.Length; i++)
            {
                due[i].Fire();
            }
        }

        /// <summary>Removes a timer from the clock.</summary>
        /// <param name="timer">The timer.</param>
        private void Remove(ManualTimer timer)
        {
            lock (_gate)
            {
                _ = _timers.Remove(timer);
            }
        }

        /// <summary>Manual timer implementation.</summary>
        private sealed class ManualTimer : ITimer
        {
            /// <summary>The owning clock.</summary>
            private readonly ManualTimerTimeProvider _owner;

            /// <summary>The callback.</summary>
            private readonly TimerCallback _callback;

            /// <summary>The callback state.</summary>
            private readonly object? _state;

            /// <summary>The timer period.</summary>
            private readonly TimeSpan _period;

            /// <summary>The next due timestamp.</summary>
            private DateTimeOffset _dueUtc;

            /// <summary>Tracks whether the timer is disposed.</summary>
            private bool _disposed;

            /// <summary>Initializes a new instance of the <see cref="ManualTimer"/> class.</summary>
            /// <param name="owner">The owning clock.</param>
            /// <param name="callback">The callback.</param>
            /// <param name="state">The callback state.</param>
            /// <param name="dueTime">The due time.</param>
            /// <param name="period">The timer period.</param>
            internal ManualTimer(ManualTimerTimeProvider owner, TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
            {
                _owner = owner;
                _callback = callback;
                _state = state;
                _period = period;
                _dueUtc = CalculateDueUtc(owner.GetUtcNow(), dueTime);
            }

            /// <summary>Gets a value indicating whether the timer is due.</summary>
            internal bool IsDue => !_disposed && _dueUtc <= _owner.GetUtcNow();

            /// <inheritdoc/>
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                _ = period;
                _dueUtc = CalculateDueUtc(_owner.GetUtcNow(), dueTime);
                return true;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                _disposed = true;
                _owner.Remove(this);
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync()
            {
                Dispose();
                return default;
            }

            /// <summary>Checks whether this timer is scheduled for the expected UTC timestamp.</summary>
            /// <param name="dueUtc">The expected UTC timestamp.</param>
            /// <returns>Whether the timer matches the expected timestamp.</returns>
            internal bool IsScheduledFor(DateTimeOffset dueUtc) => !_disposed && _dueUtc == dueUtc;

            /// <summary>Fires the timer callback.</summary>
            internal void Fire()
            {
                if (_disposed)
                {
                    return;
                }

                if (_period == Timeout.InfiniteTimeSpan)
                {
                    Dispose();
                }

                _callback(_state);
            }

            /// <summary>Calculates the next due time, preserving disabled timers.</summary>
            /// <param name="now">The current time.</param>
            /// <param name="dueTime">The requested due time.</param>
            /// <returns>The absolute due time.</returns>
            private static DateTimeOffset CalculateDueUtc(DateTimeOffset now, TimeSpan dueTime) =>
                dueTime == Timeout.InfiniteTimeSpan ? DateTimeOffset.MaxValue : now.Add(dueTime);
        }
    }
}
