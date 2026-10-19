// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Extensions.Time.Testing;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Observable fake time for lease renewal scheduling tests.</content>
public sealed partial class PreparedUploadAttemptCoordinatorTests
{
    /// <summary>Exposes one-shot timer schedules made against fake time.</summary>
    /// <param name="start">The initial clock time.</param>
    private sealed class ObservableRenewalTimeProvider(DateTimeOffset start) : TimeProvider
    {
        /// <summary>The delegated fake clock.</summary>
        private readonly FakeTimeProvider _clock = new(start);

        /// <summary>Protects schedule observations.</summary>
#if NET9_0_OR_GREATER
        private readonly Lock _gate = new();
#else
        private readonly object _gate = new();
#endif

        /// <summary>Signals a changed schedule count.</summary>
        private TaskCompletionSource _scheduleChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The number of completed one-shot schedules.</summary>
        private int _scheduleCount;

        /// <inheritdoc/>
        public override long TimestampFrequency => _clock.TimestampFrequency;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => _clock.GetUtcNow();

        /// <inheritdoc/>
        public override long GetTimestamp() => _clock.GetTimestamp();

        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            new ObservableTimer(_clock.CreateTimer(callback, state, dueTime, period), this);

        /// <summary>Advances fake time after the next renewal timer is armed.</summary>
        /// <param name="duration">The amount to advance.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Advance(TimeSpan duration) => _clock.Advance(duration);

        /// <summary>Waits until the given number of one-shot schedules have completed.</summary>
        /// <param name="expectedCount">The expected number of schedules.</param>
        /// <returns>The wait task.</returns>
        public async Task WaitForScheduleCountAsync(int expectedCount)
        {
            while (true)
            {
                Task changed;
                lock (_gate)
                {
                    if (_scheduleCount >= expectedCount)
                    {
                        return;
                    }

                    changed = _scheduleChanged.Task;
                }

                await changed.ConfigureAwait(false);
            }
        }

        /// <summary>Records one completed timer schedule.</summary>
        private void RecordSchedule()
        {
            TaskCompletionSource changed;
            lock (_gate)
            {
                _scheduleCount++;
                changed = _scheduleChanged;
                _scheduleChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            _ = changed.TrySetResult();
        }

        /// <summary>Records timer changes after the delegated timer is armed.</summary>
        /// <param name="timer">The delegated timer.</param>
        /// <param name="owner">The observable clock.</param>
        private sealed class ObservableTimer(ITimer timer, ObservableRenewalTimeProvider owner) : ITimer
        {
            /// <inheritdoc/>
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                var changed = timer.Change(dueTime, period);
                if (changed && dueTime != Timeout.InfiniteTimeSpan && period == Timeout.InfiniteTimeSpan)
                {
                    owner.RecordSchedule();
                }

                return changed;
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose() => timer.Dispose();

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => timer.DisposeAsync();
        }
    }
}
