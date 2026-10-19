// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Extensions.Time.Testing;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Recovered outbox scheduling clock fixtures for <see cref="OccasionallyConnectedBuilder"/>.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>Fake clock wrapper with observable dwell timer registration.</summary>
    /// <param name="timestamp">The initial UTC timestamp.</param>
    private sealed class RecoveredUploadTimeProvider(DateTimeOffset timestamp) : TimeProvider
    {
        /// <summary>Stores the delegated fake clock.</summary>
        private readonly FakeTimeProvider _inner = new(timestamp);

        /// <summary>Protects upload wake observations.</summary>
        private readonly Lock _gate = new();

        /// <summary>Stores positive one-shot upload wake delays.</summary>
        private readonly List<TimeSpan> _uploadWakeDelays = [];

        /// <summary>Stores the next waiter for an upload wake delay.</summary>
        private TaskCompletionSource<TimeSpan>? _uploadWakeWaiter;

        /// <summary>Gets the dwell timer registration signal.</summary>
        public TaskCompletionSource DwellTimerRegistered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the first positive one-shot upload wake timer delay.</summary>
        public TaskCompletionSource<TimeSpan> UploadWakeTimerRegistered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the number of positive one-shot upload wake timer registrations.</summary>
        public int UploadWakeTimerCount
        {
            get
            {
                lock (_gate)
                {
                    return _uploadWakeDelays.Count;
                }
            }
        }

        /// <inheritdoc />
        public override long TimestampFrequency => _inner.TimestampFrequency;

        /// <inheritdoc />
        public override DateTimeOffset GetUtcNow() => _inner.GetUtcNow();

        /// <inheritdoc />
        public override long GetTimestamp() => _inner.GetTimestamp();

        /// <inheritdoc />
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = _inner.CreateTimer(callback, state, dueTime, period);
            if (dueTime > TimeSpan.Zero && period == Timeout.InfiniteTimeSpan)
            {
                RecordUploadWakeTimer(dueTime);
            }

            return timer;
        }

        /// <summary>Waits for an upload wake registration after an observed count.</summary>
        /// <param name="observedCount">The number of upload wake registrations already observed.</param>
        /// <returns>The next upload wake delay.</returns>
        public async Task<TimeSpan> WaitForUploadWakeAfterAsync(int observedCount)
        {
            Task<TimeSpan> wakeTask;
            lock (_gate)
            {
                if (_uploadWakeDelays.Count > observedCount)
                {
                    return _uploadWakeDelays[observedCount];
                }

                _uploadWakeWaiter ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
                wakeTask = _uploadWakeWaiter.Task;
            }

            return await wakeTask.ConfigureAwait(false);
        }

        /// <summary>Advances the delegated fake clock.</summary>
        /// <param name="duration">The duration to advance.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Advance(TimeSpan duration) => _inner.Advance(duration);

        /// <summary>Records a positive one-shot upload wake timer registration.</summary>
        /// <param name="dueTime">The timer due time.</param>
        private void RecordUploadWakeTimer(TimeSpan dueTime)
        {
            TaskCompletionSource<TimeSpan>? waiter;
            lock (_gate)
            {
                _uploadWakeDelays.Add(dueTime);
                waiter = _uploadWakeWaiter;
                _uploadWakeWaiter = null;
            }

            _ = UploadWakeTimerRegistered.TrySetResult(dueTime);
            _ = waiter?.TrySetResult(dueTime);
            if (dueTime == OccasionallyConnectedOptions.Default.Batching.MaximumDwellTime)
            {
                _ = DwellTimerRegistered.TrySetResult();
            }
        }
    }
}
