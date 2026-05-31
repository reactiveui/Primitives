// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>
/// Periodic scheduling helpers used by migrated extension operators.
/// </summary>
internal static class SequencerPeriodicMixins
{
    /// <summary>
    /// Schedules <paramref name="action"/> repeatedly, starting after <paramref name="period"/>.
    /// </summary>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <param name="scheduler">The scheduler used for each tick.</param>
    /// <param name="state">The state passed to each tick.</param>
    /// <param name="period">The period between ticks.</param>
    /// <param name="action">The tick action.</param>
    /// <returns>A disposable that cancels future ticks.</returns>
    public static IDisposable SchedulePeriodic<TState>(
        this ISequencer scheduler,
        TState state,
        TimeSpan period,
        Action<TState> action) =>
        SchedulePeriodic(scheduler, state, period, period, action);

    /// <summary>
    /// Schedules <paramref name="action"/> repeatedly with an explicit first due time.
    /// </summary>
    /// <param name="scheduler">The scheduler used for each tick.</param>
    /// <param name="dueTime">The delay before the first tick.</param>
    /// <param name="period">The period between ticks.</param>
    /// <param name="action">The tick action.</param>
    /// <returns>A disposable that cancels future ticks.</returns>
    public static IDisposable SchedulePeriodic(
        this ISequencer scheduler,
        TimeSpan dueTime,
        TimeSpan period,
        Action action) =>
        SchedulePeriodic(scheduler, action, dueTime, period, static tick => tick());

    /// <summary>
    /// Schedules a stateful periodic action.
    /// </summary>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <param name="scheduler">The scheduler used for each tick.</param>
    /// <param name="state">The state passed to each tick.</param>
    /// <param name="dueTime">The delay before the first tick.</param>
    /// <param name="period">The period between ticks.</param>
    /// <param name="action">The tick action.</param>
    /// <returns>A disposable that cancels future ticks.</returns>
    private static PeriodicSubscription<TState> SchedulePeriodic<TState>(
        ISequencer scheduler,
        TState state,
        TimeSpan dueTime,
        TimeSpan period,
        Action<TState> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(action);

        var subscription = new PeriodicSubscription<TState>(scheduler, state, Sequencer.Normalize(period), action);
        subscription.ScheduleNext(Sequencer.Normalize(dueTime));
        return subscription;
    }

    /// <summary>
    /// Disposable state for one periodic schedule. Internal (rather than private) so coverage tests can
    /// drive <see cref="Tick"/> directly instead of via reflection.
    /// </summary>
    /// <typeparam name="TState">The state type.</typeparam>
    internal sealed class PeriodicSubscription<TState> : IDisposable
    {
        /// <summary>The scheduler used for each tick.</summary>
        private readonly ISequencer _scheduler;

        /// <summary>The state passed to each tick.</summary>
        private readonly TState _state;

        /// <summary>The period between ticks.</summary>
        private readonly TimeSpan _period;

        /// <summary>The tick action.</summary>
        private readonly Action<TState> _action;

        /// <summary>The current scheduled work.</summary>
        private readonly SwapDisposable _scheduled = new();

        /// <summary>0 = active, 1 = disposed.</summary>
        private int _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="PeriodicSubscription{TState}"/> class.
        /// </summary>
        /// <param name="scheduler">The scheduler used for each tick.</param>
        /// <param name="state">The state passed to each tick.</param>
        /// <param name="period">The period between ticks.</param>
        /// <param name="action">The tick action.</param>
        public PeriodicSubscription(ISequencer scheduler, TState state, TimeSpan period, Action<TState> action)
        {
            _scheduler = scheduler;
            _state = state;
            _period = period;
            _action = action;
        }

        /// <summary>
        /// Schedules the next tick.
        /// </summary>
        /// <param name="dueTime">The delay before the tick.</param>
        public void ScheduleNext(TimeSpan dueTime)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                return;
            }

            _scheduled.Disposable = _scheduler.Schedule(this, dueTime, static (_, subscription) =>
            {
                subscription.Tick();
                return EmptyDisposable.Instance;
            });
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _scheduled.Dispose();
        }

        /// <summary>
        /// Runs a tick and schedules the next one when still active.
        /// </summary>
        internal void Tick()
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                return;
            }

            _action(_state);
            ScheduleNext(_period);
        }
    }
}
