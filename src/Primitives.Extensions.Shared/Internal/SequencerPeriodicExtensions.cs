// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;

namespace ReactiveUI.Primitives.Extensions.Reactive.Internal;
#else
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Extensions.Internal;
#endif

/// <summary>Periodic scheduling helpers for <see cref="ISequencer"/>.</summary>
internal static class SequencerPeriodicExtensions
{
    /// <summary>Repeating schedule overloads hung off a sequencer instance.</summary>
    /// <param name="scheduler">The scheduler used for each tick.</param>
    extension(ISequencer scheduler)
    {
        /// <summary>Schedules <paramref name="action"/> repeatedly, starting after <paramref name="period"/>.</summary>
        /// <typeparam name="TState">The state type.</typeparam>
        /// <param name="state">The state passed to each tick.</param>
        /// <param name="period">The period between ticks.</param>
        /// <param name="action">The tick action.</param>
        /// <returns>A disposable that cancels future ticks.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IDisposable SchedulePeriodic<TState>(
            TState state,
            TimeSpan period,
            Action<TState> action) =>
            PeriodicSubscription<TState>.SchedulePeriodic(scheduler, state, period, action);

        /// <summary>Schedules <paramref name="action"/> repeatedly with an explicit first due time.</summary>
        /// <param name="dueTime">The delay before the first tick.</param>
        /// <param name="period">The period between ticks.</param>
        /// <param name="action">The tick action.</param>
        /// <returns>A disposable that cancels future ticks.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IDisposable SchedulePeriodic(
            TimeSpan dueTime,
            TimeSpan period,
            Action action) =>
            PeriodicSubscription.ScheduleAction(scheduler, dueTime, period, action);
    }
}
