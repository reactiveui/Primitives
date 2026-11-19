// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Non-generic factory methods for <see cref="PeriodicSubscription{TState}"/>.</summary>
public static class PeriodicSubscription
{
    /// <summary>Schedules a stateless <paramref name="action"/> repeatedly with an explicit initial <paramref name="dueTime"/>.</summary>
    /// <param name="scheduler">The scheduler used for each tick.</param>
    /// <param name="dueTime">The delay before the first tick.</param>
    /// <param name="period">The period between ticks.</param>
    /// <param name="action">The tick action.</param>
    /// <returns>A disposable subscription that cancels future ticks.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static PeriodicSubscription<Action> ScheduleAction(
        ISequencer scheduler,
        TimeSpan dueTime,
        TimeSpan period,
        Action action) =>
        PeriodicSubscription<Action>.SchedulePeriodic(scheduler, action, dueTime, period, static tick => tick());
}
