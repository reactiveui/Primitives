// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Concurrency;
#else
namespace ReactiveUI.Primitives.Concurrency;
#endif

/// <summary>Creates materialized <see cref="ScheduledItem{TAbsolute}"/> work items from a sequencer, state, and action.</summary>
public static class ScheduledItem
{
    /// <summary>Creates a materialized work item that invokes <paramref name="action"/> with the supplied sequencer and state.</summary>
    /// <typeparam name="TAbsolute">Absolute time representation type.</typeparam>
    /// <typeparam name="TValue">Type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Recursive scheduler to invoke the scheduled action with.</param>
    /// <param name="state">State to pass to the scheduled action.</param>
    /// <param name="action">Scheduled action.</param>
    /// <param name="dueTime">Time at which to run the scheduled action.</param>
    /// <param name="comparer">Comparer used to compare work items based on their scheduled time.</param>
    /// <returns>The materialized scheduled work item.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/>, <paramref name="action"/>, or <paramref name="comparer"/> is <c>null</c>.</exception>
    public static ScheduledItem<TAbsolute> Create<TAbsolute, TValue>(
        ISequencer scheduler,
        TValue state,
        Func<ISequencer, TValue, IDisposable> action,
        TAbsolute dueTime,
        IComparer<TAbsolute> comparer)
        where TAbsolute : IComparable<TAbsolute>
    {
        _ = scheduler ?? throw new ArgumentNullException(nameof(scheduler));
        _ = action ?? throw new ArgumentNullException(nameof(action));

        return new(dueTime, comparer, _ => action(scheduler, state));
    }

    /// <summary>Creates a materialized work item ordered by the default comparer for <typeparamref name="TAbsolute"/>.</summary>
    /// <typeparam name="TAbsolute">Absolute time representation type.</typeparam>
    /// <typeparam name="TValue">Type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Recursive scheduler to invoke the scheduled action with.</param>
    /// <param name="state">State to pass to the scheduled action.</param>
    /// <param name="action">Scheduled action.</param>
    /// <param name="dueTime">Time at which to run the scheduled action.</param>
    /// <returns>The materialized scheduled work item.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
    public static ScheduledItem<TAbsolute> Create<TAbsolute, TValue>(
        ISequencer scheduler,
        TValue state,
        Func<ISequencer, TValue, IDisposable> action,
        TAbsolute dueTime)
        where TAbsolute : IComparable<TAbsolute> =>
        Create(scheduler, state, action, dueTime, Comparer<TAbsolute>.Default);
}
