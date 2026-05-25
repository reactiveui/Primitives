// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Sequencer.
/// </summary>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public static partial class Sequencer
{
    /// <summary>
    /// Schedules an action to be executed.
    /// </summary>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
    public static IDisposable Schedule(this ISequencer scheduler, Action action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return scheduler.Schedule(action, static (_, a) => Invoke(a));
    }

    /// <summary>
    /// Schedules an action to be executed after the specified relative due time.
    /// </summary>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    /// <exception cref="System.ArgumentNullException">
    /// scheduler
    /// or
    /// action.
    /// </exception>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
    public static IDisposable Schedule(this ISequencer scheduler, TimeSpan dueTime, Action action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return scheduler.Schedule(action, dueTime, static (_, a) => Invoke(a));
    }

    /// <summary>
    /// Schedules an action to be executed at the specified absolute due time.
    /// </summary>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    /// <exception cref="System.ArgumentNullException">
    /// scheduler
    /// or
    /// action.
    /// </exception>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
    public static IDisposable Schedule(this ISequencer scheduler, DateTimeOffset dueTime, Action action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return scheduler.Schedule(action, dueTime, static (_, a) => Invoke(a));
    }

    /// <summary>
    /// Schedules the specified action.
    /// </summary>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="action">The action.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    public static IDisposable Schedule(this ISequencer scheduler, Action<Action> action)
    {
        // InvokeRec1
        var group = new MultipleDisposable();
        var gate = new object();

#pragma warning disable IDE0039 // Use local function
        Action? recursiveAction = null;
#pragma warning restore IDE0039 // Use local function
        recursiveAction = () => action(() =>
        {
            var isAdded = false;
            var isDone = false;
            var d = default(IDisposable);
            d = scheduler.Schedule(() =>
            {
                lock (gate)
                {
                    if (isAdded)
                    {
                        group.Remove(d);
                    }
                    else
                    {
                        isDone = true;
                    }
                }

                recursiveAction!();
            });

            lock (gate)
            {
                if (!isDone)
                {
                    group.Add(d);
                    isAdded = true;
                }
            }
        });

        group.Add(scheduler.Schedule(recursiveAction));

        return group;
    }

    /// <summary>
    /// Schedules an action to be executed.
    /// </summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    /// <exception cref="System.ArgumentNullException">
    /// scheduler
    /// or
    /// action.
    /// </exception>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
    // Note: The naming of that method differs because otherwise, the signature would cause ambiguities.
    public static IDisposable ScheduleAction<TState>(this ISequencer scheduler, TState state, Action<TState> action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return scheduler.Schedule(
            (action, state),
            (_, tuple) =>
            {
                tuple.action(tuple.state);
                return Disposable.Empty;
            });
    }

    /// <summary>
    /// Schedules an action to be executed.
    /// </summary>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">A state object to be passed to <paramref name="action"/>.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
    // Note: The naming of that method differs because otherwise, the signature would cause ambiguities.
    internal static IDisposable ScheduleAction<TState>(this ISequencer scheduler, TState state, Func<TState, IDisposable> action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return scheduler.Schedule(
            (action, state),
            static (_, tuple) => tuple.action(tuple.state));
    }

    /// <summary>
    /// Schedules an action to be executed after the specified relative due time.
    /// </summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    /// <exception cref="System.ArgumentNullException">
    /// scheduler
    /// or
    /// action.
    /// </exception>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
    internal static IDisposable ScheduleAction<TState>(this ISequencer scheduler, TState state, TimeSpan dueTime, Action<TState> action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return scheduler.Schedule((state, action), dueTime, static (_, tuple) => Invoke(tuple));
    }

    /// <summary>
    /// Schedules an action to be executed after the specified relative due time.
    /// </summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    /// <exception cref="System.ArgumentNullException">
    /// scheduler
    /// or
    /// action.
    /// </exception>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
    internal static IDisposable ScheduleAction<TState>(this ISequencer scheduler, TState state, TimeSpan dueTime, Func<TState, IDisposable> action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return scheduler.Schedule((state, action), dueTime, static (_, tuple) => Invoke(tuple));
    }

    /// <summary>
    /// Schedules an action to be executed after the specified relative due time.
    /// </summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    /// <exception cref="System.ArgumentNullException">
    /// scheduler
    /// or
    /// action.
    /// </exception>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
    internal static IDisposable ScheduleAction<TState>(this ISequencer scheduler, TState state, DateTimeOffset dueTime, Action<TState> action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return scheduler.Schedule((state, action), dueTime, static (_, tuple) => Invoke(tuple));
    }

    /// <summary>
    /// Schedules an action to be executed after the specified relative due time.
    /// </summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    /// <exception cref="System.ArgumentNullException">
    /// scheduler
    /// or
    /// action.
    /// </exception>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
    internal static IDisposable ScheduleAction<TState>(this ISequencer scheduler, TState state, DateTimeOffset dueTime, Func<TState, IDisposable> action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return scheduler.Schedule((state, action), dueTime, static (_, tuple) => Invoke(tuple));
    }

    /////// <summary>
    /////// Schedules an action to be executed.
    /////// </summary>
    /////// <param name="scheduler">Sequencer to execute the action on.</param>
    /////// <param name="action">Action to execute.</param>
    /////// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /////// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
    ////public static IDisposable ScheduleLongRunning(this ISequencerLongRunning scheduler, Action<ICancelable> action)
    ////{
    ////    if (scheduler == null)
    ////    {
    ////        throw new ArgumentNullException(nameof(scheduler));
    ////    }

    ////    if (action == null)
    ////    {
    ////        throw new ArgumentNullException(nameof(action));
    ////    }

    ////    return scheduler.ScheduleLongRunning(action, static (a, c) => a(c));
    ////}

    private static IDisposable Invoke(Action action)
    {
        action();
        return Disposable.Empty;
    }

    private static IDisposable Invoke<TState>((TState state, Action<TState> action) tuple)
    {
        tuple.action(tuple.state);
        return Disposable.Empty;
    }

    private static IDisposable Invoke<TState>((TState state, Func<TState, IDisposable> action) tuple) =>
        tuple.action(tuple.state);
}
