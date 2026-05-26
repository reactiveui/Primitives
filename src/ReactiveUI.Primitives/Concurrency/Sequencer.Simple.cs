// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Sequencer.
/// </summary>
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
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return new RecursiveScheduleState(scheduler, action).Start();
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
    /// <typeparam name="TState">The type of the state.</typeparam>
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

    /// <summary>
    /// Invokes an action and returns an empty disposable.
    /// </summary>
    /// <param name="action">Action to invoke.</param>
    /// <returns>An empty disposable.</returns>
    private static IDisposable Invoke(Action action)
    {
        action();
        return Disposable.Empty;
    }

    /// <summary>
    /// Invokes a stateful action and returns an empty disposable.
    /// </summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="tuple">Tuple containing the state and action.</param>
    /// <returns>An empty disposable.</returns>
    private static IDisposable Invoke<TState>((TState state, Action<TState> action) tuple)
    {
        tuple.action(tuple.state);
        return Disposable.Empty;
    }

    /// <summary>
    /// Invokes a stateful disposable-returning action.
    /// </summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="tuple">Tuple containing the state and action.</param>
    /// <returns>The disposable returned by the action.</returns>
    private static IDisposable Invoke<TState>((TState state, Func<TState, IDisposable> action) tuple) =>
        tuple.action(tuple.state);

    /// <summary>
    /// Holds state for recursive action scheduling.
    /// </summary>
    private sealed class RecursiveScheduleState : MultipleDisposable
    {
        /// <summary>
        /// Sequencer used for recursive scheduling.
        /// </summary>
        private readonly ISequencer _scheduler;

        /// <summary>
        /// Recursive action supplied by the caller.
        /// </summary>
        private readonly Action<Action> _action;

        /// <summary>
        /// Guards handoff between scheduling and execution.
        /// </summary>
        private readonly object _gate = new();

        /// <summary>
        /// Cached delegate used to avoid recreating the recursive action.
        /// </summary>
        private readonly Action _recursiveAction;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecursiveScheduleState"/> class.
        /// </summary>
        /// <param name="scheduler">Sequencer used for recursive scheduling.</param>
        /// <param name="action">Recursive action supplied by the caller.</param>
        public RecursiveScheduleState(ISequencer scheduler, Action<Action> action)
        {
            _scheduler = scheduler;
            _action = action;
            _recursiveAction = RunRecursiveAction;
        }

        /// <summary>
        /// Starts recursive scheduling.
        /// </summary>
        /// <returns>The disposable object used to cancel recursive work.</returns>
        public RecursiveScheduleState Start()
        {
            Add(_scheduler.Schedule(_recursiveAction));
            return this;
        }

        /// <summary>
        /// Invokes the caller-provided recursive action.
        /// </summary>
        private void RunRecursiveAction() => _action(Reschedule);

        /// <summary>
        /// Schedules the next recursive action invocation.
        /// </summary>
        private void Reschedule()
        {
            var isAdded = false;
            var isDone = false;
            IDisposable? disposable = null;
            disposable = _scheduler.Schedule(() =>
            {
                lock (_gate)
                {
                    if (isAdded)
                    {
                        Remove(disposable!);
                    }
                    else
                    {
                        isDone = true;
                    }
                }

                RunRecursiveAction();
            });

            lock (_gate)
            {
                if (!isDone)
                {
                    Add(disposable);
                    isAdded = true;
                }
            }
        }
    }
}
