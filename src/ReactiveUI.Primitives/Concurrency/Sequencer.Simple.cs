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
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
    public static IDisposable Schedule<TState>(this ISequencer scheduler, TState state, Func<ISequencer, TState, IDisposable> action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var item = new DelegateWorkItem<TState>(scheduler, state, action);
        scheduler.Schedule(item);
        return item;
    }

    /// <summary>
    /// Schedules a stateful action to be executed without capturing state in a closure.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
    public static IDisposable Schedule<TState>(this ISequencer scheduler, TState state, Action<TState> action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var item = new ActionWorkItem<TState>(state, action);
        scheduler.Schedule(item);
        return item;
    }

    /// <summary>
    /// Schedules an action to be executed after a relative due time.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
    public static IDisposable Schedule<TState>(this ISequencer scheduler, TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var normalized = Normalize(dueTime);
        var item = new DelegateWorkItem<TState>(scheduler, state, action);
        if (normalized == TimeSpan.Zero)
        {
            scheduler.Schedule(item);
        }
        else
        {
            scheduler.Schedule(item, AddTimestamp(scheduler.Timestamp, normalized));
        }

        return item;
    }

    /// <summary>
    /// Schedules a stateful action to be executed after a relative due time without capturing state in a closure.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
    public static IDisposable Schedule<TState>(this ISequencer scheduler, TState state, TimeSpan dueTime, Action<TState> action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return scheduler.Schedule(state, AddTimestamp(scheduler.Timestamp, Normalize(dueTime)), action);
    }

    /// <summary>
    /// Schedules a stateful action to be executed at a monotonic timestamp without capturing state in a closure.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
    public static IDisposable Schedule<TState>(this ISequencer scheduler, TState state, long dueTimestamp, Action<TState> action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var item = new ActionWorkItem<TState>(state, action);
        scheduler.Schedule(item, dueTimestamp);
        return item;
    }

    /// <summary>
    /// Schedules an action to be executed at an absolute wall-clock due time.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
    public static IDisposable Schedule<TState>(this ISequencer scheduler, TState state, DateTimeOffset dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        if (scheduler == null)
        {
            throw new ArgumentNullException(nameof(scheduler));
        }

        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return scheduler.Schedule(state, Normalize(dueTime - scheduler.Now), action);
    }

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
    /// <exception cref="ArgumentNullException">
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
    /// <exception cref="ArgumentNullException">
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
    /// <exception cref="ArgumentNullException">
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
                return EmptyDisposable.Instance;
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
    /// <exception cref="ArgumentNullException">
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
    /// <exception cref="ArgumentNullException">
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
    /// <exception cref="ArgumentNullException">
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
    /// <exception cref="ArgumentNullException">
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
    /// Determines whether a scheduled work item has been cancelled.
    /// </summary>
    /// <param name="item">Work item to inspect.</param>
    /// <returns><see langword="true"/> when the work item has been disposed.</returns>
    internal static bool IsCancelled(IWorkItem item) => item is IsDisposed disposable && disposable.IsDisposed;

    /// <summary>
    /// Invokes an action and returns an empty disposable.
    /// </summary>
    /// <param name="action">Action to invoke.</param>
    /// <returns>An empty disposable.</returns>
    private static EmptyDisposable Invoke(Action action)
    {
        action();
        return EmptyDisposable.Instance;
    }

    /// <summary>
    /// Invokes a stateful action and returns an empty disposable.
    /// </summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="tuple">Tuple containing the state and action.</param>
    /// <returns>An empty disposable.</returns>
    private static EmptyDisposable Invoke<TState>((TState state, Action<TState> action) tuple)
    {
        tuple.action(tuple.state);
        return EmptyDisposable.Instance;
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
    /// Disposable work item used by closure-free stateful scheduler overloads.
    /// </summary>
    /// <typeparam name="TState">The scheduled state type.</typeparam>
    private sealed class ActionWorkItem<TState> : IWorkItem, IsDisposed
    {
        /// <summary>
        /// Scheduled state.
        /// </summary>
        private TState _state;

        /// <summary>
        /// Scheduled action.
        /// </summary>
        private Action<TState>? _action;

        /// <summary>
        /// Tracks cancellation.
        /// </summary>
        private int _isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ActionWorkItem{TState}"/> class.
        /// </summary>
        /// <param name="state">Scheduled state.</param>
        /// <param name="action">Scheduled action.</param>
        public ActionWorkItem(TState state, Action<TState> action)
        {
            _state = state;
            _action = action;
        }

        /// <inheritdoc/>
        public bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            {
                return;
            }

            _state = default!;
            Volatile.Write(ref _action, null);
        }

        /// <inheritdoc/>
        public void Execute()
        {
            var action = Volatile.Read(ref _action);
            if (action == null || IsDisposed)
            {
                return;
            }

            action(_state);
        }
    }

    /// <summary>
    /// Disposable work item used by the compatibility delegate scheduler overloads.
    /// </summary>
    /// <typeparam name="TState">The scheduled state type.</typeparam>
    private sealed class DelegateWorkItem<TState> : IWorkItem, IsDisposed
    {
        /// <summary>
        /// The sequencer passed back to the scheduled action.
        /// </summary>
        private readonly ISequencer _scheduler;

        /// <summary>
        /// Scheduled state.
        /// </summary>
        private readonly TState _state;

        /// <summary>
        /// Scheduled action.
        /// </summary>
        private readonly Func<ISequencer, TState, IDisposable> _action;

        /// <summary>
        /// Disposable returned by the scheduled action after it starts.
        /// </summary>
        private IDisposable? _disposable;

        /// <summary>
        /// Tracks cancellation.
        /// </summary>
        private int _isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="DelegateWorkItem{TState}"/> class.
        /// </summary>
        /// <param name="scheduler">The sequencer passed back to the scheduled action.</param>
        /// <param name="state">The scheduled state.</param>
        /// <param name="action">The scheduled action.</param>
        public DelegateWorkItem(ISequencer scheduler, TState state, Func<ISequencer, TState, IDisposable> action)
        {
            _scheduler = scheduler;
            _state = state;
            _action = action;
        }

        /// <inheritdoc/>
        public bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            {
                return;
            }

            Interlocked.Exchange(ref _disposable, EmptyDisposable.Instance)?.Dispose();
        }

        /// <inheritdoc/>
        public void Execute()
        {
            if (IsDisposed)
            {
                return;
            }

            var disposable = _action(_scheduler, _state) ?? EmptyDisposable.Instance;
            var previous = Interlocked.CompareExchange(ref _disposable, disposable, null);
            if (previous != null)
            {
                disposable.Dispose();
                return;
            }

            if (!IsDisposed)
            {
                return;
            }

            disposable.Dispose();
        }
    }

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
        private readonly Lock _gate = new();

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
