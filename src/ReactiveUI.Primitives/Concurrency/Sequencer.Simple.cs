// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Provides built-in sequencers for scheduling work over time.</summary>
public static partial class Sequencer
{
    /// <summary>Determines whether a scheduled work item has been cancelled.</summary>
    /// <param name="item">Work item to inspect.</param>
    /// <returns><see langword="true"/> when the work item has been disposed.</returns>
    internal static bool IsCancelled(IWorkItem item) => item is IsDisposed disposable && disposable.IsDisposed;

    /// <summary>Invokes an action and returns an empty disposable.</summary>
    /// <param name="action">Action to invoke.</param>
    /// <returns>An empty disposable.</returns>
    internal static EmptyDisposable Invoke(Action action)
    {
        action();
        return EmptyDisposable.Instance;
    }

    /// <summary>Invokes a stateful action and returns an empty disposable.</summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="tuple">Tuple containing the state and action.</param>
    /// <returns>An empty disposable.</returns>
    internal static EmptyDisposable Invoke<TState>((TState state, Action<TState> action) tuple)
    {
        tuple.action(tuple.state);
        return EmptyDisposable.Instance;
    }

    /// <summary>Invokes a stateful disposable-returning action.</summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="tuple">Tuple containing the state and action.</param>
    /// <returns>The disposable returned by the action.</returns>
    internal static IDisposable Invoke<TState>((TState state, Func<TState, IDisposable> action) tuple) =>
        tuple.action(tuple.state);

    /// <summary>Disposable work item used by closure-free stateful scheduler overloads.</summary>
    /// <typeparam name="TState">The scheduled state type.</typeparam>
    /// <param name="state">Scheduled state.</param>
    /// <param name="action">Scheduled action.</param>
    internal sealed class ActionWorkItem<TState>(TState state, Action<TState> action) : IWorkItem, IsDisposed
    {
        /// <summary>Scheduled state.</summary>
        private TState _state = state;

        /// <summary>Scheduled action.</summary>
        private Action<TState>? _action = action;

        /// <summary>Tracks cancellation.</summary>
        private int _isDisposed;

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
            if (action is null || IsDisposed)
            {
                return;
            }

            action(_state);
        }
    }

    /// <summary>Disposable work item used by the compatibility delegate scheduler overloads.</summary>
    /// <typeparam name="TState">The scheduled state type.</typeparam>
    /// <param name="scheduler">The sequencer passed back to the scheduled action.</param>
    /// <param name="state">The scheduled state.</param>
    /// <param name="action">The scheduled action.</param>
    internal sealed class DelegateWorkItem<TState>(
        ISequencer scheduler,
        TState state,
        Func<ISequencer, TState, IDisposable> action) : IWorkItem, IsDisposed
    {
        /// <summary>The sequencer passed back to the scheduled action.</summary>
        private readonly ISequencer _scheduler = scheduler;

        /// <summary>Scheduled state.</summary>
        private readonly TState _state = state;

        /// <summary>Scheduled action.</summary>
        private readonly Func<ISequencer, TState, IDisposable> _action = action;

        /// <summary>Disposable returned by the scheduled action after it starts.</summary>
        private IDisposable? _disposable;

        /// <summary>Tracks cancellation.</summary>
        private int _isDisposed;

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
            if (previous is not null)
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

    /// <summary>Holds state for recursive action scheduling.</summary>
    internal sealed class RecursiveScheduleState : MultipleDisposable
    {
        /// <summary>Sequencer used for recursive scheduling.</summary>
        private readonly ISequencer _scheduler;

        /// <summary>Recursive action supplied by the caller.</summary>
        private readonly Action<Action> _action;

        /// <summary>Guards handoff between scheduling and execution.</summary>
        private readonly Lock _gate = new();

        /// <summary>Cached delegate used to avoid recreating the recursive action.</summary>
        private readonly Action _recursiveAction;

        /// <summary>Initializes a new instance of the <see cref="RecursiveScheduleState"/> class.</summary>
        /// <param name="scheduler">Sequencer used for recursive scheduling.</param>
        /// <param name="action">Recursive action supplied by the caller.</param>
        public RecursiveScheduleState(ISequencer scheduler, Action<Action> action)
        {
            _scheduler = scheduler;
            _action = action;
            _recursiveAction = RunRecursiveAction;
        }

        /// <summary>Starts recursive scheduling.</summary>
        /// <returns>The disposable object used to cancel recursive work.</returns>
        public RecursiveScheduleState Start()
        {
            Add(_scheduler.Schedule(_recursiveAction));
            return this;
        }

        /// <summary>Invokes the caller-provided recursive action.</summary>
        private void RunRecursiveAction() => _action(Reschedule);

        /// <summary>Schedules the next recursive action invocation.</summary>
        private void Reschedule()
        {
            RescheduleHandoff handoff = new();
            handoff.Disposable = _scheduler.Schedule(
                (self: this, handoff),
                static (_, s) => s.self.RunRescheduledWork(s.handoff));

            lock (_gate)
            {
                if (!handoff.IsDone)
                {
                    Add(handoff.Disposable);
                    handoff.IsAdded = true;
                }
            }
        }

        /// <summary>Reconciles the handoff with the scheduling call under the gate, then invokes the recursive action.</summary>
        /// <param name="handoff">The handoff shared with the scheduling call.</param>
        /// <returns>An empty disposable; the rescheduled work item needs no further cancellation.</returns>
        private EmptyDisposable RunRescheduledWork(RescheduleHandoff handoff)
        {
            lock (_gate)
            {
                if (handoff.IsAdded)
                {
                    _ = Remove(handoff.Disposable!);
                }
                else
                {
                    handoff.IsDone = true;
                }
            }

            RunRecursiveAction();
            return EmptyDisposable.Instance;
        }

        /// <summary>Carries the mutable handoff state shared between a rescheduled work item and its scheduling call.</summary>
        private sealed class RescheduleHandoff
        {
            /// <summary>Gets or sets the disposable returned for the rescheduled work item.</summary>
            public IDisposable? Disposable { get; set; }

            /// <summary>Gets or sets a value indicating whether the disposable was added to the collection.</summary>
            public bool IsAdded { get; set; }

            /// <summary>Gets or sets a value indicating whether the rescheduled work item already ran.</summary>
            public bool IsDone { get; set; }
        }
    }
}
