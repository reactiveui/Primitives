// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Scheduling extension operators for an <see cref="ISequencer"/>.</summary>
public static class SequencerExtensions
{
    /// <summary>Action-scheduling operators for an <see cref="ISequencer"/>.</summary>
    /// <param name="scheduler">Sequencer to execute the action on.</param>
    extension(ISequencer scheduler)
    {
        /// <summary>Schedules an action to be executed.</summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        public IDisposable Schedule<TState>(TState state, Func<ISequencer, TState, IDisposable> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            var item = new Sequencer.DelegateWorkItem<TState>(scheduler, state, action);
            scheduler.Schedule(item);
            return item;
        }

        /// <summary>Schedules a stateful action to be executed without capturing state in a closure.</summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        public IDisposable Schedule<TState>(TState state, Action<TState> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            var item = new Sequencer.ActionWorkItem<TState>(state, action);
            scheduler.Schedule(item);
            return item;
        }

        /// <summary>Schedules an action to be executed after a relative due time.</summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            var normalized = Sequencer.Normalize(dueTime);
            var item = new Sequencer.DelegateWorkItem<TState>(scheduler, state, action);
            if (normalized == TimeSpan.Zero)
            {
                scheduler.Schedule(item);
            }
            else
            {
                scheduler.Schedule(item, Sequencer.AddTimestamp(scheduler.Timestamp, normalized));
            }

            return item;
        }

        /// <summary>Schedules a stateful action to be executed after a relative due time without capturing state in a closure.</summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Action<TState> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.Schedule(state, Sequencer.AddTimestamp(scheduler.Timestamp, Sequencer.Normalize(dueTime)), action);
        }

        /// <summary>Schedules a stateful action to be executed at a monotonic timestamp without capturing state in a closure.</summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        public IDisposable Schedule<TState>(TState state, long dueTimestamp, Action<TState> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            var item = new Sequencer.ActionWorkItem<TState>(state, action);
            scheduler.Schedule(item, dueTimestamp);
            return item;
        }

        /// <summary>Schedules an action to be executed at an absolute wall-clock due time.</summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="dueTime">Absolute time at which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<ISequencer, TState, IDisposable> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.Schedule(state, Sequencer.Normalize(dueTime - scheduler.Now), action);
        }

        /// <summary>Schedules an action to be executed.</summary>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        public IDisposable Schedule(Action action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.Schedule(action, static (_, a) => Sequencer.Invoke(a));
        }

        /// <summary>Schedules an action to be executed after the specified relative due time.</summary>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>
        /// The disposable object used to cancel the scheduled action (best effort).
        /// </returns>
        /// <exception cref="ArgumentExceptionHelper">
        /// scheduler
        /// or
        /// action.
        /// </exception>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        public IDisposable Schedule(TimeSpan dueTime, Action action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.Schedule(action, dueTime, static (_, a) => Sequencer.Invoke(a));
        }

        /// <summary>Schedules an action to be executed at the specified absolute due time.</summary>
        /// <param name="dueTime">Absolute time at which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>
        /// The disposable object used to cancel the scheduled action (best effort).
        /// </returns>
        /// <exception cref="ArgumentExceptionHelper">
        /// scheduler
        /// or
        /// action.
        /// </exception>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        public IDisposable Schedule(DateTimeOffset dueTime, Action action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.Schedule(action, dueTime, static (_, a) => Sequencer.Invoke(a));
        }

        /// <summary>Schedules the specified action.</summary>
        /// <param name="action">The action.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        public IDisposable Schedule(Action<Action> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return new Sequencer.RecursiveScheduleState(scheduler, action).Start();
        }

        /// <summary>Schedules an action to be executed.</summary>
        /// <remarks>The naming of this method differs from <c>Schedule</c> because otherwise the signature would cause ambiguities.</remarks>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        public IDisposable ScheduleAction<TState>(TState state, Action<TState> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.Schedule(
                (action, state),
                (_, tuple) =>
                {
                    tuple.action(tuple.state);
                    return EmptyDisposable.Instance;
                });
        }

        /// <summary>Schedules an action to be executed.</summary>
        /// <remarks>The naming of this method differs from <c>Schedule</c> because otherwise the signature would cause ambiguities.</remarks>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <param name="state">A state object to be passed to <paramref name="action"/>.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        internal IDisposable ScheduleAction<TState>(TState state, Func<TState, IDisposable> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.Schedule(
                (action, state),
                static (_, tuple) => tuple.action(tuple.state));
        }

        /// <summary>Schedules an action to be executed after the specified relative due time.</summary>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>
        /// The disposable object used to cancel the scheduled action (best effort).
        /// </returns>
        /// <exception cref="ArgumentExceptionHelper">
        /// scheduler
        /// or
        /// action.
        /// </exception>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        internal IDisposable ScheduleAction<TState>(TState state, TimeSpan dueTime, Action<TState> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.Schedule((state, action), dueTime, static (_, tuple) => Sequencer.Invoke(tuple));
        }

        /// <summary>Schedules an action to be executed after the specified relative due time.</summary>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>
        /// The disposable object used to cancel the scheduled action (best effort).
        /// </returns>
        /// <exception cref="ArgumentExceptionHelper">
        /// scheduler
        /// or
        /// action.
        /// </exception>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        internal IDisposable ScheduleAction<TState>(TState state, TimeSpan dueTime, Func<TState, IDisposable> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.Schedule((state, action), dueTime, static (_, tuple) => Sequencer.Invoke(tuple));
        }

        /// <summary>Schedules an action to be executed after the specified relative due time.</summary>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>
        /// The disposable object used to cancel the scheduled action (best effort).
        /// </returns>
        /// <exception cref="ArgumentExceptionHelper">
        /// scheduler
        /// or
        /// action.
        /// </exception>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        internal IDisposable ScheduleAction<TState>(TState state, DateTimeOffset dueTime, Action<TState> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.Schedule((state, action), dueTime, static (_, tuple) => Sequencer.Invoke(tuple));
        }

        /// <summary>Schedules an action to be executed after the specified relative due time.</summary>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>
        /// The disposable object used to cancel the scheduled action (best effort).
        /// </returns>
        /// <exception cref="ArgumentExceptionHelper">
        /// scheduler
        /// or
        /// action.
        /// </exception>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        internal IDisposable ScheduleAction<TState>(TState state, DateTimeOffset dueTime, Func<TState, IDisposable> action)
        {
            ArgumentExceptionHelper.ThrowIfNull(scheduler);

            ArgumentExceptionHelper.ThrowIfNull(action);

            return scheduler.Schedule((state, action), dueTime, static (_, tuple) => Sequencer.Invoke(tuple));
        }
    }
}
