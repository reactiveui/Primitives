// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

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
        /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule<TState>(TState state, Func<ISequencer, TState, IDisposable> action) =>
            Sequencer.Schedule(scheduler, state, action);

        /// <summary>Schedules a stateful action to be executed without capturing state in a closure.</summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule<TState>(TState state, Action<TState> action) =>
            Sequencer.Schedule(scheduler, state, action);

        /// <summary>Schedules an action to be executed after a relative due time.</summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule<TState>(
            TState state,
            TimeSpan dueTime,
            Func<ISequencer, TState, IDisposable> action) =>
            Sequencer.Schedule(scheduler, state, dueTime, action);

        /// <summary>Schedules a stateful action to be executed after a relative due time without capturing state in a closure.</summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Action<TState> action) =>
            Sequencer.Schedule(scheduler, state, dueTime, action);

        /// <summary>Schedules a stateful action to be executed at a monotonic timestamp without capturing state in a closure.</summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule<TState>(TState state, long dueTimestamp, Action<TState> action) =>
            Sequencer.Schedule(scheduler, state, dueTimestamp, action);

        /// <summary>Schedules an action to be executed at an absolute wall-clock due time.</summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="dueTime">Absolute time at which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule<TState>(
            TState state,
            DateTimeOffset dueTime,
            Func<ISequencer, TState, IDisposable> action) =>
            Sequencer.Schedule(scheduler, state, dueTime, action);

        /// <summary>Schedules an action to be executed.</summary>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule(Action action) =>
            Sequencer.Schedule(scheduler, action);

        /// <summary>Schedules an action to be executed after the specified relative due time.</summary>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule(TimeSpan dueTime, Action action) =>
            Sequencer.Schedule(scheduler, dueTime, action);

        /// <summary>Schedules an action to be executed at the specified absolute due time.</summary>
        /// <param name="dueTime">Absolute time at which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule(DateTimeOffset dueTime, Action action) =>
            Sequencer.Schedule(scheduler, dueTime, action);

        /// <summary>Schedules a recursive action, which receives a delegate that schedules its next invocation.</summary>
        /// <param name="action">Action to execute; its argument schedules the next iteration.</param>
        /// <returns>The disposable object used to cancel the recursive work (best effort).</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule(Action<Action> action) =>
            Sequencer.Schedule(scheduler, action);

        /// <summary>Schedules an action to be executed.</summary>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable ScheduleAction<TState>(TState state, Action<TState> action) =>
            Sequencer.ScheduleAction(scheduler, state, action);

        /// <summary>Schedules an action to be executed.</summary>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <param name="state">A state object to be passed to <paramref name="action"/>.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IDisposable ScheduleAction<TState>(TState state, Func<TState, IDisposable> action) =>
            Sequencer.ScheduleAction(scheduler, state, action);

        /// <summary>Schedules an action to be executed after the specified relative due time.</summary>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IDisposable ScheduleAction<TState>(TState state, TimeSpan dueTime, Action<TState> action) =>
            Sequencer.ScheduleAction(scheduler, state, dueTime, action);

        /// <summary>Schedules an action to be executed after the specified relative due time.</summary>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IDisposable ScheduleAction<TState>(TState state, TimeSpan dueTime, Func<TState, IDisposable> action) =>
            Sequencer.ScheduleAction(scheduler, state, dueTime, action);

        /// <summary>Schedules an action to be executed after the specified relative due time.</summary>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IDisposable ScheduleAction<TState>(TState state, DateTimeOffset dueTime, Action<TState> action) =>
            Sequencer.ScheduleAction(scheduler, state, dueTime, action);

        /// <summary>Schedules an action to be executed after the specified relative due time.</summary>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <param name="state">A state object to be passed to <paramref name="action" />.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="scheduler" /> or <paramref name="action" /> is <c>null</c>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IDisposable ScheduleAction<TState>(
            TState state,
            DateTimeOffset dueTime,
            Func<TState, IDisposable> action) =>
            Sequencer.ScheduleAction(scheduler, state, dueTime, action);
    }
}
