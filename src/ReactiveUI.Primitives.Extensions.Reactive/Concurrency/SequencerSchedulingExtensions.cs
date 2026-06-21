// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using RxScheduler = System.Reactive.Concurrency.Scheduler;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>
/// Gives <see cref="IScheduler"/> the <c>ISequencer</c> scheduling shape the shared Extensions source expects,
/// by forwarding to <see cref="System.Reactive.Concurrency.Scheduler"/>. Kept internal: it is the seam the leaf
/// uses to invoke schedulers, not public surface.
/// </summary>
internal static class SequencerSchedulingExtensions
{
    /// <summary>Scheduling operators that give an <see cref="IScheduler"/> the shared-source sequencer shape.</summary>
    /// <param name="scheduler">Scheduler to execute work on.</param>
    extension(IScheduler scheduler)
    {
        /// <summary>Schedules a work item for execution as soon as possible.</summary>
        /// <param name="item">Work item to execute.</param>
        public void Schedule(IWorkItem item)
        {
            ArgumentExceptionHelper.ThrowIfNull(item);

            _ = scheduler.Schedule(item, static (_, w) =>
            {
                w.Execute();
                return EmptyDisposable.Instance;
            });
        }

        /// <summary>Schedules an action to be executed.</summary>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable used to cancel the scheduled action.</returns>
        public IDisposable Schedule(Action action) => RxScheduler.Schedule(scheduler, action);

        /// <summary>Schedules an action to be executed after a relative due time.</summary>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable used to cancel the scheduled action.</returns>
        public IDisposable Schedule(TimeSpan dueTime, Action action) => RxScheduler.Schedule(scheduler, dueTime, action);

        /// <summary>Schedules an action to be executed at an absolute due time.</summary>
        /// <param name="dueTime">Absolute time at which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable used to cancel the scheduled action.</returns>
        public IDisposable Schedule(DateTimeOffset dueTime, Action action) => RxScheduler.Schedule(scheduler, dueTime, action);

        /// <summary>Schedules a recursive action to be executed.</summary>
        /// <param name="action">Recursive action; invoke its argument to schedule the next iteration.</param>
        /// <returns>The disposable used to cancel recursive work.</returns>
        public IDisposable Schedule(Action<Action> action) => RxScheduler.Schedule(scheduler, action);

        /// <summary>Schedules a stateful action without capturing state in a closure.</summary>
        /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
        /// <param name="state">State passed to the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable used to cancel the scheduled action.</returns>
        public IDisposable Schedule<TState>(TState state, Action<TState> action) =>
            scheduler.Schedule((state, action), static (_, tuple) =>
            {
                tuple.action(tuple.state);
                return EmptyDisposable.Instance;
            });

        /// <summary>Schedules a stateful action after a relative due time without capturing state in a closure.</summary>
        /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
        /// <param name="state">State passed to the action.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to execute.</param>
        /// <returns>The disposable used to cancel the scheduled action.</returns>
        public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Action<TState> action) =>
            scheduler.Schedule((state, action), dueTime, static (_, tuple) =>
            {
                tuple.action(tuple.state);
                return EmptyDisposable.Instance;
            });
    }
}
