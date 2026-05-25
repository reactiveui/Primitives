// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.ComponentModel;
using System.Diagnostics;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// CurrentThreadScheduler.
/// </summary>
/// <seealso cref="ReactiveUI.Primitives.Concurrency.ISequencer" />
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public sealed class CurrentThreadScheduler : ISequencer
{
    private static readonly Lazy<CurrentThreadScheduler> StaticInstance = new(() => new CurrentThreadScheduler());

    [ThreadStatic]
    private static bool _running;

    [ThreadStatic]
    private static SchedulerQueue<TimeSpan>? _threadLocalQueue;

    [ThreadStatic]
    private static Stopwatch? clock;

    private CurrentThreadScheduler()
    {
    }

    /// <summary>
    /// Gets the singleton instance of the current thread scheduler.
    /// </summary>
    public static CurrentThreadScheduler Instance => StaticInstance.Value;

    /// <summary>
    /// Gets a value indicating whether gets a value that indicates whether the caller must call a Schedule method.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Advanced)]
#pragma warning disable CA1822 // Mark members as static
    public bool IsScheduleRequired => !_running;
#pragma warning restore CA1822 // Mark members as static

    /// <summary>
    /// Gets the scheduler's notion of current time.
    /// </summary>
    public DateTimeOffset Now => DateTimeOffset.UtcNow;

    private static TimeSpan Time
    {
        get
        {
            clock ??= Stopwatch.StartNew();

            return clock.Elapsed;
        }
    }

    /// <summary>
    /// Schedules an action to be executed.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    public IDisposable Schedule<TState>(TState state, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        return Schedule(state, TimeSpan.Zero, action);
    }

    /// <summary>
    /// Schedules an action to be executed after dueTime.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    /// <exception cref="System.ArgumentNullException">action.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="action" /> is <c>null</c>.</exception>
    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        SchedulerQueue<TimeSpan>? queue;

        // There is no timed task and no task is currently running
        if (!_running)
        {
            _running = true;

            if (dueTime > TimeSpan.Zero)
            {
                Thread.Sleep(dueTime);
            }

            // execute directly without queueing
            IDisposable d;
            try
            {
                d = action(this, state);
            }
            catch
            {
                SetQueue(null);
                _running = false;
                throw;
            }

            // did recursive tasks arrive?
            queue = GetQueue();

            // yes, run those in the queue as well
            if (queue != null)
            {
                try
                {
                    Trampoline.Run(queue);
                }
                finally
                {
                    SetQueue(null);
                    _running = false;
                }
            }
            else
            {
                _running = false;
            }

            return d;
        }

        queue = GetQueue();

        // if there is a task running or there is a queue
        if (queue == null)
        {
            queue = new SchedulerQueue<TimeSpan>(4);
            SetQueue(queue);
        }

        var dt = Time + Scheduler.Normalize(dueTime);

        // queue up more work
        var si = new ScheduledItem<TimeSpan, TState>(this, state, action, dt);
        queue.Enqueue(si);
        return si;
    }

    /// <summary>
    /// Schedules an action to be executed at dueTime.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        var due = Scheduler.Normalize(dueTime - Now);
        return Schedule(state, due, action);
    }

    private static SchedulerQueue<TimeSpan>? GetQueue() => _threadLocalQueue;

    private static void SetQueue(SchedulerQueue<TimeSpan>? newQueue) => _threadLocalQueue = newQueue;

    private static class Trampoline
    {
        public static void Run(SchedulerQueue<TimeSpan> queue)
        {
            while (queue.Count > 0)
            {
                var item = queue.Dequeue();
                if (!item.IsDisposed)
                {
                    var wait = item.DueTime - Time;
                    if (wait.Ticks > 0)
                    {
                        Thread.Sleep(wait);
                    }

                    if (!item.IsDisposed)
                    {
                        item.Invoke();
                    }
                }
            }
        }
    }
}
