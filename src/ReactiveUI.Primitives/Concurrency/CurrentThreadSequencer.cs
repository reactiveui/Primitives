// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.ComponentModel;
using System.Diagnostics;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// CurrentThreadSequencer.
/// </summary>
/// <seealso cref="ISequencer" />
public sealed class CurrentThreadSequencer : ISequencer
{
    /// <summary>
    /// Singleton holder for the current-thread sequencer.
    /// </summary>
    private static readonly Lazy<CurrentThreadSequencer> StaticInstance = new(() => new CurrentThreadSequencer());

    /// <summary>
    /// Tracks whether the current thread is running scheduled work.
    /// </summary>
    [ThreadStatic]
    private static bool _running;

    /// <summary>
    /// Holds recursive work queued for the current thread.
    /// </summary>
    [ThreadStatic]
    private static SequencerQueue<TimeSpan>? _threadLocalQueue;

    /// <summary>
    /// Measures relative due times for the current thread.
    /// </summary>
    [ThreadStatic]
    private static Stopwatch? clock;

    /// <summary>
    /// Initializes a new instance of the <see cref="CurrentThreadSequencer"/> class.
    /// </summary>
    private CurrentThreadSequencer()
    {
    }

    /// <summary>
    /// Gets the singleton instance of the current thread scheduler.
    /// </summary>
    public static CurrentThreadSequencer Instance => StaticInstance.Value;

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
    public DateTimeOffset Now => Sequencer.Now;

    /// <summary>
    /// Gets elapsed time on the current thread.
    /// </summary>
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
    /// <exception cref="ArgumentNullException">action.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="action" /> is <c>null</c>.</exception>
    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        SequencerQueue<TimeSpan>? queue;

        // There is no timed task and no task is currently running
        if (!_running)
        {
            SetRunning(true);

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
                SetRunning(false);
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
                    SetRunning(false);
                }
            }
            else
            {
                SetRunning(false);
            }

            return d;
        }

        queue = GetQueue();

        // if there is a task running or there is a queue
        if (queue == null)
        {
            queue = new SequencerQueue<TimeSpan>(4);
            SetQueue(queue);
        }

        var dt = Time + Sequencer.Normalize(dueTime);

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
        var due = Sequencer.Normalize(dueTime - Now);
        return Schedule(state, due, action);
    }

    /// <summary>
    /// Gets the queued recursive work for the current thread.
    /// </summary>
    /// <returns>The current thread queue, if one exists.</returns>
    private static SequencerQueue<TimeSpan>? GetQueue() => _threadLocalQueue;

    /// <summary>
    /// Sets the queued recursive work for the current thread.
    /// </summary>
    /// <param name="newQueue">The queue to assign.</param>
    private static void SetQueue(SequencerQueue<TimeSpan>? newQueue) => _threadLocalQueue = newQueue;

    /// <summary>
    /// Sets the current-thread running marker.
    /// </summary>
    /// <param name="running">Value indicating whether work is running.</param>
    private static void SetRunning(bool running) => _running = running;

    /// <summary>
    /// Runs queued current-thread work.
    /// </summary>
    private static class Trampoline
    {
        /// <summary>
        /// Runs all work currently in the queue.
        /// </summary>
        /// <param name="queue">Queue to drain.</param>
        public static void Run(SequencerQueue<TimeSpan> queue)
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
