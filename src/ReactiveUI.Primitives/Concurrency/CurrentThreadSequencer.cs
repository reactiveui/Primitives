// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.ComponentModel;
using System.Diagnostics;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// CurrentThreadSequencer.
/// </summary>
/// <seealso cref="ISequencer" />
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed partial class CurrentThreadSequencer : ISequencer
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
    private static SequencerQueue<long>? _threadLocalQueue;

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
    /// Gets the scheduler's monotonic timestamp.
    /// </summary>
    public long Timestamp => Sequencer.Timestamp;

    /// <summary>
    /// Schedules an action to be executed on the current-thread trampoline.
    /// </summary>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable object used to cancel queued work, or an empty disposable when the action has already run.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    public IDisposable Schedule(Action action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        if (!_running)
        {
            SetRunning(true);
            try
            {
                action();
                var queue = GetQueue();
                if (queue != null)
                {
                    Trampoline.Run(queue);
                }
            }
            finally
            {
                SetQueue(null);
                SetRunning(false);
            }

            return Disposable.Empty;
        }

        var item = new ActionWorkItem(action);
        Schedule(item);
        return item;
    }

    /// <summary>
    /// Schedules a work item to be executed.
    /// </summary>
    /// <param name="item">Work item to execute.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        Schedule(item, Timestamp);
    }

    /// <summary>
    /// Schedules a work item to be executed at the specified monotonic timestamp.
    /// </summary>
    /// <param name="item">Work item to execute.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item, long dueTimestamp)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        SequencerQueue<long>? queue;

        // There is no timed task and no task is currently running
        if (!_running)
        {
            SetRunning(true);

            var dueTime = Sequencer.TimeUntil(dueTimestamp);
            if (dueTime > TimeSpan.Zero)
            {
                Thread.Sleep(dueTime);
            }

            // execute directly without queueing
            try
            {
                if (!Sequencer.IsCancelled(item))
                {
                    item.Execute();
                }
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

            return;
        }

        queue = GetQueue();

        // if there is a task running or there is a queue
        if (queue == null)
        {
            queue = new SequencerQueue<long>(4);
            SetQueue(queue);
        }

        // queue up more work
        var si = new CurrentThreadScheduledItem(item, dueTimestamp);
        queue.Enqueue(si);
    }

    /// <summary>
    /// Gets the queued recursive work for the current thread.
    /// </summary>
    /// <returns>The current thread queue, if one exists.</returns>
    private static SequencerQueue<long>? GetQueue() => _threadLocalQueue;

    /// <summary>
    /// Sets the queued recursive work for the current thread.
    /// </summary>
    /// <param name="newQueue">The queue to assign.</param>
    private static void SetQueue(SequencerQueue<long>? newQueue) => _threadLocalQueue = newQueue;

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
        public static void Run(SequencerQueue<long> queue)
        {
            while (queue.Count > 0)
            {
                var item = queue.Dequeue();
                if (!item.IsDisposed)
                {
                    var wait = Sequencer.TimeUntil(item.DueTime);
                    if (wait > TimeSpan.Zero)
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

    /// <summary>
    /// Cancellable action work item.
    /// </summary>
    private sealed class ActionWorkItem : IWorkItem, IsDisposed
    {
        /// <summary>
        /// Action to execute.
        /// </summary>
        private readonly Action _action;

        /// <summary>
        /// Tracks cancellation.
        /// </summary>
        private int _isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ActionWorkItem"/> class.
        /// </summary>
        /// <param name="action">Action to execute.</param>
        public ActionWorkItem(Action action) => _action = action;

        /// <inheritdoc/>
        public bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

        /// <inheritdoc/>
        public void Dispose() => Interlocked.Exchange(ref _isDisposed, 1);

        /// <inheritdoc/>
        public void Execute()
        {
            if (IsDisposed)
            {
                return;
            }

            _action();
        }
    }

    /// <summary>
    /// Current-thread queued work item.
    /// </summary>
    private sealed class CurrentThreadScheduledItem : ScheduledItem<long>
    {
        /// <summary>
        /// Scheduled work item.
        /// </summary>
        private readonly IWorkItem _item;

        /// <summary>
        /// Initializes a new instance of the <see cref="CurrentThreadScheduledItem"/> class.
        /// </summary>
        /// <param name="item">Scheduled work item.</param>
        /// <param name="dueTimestamp">Absolute monotonic due timestamp.</param>
        public CurrentThreadScheduledItem(IWorkItem item, long dueTimestamp)
            : base(dueTimestamp, Comparer<long>.Default) =>
            _item = item;

        /// <inheritdoc/>
        protected override IDisposable InvokeCore()
        {
            if (!Sequencer.IsCancelled(_item))
            {
                _item.Execute();
            }

            return Disposable.Empty;
        }
    }
}
