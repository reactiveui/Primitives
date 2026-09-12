// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>A sequencer that schedules work on the current thread using a trampoline queue.</summary>
/// <seealso cref="ISequencer" />
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class CurrentThreadSequencer : ISequencer
{
    /// <summary>Initial capacity for a freshly created thread-local work queue.</summary>
    private const int InitialQueueCapacity = 4;

    /// <summary>Singleton holder for the current-thread sequencer.</summary>
    private static readonly Lazy<CurrentThreadSequencer> StaticInstance = new(static () => new());

    /// <summary>Tracks whether the current thread is running scheduled work.</summary>
    [ThreadStatic]
    private static bool _running;

    /// <summary>Holds recursive work queued for the current thread.</summary>
    [ThreadStatic]
    private static SequencerQueue<long>? _threadLocalQueue;

    /// <summary>Initializes a new instance of the <see cref="CurrentThreadSequencer"/> class.</summary>
    private CurrentThreadSequencer()
    {
    }

    /// <summary>Gets the singleton instance of the current thread scheduler.</summary>
    public static CurrentThreadSequencer Instance => StaticInstance.Value;

    /// <summary>Gets a value indicating whether the caller must schedule work instead of running it inline, true when the current thread is outside any scheduled call.</summary>
    [EditorBrowsable(EditorBrowsableState.Advanced)]
    public static bool IsScheduleRequired => !_running;

    /// <summary>Gets the scheduler's notion of current time.</summary>
    public DateTimeOffset Now => Sequencer.Now;

    /// <summary>Gets the scheduler's monotonic timestamp.</summary>
    public long Timestamp => Sequencer.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [DebuggerBrowsable(DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Schedules an action to be executed on the current-thread trampoline.</summary>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable object used to cancel queued work, or an empty disposable when the action ran inline.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="action"/> is <see langword="null"/>.</exception>
    public IDisposable Schedule(Action action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        if (!_running)
        {
            SetRunning(true);
            try
            {
                action();
                var queue = GetQueue();
                if (queue is not null)
                {
                    Trampoline.Run(queue);
                }
            }
            finally
            {
                SetQueue(null);
                SetRunning(false);
            }

            return EmptyDisposable.Instance;
        }

        ActionWorkItem item = new(action);
        Schedule(item);
        return item;
    }

    /// <summary>Schedules a work item to be executed.</summary>
    /// <param name="item">Work item to execute.</param>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        Schedule(item, Timestamp);
    }

    /// <summary>Runs the work item on the calling thread once due, blocking until then, or queues it on the trampoline when that thread is running scheduled work.</summary>
    /// <param name="item">Work item to execute.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item, long dueTimestamp)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        SequencerQueue<long>? queue;

        // Initial work executes before Schedule returns.
        if (!_running)
        {
            SetRunning(true);

            WaitIfNeeded(Sequencer.TimeUntil(dueTimestamp), Wait);

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

            // Nested work finishes before the outer Schedule call returns.
            queue = GetQueue();
            if (queue is not null)
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

        // Nested work waits for the current item to finish.
        if (queue is null)
        {
            queue = new(InitialQueueCapacity);
            SetQueue(queue);
        }

        ScheduledItem<long> si = new(dueTimestamp, Comparer<long>.Default, _ =>
        {
            if (!Sequencer.IsCancelled(item))
            {
                item.Execute();
            }

            return EmptyDisposable.Instance;
        });
        queue.Enqueue(si);
    }

    /// <summary>Waits only when work remains in the future.</summary>
    /// <param name="dueTime">The remaining delay.</param>
    /// <param name="wait">The wait operation.</param>
    internal static void WaitIfNeeded(TimeSpan dueTime, Action<TimeSpan> wait)
    {
        if (dueTime <= TimeSpan.Zero)
        {
            return;
        }

        wait(dueTime);
    }

    /// <summary>Gets the queued recursive work for the current thread.</summary>
    /// <returns>The current thread queue, if one exists.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SequencerQueue<long>? GetQueue() => _threadLocalQueue;

    /// <summary>Sets the queued recursive work for the current thread.</summary>
    /// <param name="newQueue">The queue to assign.</param>
    private static void SetQueue(SequencerQueue<long>? newQueue) => _threadLocalQueue = newQueue;

    /// <summary>Sets the current-thread running marker.</summary>
    /// <param name="running">Value indicating whether work is running.</param>
    private static void SetRunning(bool running) => _running = running;

    /// <summary>Blocks the scheduling thread until delayed work becomes due.</summary>
    /// <param name="dueTime">The remaining delay.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Wait(TimeSpan dueTime) => Thread.Sleep(dueTime);

    /// <summary>Runs queued current-thread work.</summary>
    internal static class Trampoline
    {
        /// <summary>Runs all work currently in the queue.</summary>
        /// <param name="queue">Queue to drain.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Run(SequencerQueue<long> queue) => Run(queue, static () => Sequencer.Timestamp, Wait);

        /// <summary>Drains work using the supplied clock and wait operation.</summary>
        /// <param name="queue">The pending work.</param>
        /// <param name="timestamp">The monotonic clock.</param>
        /// <param name="wait">The wait operation.</param>
        internal static void Run(SequencerQueue<long> queue, Func<long> timestamp, Action<TimeSpan> wait)
        {
            while (queue.Count > 0)
            {
                var item = queue.Dequeue();
                if (item.IsDisposed)
                {
                    continue;
                }

                WaitIfNeeded(Sequencer.TimeUntil(item.DueTime, timestamp()), wait);

                if (!item.IsDisposed)
                {
                    item.Invoke();
                }
            }
        }
    }

    /// <summary>Cancellable action work item.</summary>
    internal sealed class ActionWorkItem : IWorkItem, IsDisposed
    {
        /// <summary>Action to execute.</summary>
        private readonly Action _action;

        /// <summary>Tracks cancellation.</summary>
        private int _isDisposed;

        /// <summary>Initializes a new instance of the <see cref="ActionWorkItem"/> class.</summary>
        /// <param name="action">Action to execute.</param>
        public ActionWorkItem(Action action) => _action = action;

        /// <inheritdoc/>
        public bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
}
