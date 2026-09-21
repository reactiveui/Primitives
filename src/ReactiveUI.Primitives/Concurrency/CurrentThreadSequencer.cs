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

    /// <summary>The clock of the sequencer whose call started the trampoline running on the current thread.</summary>
    [ThreadStatic]
    private static SequencerClock? _runningClock;

    /// <summary>The clock supplying time and blocking waits.</summary>
    private readonly SequencerClock _clock;

    /// <summary>Reads the monotonic clock.</summary>
    private readonly Func<long> _timestamp;

    /// <summary>Blocks the scheduling thread for a delay measured by the clock.</summary>
    private readonly Action<TimeSpan> _wait;

    /// <summary>Initializes a new instance of the <see cref="CurrentThreadSequencer"/> class that reads time and waits through a <see cref="TimeProvider"/>.</summary>
    /// <param name="timeProvider">The provider supplying the current time, timestamps and timers.</param>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider"/> is <see langword="null"/>.</exception>
    /// <remarks>Work scheduled while another sequencer instance is running on the same thread joins that instance's trampoline and waits on its provider.</remarks>
    public CurrentThreadSequencer(TimeProvider timeProvider)
        : this(new SequencerClock(timeProvider))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CurrentThreadSequencer"/> class; callers use <see cref="Instance"/>.</summary>
    private CurrentThreadSequencer()
        : this(SequencerClock.Default)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CurrentThreadSequencer"/> class.</summary>
    /// <param name="clock">The clock supplying time and blocking waits.</param>
    private CurrentThreadSequencer(SequencerClock clock)
    {
        _clock = clock;
        _timestamp = clock.GetTimestamp;
        _wait = clock.Wait;
    }

    /// <summary>Gets the singleton instance of the current thread scheduler.</summary>
    public static CurrentThreadSequencer Instance => StaticInstance.Value;

    /// <summary>Gets a value indicating whether the caller must schedule work instead of running it inline, true when the current thread is outside any scheduled call.</summary>
    [EditorBrowsable(EditorBrowsableState.Advanced)]
    public static bool IsScheduleRequired => !_running;

    /// <summary>Gets the scheduler's notion of current time.</summary>
    public DateTimeOffset Now => _clock.GetUtcNow();

    /// <summary>Gets the scheduler's monotonic timestamp.</summary>
    public long Timestamp => _clock.GetTimestamp();

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
            StartRunning();
            try
            {
                action();
                var queue = GetQueue();
                if (queue is not null)
                {
                    Trampoline.Run(queue, _timestamp, _wait);
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
            StartRunning();

            try
            {
                WaitIfNeeded(_clock.TimeUntil(dueTimestamp), _wait);

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
                    Trampoline.Run(queue, _timestamp, _wait);
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

        ScheduledItem<long> si = new(ToRunningTimeline(dueTimestamp), Comparer<long>.Default, _ =>
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

    /// <summary>Marks the current thread as running scheduled work on behalf of this sequencer.</summary>
    private void StartRunning()
    {
        _running = true;
        _runningClock = _clock;
    }

    /// <summary>Expresses a due timestamp on the clock of the sequencer that started the running trampoline.</summary>
    /// <param name="dueTimestamp">The absolute due timestamp on this sequencer's clock.</param>
    /// <returns>The equivalent due timestamp on the running trampoline's clock.</returns>
    private long ToRunningTimeline(long dueTimestamp)
    {
        var runningClock = _runningClock!;
        return ReferenceEquals(runningClock, _clock)
            ? dueTimestamp
            : Sequencer.AddTimestamp(runningClock.GetTimestamp(), _clock.TimeUntil(dueTimestamp));
    }

    /// <summary>Runs queued current-thread work.</summary>
    internal static class Trampoline
    {
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
