// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using Timer = System.Threading.Timer;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>ThreadPoolSequencer.</summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ThreadPoolSequencer : ISequencer, IDisposable
{
    /// <summary>Gets the shared thread-pool scheduler instance.</summary>
    public static readonly ThreadPoolSequencer Instance = new();

    /// <summary>Cached queue callback for immediate work.</summary>
    private static readonly WaitCallback ImmediateCallback = static state => ExecuteQueued((IWorkItem)state!);

    /// <summary>Guards access to delayed work.</summary>
    private readonly Lock _gate = new();

    /// <summary>Pending delayed work, ordered by monotonic due timestamp.</summary>
    private readonly PriorityQueue<TimedWorkItem> _queue = new();

    /// <summary>Single timer owned by the sequencer for all delayed work.</summary>
    private readonly Timer _timer;

    /// <summary>Initializes a new instance of the <see cref="ThreadPoolSequencer"/> class.</summary>
    private ThreadPoolSequencer() =>
        _timer = new(static state => ((ThreadPoolSequencer)state!).RunDue(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

    /// <summary>Gets the scheduler's notion of current time.</summary>
    public DateTimeOffset Now => Sequencer.Now;

    /// <summary>Gets the scheduler's monotonic timestamp.</summary>
    public long Timestamp => Sequencer.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Schedules a work item to be executed through the thread pool.</summary>
    /// <param name="item">Work item to execute.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        ThreadPool.UnsafeQueueUserWorkItem(ImmediateCallback, item);
    }

    /// <summary>Schedules a work item to be executed through the thread pool at a monotonic timestamp.</summary>
    /// <param name="item">Work item to execute.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item, long dueTimestamp)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        if (dueTimestamp <= Timestamp)
        {
            Schedule(item);
            return;
        }

        lock (_gate)
        {
            _queue.Enqueue(new(item, dueTimestamp));
            ArmTimerNoLock();
        }
    }

    /// <summary>Disposes the shared delay timer owned by this sequencer.</summary>
    public void Dispose() => _timer.Dispose();

    /// <summary>Executes a work item when it has not already been cancelled.</summary>
    /// <param name="item">Work item to execute.</param>
    private static void ExecuteQueued(IWorkItem item)
    {
        if (Sequencer.IsCancelled(item))
        {
            return;
        }

        item.Execute();
    }

    /// <summary>Runs due delayed work.</summary>
    private void RunDue()
    {
        while (true)
        {
            TimedWorkItem next;
            lock (_gate)
            {
                if (!TryDequeueDueNoLock(out next))
                {
                    ArmTimerNoLock();
                    return;
                }
            }

            ExecuteQueued(next.Item);
        }
    }

    /// <summary>Attempts to dequeue the next due item.</summary>
    /// <param name="item">The dequeued item.</param>
    /// <returns><see langword="true"/> when an item was dequeued.</returns>
    private bool TryDequeueDueNoLock(out TimedWorkItem item)
    {
        while (_queue.Count > 0)
        {
            var next = _queue.Peek();
            if (Sequencer.IsCancelled(next.Item))
            {
                _queue.Dequeue();
                continue;
            }

            if (next.DueTimestamp > Timestamp)
            {
                item = default;
                return false;
            }

            item = _queue.Dequeue();
            return true;
        }

        item = default;
        return false;
    }

    /// <summary>Arms the owned timer for the current queue head.</summary>
    private void ArmTimerNoLock()
    {
        while (_queue.Count > 0 && Sequencer.IsCancelled(_queue.Peek().Item))
        {
            _queue.Dequeue();
        }

        if (_queue.Count == 0)
        {
            _timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            return;
        }

        _timer.Change(Sequencer.TimeUntil(_queue.Peek().DueTimestamp), Timeout.InfiniteTimeSpan);
    }

    /// <summary>Delayed thread-pool work item queued in the sequencer heap.</summary>
    private readonly struct TimedWorkItem : IComparable<TimedWorkItem>, IEquatable<TimedWorkItem>
    {
        /// <summary>Initializes a new instance of the <see cref="TimedWorkItem"/> struct.</summary>
        /// <param name="item">The scheduled item.</param>
        /// <param name="dueTimestamp">Absolute monotonic due timestamp.</param>
        public TimedWorkItem(IWorkItem item, long dueTimestamp)
        {
            Item = item;
            DueTimestamp = dueTimestamp;
        }

        /// <summary>Gets the scheduled item.</summary>
        public IWorkItem Item { get; }

        /// <summary>Gets the monotonic due timestamp.</summary>
        public long DueTimestamp { get; }

        /// <inheritdoc/>
        public int CompareTo(TimedWorkItem other) => DueTimestamp.CompareTo(other.DueTimestamp);

        /// <inheritdoc/>
        public bool Equals(TimedWorkItem other) =>
            ReferenceEquals(Item, other.Item) && DueTimestamp == other.DueTimestamp;

        /// <inheritdoc/>
        public override bool Equals(object? obj) =>
            obj is TimedWorkItem other && Equals(other);

        /// <inheritdoc/>
        public override int GetHashCode() =>
            unchecked((RuntimeHelpers.GetHashCode(Item) * 397) ^ DueTimestamp.GetHashCode());
    }

    /// <summary>Compatibility work item retained for coverage and direct reflection tests.</summary>
    /// <typeparam name="TState">The scheduled state type.</typeparam>
    internal sealed class ScheduledWorkItem<TState> : IWorkItem, IsDisposed
    {
        /// <summary>Owning sequencer.</summary>
        private readonly ThreadPoolSequencer _owner;

        /// <summary>Scheduled state.</summary>
        private readonly TState _state;

        /// <summary>Scheduled action.</summary>
        private readonly Func<ISequencer, TState, IDisposable> _action;

        /// <summary>Disposable returned by the scheduled action after it starts.</summary>
        private IDisposable? _disposable;

        /// <summary>Tracks cancellation.</summary>
        private int _isDisposed;

        /// <summary>Initializes a new instance of the <see cref="ScheduledWorkItem{TState}"/> class.</summary>
        /// <param name="owner">The owning sequencer.</param>
        /// <param name="state">The scheduled state.</param>
        /// <param name="action">The scheduled action.</param>
        internal ScheduledWorkItem(ThreadPoolSequencer owner, TState state, Func<ISequencer, TState, IDisposable> action)
        {
            _owner = owner;
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
        public void Execute() => Run();

        /// <summary>Queues the work item for immediate execution.</summary>
        internal void Queue() => _owner.Schedule(this);

        /// <summary>Queues the work item for delayed execution.</summary>
        /// <param name="dueTime">The normalized due time.</param>
        internal void Queue(TimeSpan dueTime) => _owner.Schedule(this, Sequencer.AddTimestamp(_owner.Timestamp, dueTime));

        /// <summary>Runs scheduled work.</summary>
        private void Run()
        {
            if (IsDisposed)
            {
                return;
            }

            var disposable = _action(_owner, _state) ?? EmptyDisposable.Instance;
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
}
