// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Timer = System.Threading.Timer;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>A sequencer that schedules work on the thread pool.</summary>
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

    /// <summary>
    /// Non-zero once <see cref="Dispose"/> has released the timer and the queue. Written under <see cref="_gate"/>
    /// so every path that touches the timer is ordered against disposal, but read without it on the immediate path,
    /// which never goes near the timer.
    /// </summary>
    private int _isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ThreadPoolSequencer"/> class. Callers use <see cref="Instance"/>;
    /// this is internal so a test can own an isolated sequencer it may dispose without shutting the shared singleton
    /// down for every other test.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Correctness",
        "SST2403:Do not let 'this' escape from a constructor",
        Justification =
            "The timer is created disarmed, so nothing can call back into it until Schedule arms it after construction.")]
    internal ThreadPoolSequencer() =>
        _timer = new(
            static state => ((ThreadPoolSequencer)state!).RunDue(),
            this,
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);

    /// <summary>Gets the scheduler's notion of current time.</summary>
    public DateTimeOffset Now => Sequencer.Now;

    /// <summary>Gets the scheduler's monotonic timestamp.</summary>
    public long Timestamp => Sequencer.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Gets a value indicating whether the sequencer has been disposed.</summary>
    private bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

    /// <summary>Schedules a work item to be executed through the thread pool.</summary>
    /// <param name="item">Work item to execute.</param>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="item"/> is <see langword="null"/>.</exception>
    /// <exception cref="ObjectDisposedException">The sequencer has been disposed.</exception>
    public void Schedule(IWorkItem item)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);
        ObjectDisposedExceptionHelper.ThrowIf(IsDisposed, this);

        _ = ThreadPool.UnsafeQueueUserWorkItem(ImmediateCallback, item);
    }

    /// <summary>Schedules a work item to be executed through the thread pool at a monotonic timestamp.</summary>
    /// <param name="item">Work item to execute.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="item"/> is <see langword="null"/>.</exception>
    /// <exception cref="ObjectDisposedException">The sequencer has been disposed.</exception>
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
            // Tested under the same gate disposal takes, so an item that makes it into the queue is one disposal is
            // guaranteed to see and release. It can never be enqueued behind an already-released timer.
            ObjectDisposedExceptionHelper.ThrowIf(IsDisposed, this);

            _queue.Enqueue(new(item, dueTimestamp));
            ArmTimerNoLock();
        }
    }

    /// <summary>
    /// Releases the delay timer this sequencer owns and cancels the delayed work still queued behind it. Scheduling
    /// through a disposed sequencer throws <see cref="ObjectDisposedException"/> rather than accepting work that
    /// could never become due. Work the thread pool has already picked up runs to completion.
    /// </summary>
    public void Dispose()
    {
        // Under the gate: every arm of the timer happens under it too, so the timer can never be re-armed after it
        // is released here. Timer.Dispose does not wait for an in-flight callback, so a drain blocked on the gate
        // cannot deadlock this — it simply observes the disposed flag once it gets in.
        lock (_gate)
        {
            if (IsDisposed)
            {
                return;
            }

            Volatile.Write(ref _isDisposed, 1);
            _timer.Dispose();
            ReleaseQueuedNoLock();
        }
    }

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
                _ = _queue.Dequeue();
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

    /// <summary>
    /// Cancels and drops every queued delayed item. The items are the handles their callers hold, so disposing them
    /// releases the caller's work instead of stranding it in a queue nothing will ever drain again.
    /// </summary>
    private void ReleaseQueuedNoLock()
    {
        while (_queue.TryDequeue(out var pending))
        {
            if (pending.Item is IDisposable cancellable)
            {
                cancellable.Dispose();
            }
        }
    }

    /// <summary>Arms the owned timer for the current queue head.</summary>
    private void ArmTimerNoLock()
    {
        if (IsDisposed)
        {
            // Disposal released the timer and the queue under this same gate. A drain that is still unwinding on
            // the timer's callback thread lands here, and must not re-arm a timer that no longer exists.
            return;
        }

        while (_queue.Count > 0 && Sequencer.IsCancelled(_queue.Peek().Item))
        {
            _ = _queue.Dequeue();
        }

        if (_queue.Count == 0)
        {
            _ = _timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            return;
        }

        _ = _timer.Change(Sequencer.TimeUntil(_queue.Peek().DueTimestamp), Timeout.InfiniteTimeSpan);
    }

    /// <summary>Delayed thread-pool work item queued in the sequencer heap.</summary>
    internal readonly struct TimedWorkItem : IComparable<TimedWorkItem>, IEquatable<TimedWorkItem>
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
        internal IWorkItem Item { get; }

        /// <summary>Gets the monotonic due timestamp.</summary>
        internal long DueTimestamp { get; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    /// <summary>
    /// Stateful work item carrying the scheduled state and the sequencer handed back to the action, with the
    /// run/cancel handshake that lets a cancellation arriving mid-run still release whatever the action returned.
    /// </summary>
    /// <typeparam name="TState">The scheduled state type.</typeparam>
    /// <param name="owner">The owning sequencer.</param>
    /// <param name="state">The scheduled state.</param>
    /// <param name="action">The scheduled action.</param>
    internal sealed class ScheduledWorkItem<TState>(
        ThreadPoolSequencer owner,
        TState state,
        Func<ISequencer, TState, IDisposable> action) : IWorkItem, IsDisposed
    {
        /// <summary>Owning sequencer.</summary>
        [SuppressMessage(
            "Usage",
            "CA2213:Disposable fields should be disposed",
            Justification = "_owner is the sequencer that queued this work item, not a resource it owns; disposing it would shut the sequencer down when one item completes.")]
        private readonly ThreadPoolSequencer _owner = owner;

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => Run();

        /// <summary>Queues the work item for immediate execution.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Queue() => _owner.Schedule(this);

        /// <summary>Queues the work item for delayed execution.</summary>
        /// <param name="dueTime">The normalized due time.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Queue(TimeSpan dueTime) =>
            _owner.Schedule(this, Sequencer.AddTimestamp(_owner.Timestamp, dueTime));

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
