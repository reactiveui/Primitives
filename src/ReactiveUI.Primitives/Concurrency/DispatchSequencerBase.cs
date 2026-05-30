// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Base class for UI-thread sequencers that coalesce dispatcher posts and share delayed scheduling.
/// </summary>
/// <seealso cref="ISequencer" />
public abstract class DispatchSequencerBase : ISequencer
{
    /// <summary>
    /// Ready work items awaiting a UI-thread drain.
    /// </summary>
    private readonly ConcurrentQueue<IWorkItem> _ready = new();

    /// <summary>
    /// Cached drain callback passed to the platform dispatcher.
    /// </summary>
    private readonly Action _drain;

    /// <summary>
    /// Approximate number of ready items. Used to snapshot a drain batch.
    /// </summary>
    private int _readyCount;

    /// <summary>
    /// Gate that keeps at most one queued drain callback pending.
    /// </summary>
    private int _drainPosted;

    /// <summary>
    /// Initializes a new instance of the <see cref="DispatchSequencerBase"/> class.
    /// </summary>
    protected DispatchSequencerBase() =>
        _drain = RunDrain;

    /// <inheritdoc/>
    public DateTimeOffset Now => Sequencer.Now;

    /// <inheritdoc/>
    public long Timestamp => Sequencer.Timestamp;

    /// <summary>
    /// Schedules a work item to be executed on the dispatcher.
    /// </summary>
    /// <param name="item">Work item to execute.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        _ready.Enqueue(item);
        Interlocked.Increment(ref _readyCount);
        PostDrain();
    }

    /// <summary>
    /// Schedules a work item to be executed on the dispatcher at a monotonic timestamp.
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

        if (dueTimestamp <= Timestamp)
        {
            Schedule(item);
            return;
        }

        ScheduleDelayed(item, dueTimestamp);
    }

    /// <summary>
    /// Gets the delay from the sequencer's current time until the given monotonic timestamp.
    /// </summary>
    /// <param name="dueTimestamp">The absolute monotonic due timestamp.</param>
    /// <returns>The remaining delay.</returns>
    protected static TimeSpan DelayUntil(long dueTimestamp) => Sequencer.TimeUntil(dueTimestamp);

    /// <summary>
    /// Executes the work item on the current (dispatcher) thread unless it has already been cancelled.
    /// </summary>
    /// <param name="item">The work item to execute.</param>
    protected static void RunIfActive(IWorkItem item)
    {
        if (Sequencer.IsCancelled(item))
        {
            return;
        }

        item.Execute();
    }

    /// <summary>
    /// Schedules a work item to run at a future monotonic timestamp. The default implementation uses
    /// the shared thread-pool timer and marshals the due item back through the dispatcher via
    /// <see cref="Schedule(IWorkItem)"/>. UI dispatchers that expose a native timer (for example MAUI's
    /// <c>DispatchDelayed</c> or the WPF/WinUI dispatcher timers) override this to run the due item
    /// directly on the dispatcher thread, avoiding the thread-pool-to-UI marshal hop.
    /// </summary>
    /// <param name="item">Work item to execute at the due time.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    protected virtual void ScheduleDelayed(IWorkItem item, long dueTimestamp) =>
        ThreadPoolSequencer.Instance.Schedule(new MarshalOnDueWorkItem(this, item), dueTimestamp);

    /// <summary>
    /// Posts a drain request to the platform dispatcher.
    /// </summary>
    /// <param name="drain">The cached drain callback to marshal.</param>
    /// <returns><see langword="true"/> when the drain was posted; otherwise, <see langword="false"/>.</returns>
    protected abstract bool Post(Action drain);

    /// <summary>
    /// Attempts to post a drain if queued work is waiting.
    /// </summary>
    protected void PostDrain()
    {
        if (Volatile.Read(ref _readyCount) == 0)
        {
            return;
        }

        if (Interlocked.Exchange(ref _drainPosted, 1) != 0)
        {
            return;
        }

        try
        {
            if (Post(_drain))
            {
                return;
            }
        }
        catch
        {
            Volatile.Write(ref _drainPosted, 0);
            throw;
        }

        Volatile.Write(ref _drainPosted, 0);
    }

    /// <summary>
    /// Runs one dispatcher batch.
    /// </summary>
    private void RunDrain()
    {
        Volatile.Write(ref _drainPosted, 0);

        try
        {
            var remaining = Volatile.Read(ref _readyCount);
            while (remaining-- > 0 && _ready.TryDequeue(out var item))
            {
                Interlocked.Decrement(ref _readyCount);
                if (!Sequencer.IsCancelled(item))
                {
                    item.Execute();
                }
            }
        }
        finally
        {
            if (Volatile.Read(ref _readyCount) != 0)
            {
                PostDrain();
            }
        }
    }

    /// <summary>
    /// Work item used by the shared timer path to marshal delayed work back to the dispatcher.
    /// </summary>
    private sealed class MarshalOnDueWorkItem : IWorkItem
    {
        /// <summary>
        /// Owning dispatch sequencer.
        /// </summary>
        private readonly DispatchSequencerBase _owner;

        /// <summary>
        /// Work item to marshal.
        /// </summary>
        private readonly IWorkItem _item;

        /// <summary>
        /// Initializes a new instance of the <see cref="MarshalOnDueWorkItem"/> class.
        /// </summary>
        /// <param name="owner">Owning dispatch sequencer.</param>
        /// <param name="item">Work item to marshal.</param>
        public MarshalOnDueWorkItem(DispatchSequencerBase owner, IWorkItem item)
        {
            _owner = owner;
            _item = item;
        }

        /// <inheritdoc/>
        public void Execute()
        {
            if (Sequencer.IsCancelled(_item))
            {
                return;
            }

            _owner.Schedule(_item);
        }
    }
}
