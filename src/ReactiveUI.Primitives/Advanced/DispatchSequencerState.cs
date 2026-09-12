// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Queues immediate work into posted batches and schedules delayed work through the supplied delegates.</summary>
[SuppressMessage(
    "Performance",
    "SST1803:Make record struct readonly",
    Justification =
        "Mutable dispatch-coalescing engine: holds the ready queue plus drain/post latches that mutate in place.")]
[System.Diagnostics.DebuggerDisplay("DispatchSequencerState: ReadyCount = {_readyCount}, DrainPosted = {_drainPosted}")]
public record struct DispatchSequencerState
{
    /// <summary>Ready work items awaiting a UI-thread drain.</summary>
    private readonly ConcurrentQueue<IWorkItem> _ready;

    /// <summary>Posts a cached drain callback to the platform dispatcher; returns whether the post was accepted.</summary>
    private readonly Func<Action, bool> _post;

    /// <summary>Cached drain callback (the owner's instance method) marshalled by <see cref="_post"/>.</summary>
    private readonly Action _drain;

    /// <summary>Owner used by the default delayed path to marshal due work back through the dispatcher.</summary>
    private readonly ISequencer _owner;

    /// <summary>Optional platform delayed-scheduling override; <see langword="null"/> uses the shared thread-pool timer.</summary>
    private readonly Action<IWorkItem, long>? _scheduleDelayed;

    /// <summary>Schedules delayed work when the platform provides no override.</summary>
    private readonly ISequencer _sharedTimer;

    /// <summary>Approximate number of ready items; snapshots a drain batch.</summary>
    private int _readyCount;

    /// <summary>Gate that keeps at most one queued drain callback pending.</summary>
    private int _drainPosted;

    /// <summary>Initializes a new instance of the <see cref="DispatchSequencerState"/> struct using the shared thread-pool timer for delayed work.</summary>
    /// <param name="owner">The owning sequencer, used by the default delayed path.</param>
    /// <param name="post">Posts the cached drain callback to the platform dispatcher.</param>
    /// <param name="drain">The cached drain callback (the owner's drain instance method).</param>
    public DispatchSequencerState(ISequencer owner, Func<Action, bool> post, Action drain)
        : this(owner, post, drain, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DispatchSequencerState"/> struct.</summary>
    /// <param name="owner">The owning sequencer, used by the default delayed path.</param>
    /// <param name="post">Posts the cached drain callback to the platform dispatcher.</param>
    /// <param name="drain">The cached drain callback (the owner's drain instance method).</param>
    /// <param name="scheduleDelayed">Platform delayed-scheduling override, or <see langword="null"/> to use the shared thread-pool timer.</param>
    public DispatchSequencerState(
        ISequencer owner,
        Func<Action, bool> post,
        Action drain,
        Action<IWorkItem, long>? scheduleDelayed)
        : this(owner, post, drain, scheduleDelayed, ThreadPoolSequencer.Instance)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DispatchSequencerState"/> struct.</summary>
    /// <param name="owner">The owning sequencer.</param>
    /// <param name="post">Posts the cached drain.</param>
    /// <param name="drain">The cached drain callback.</param>
    /// <param name="scheduleDelayed">The optional platform delay override.</param>
    /// <param name="sharedTimer">Schedules work without a platform delay override.</param>
    internal DispatchSequencerState(
        ISequencer owner,
        Func<Action, bool> post,
        Action drain,
        Action<IWorkItem, long>? scheduleDelayed,
        ISequencer sharedTimer)
    {
        _ready = new();
        _owner = owner;
        _post = post;
        _drain = drain;
        _scheduleDelayed = scheduleDelayed;
        _sharedTimer = sharedTimer;
    }

    /// <summary>Gets the sequencer's notion of current time.</summary>
    public static DateTimeOffset Now => Sequencer.Now;

    /// <summary>Gets the sequencer's monotonic timestamp.</summary>
    public static long Timestamp => Sequencer.Timestamp;

    /// <summary>Gets the delay from the sequencer's current time until the given monotonic timestamp.</summary>
    /// <param name="dueTimestamp">The absolute monotonic due timestamp.</param>
    /// <returns>The remaining delay.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TimeSpan DelayUntil(long dueTimestamp) => Sequencer.TimeUntil(dueTimestamp);

    /// <summary>Executes the work item on the current (dispatcher) thread unless it has been cancelled.</summary>
    /// <param name="item">The work item to execute.</param>
    public static void RunIfActive(IWorkItem item)
    {
        if (Sequencer.IsCancelled(item))
        {
            return;
        }

        item.Execute();
    }

    /// <summary>Schedules a work item to be executed on the dispatcher.</summary>
    /// <param name="item">Work item to execute.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        _ready.Enqueue(item);
        _ = Interlocked.Increment(ref _readyCount);
        PostDrain();
    }

    /// <summary>Schedules a work item to be executed on the dispatcher at a monotonic timestamp.</summary>
    /// <param name="item">Work item to execute.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item, long dueTimestamp)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        if (dueTimestamp <= Sequencer.Timestamp)
        {
            Schedule(item);
            return;
        }

        if (_scheduleDelayed is not null)
        {
            _scheduleDelayed(item, dueTimestamp);
            return;
        }

        ScheduleOnSharedTimer(item, dueTimestamp);
    }

    /// <summary>Attempts to post a drain if queued work is waiting.</summary>
    public void PostDrain()
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
            if (_post(_drain))
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

    /// <summary>Runs one dispatcher batch.</summary>
    public void RunDrain()
    {
        Volatile.Write(ref _drainPosted, 0);

        try
        {
            for (var remaining = Volatile.Read(ref _readyCount); remaining > 0; remaining--)
            {
                if (!_ready.TryDequeue(out var item))
                {
                    break;
                }

                _ = Interlocked.Decrement(ref _readyCount);
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

    /// <summary>Releases queued items when the sequencer owns its dispatcher lifetime.</summary>
    internal void ReleaseQueued()
    {
        while (_ready.TryDequeue(out var item))
        {
            _ = Interlocked.Decrement(ref _readyCount);
            if (item is IDisposable cancellable)
            {
                cancellable.Dispose();
            }
        }
    }

    /// <summary>Parks delayed work on the shared thread-pool timer, which marshals it back to the dispatcher when due.</summary>
    /// <param name="item">Work item to execute once due.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private readonly void ScheduleOnSharedTimer(IWorkItem item, long dueTimestamp) =>
        _sharedTimer.Schedule(new MarshalOnDueWorkItem(_owner, item), dueTimestamp);

    /// <summary>Work item used by the shared timer path to marshal delayed work back to the dispatcher.</summary>
    /// <param name="owner">Owning dispatch sequencer.</param>
    /// <param name="item">Work item to marshal.</param>
    internal sealed class MarshalOnDueWorkItem(ISequencer owner, IWorkItem item) : IWorkItem
    {
        /// <summary>Owning dispatch sequencer.</summary>
        private readonly ISequencer _owner = owner;

        /// <summary>Work item to marshal.</summary>
        private readonly IWorkItem _item = item;

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
