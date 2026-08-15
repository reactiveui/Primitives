// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>
/// Base <see cref="IScheduler"/> for UI-thread dispatchers that coalesces dispatcher posts: immediate work is queued
/// and drained one batch per post. A sealed platform scheduler supplies its dispatcher <see cref="Post"/> (and
/// optionally a native delayed path via <see cref="ScheduleOnDispatcher"/>).
/// </summary>
[System.Diagnostics.DebuggerDisplay("ReadyCount = {_readyCount}, DrainPosted = {_drainPosted}")]
public abstract class CoalescingDispatchScheduler : LocalScheduler
{
    /// <summary>Ready work items awaiting a UI-thread drain.</summary>
    private readonly ConcurrentQueue<IDispatchWorkItem> _ready = new();

    /// <summary>Cached drain callback (this scheduler's <see cref="RunDrain"/>) marshalled by <see cref="Post"/>.</summary>
    private readonly Action _drain;

    /// <summary>Approximate number of ready items; snapshots a drain batch.</summary>
    private int _readyCount;

    /// <summary>Gate that keeps at most one queued drain callback pending.</summary>
    private int _drainPosted;

    /// <summary>Initializes a new instance of the <see cref="CoalescingDispatchScheduler"/> class.</summary>
    protected CoalescingDispatchScheduler() => _drain = RunDrain;

    /// <summary>Schedules an action to be executed as soon as possible on the dispatcher.</summary>
    /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
    /// <param name="state">State passed to the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable used to cancel the scheduled action.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    public override IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        var item = new DispatchWorkItem<TState>(this, state, action);
        Enqueue(item);
        return item;
    }

    /// <summary>Schedules an action to be executed after the specified relative due time on the dispatcher.</summary>
    /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
    /// <param name="state">State passed to the action.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable used to cancel the scheduled action.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    public override IDisposable Schedule<TState>(
        TState state,
        TimeSpan dueTime,
        Func<IScheduler, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        var item = new DispatchWorkItem<TState>(this, state, action);
        if (Scheduler.Normalize(dueTime) == TimeSpan.Zero)
        {
            Enqueue(item);
            return item;
        }

        return StableCompositeDisposable.Create(ScheduleOnDispatcher(item.Run, dueTime), item);
    }

    /// <summary>Posts the cached drain callback to the platform dispatcher.</summary>
    /// <param name="drain">The drain callback to marshal to the UI thread.</param>
    /// <returns><see langword="true"/> when the dispatcher accepted the work.</returns>
    protected abstract bool Post(Action drain);

    /// <summary>
    /// Runs <paramref name="work"/> on the dispatcher after <paramref name="dueTime"/>. The default marshals the wait
    /// onto the shared timer, then re-queues the work onto the dispatcher when it is due. Platforms with a native
    /// UI-thread timer override this to run the work directly on the dispatcher thread.
    /// </summary>
    /// <param name="work">Callback to invoke on the dispatcher thread when due.</param>
    /// <param name="dueTime">Relative time after which to invoke <paramref name="work"/>.</param>
    /// <returns>The disposable used to cancel the delayed dispatch.</returns>
    protected virtual IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime) =>
        DefaultScheduler.Instance.Schedule(
            (Owner: this, work),
            dueTime,
            static (_, state) => state.Owner.Schedule(
                state.work,
                static (_, due) =>
                {
                    due();
                    return Disposable.Empty;
                }));

    /// <summary>Re-posts a drain if work is still queued; platform adapters call this when the dispatcher becomes ready.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected void RequestDrain() => PostDrain();

    /// <summary>Enqueues immediate work and coalesces a single drain post.</summary>
    /// <param name="item">Work item to execute on the dispatcher.</param>
    private void Enqueue(IDispatchWorkItem item)
    {
        _ready.Enqueue(item);
        _ = Interlocked.Increment(ref _readyCount);
        PostDrain();
    }

    /// <summary>Attempts to post a drain if queued work is waiting.</summary>
    private void PostDrain()
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

    /// <summary>Runs one dispatcher batch.</summary>
    private void RunDrain()
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
                item.Run();
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
}
