// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>
/// The queue and drain state of a UI-thread scheduler that drains queued work one batch per dispatcher post, embedded as a
/// mutable field by the scheduler.
/// </summary>
/// <remarks>
/// Work runs in posted dispatcher batches without inline reentrancy. Disposing a scheduled action suppresses unstarted work
/// and disposes the resource returned by an action that has started. Keep the field non-readonly and call it in place, since
/// a copy is a separate queue. The scheduler passes a host whose members reach its platform dispatcher.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("CoalescingDispatchState: ReadyCount = {_readyCount}, DrainPosted = {_drainPosted}")]
public record struct CoalescingDispatchState
{
    /// <summary>Ready work items awaiting a UI-thread drain.</summary>
    private readonly ConcurrentQueue<IDispatchWorkItem> _ready;

    /// <summary>The scheduler's cached drain callback, marshalled by the host's post.</summary>
    private readonly Action _drain;

    /// <summary>Schedules delays before work returns to the dispatcher.</summary>
    private readonly IScheduler _delayScheduler;

    /// <summary>Approximate count of ready items; bounds the batch one drain dequeues.</summary>
    private int _readyCount;

    /// <summary>Gate that keeps at most one queued drain callback pending.</summary>
    private int _drainPosted;

    /// <summary>Initializes a new instance of the <see cref="CoalescingDispatchState"/> struct.</summary>
    /// <param name="drain">The scheduler's drain callback, which calls <see cref="RunDrain{THost}"/>.</param>
    /// <param name="delayScheduler">Scheduler used for relative delays when the platform has no native timer.</param>
    public CoalescingDispatchState(Action drain, IScheduler delayScheduler)
    {
        _ready = new();
        _drain = drain;
        _delayScheduler = delayScheduler;
    }

    /// <summary>Schedules an action to be executed as soon as possible on the dispatcher.</summary>
    /// <typeparam name="THost">The host type.</typeparam>
    /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
    /// <param name="host">Reaches the platform dispatcher.</param>
    /// <param name="scheduler">The scheduler passed back to the action.</param>
    /// <param name="state">State passed to the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable used to cancel the scheduled action.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    public IDisposable Schedule<THost, TState>(THost host, IScheduler scheduler, TState state, Func<IScheduler, TState, IDisposable> action)
        where THost : struct, IDispatchHost
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        DispatchWorkItem<TState> item = new(scheduler, state, action);
        Enqueue(host, item);
        return item;
    }

    /// <summary>Schedules an action to be executed after the specified relative due time on the dispatcher.</summary>
    /// <typeparam name="THost">The host type.</typeparam>
    /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
    /// <param name="host">Reaches the platform dispatcher.</param>
    /// <param name="scheduler">The scheduler passed back to the action.</param>
    /// <param name="state">State passed to the action.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable used to cancel the scheduled action.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    public IDisposable Schedule<THost, TState>(
        THost host,
        IScheduler scheduler,
        TState state,
        TimeSpan dueTime,
        Func<IScheduler, TState, IDisposable> action)
        where THost : struct, IDispatchHost
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        DispatchWorkItem<TState> item = new(scheduler, state, action);
        if (Scheduler.Normalize(dueTime) == TimeSpan.Zero)
        {
            Enqueue(host, item);
            return item;
        }

        return StableCompositeDisposable.Create(host.ScheduleOnDispatcher(item.Run, dueTime), item);
    }

    /// <summary>Waits out a delay on the delay scheduler, then schedules the work back onto the dispatcher.</summary>
    /// <param name="scheduler">The dispatcher scheduler the work returns to.</param>
    /// <param name="work">Callback to invoke on the dispatcher thread when due.</param>
    /// <param name="dueTime">Relative time after which to invoke <paramref name="work"/>.</param>
    /// <returns>The disposable used to cancel the delayed dispatch.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly IDisposable ScheduleThroughDelayScheduler(IScheduler scheduler, Action work, TimeSpan dueTime) =>
        _delayScheduler.Schedule(
            (Owner: scheduler, work),
            dueTime,
            static (_, state) => state.Owner.Schedule(
                state.work,
                static (_, due) =>
                {
                    due();
                    return Disposable.Empty;
                }));

    /// <summary>Posts a drain when queued work remains; platform adapters call this when the dispatcher becomes ready.</summary>
    /// <typeparam name="THost">The host type.</typeparam>
    /// <param name="host">Reaches the platform dispatcher.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RequestDrain<THost>(THost host)
        where THost : struct, IDispatchHost =>
        PostDrain(host);

    /// <summary>Runs one dispatcher batch.</summary>
    /// <typeparam name="THost">The host type.</typeparam>
    /// <param name="host">Reaches the platform dispatcher.</param>
    public void RunDrain<THost>(THost host)
        where THost : struct, IDispatchHost
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
                PostDrain(host);
            }
        }
    }

    /// <summary>Enqueues immediate work and coalesces a single drain post.</summary>
    /// <typeparam name="THost">The host type.</typeparam>
    /// <param name="host">Reaches the platform dispatcher.</param>
    /// <param name="item">Work item to execute on the dispatcher.</param>
    private void Enqueue<THost>(THost host, IDispatchWorkItem item)
        where THost : struct, IDispatchHost
    {
        _ready.Enqueue(item);
        _ = Interlocked.Increment(ref _readyCount);
        PostDrain(host);
    }

    /// <summary>Attempts to post a drain if queued work is waiting.</summary>
    /// <typeparam name="THost">The host type.</typeparam>
    /// <param name="host">Reaches the platform dispatcher.</param>
    private void PostDrain<THost>(THost host)
        where THost : struct, IDispatchHost
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
            if (host.Post(_drain))
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
}
