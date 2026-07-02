// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using Timer = System.Threading.Timer;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>
/// Task-pool replacement for single-threaded event-loop runtimes such as browser WebAssembly: it never starts
/// threads, never blocks, and does not support long-running scheduling. Immediate work is batched one drain per
/// event-loop turn through a zero-due timer (a <c>setTimeout(0)</c> macrotask on WebAssembly, so the browser can
/// render between batches); delayed and periodic work use one-shot/periodic timers, which the WebAssembly runtime
/// backs with the JS event loop. Successor to the retired <c>Reactive.Wasm</c> package's scheduler, whose runtime
/// reflection no longer exists on modern .NET.
/// </summary>
public sealed class WasmScheduler : LocalScheduler, ISchedulerPeriodic, IDisposable
{
    /// <summary>Smallest period the underlying timers reliably support.</summary>
    private static readonly TimeSpan OneMillisecond = TimeSpan.FromMilliseconds(1);

    /// <summary>Ready work items awaiting an event-loop drain.</summary>
    private readonly ConcurrentQueue<IReadyWorkItem> _ready = new();

    /// <summary>One-shot timer used to yield a drain to the event loop.</summary>
    private readonly Timer _drainTimer;

    /// <summary>Approximate number of ready items; snapshots a drain batch.</summary>
    private int _readyCount;

    /// <summary>Gate that keeps at most one armed drain pending.</summary>
    private int _drainPosted;

    /// <summary>Initializes a new instance of the <see cref="WasmScheduler"/> class.</summary>
    private WasmScheduler() =>
        _drainTimer = new(static state => ((WasmScheduler)state!).RunDrain(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

    /// <summary>A queued work item awaiting an event-loop drain or a one-shot timer.</summary>
    private interface IReadyWorkItem
    {
        /// <summary>Runs the scheduled action unless cancelled.</summary>
        void Run();
    }

    /// <summary>Gets the shared WebAssembly scheduler.</summary>
    public static WasmScheduler Default { get; } = new();

    /// <summary>Schedules an action to be executed on the next event-loop turn.</summary>
    /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
    /// <param name="state">State passed to the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable used to cancel the scheduled action.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    public override IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        var item = new StatefulWorkItem<TState>(this, state, action);
        Enqueue(item);
        return item;
    }

    /// <summary>Schedules an action to be executed after the specified relative due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
    /// <param name="state">State passed to the action.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable used to cancel the scheduled action.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    public override IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        var dt = Scheduler.Normalize(dueTime);
        if (dt == TimeSpan.Zero)
        {
            return Schedule(state, action);
        }

        var item = new StatefulWorkItem<TState>(this, state, action);

        // The timer roots itself while armed through the callback's target (the work item), which stores the
        // timer; the item's Dispose cancels and releases it.
        item.AttachTimer(new(static s => ((IReadyWorkItem)s!).Run(), item, dt, Timeout.InfiniteTimeSpan));
        return item;
    }

    /// <summary>
    /// Schedules a periodic action. Periods below one millisecond (including zero) are clamped to one millisecond:
    /// a tight sequential loop would starve a single-threaded event loop, and browsers clamp nested
    /// <c>setTimeout</c> anyway.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
    /// <param name="state">Initial state passed to the action upon the first iteration.</param>
    /// <param name="period">Period for running the work periodically.</param>
    /// <param name="action">Action to be executed, potentially updating the state.</param>
    /// <returns>The disposable used to cancel the scheduled recurring action.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="period"/> is negative.</exception>
    public IDisposable SchedulePeriodic<TState>(TState state, TimeSpan period, Func<TState, TState> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);
        if (period < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(period));
        }

        if (period < OneMillisecond)
        {
            period = OneMillisecond;
        }

        return new PeriodicWorkItem<TState>(state, period, action);
    }

    /// <summary>Disposes the drain timer owned by this scheduler.</summary>
    public void Dispose() => _drainTimer.Dispose();

    /// <summary>Enqueues immediate work and coalesces a single drain post.</summary>
    /// <param name="item">Work item to execute on the next event-loop turn.</param>
    private void Enqueue(IReadyWorkItem item)
    {
        _ready.Enqueue(item);
        _ = Interlocked.Increment(ref _readyCount);
        PostDrain();
    }

    /// <summary>Arms the drain timer if queued work is waiting.</summary>
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
            if (_drainTimer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan))
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

    /// <summary>Runs one event-loop batch.</summary>
    private void RunDrain()
    {
        Volatile.Write(ref _drainPosted, 0);

        try
        {
            var remaining = Volatile.Read(ref _readyCount);
            while (remaining-- > 0 && _ready.TryDequeue(out var item))
            {
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

    /// <summary>
    /// A cancellable scheduled work item carrying closure-free state and the scheduler passed back to the action;
    /// also the target that roots a delayed one-shot timer.
    /// </summary>
    /// <typeparam name="TState">The scheduled state type.</typeparam>
    private sealed class StatefulWorkItem<TState> : IReadyWorkItem, IDisposable
    {
        /// <summary>The scheduler passed back to the scheduled action.</summary>
        private readonly WasmScheduler _scheduler;

        /// <summary>Scheduled state.</summary>
        private readonly TState _state;

        /// <summary>Scheduled action.</summary>
        private readonly Func<IScheduler, TState, IDisposable> _action;

        /// <summary>Timer driving a delayed item; <see langword="null"/> for immediate work.</summary>
        private Timer? _timer;

        /// <summary>Disposable returned by the scheduled action after it starts.</summary>
        private IDisposable? _disposable;

        /// <summary>Tracks cancellation.</summary>
        private int _isDisposed;

        /// <summary>Initializes a new instance of the <see cref="StatefulWorkItem{TState}"/> class.</summary>
        /// <param name="scheduler">The scheduler passed back to the scheduled action.</param>
        /// <param name="state">Scheduled state.</param>
        /// <param name="action">Scheduled action.</param>
        public StatefulWorkItem(WasmScheduler scheduler, TState state, Func<IScheduler, TState, IDisposable> action)
        {
            _scheduler = scheduler;
            _state = state;
            _action = action;
        }

        /// <summary>Gets a value indicating whether the work item has been cancelled.</summary>
        private bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

        /// <summary>Stores the one-shot timer so the caller's disposable cancels and releases it.</summary>
        /// <param name="timer">The armed timer.</param>
        public void AttachTimer(Timer timer)
        {
            Volatile.Write(ref _timer, timer);
            if (!IsDisposed)
            {
                return;
            }

            timer.Dispose();
        }

        /// <inheritdoc/>
        public void Run()
        {
            if (IsDisposed)
            {
                return;
            }

            var disposable = _action(_scheduler, _state) ?? Disposable.Empty;
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

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            {
                return;
            }

            Interlocked.Exchange(ref _timer, null)?.Dispose();
            Interlocked.Exchange(ref _disposable, Disposable.Empty)?.Dispose();
        }
    }

    /// <summary>Periodic work driven by a timer; ticks are serialized under a gate.</summary>
    /// <typeparam name="TState">The scheduled state type.</typeparam>
    private sealed class PeriodicWorkItem<TState> : IDisposable
    {
        /// <summary>Serializes ticks and guards state transitions.</summary>
        private readonly Lock _gate = new();

        /// <summary>Periodic timer; rooted through the tick callback's target while armed.</summary>
        private readonly Timer _timer;

        /// <summary>Scheduled action.</summary>
        private readonly Func<TState, TState> _action;

        /// <summary>State threaded through the periodic action.</summary>
        private TState _state;

        /// <summary>Tracks cancellation.</summary>
        private bool _isDisposed;

        /// <summary>Initializes a new instance of the <see cref="PeriodicWorkItem{TState}"/> class.</summary>
        /// <param name="state">Initial state.</param>
        /// <param name="period">Tick period.</param>
        /// <param name="action">Scheduled action.</param>
        public PeriodicWorkItem(TState state, TimeSpan period, Func<TState, TState> action)
        {
            _state = state;
            _action = action;
            _timer = new(static s => ((PeriodicWorkItem<TState>)s!).Tick(), this, period, period);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                if (_isDisposed)
                {
                    return;
                }

                _isDisposed = true;
                _timer.Dispose();
                _state = default!;
            }
        }

        /// <summary>Runs one periodic tick.</summary>
        private void Tick()
        {
            lock (_gate)
            {
                if (_isDisposed)
                {
                    return;
                }

                _state = _action(_state);
            }
        }
    }
}
