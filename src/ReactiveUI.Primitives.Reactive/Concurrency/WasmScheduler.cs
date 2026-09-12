// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Schedules immediate batches and timed work on a single-threaded event loop.</summary>
/// <remarks>Immediate batches yield between event-loop turns; long-running scheduling is unsupported.</remarks>
[System.Diagnostics.DebuggerDisplay("WasmScheduler: ReadyCount = {_readyCount}, DrainState = {_drainState}, Disposed = {_isDisposed}")]
public sealed class WasmScheduler : LocalScheduler, ISchedulerPeriodic, IDisposable
{
    /// <summary>Drain state indicating no drain is in flight.</summary>
    private const int DrainIdle = 0;

    /// <summary>Drain state indicating a drain is running.</summary>
    private const int DrainRunning = 1;

    /// <summary>Drain state indicating a drain is running and more work arrived while it ran.</summary>
    private const int DrainRunningPending = 2;

    /// <summary>Smallest period the underlying timers reliably support.</summary>
    private static readonly TimeSpan OneMillisecond = TimeSpan.FromMilliseconds(1);

    /// <summary>Ready work items awaiting an event-loop drain.</summary>
    private readonly ConcurrentQueue<IReadyWorkItem> _ready = new();

    /// <summary>One-shot timer used to yield a drain to the event loop.</summary>
    private readonly ITimer _drainTimer;

    /// <summary>Creates the timers that dispatch scheduled work.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>Approximate number of ready items; snapshots a drain batch.</summary>
    private int _readyCount;

    /// <summary>Drain state: zero idle, one active, two active with another pass requested.</summary>
    private int _drainState;

    /// <summary>Non-zero once <see cref="Dispose"/> has released the drain timer and the ready queue.</summary>
    private int _isDisposed;

    /// <summary>Initializes a new instance of the <see cref="WasmScheduler"/> class.</summary>
    /// <param name="timeProvider">Timer provider; null selects the system provider.</param>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Correctness",
        "SST2403:Do not let 'this' escape from a constructor",
        Justification =
            "The drain timer is created disarmed, so nothing can call back into it until Schedule arms it after construction.")]
    internal WasmScheduler(TimeProvider? timeProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _drainTimer = CreateTimer(
            _timeProvider,
            static state => ((WasmScheduler)state!).RunDrain(),
            this,
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);
    }

    /// <summary>Represents queued work that disposal cancels before execution.</summary>
    internal interface IReadyWorkItem : IDisposable
    {
        /// <summary>Runs the scheduled action unless cancelled.</summary>
        void Run();
    }

    /// <summary>Gets the shared WebAssembly scheduler.</summary>
    public static WasmScheduler Default { get; } = new();

    /// <summary>Gets a value indicating whether the scheduler has been disposed.</summary>
    private bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

    /// <summary>Schedules an action to be executed on the next event-loop turn.</summary>
    /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
    /// <param name="state">State passed to the action.</param>
    /// <param name="action">Action to execute.</param>
    /// <returns>The disposable used to cancel the scheduled action.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    /// <exception cref="ObjectDisposedException">The scheduler has been disposed.</exception>
    public override IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);
        ObjectDisposedExceptionHelper.ThrowIf(IsDisposed, this);

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
    /// <exception cref="ObjectDisposedException">The scheduler has been disposed.</exception>
    public override IDisposable Schedule<TState>(
        TState state,
        TimeSpan dueTime,
        Func<IScheduler, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);
        ObjectDisposedExceptionHelper.ThrowIf(IsDisposed, this);

        var dt = Scheduler.Normalize(dueTime);
        if (dt == TimeSpan.Zero)
        {
            return Schedule(state, action);
        }

        var item = new StatefulWorkItem<TState>(this, state, action);

        item.AttachTimer(CreateTimer(_timeProvider, static s => ((IReadyWorkItem)s!).Run(), item, dt, Timeout.InfiniteTimeSpan));
        return item;
    }

    /// <summary>Schedules a periodic action, clamping periods below one millisecond to one millisecond.</summary>
    /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
    /// <param name="state">Initial state passed to the action upon the first iteration.</param>
    /// <param name="period">Period for running the work periodically.</param>
    /// <param name="action">Action to be executed, potentially updating the state.</param>
    /// <returns>The disposable used to cancel the scheduled recurring action.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="period"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The scheduler has been disposed.</exception>
    public IDisposable SchedulePeriodic<TState>(TState state, TimeSpan period, Func<TState, TState> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);
        ObjectDisposedExceptionHelper.ThrowIf(IsDisposed, this);

        if (period < OneMillisecond)
        {
            period = OneMillisecond;
        }

        return PeriodicWorkItem<TState>.Start(state, period, action, _timeProvider);
    }

    /// <summary>Cancels queued immediate work and rejects further scheduling, allowing running work to finish.</summary>
    /// <remarks>Delayed work remains owned by its returned cancellation handle.</remarks>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
        {
            return;
        }

        _drainTimer.Dispose();
        ReleaseReady();
    }

    /// <summary>Enqueues work and requests a drain, releasing the item if disposal overlaps.</summary>
    /// <param name="item">Work item to execute on the next event-loop turn.</param>
    internal void Enqueue(IReadyWorkItem item)
    {
        QueueReady(item);
        PostDrain();

        if (!IsDisposed)
        {
            return;
        }

        ReleaseReady();
    }

    /// <summary>Adds ready work without requesting a drain.</summary>
    /// <param name="item">Work item to add to the ready queue.</param>
    internal void QueueReady(IReadyWorkItem item)
    {
        _ready.Enqueue(item);
        _ = Interlocked.Increment(ref _readyCount);
    }

    /// <summary>Runs the ready items in one batch.</summary>
    internal void RunReadyBatch()
    {
        for (var remaining = Volatile.Read(ref _readyCount);
             remaining > 0 && _ready.TryDequeue(out var item);
             remaining--)
        {
            _ = Interlocked.Decrement(ref _readyCount);
            item.Run();
        }
    }

    /// <summary>Claims a drain, or requests another pass when the observed state has not changed.</summary>
    /// <param name="observedState">The drain state observed before attempting the transition.</param>
    /// <returns>True when no further claim attempt is needed; false when the observed state changed.</returns>
    internal bool TryPostDrain(int observedState)
    {
        if (Volatile.Read(ref _readyCount) == 0)
        {
            return true;
        }

        if (observedState != DrainIdle)
        {
            return Interlocked.CompareExchange(ref _drainState, DrainRunningPending, observedState) == observedState;
        }

        if (Interlocked.CompareExchange(ref _drainState, DrainRunning, DrainIdle) != DrainIdle)
        {
            return false;
        }

        ArmDrain();
        return true;
    }

    /// <summary>Registers a timer callback with the supplied provider.</summary>
    /// <param name="provider">The timer provider.</param>
    /// <param name="callback">The callback invoked when due.</param>
    /// <param name="state">The callback state.</param>
    /// <param name="dueTime">The initial delay.</param>
    /// <param name="period">The repeat interval.</param>
    /// <returns>The timer's cancellation handle.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ITimer CreateTimer(TimeProvider provider, TimerCallback callback, object state, TimeSpan dueTime, TimeSpan period) =>
        provider.CreateTimer(callback, state, dueTime, period);

    /// <summary>Updates a timer's next firing and repeat interval.</summary>
    /// <param name="timer">The timer to update.</param>
    /// <param name="dueTime">The initial delay.</param>
    /// <param name="period">The repeat interval.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ChangeTimer(ITimer timer, TimeSpan dueTime, TimeSpan period) => _ = timer.Change(dueTime, period);

    /// <summary>Cancels and removes every queued work item.</summary>
    private void ReleaseReady()
    {
        while (_ready.TryDequeue(out var item))
        {
            _ = Interlocked.Decrement(ref _readyCount);
            item.Dispose();
        }
    }

    /// <summary>Retries drain claims after competing state updates.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private void PostDrain()
    {
        int state;
        do
        {
            state = Volatile.Read(ref _drainState);
        }
        while (!TryPostDrain(state));
    }

    /// <summary>Yields the claimed drain batch to the event loop, or hands the latch back when disposal beat it.</summary>
    private void ArmDrain()
    {
        if (IsDisposed)
        {
            Volatile.Write(ref _drainState, DrainIdle);
            return;
        }

        ChangeTimer(_drainTimer, TimeSpan.Zero, Timeout.InfiniteTimeSpan);
    }

    /// <summary>Drains queued batches until no further pass is requested.</summary>
    private void RunDrain()
    {
        do
        {
            Volatile.Write(ref _drainState, DrainRunning);
            RunReadyBatch();
        }
        while (Interlocked.CompareExchange(ref _drainState, DrainIdle, DrainRunning) != DrainRunning);

        if (Volatile.Read(ref _readyCount) == 0)
        {
            return;
        }

        PostDrain();
    }

    /// <summary>Owns scheduled work and its optional one-shot timer.</summary>
    /// <typeparam name="TState">The scheduled state type.</typeparam>
    internal sealed class StatefulWorkItem<TState> : DispatchWorkItemBase<TState>, IReadyWorkItem
    {
        /// <summary>The delayed item's timer release handle, or null for immediate work.</summary>
        private IDisposable? _timer;

        /// <summary>Initializes a new instance of the <see cref="StatefulWorkItem{TState}"/> class.</summary>
        /// <param name="scheduler">The scheduler passed back to the scheduled action.</param>
        /// <param name="state">Scheduled state.</param>
        /// <param name="action">Scheduled action.</param>
        public StatefulWorkItem(WasmScheduler scheduler, TState state, Func<IScheduler, TState, IDisposable> action)
            : base(scheduler, state, action)
        {
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (!TryClaimDispose())
            {
                return;
            }

            Interlocked.Exchange(ref _timer, null)?.Dispose();
            ReleaseStartedWork();
        }

        /// <summary>Stores the one-shot timer so the caller's disposable cancels and releases it.</summary>
        /// <param name="timer">The armed timer.</param>
        internal void AttachTimer(IDisposable timer)
        {
            Volatile.Write(ref _timer, timer);
            ReleaseCanceledTimer();
        }

        /// <summary>Releases the attached timer when the work item is cancelled.</summary>
        internal void ReleaseCanceledTimer()
        {
            if (!IsDisposed)
            {
                return;
            }

            Interlocked.Exchange(ref _timer, null)?.Dispose();
        }
    }

    /// <summary>Periodic work driven by a timer; ticks are serialized under a gate.</summary>
    /// <typeparam name="TState">The scheduled state type.</typeparam>
    internal sealed class PeriodicWorkItem<TState> : IDisposable
    {
        /// <summary>Serializes ticks and guards state transitions.</summary>
        private readonly Lock _gate = new();

        /// <summary>Scheduled action.</summary>
        private readonly Func<TState, TState> _action;

        /// <summary>Periodic timer, attached after construction and rooted by its callback while armed.</summary>
        private ITimer? _timer;

        /// <summary>State threaded through the periodic action.</summary>
        private TState _state;

        /// <summary>Tracks cancellation.</summary>
        private bool _isDisposed;

        /// <summary>Initializes a new instance of the <see cref="PeriodicWorkItem{TState}"/> class.</summary>
        /// <param name="state">Initial state.</param>
        /// <param name="action">Scheduled action.</param>
        private PeriodicWorkItem(TState state, Func<TState, TState> action)
        {
            _state = state;
            _action = action;
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

                _timer!.Dispose();
                _timer = null;
                _state = default!;
            }
        }

        /// <summary>Publishes the periodic item and its timer before enabling ticks.</summary>
        /// <param name="state">Initial state.</param>
        /// <param name="period">Tick period.</param>
        /// <param name="action">Scheduled action.</param>
        /// <param name="timeProvider">Timer provider.</param>
        /// <returns>The armed periodic work item, which cancels the ticks when disposed.</returns>
        internal static PeriodicWorkItem<TState> Start(
            TState state,
            TimeSpan period,
            Func<TState, TState> action,
            TimeProvider timeProvider)
        {
            PeriodicWorkItem<TState> item = new(state, action);
            var timer = CreateTimer(
                timeProvider,
                static s => ((PeriodicWorkItem<TState>)s!).Tick(),
                item,
                Timeout.InfiniteTimeSpan,
                Timeout.InfiniteTimeSpan);

            item._timer = timer;
            ChangeTimer(timer, period, period);
            return item;
        }

        /// <summary>Runs one periodic tick unless the work item is disposed.</summary>
        internal void Tick()
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
