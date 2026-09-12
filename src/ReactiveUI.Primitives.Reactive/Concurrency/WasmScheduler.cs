// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>
/// Schedules work on a single-threaded event loop without blocking or starting threads. Immediate work runs in
/// batches between event-loop turns; delayed and periodic work use timers. Long-running scheduling is unsupported.
/// </summary>
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
        _drainTimer = _timeProvider.CreateTimer(
            static state => ((WasmScheduler)state!).RunDrain(),
            this,
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);
    }

    /// <summary>A queued work item awaiting an event-loop drain or a one-shot timer. Disposing it cancels it.</summary>
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

        // The timer roots itself while armed through the callback's target (the work item), which stores the
        // timer; the item's Dispose cancels and releases it.
        item.AttachTimer(_timeProvider.CreateTimer(static s => ((IReadyWorkItem)s!).Run(), item, dt, Timeout.InfiniteTimeSpan));
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

    /// <summary>
    /// Releases the drain timer and cancels queued work. Subsequent scheduling throws ObjectDisposedException.
    /// Running work completes; callers retain responsibility for cancelling delayed work through its returned handle.
    /// </summary>
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

        // Release work enqueued after disposal drained the queue.
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
        // Release the drain claim when disposal prevents arming the timer.
        if (IsDisposed)
        {
            Volatile.Write(ref _drainState, DrainIdle);
            return;
        }

        _ = _drainTimer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);
    }

    /// <summary>Drains queued batches until no further pass is requested.</summary>
    private void RunDrain()
    {
        do
        {
            // Claim this pass; a concurrent PostDrain that observes DrainRunning will bump it to DrainRunningPending.
            Volatile.Write(ref _drainState, DrainRunning);
            RunReadyBatch();

            // Finish only when no work was flagged during this pass.
        }
        while (Interlocked.CompareExchange(ref _drainState, DrainIdle, DrainRunning) != DrainRunning);

        if (Volatile.Read(ref _readyCount) == 0)
        {
            return;
        }

        // Cover the narrow window where an item was enqueued but its PostDrain has not run yet.
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
            // Publish cancellation before reclaiming timers so a concurrent attachment releases its handle.
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
            if (!IsDisposed)
            {
                return;
            }

            timer.Dispose();
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

                // Start assigns the timer before returning; only the first Dispose reaches this point.
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
            var timer = timeProvider.CreateTimer(
                static s => ((PeriodicWorkItem<TState>)s!).Tick(),
                item,
                Timeout.InfiniteTimeSpan,
                Timeout.InfiniteTimeSpan);

            item._timer = timer;
            _ = timer.Change(period, period);
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
