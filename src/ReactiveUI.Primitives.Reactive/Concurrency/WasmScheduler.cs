// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reactive.Concurrency;
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
    private readonly Timer _drainTimer;

    /// <summary>Approximate number of ready items; snapshots a drain batch.</summary>
    private int _readyCount;

    /// <summary>
    /// Single-flight drain state: <c>0</c> idle, <c>1</c> a drain is running, <c>2</c> a drain is running and more
    /// work arrived while it ran. Keeping at most one drain in flight preserves the single-threaded, FIFO,
    /// one-drain-per-event-loop-turn semantics the type promises even though the backing timer may fire callbacks
    /// on more than one thread-pool thread.
    /// </summary>
    private int _drainState;

    /// <summary>Non-zero once <see cref="Dispose"/> has released the drain timer and the ready queue.</summary>
    private int _isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="WasmScheduler"/> class. Callers use <see cref="Default"/>; this is
    /// internal so a test can own an isolated scheduler it may dispose without shutting the shared singleton down for
    /// every other test.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Correctness",
        "SST2403:Do not let 'this' escape from a constructor",
        Justification =
            "The drain timer is created disarmed, so nothing can call back into it until Schedule arms it after construction.")]
    internal WasmScheduler() =>
        _drainTimer = new(
            static state => ((WasmScheduler)(
                state ?? throw new InvalidOperationException("The scheduler state is missing."))).RunDrain(),
            this,
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);

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
        item.AttachTimer(
            new(
                static state => ((IReadyWorkItem)(
                    state ?? throw new InvalidOperationException("The work-item state is missing."))).Run(),
                item,
                dt,
                Timeout.InfiniteTimeSpan));
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
        if (period < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(period));
        }

        ObjectDisposedExceptionHelper.ThrowIf(IsDisposed, this);

        if (period < OneMillisecond)
        {
            period = OneMillisecond;
        }

        return PeriodicWorkItem<TState>.Start(state, period, action);
    }

    /// <summary>
    /// Releases the drain timer this scheduler owns and cancels the ready work still queued behind it. Scheduling
    /// through a disposed scheduler throws <see cref="ObjectDisposedException"/> rather than queueing work no drain
    /// will ever reach. Work an in-flight drain has already dequeued runs to completion, and a delayed item that
    /// already owns its one-shot timer keeps it — the caller cancels those through the disposable it was handed.
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

    /// <summary>
    /// Enqueues immediate work and coalesces a single drain post. Internal rather than private so a test can drive the
    /// enqueue that was already past <see cref="Schedule{TState}(TState, Func{IScheduler, TState, IDisposable})"/>'s
    /// disposed check when disposal drained the ready queue, and prove the item is released rather than stranded.
    /// </summary>
    /// <param name="item">Work item to execute on the next event-loop turn.</param>
    internal void Enqueue(IReadyWorkItem item)
    {
        _ready.Enqueue(item);
        _ = Interlocked.Increment(ref _readyCount);
        PostDrain();

        // A disposal that raced the enqueue above may have drained the queue before this item joined it. Re-check
        // the flag the disposal published first, so the loser of that race releases the item instead of leaving it
        // queued behind a timer that can no longer fire.
        if (!IsDisposed)
        {
            return;
        }

        ReleaseReady();
    }

    /// <summary>
    /// Cancels and drops every ready item. The items are the handles their callers hold, so disposing them releases
    /// the caller's work instead of stranding it in a queue nothing will ever drain again.
    /// </summary>
    private void ReleaseReady()
    {
        while (_ready.TryDequeue(out var item))
        {
            _ = Interlocked.Decrement(ref _readyCount);
            item.Dispose();
        }
    }

    /// <summary>
    /// Arms a single drain if none is in flight, otherwise flags the running drain to loop again.
    /// <para>
    /// The whole body is the claim protocol: it spins only while a compare-exchange
    /// loses to a concurrent claim, and exits early only when a concurrent drain empties the queue between the
    /// caller's enqueue and this read. Neither path is reachable without a second thread interleaving, so the
    /// shell carries the coverage exclusion; the work it schedules lives in ArmDrain, which is covered.
    /// </para>
    /// </summary>
    [ExcludeFromCodeCoverage]
    private void PostDrain()
    {
        while (Volatile.Read(ref _readyCount) != 0)
        {
            var state = Volatile.Read(ref _drainState);
            if (state != DrainIdle)
            {
                // A drain is already running; flag that more work arrived so it drains again.
                if (Interlocked.CompareExchange(ref _drainState, DrainRunningPending, state) == state)
                {
                    return;
                }

                continue;
            }

            // Become the sole drainer, then yield a batch to the event loop.
            if (Interlocked.CompareExchange(ref _drainState, DrainRunning, DrainIdle) == DrainIdle)
            {
                ArmDrain();
                return;
            }
        }
    }

    /// <summary>Yields the claimed drain batch to the event loop, or hands the latch back when disposal beat it.</summary>
    private void ArmDrain()
    {
        // Arming a released timer is a silent no-op, so a claim made while the scheduler was being disposed would
        // leave the latch set on a drain that can never run. Hand the latch back instead: with scheduling closed and
        // the ready queue released, there is nothing left for that drain to do anyway.
        if (IsDisposed)
        {
            Volatile.Write(ref _drainState, DrainIdle);
            return;
        }

        _ = _drainTimer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);
    }

    /// <summary>
    /// Runs event-loop batches for the single in-flight drain until no more work is queued.
    /// <para>
    /// This is a thin batching shell around <see cref="RunReadyBatch"/>. It repeats a pass only when a concurrent
    /// <see cref="PostDrain"/> flagged more work mid-pass, which needs a second thread to interleave, so the shell
    /// carries the coverage exclusion and the per-item work lives in the method it calls.
    /// </para>
    /// </summary>
    [ExcludeFromCodeCoverage]
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

    /// <summary>Runs one batch: every item the ready count promised, stopping early if a concurrent drain took one first.</summary>
    private void RunReadyBatch()
    {
        for (var remaining = Volatile.Read(ref _readyCount);
             remaining > 0 && _ready.TryDequeue(out var item);
             remaining--)
        {
            _ = Interlocked.Decrement(ref _readyCount);
            item.Run();
        }
    }

    /// <summary>
    /// A cancellable scheduled work item carrying closure-free state and the scheduler passed back to the action;
    /// also the target that roots a delayed one-shot timer. The run/cancel handshake lives in the shared
    /// <see cref="DispatchWorkItemBase{TState}"/>; this item only adds the optional one-shot timer a delayed schedule
    /// attaches.
    /// </summary>
    /// <typeparam name="TState">The scheduled state type.</typeparam>
    internal sealed class StatefulWorkItem<TState> : DispatchWorkItemBase<TState>, IReadyWorkItem
    {
        /// <summary>Timer driving a delayed item; <see langword="null"/> for immediate work.</summary>
        private Timer? _timer;

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
            // Claim cancellation first so a racing AttachTimer observes the disposed state and releases the timer it
            // just stored, then reclaim any timer this item already owns and the disposable the action returned.
            if (!TryClaimDispose())
            {
                return;
            }

            Interlocked.Exchange(ref _timer, null)?.Dispose();
            ReleaseStartedWork();
        }

        /// <summary>Stores the one-shot timer so the caller's disposable cancels and releases it.</summary>
        /// <param name="timer">The armed timer.</param>
        internal void AttachTimer(Timer timer)
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

        /// <summary>
        /// Periodic timer; rooted through the tick callback's target while armed. Attached by <see cref="Start"/>
        /// once the item is fully constructed, so it is never <see langword="null"/> for an item a caller can see.
        /// </summary>
        private Timer? _timer;

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

                // Start is the only construction path and always assigns the timer before returning, and a second
                // Dispose exits at the flag above, so the timer is always present on the one pass that reaches here.
                _timer!.Dispose();
                _timer = null;
                _state = default!;
            }
        }

        /// <summary>
        /// Creates a periodic item and arms its timer. Arming it here rather than in the constructor is what keeps
        /// the tick callback from ever seeing a half-built item: the timer is created disarmed, attached, and only
        /// then started, so the first tick runs against an item whose fields are all published.
        /// </summary>
        /// <param name="state">Initial state.</param>
        /// <param name="period">Tick period.</param>
        /// <param name="action">Scheduled action.</param>
        /// <returns>The armed periodic work item, which cancels the ticks when disposed.</returns>
        internal static PeriodicWorkItem<TState> Start(TState state, TimeSpan period, Func<TState, TState> action)
        {
            PeriodicWorkItem<TState> item = new(state, action);
            Timer timer = new(
                static state => ((PeriodicWorkItem<TState>)(
                    state ?? throw new InvalidOperationException("The periodic state is missing."))).Tick(),
                item,
                Timeout.InfiniteTimeSpan,
                Timeout.InfiniteTimeSpan);

            item._timer = timer;
            _ = timer.Change(period, period);
            return item;
        }

        /// <summary>
        /// Runs one periodic tick. Internal rather than private so a test can drive the tick a timer callback already
        /// in flight would deliver after <see cref="Dispose"/> won the race, and prove the action does not run.
        /// </summary>
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
