// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Schedules immediate batches and delayed work on a single-threaded event loop.</summary>
/// <remarks>Immediate batches yield between event-loop turns.</remarks>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class WasmSequencer : ISequencer, IDisposable
{
    /// <summary>Serializes timer arming and disposal.</summary>
    private readonly Lock _gate = new();

    /// <summary>The clock supplying time and the drain timer.</summary>
    private readonly SequencerClock _clock;

    /// <summary>Posts a drain to the event loop.</summary>
    private readonly Func<Action, bool> _postDrain;

    /// <summary>Schedules the delayed marshal callback.</summary>
    private readonly Action<IWorkItem, long> _scheduleDelayed;

    /// <summary>One-shot timer used to yield a drain to the event loop, created when the first drain is posted.</summary>
    private ITimer? _timer;

    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>Non-zero after disposal releases the timer and queue; timer access is serialized by the gate.</summary>
    private int _isDisposed;

    /// <summary>Initializes a new instance of the <see cref="WasmSequencer"/> class that reads time and arms its timers through a <see cref="TimeProvider"/>.</summary>
    /// <param name="timeProvider">The provider supplying the current time, timestamps and timers.</param>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider"/> is <see langword="null"/>.</exception>
    public WasmSequencer(TimeProvider timeProvider)
        : this(new SequencerClock(timeProvider))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WasmSequencer"/> class.</summary>
    internal WasmSequencer()
        : this(SequencerClock.Default, ThreadPoolSequencer.Instance)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WasmSequencer"/> class.</summary>
    /// <param name="postDrain">Posts a drain to the event loop.</param>
    /// <param name="scheduleDelayed">Schedules the delayed marshal callback.</param>
    internal WasmSequencer(Func<Action, bool> postDrain, Action<IWorkItem, long> scheduleDelayed)
    {
        _clock = SequencerClock.Default;
        _postDrain = postDrain;
        _scheduleDelayed = scheduleDelayed;
        _state = new(this, Post, RunDrain, ScheduleDelayed);
    }

    /// <summary>Initializes a new instance of the <see cref="WasmSequencer"/> class whose delay queue shares its clock.</summary>
    /// <param name="clock">The clock supplying time and timers.</param>
    private WasmSequencer(SequencerClock clock)
        : this(clock, new ThreadPoolSequencer(clock))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WasmSequencer"/> class.</summary>
    /// <param name="clock">The clock supplying time and the drain timer.</param>
    /// <param name="delaySequencer">The sequencer whose timestamps use <paramref name="clock"/> and that delivers delayed callbacks.</param>
    private WasmSequencer(SequencerClock clock, ISequencer delaySequencer)
    {
        _clock = clock;
        _postDrain = ArmDrainTimer;
        _scheduleDelayed = delaySequencer.Schedule;
        _state = new(this, Post, RunDrain, ScheduleDelayed, delaySequencer, clock);
    }

    /// <summary>Gets the shared WebAssembly sequencer.</summary>
    public static WasmSequencer Default { get; } = new();

    /// <inheritdoc/>
    public DateTimeOffset Now => _clock.GetUtcNow();

    /// <inheritdoc/>
    public long Timestamp => _clock.GetTimestamp();

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Gets a value indicating whether the sequencer has been disposed.</summary>
    private bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

    /// <inheritdoc/>
    /// <exception cref="ObjectDisposedException">The sequencer has been disposed.</exception>
    public void Schedule(IWorkItem item)
    {
        ObjectDisposedExceptionHelper.ThrowIf(IsDisposed, this);

        ScheduleReady(item);
    }

    /// <inheritdoc/>
    /// <exception cref="ObjectDisposedException">The sequencer has been disposed.</exception>
    public void Schedule(IWorkItem item, long dueTimestamp)
    {
        ObjectDisposedExceptionHelper.ThrowIf(IsDisposed, this);

        _state.Schedule(item, dueTimestamp);
        ReleaseQueuedIfDisposed();
    }

    /// <summary>Cancels queued immediate work and rejects further scheduling.</summary>
    /// <remarks>Delayed work is released when due unless its caller cancels it first.</remarks>
    public void Dispose()
    {
        lock (_gate)
        {
            if (IsDisposed)
            {
                return;
            }

            Volatile.Write(ref _isDisposed, 1);
            _timer?.Dispose();
        }

        _state.ReleaseQueued();
    }

    /// <summary>Queues ready work and releases it if disposal overlaps the enqueue.</summary>
    /// <param name="item">Work item to execute on the next event-loop turn.</param>
    internal void ScheduleReady(IWorkItem item)
    {
        _state.Schedule(item);
        ReleaseQueuedIfDisposed();
    }

    /// <summary>Arms the drain timer to fire on the next event-loop turn.</summary>
    /// <param name="drain">The callback to post.</param>
    /// <returns><see langword="true"/> when the timer accepted the change.</returns>
    private bool Post(Action drain)
    {
        lock (_gate)
        {
            return !IsDisposed && _postDrain(drain);
        }
    }

    /// <summary>Returns due work to the drain, releasing it if the sequencer is disposed.</summary>
    /// <param name="item">Work item to run once it is due.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ScheduleDelayed(IWorkItem item, long dueTimestamp) =>
        _scheduleDelayed(new DelayedWorkItem(this, item), dueTimestamp);

    /// <summary>Arms the drain timer, creating it disarmed on first use.</summary>
    /// <param name="drain">The cached callback carried by the timer.</param>
    /// <returns>Whether the timer accepted the callback.</returns>
    private bool ArmDrainTimer(Action drain)
    {
        _timer ??= _clock.CreateTimer(
            static state => ((WasmSequencer)state!).RunDrain(),
            this,
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);
        return _timer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);
    }

    /// <summary>Forwards the cached drain callback to the engine.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RunDrain() => _state.RunDrain();

    /// <summary>Releases the ready queue when a disposal raced the enqueue that just happened.</summary>
    private void ReleaseQueuedIfDisposed()
    {
        if (!IsDisposed)
        {
            return;
        }

        _state.ReleaseQueued();
    }

    /// <summary>Requeues work when due, or releases it if the sequencer is disposed.</summary>
    /// <param name="owner">The sequencer whose drain runs the item.</param>
    /// <param name="item">The work item to marshal.</param>
    private sealed class DelayedWorkItem(WasmSequencer owner, IWorkItem item) : IWorkItem
    {
        /// <summary>The sequencer whose drain runs the item.</summary>
        private readonly WasmSequencer _owner = owner;

        /// <summary>The work item to marshal.</summary>
        private readonly IWorkItem _item = item;

        /// <inheritdoc/>
        public void Execute()
        {
            if (Sequencer.IsCancelled(_item))
            {
                return;
            }

            if (_owner.IsDisposed)
            {
                Release();
                return;
            }

            _owner.ScheduleReady(_item);
        }

        /// <summary>Cancels the marshalled item, handing it back to the caller holding it.</summary>
        private void Release()
        {
            if (_item is not IDisposable cancellable)
            {
                return;
            }

            cancellable.Dispose();
        }
    }
}
