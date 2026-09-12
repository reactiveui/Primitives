// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using Timer = System.Threading.Timer;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Schedules batches on a single-threaded event loop without blocking or starting threads.
/// Delayed work uses the shared timer; immediate batches yield between event-loop turns.
/// </summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class WasmSequencer : ISequencer, IDisposable
{
    /// <summary>Serializes timer arming and disposal.</summary>
    private readonly Lock _gate = new();

    /// <summary>One-shot timer used to yield a drain to the event loop.</summary>
    private readonly Timer? _timer;

    /// <summary>Posts a drain to the event loop.</summary>
    private readonly Func<Action, bool> _postDrain;

    /// <summary>Schedules the delayed marshal callback.</summary>
    private readonly Action<IWorkItem, long> _scheduleDelayed;

    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>Non-zero after disposal releases the timer and queue; timer access is serialized by the gate.</summary>
    private int _isDisposed;

    /// <summary>Initializes a new instance of the <see cref="WasmSequencer"/> class.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Correctness",
        "SST2403:Do not let 'this' escape from a constructor",
        Justification =
            "The timer is created disarmed, and _state is a struct held inline in this object, so neither reference escapes.")]
    internal WasmSequencer()
    {
        _timer = new(
            static state => ((WasmSequencer)state!).RunDrain(),
            this,
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);
        _postDrain = ArmDrainTimer;
        _scheduleDelayed = ThreadPoolSequencer.Instance.Schedule;
        _state = new(this, Post, RunDrain, ScheduleDelayed);
    }

    /// <summary>Initializes a new instance of the <see cref="WasmSequencer"/> class.</summary>
    /// <param name="postDrain">Posts a drain to the event loop.</param>
    /// <param name="scheduleDelayed">Schedules the delayed marshal callback.</param>
    internal WasmSequencer(Func<Action, bool> postDrain, Action<IWorkItem, long> scheduleDelayed)
    {
        _postDrain = postDrain;
        _scheduleDelayed = scheduleDelayed;
        _state = new(this, Post, RunDrain, ScheduleDelayed);
    }

    /// <summary>Gets the shared WebAssembly sequencer.</summary>
    public static WasmSequencer Default { get; } = new();

    /// <inheritdoc/>
    public DateTimeOffset Now => DispatchSequencerState.Now;

    /// <inheritdoc/>
    public long Timestamp => DispatchSequencerState.Timestamp;

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

    /// <summary>
    /// Releases the drain timer and cancels queued work. Further scheduling throws.
    /// Delayed work on the shared timer is released when due unless its caller cancels it first.
    /// </summary>
    public void Dispose()
    {
        // Timer arming and disposal share the gate; disposal does not wait for active callbacks.
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
            // Reject posts after disposal so the drain claim is released.
            return !IsDisposed && _postDrain(drain);
        }
    }

    /// <summary>Returns due work to the drain, releasing it if the sequencer is disposed.</summary>
    /// <param name="item">Work item to run once it is due.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ScheduleDelayed(IWorkItem item, long dueTimestamp) =>
        _scheduleDelayed(new DelayedWorkItem(this, item), dueTimestamp);

    /// <summary>Arms the runtime drain timer.</summary>
    /// <param name="drain">The cached callback carried by the timer.</param>
    /// <returns>Whether the timer accepted the callback.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool ArmDrainTimer(Action drain) => _timer!.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);

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

            // A disposal racing this enqueue is caught by ScheduleReady, which releases the queue it just joined.
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
