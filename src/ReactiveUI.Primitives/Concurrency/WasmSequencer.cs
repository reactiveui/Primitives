// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using Timer = System.Threading.Timer;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Task-pool replacement for single-threaded event-loop runtimes such as browser WebAssembly: it never starts
/// threads and never blocks. Immediate work is batched one drain per event-loop turn through a zero-due timer
/// (a <c>setTimeout(0)</c> macrotask on WebAssembly, so the browser can render between batches); delayed work
/// uses the shared timer, which the WebAssembly runtime backs with the JS event loop.
/// </summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class WasmSequencer : ISequencer, IDisposable
{
    /// <summary>
    /// Guards the drain timer. Every arm of the timer goes through <see cref="Post"/>, which takes this gate, and
    /// <see cref="Dispose"/> releases the timer while holding it — so the timer can never be armed after it is gone.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>One-shot timer used to yield a drain to the event loop.</summary>
    private readonly Timer _timer;

    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>
    /// Non-zero once <see cref="Dispose"/> has released the drain timer and the ready queue. Written under
    /// <see cref="_gate"/> so every path that touches the timer is ordered against disposal, but read without it
    /// on the scheduling paths, which re-check it after enqueueing rather than holding the gate across a queue.
    /// </summary>
    private int _isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="WasmSequencer"/> class. Callers use <see cref="Default"/>; this is
    /// internal so a test can own an isolated sequencer it may dispose without shutting the shared singleton down for
    /// every other test.
    /// </summary>
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
    /// Releases the drain timer this sequencer owns and cancels the ready work still queued behind it. Scheduling
    /// through a disposed sequencer throws <see cref="ObjectDisposedException"/> rather than queueing work no drain
    /// will ever reach. Delayed work still parked on the shared timer is released when it comes due, because the
    /// caller cancels it through the handle it was given rather than through this sequencer.
    /// </summary>
    public void Dispose()
    {
        // Under the gate: every arm of the timer takes it too, so the timer can never be re-armed after it is
        // released here. Timer.Dispose does not wait for an in-flight callback, so a drain blocked on the gate
        // inside Post cannot deadlock this — it simply observes the disposed flag once it gets in, and backs off.
        lock (_gate)
        {
            if (IsDisposed)
            {
                return;
            }

            Volatile.Write(ref _isDisposed, 1);
            _timer.Dispose();
        }

        _state.ReleaseQueued();
    }

    /// <summary>
    /// Enqueues ready work onto the drain without the disposed guard, releasing it again when a disposal raced the
    /// enqueue. Internal rather than private so a test can drive the enqueue that was already past
    /// <see cref="Schedule(IWorkItem)"/>'s disposed check when the disposal drained the ready queue, and prove the
    /// item is handed back rather than stranded.
    /// </summary>
    /// <param name="item">Work item to execute on the next event-loop turn.</param>
    internal void ScheduleReady(IWorkItem item)
    {
        _state.Schedule(item);
        ReleaseQueuedIfDisposed();
    }

    /// <summary>Arms the drain timer to fire on the next event-loop turn.</summary>
    /// <param name="_">
    /// Ignored. The parameter exists only because <see cref="DispatchSequencerState"/> posts through a
    /// <see cref="Func{T, TResult}"/> of <see cref="Action"/>; the drain callback is already carried by the timer's state.
    /// </param>
    /// <returns><see langword="true"/> when the timer accepted the change.</returns>
    private bool Post(Action _)
    {
        lock (_gate)
        {
            // Arming a released timer is a silent no-op that would leave the drain latch set on a drain that can
            // never run. Refusing the post instead lets the engine hand the latch straight back; with the ready
            // queue released and scheduling closed, there is nothing left for that drain to do anyway.
            return !IsDisposed && _timer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);
        }
    }

    /// <summary>
    /// Marshals delayed work back onto this sequencer's drain once the shared timer says it is due. This replaces the
    /// engine's default marshal step, which would call back through <see cref="Schedule(IWorkItem)"/> and throw
    /// <see cref="ObjectDisposedException"/> on the timer's thread for an item that came due after disposal.
    /// </summary>
    /// <param name="item">Work item to run once it is due.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ScheduleDelayed(IWorkItem item, long dueTimestamp) =>
        ThreadPoolSequencer.Instance.Schedule(new DelayedWorkItem(this, item), dueTimestamp);

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

    /// <summary>
    /// Delayed work held by the shared timer until it comes due, then marshalled onto the owner's drain. A sequencer
    /// disposed while this waits can no longer drain anything, so the item is released to its caller instead of being
    /// pushed into a queue that will never move again.
    /// </summary>
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

        /// <summary>Cancels the marshalled item, handing it back to the caller that still holds it.</summary>
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
