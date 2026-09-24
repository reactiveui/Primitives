// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.UI.Dispatching;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>WinUI dispatcher queue sequencer that coalesces scheduled work through a <see cref="DispatcherQueue"/>.</summary>
/// <remarks>Callbacks run in posted dispatcher queue batches without inline reentrancy; cancellation suppresses unstarted work.</remarks>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class DispatcherQueueSequencer : ISequencer
{
    /// <summary>The shared main-thread sequencer, set once a UI thread first reads it.</summary>
    private static DispatcherQueueSequencer? _main;

    /// <summary>The sequencer for the calling thread's dispatcher queue, cached per thread.</summary>
    [ThreadStatic]
    private static DispatcherQueueSequencer? _current;

    /// <summary>Optional callback for enqueueing native drain delegates.</summary>
    private readonly Func<DispatcherQueuePriority, DispatcherQueueHandler, bool>? _tryEnqueue;

    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>Cached dispatcher queue handler used for the drain.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Maintainability",
        "SST1422:Move this field into the method that uses it",
        Justification = "The handler delegate is cached across every post, so it cannot be a method local.")]
    private DispatcherQueueHandler? _handler;

    /// <summary>Initializes a new instance of the <see cref="DispatcherQueueSequencer"/> class.</summary>
    /// <param name="dispatcherQueue">The dispatcher queue used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcherQueue"/> is <see langword="null"/>.</exception>
    public DispatcherQueueSequencer(DispatcherQueue dispatcherQueue)
        : this(dispatcherQueue, DispatcherQueuePriority.Normal)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DispatcherQueueSequencer"/> class.</summary>
    /// <param name="dispatcherQueue">The dispatcher queue used to marshal work to the UI thread.</param>
    /// <param name="priority">Dispatcher queue priority used for posted drains.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcherQueue"/> is <see langword="null"/>.</exception>
    public DispatcherQueueSequencer(DispatcherQueue dispatcherQueue, DispatcherQueuePriority priority)
    {
        DispatcherQueue = dispatcherQueue ?? throw new ArgumentNullException(nameof(dispatcherQueue));
        Priority = priority;
        _state = new(this, Post, RunDrain, ScheduleDelayed);
    }

    /// <summary>Initializes a new instance of the <see cref="DispatcherQueueSequencer"/> class.</summary>
    /// <param name="priority">Priority passed to the enqueue callback.</param>
    /// <param name="tryEnqueue">Attempts to enqueue each drain.</param>
    /// <param name="scheduleDelayed">Schedules delayed work.</param>
    internal DispatcherQueueSequencer(
        DispatcherQueuePriority priority,
        Func<DispatcherQueuePriority, DispatcherQueueHandler, bool> tryEnqueue,
        Action<IWorkItem, long> scheduleDelayed)
    {
        DispatcherQueue = null!;
        Priority = priority;
        _tryEnqueue = tryEnqueue;
        _state = new(this, Post, RunDrain, scheduleDelayed);
    }

    /// <summary>Gets the shared sequencer for the WinUI main (UI) thread.</summary>
    /// <exception cref="InvalidOperationException">Main is not bound yet and the calling thread has no dispatcher queue.</exception>
    /// <remarks>
    /// WinUI has no dispatcher queue that any thread can reach, so the first read from a UI thread binds Main to that
    /// thread's dispatcher queue. After that, every thread gets the same sequencer. A read from a thread without a
    /// dispatcher queue before then throws, and nothing is cached, so a later read from the UI thread still binds.
    /// </remarks>
    public static DispatcherQueueSequencer Main =>
        Volatile.Read(ref _main) ?? BindMain(ref _main, DispatcherQueue.GetForCurrentThread()) ?? throw NoDispatcherQueue();

    /// <summary>Gets the sequencer for the calling thread's dispatcher queue.</summary>
    /// <exception cref="InvalidOperationException">The calling thread has no dispatcher queue.</exception>
    /// <remarks>
    /// Cached per thread, for applications that run UI on more than one thread. Never creates a dispatcher queue: a
    /// thread that has none cannot run the scheduled work.
    /// </remarks>
    public static DispatcherQueueSequencer Current =>
        _current ??= new(DispatcherQueue.GetForCurrentThread() ?? throw NoDispatcherQueue());

    /// <summary>Gets the dispatcher queue used to marshal work to the UI thread.</summary>
    public DispatcherQueue DispatcherQueue { get; }

    /// <summary>Gets the dispatcher queue priority used for posted drains.</summary>
    public DispatcherQueuePriority Priority { get; }

    /// <inheritdoc/>
    public DateTimeOffset Now => DispatchSequencerState.Now;

    /// <inheritdoc/>
    public long Timestamp => DispatchSequencerState.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    internal string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item) => _state.Schedule(item);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

    /// <summary>Caches the shared main-thread sequencer for a dispatcher queue, keeping the first one bound.</summary>
    /// <param name="slot">The field that holds the shared sequencer.</param>
    /// <param name="dispatcherQueue">The calling thread's dispatcher queue, or <see langword="null"/> when it has none.</param>
    /// <returns>The shared sequencer, or <see langword="null"/> when <paramref name="dispatcherQueue"/> is <see langword="null"/>.</returns>
    internal static DispatcherQueueSequencer? BindMain(ref DispatcherQueueSequencer? slot, DispatcherQueue? dispatcherQueue)
    {
        if (dispatcherQueue is null)
        {
            return null;
        }

        DispatcherQueueSequencer created = new(dispatcherQueue);
        return Interlocked.CompareExchange(ref slot, created, null) ?? created;
    }

    /// <summary>Creates the error for a thread that has no dispatcher queue.</summary>
    /// <returns>The exception to throw.</returns>
    internal static InvalidOperationException NoDispatcherQueue() =>
        new("The calling thread has no WinUI dispatcher queue. Use DispatcherQueueSequencer.Main or Current from a UI thread.");

    /// <summary>Marshals the cached drain callback through the dispatcher queue.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns><see langword="true"/> when the drain was enqueued.</returns>
    /// <exception cref="InvalidOperationException">The dispatcher queue rejected the work.</exception>
    private bool Post(Action drain)
    {
        _handler ??= drain.Invoke;
        if (_tryEnqueue is null ? TryEnqueue(_handler) : _tryEnqueue(Priority, _handler))
        {
            return true;
        }

        throw new InvalidOperationException("The dispatcher queue is no longer accepting work.");
    }

    /// <summary>Attempts a native dispatcher queue post.</summary>
    /// <param name="handler">The callback to enqueue.</param>
    /// <returns>Whether the dispatcher accepted the callback.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryEnqueue(DispatcherQueueHandler handler) => DispatcherQueue.TryEnqueue(Priority, handler);

    /// <summary>Runs delayed work on a dispatcher queue timer so it executes directly on the dispatcher thread.</summary>
    /// <param name="item">Work item to execute at the due time.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private void ScheduleDelayed(IWorkItem item, long dueTimestamp)
    {
        var timer = DispatcherQueue.CreateTimer();
        timer.Interval = DispatchSequencerState.DelayUntil(dueTimestamp);
        timer.IsRepeating = false;
        timer.Tick += (_, _) =>
        {
            timer.Stop();
            DispatchSequencerState.RunIfActive(item);
        };
        timer.Start();
    }

    /// <summary>Runs one queued batch on the coalescing engine.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RunDrain() => _state.RunDrain();
}
