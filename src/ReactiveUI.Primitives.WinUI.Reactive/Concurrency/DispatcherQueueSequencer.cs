// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using Microsoft.UI.Dispatching;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>WinUI dispatcher queue scheduler that coalesces scheduled work through a <see cref="DispatcherQueue"/>.</summary>
/// <remarks>Callbacks run on the dispatcher queue thread; cancellation stops pending timers and suppresses unstarted actions.</remarks>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("DispatcherQueueSequencer: DispatcherQueue = {DispatcherQueue}, Priority = {Priority}")]
public sealed class DispatcherQueueSequencer : LocalScheduler
{
    /// <summary>The shared main-thread scheduler, set once a UI thread first reads it.</summary>
    private static DispatcherQueueSequencer? _main;

    /// <summary>The scheduler for the calling thread's dispatcher queue, cached per thread.</summary>
    [ThreadStatic]
    private static DispatcherQueueSequencer? _current;

    /// <summary>Optional callback for enqueueing native drain delegates.</summary>
    private readonly Func<DispatcherQueuePriority, DispatcherQueueHandler, bool>? _tryEnqueue;

    /// <summary>Optional callback for delayed work.</summary>
    private readonly Func<Action, TimeSpan, IDisposable>? _scheduleDelayed;

    /// <summary>Queues work and coalesces dispatcher queue drains.</summary>
    private CoalescingDispatchState _dispatch;

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
        _dispatch = new(RunDrain, DefaultScheduler.Instance);
    }

    /// <summary>Initializes a new instance of the <see cref="DispatcherQueueSequencer"/> class.</summary>
    /// <param name="priority">Priority passed to the enqueue callback.</param>
    /// <param name="tryEnqueue">Attempts to enqueue each drain.</param>
    /// <param name="scheduleDelayed">Schedules delayed work.</param>
    internal DispatcherQueueSequencer(
        DispatcherQueuePriority priority,
        Func<DispatcherQueuePriority, DispatcherQueueHandler, bool> tryEnqueue,
        Func<Action, TimeSpan, IDisposable> scheduleDelayed)
    {
        DispatcherQueue = null!;
        Priority = priority;
        _tryEnqueue = tryEnqueue;
        _scheduleDelayed = scheduleDelayed;
        _dispatch = new(RunDrain, DefaultScheduler.Instance);
    }

    /// <summary>Gets the shared scheduler for the WinUI main (UI) thread.</summary>
    /// <exception cref="InvalidOperationException">Main is not bound yet and the calling thread has no dispatcher queue.</exception>
    /// <remarks>
    /// WinUI has no dispatcher queue that any thread can reach, so the first read from a UI thread binds Main to that
    /// thread's dispatcher queue. After that, every thread gets the same scheduler. A read from a thread without a
    /// dispatcher queue before then throws, and nothing is cached, so a later read from the UI thread still binds.
    /// </remarks>
    public static DispatcherQueueSequencer Main =>
        Volatile.Read(ref _main) ?? BindMain(ref _main, DispatcherQueue.GetForCurrentThread()) ?? throw NoDispatcherQueue();

    /// <summary>Gets the scheduler for the calling thread's dispatcher queue.</summary>
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
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The dispatcher queue rejected the work.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action) =>
        _dispatch.Schedule(new DispatchHost(this), this, state, action);

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The dispatcher queue rejected the work.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action) =>
        _dispatch.Schedule(new DispatchHost(this), this, state, dueTime, action);

    /// <summary>Caches the shared main-thread scheduler for a dispatcher queue, keeping the first one bound.</summary>
    /// <param name="slot">The field that holds the shared scheduler.</param>
    /// <param name="dispatcherQueue">The calling thread's dispatcher queue, or <see langword="null"/> when it has none.</param>
    /// <returns>The shared scheduler, or <see langword="null"/> when <paramref name="dispatcherQueue"/> is <see langword="null"/>.</returns>
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

    /// <summary>Enqueues the drain callback on the dispatcher queue.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns>Always <see langword="true"/>.</returns>
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

    /// <summary>Schedules delayed work on a dispatcher queue timer, or through the test hook when one was supplied.</summary>
    /// <param name="work">The callback to run.</param>
    /// <param name="dueTime">The requested delay.</param>
    /// <returns>The timer cancellation handle.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime) =>
        _scheduleDelayed is null ? StartDispatcherTimer(work, dueTime) : _scheduleDelayed(work, dueTime);

    /// <summary>Runs one dispatcher queue batch.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RunDrain() => _dispatch.RunDrain(new DispatchHost(this));

    /// <summary>Schedules a cancellable native dispatcher timer.</summary>
    /// <param name="work">The callback to run.</param>
    /// <param name="dueTime">The requested delay.</param>
    /// <returns>The timer cancellation handle.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private IDisposable StartDispatcherTimer(Action work, TimeSpan dueTime)
    {
        var timer = DispatcherQueue.CreateTimer();
        timer.Interval = dueTime;
        timer.IsRepeating = false;
        timer.Tick += (_, _) =>
        {
            timer.Stop();
            work();
        };
        timer.Start();
        return Disposable.Create(timer, static t => t.Stop());
    }

    /// <summary>Attempts a native dispatcher queue post.</summary>
    /// <param name="handler">The callback to enqueue.</param>
    /// <returns>Whether the dispatcher accepted the callback.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryEnqueue(DispatcherQueueHandler handler) => DispatcherQueue.TryEnqueue(Priority, handler);

    /// <summary>Reaches this scheduler's dispatcher queue for its dispatch state.</summary>
    /// <param name="Owner">The scheduler.</param>
    private readonly record struct DispatchHost(DispatcherQueueSequencer Owner) : IDispatchHost
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Post(Action drain) => Owner.Post(drain);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime) => Owner.ScheduleOnDispatcher(work, dueTime);
    }
}
