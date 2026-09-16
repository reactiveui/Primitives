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
