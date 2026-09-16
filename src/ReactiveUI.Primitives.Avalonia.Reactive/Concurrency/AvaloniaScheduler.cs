// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using Avalonia.Threading;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Avalonia UI-thread scheduler that coalesces scheduled work onto a dispatcher drain.</summary>
/// <remarks>Callbacks run on the dispatcher thread at Priority; cancellation stops pending timers and suppresses unstarted actions.</remarks>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("AvaloniaScheduler: Dispatcher = {Dispatcher}, Priority = {Priority}")]
public sealed class AvaloniaScheduler : LocalScheduler
{
    /// <summary>Gets the shared scheduler for <see cref="Dispatcher.UIThread"/>.</summary>
    public static readonly AvaloniaScheduler Instance =
        new(Dispatcher.UIThread, DispatcherPriority.Background);

    /// <summary>Posts immediate work.</summary>
    private readonly Action<Action> _post;

    /// <summary>Schedules delayed work.</summary>
    private readonly Func<Action, TimeSpan, IDisposable> _scheduleDelayed;

    /// <summary>Queues work and coalesces dispatcher drains.</summary>
    private CoalescingDispatchState _dispatch;

    /// <summary>Initializes a new instance of the <see cref="AvaloniaScheduler"/> class.</summary>
    /// <param name="dispatcher">The dispatcher used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public AvaloniaScheduler(Dispatcher dispatcher)
        : this(dispatcher, DispatcherPriority.Background)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="AvaloniaScheduler"/> class.</summary>
    /// <param name="dispatcher">The dispatcher used to marshal work to the UI thread.</param>
    /// <param name="priority">Dispatcher priority used for posted drains and delayed work.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public AvaloniaScheduler(Dispatcher dispatcher, DispatcherPriority priority)
        : this(dispatcher, priority, null, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="AvaloniaScheduler"/> class.</summary>
    /// <param name="dispatcher">The dispatcher exposed by the scheduler.</param>
    /// <param name="priority">The selected dispatcher priority.</param>
    /// <param name="post">Posts drains, or null to use the dispatcher.</param>
    /// <param name="scheduleDelayed">Schedules delayed work, or null to use dispatcher timers.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is null.</exception>
    internal AvaloniaScheduler(
        Dispatcher dispatcher,
        DispatcherPriority priority,
        Action<Action>? post,
        Func<Action, TimeSpan, IDisposable>? scheduleDelayed)
    {
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        Priority = priority;
        _post = post ?? PostToDispatcher;
        _scheduleDelayed = scheduleDelayed ?? StartDispatcherTimer;
        _dispatch = new(RunDrain, DefaultScheduler.Instance);
    }

    /// <summary>Gets the dispatcher used to marshal work to the UI thread.</summary>
    public Dispatcher Dispatcher { get; }

    /// <summary>Gets the dispatcher priority used for posted drains and delayed work.</summary>
    public DispatcherPriority Priority { get; }

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action) =>
        _dispatch.Schedule(new DispatchHost(this), this, state, action);

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action) =>
        _dispatch.Schedule(new DispatchHost(this), this, state, dueTime, action);

    /// <summary>Posts the drain callback.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns>Always <see langword="true"/>.</returns>
    private bool Post(Action drain)
    {
        _post(drain);
        return true;
    }

    /// <summary>Runs one dispatcher batch.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RunDrain() => _dispatch.RunDrain(new DispatchHost(this));

    /// <summary>Starts a cancellable dispatcher timer.</summary>
    /// <param name="work">The callback to run when due.</param>
    /// <param name="dueTime">The requested delay.</param>
    /// <returns>The timer cancellation handle.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private IDisposable StartDispatcherTimer(Action work, TimeSpan dueTime)
    {
        DispatcherTimer timer = new(Priority, Dispatcher) { Interval = dueTime };
        timer.Tick += (_, _) =>
        {
            timer.Stop();
            work();
        };
        timer.Start();
        return Disposable.Create(timer, static value => value.Stop());
    }

    /// <summary>Posts the callback at the configured dispatcher priority.</summary>
    /// <param name="drain">The callback to post.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PostToDispatcher(Action drain) => Dispatcher.Post(drain, Priority);

    /// <summary>Reaches this scheduler's dispatcher for its dispatch state.</summary>
    /// <param name="Owner">The scheduler.</param>
    private readonly record struct DispatchHost(AvaloniaScheduler Owner) : IDispatchHost
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Post(Action drain) => Owner.Post(drain);

        /// <inheritdoc/>
        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime) => Owner._scheduleDelayed(work, dueTime);
    }
}
