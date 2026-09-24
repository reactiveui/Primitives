// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using System.Windows.Threading;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>WPF dispatcher scheduler that coalesces scheduled work onto a dispatcher drain.</summary>
/// <remarks>Callbacks run on the dispatcher thread at Priority; cancellation stops pending timers and suppresses unstarted actions.</remarks>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("DispatcherSequencer: Dispatcher = {Dispatcher}, Priority = {Priority}")]
public sealed class DispatcherSequencer : LocalScheduler
{
    /// <summary>Lazily binds the shared main-thread scheduler to the application's dispatcher on first use.</summary>
    private static readonly Lazy<DispatcherSequencer> LazyMain = new(static () => new(ResolveMainDispatcher()));

    /// <summary>Optional callback for posting ready work.</summary>
    private readonly Func<Action, bool>? _post;

    /// <summary>Optional callback for delayed work.</summary>
    private readonly Func<Action, TimeSpan, IDisposable>? _scheduleDelayed;

    /// <summary>Queues work and coalesces dispatcher drains.</summary>
    private CoalescingDispatchState _dispatch;

    /// <summary>Initializes a new instance of the <see cref="DispatcherSequencer"/> class.</summary>
    /// <param name="dispatcher">The dispatcher whose thread runs the scheduled work.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public DispatcherSequencer(Dispatcher dispatcher)
        : this(dispatcher, DispatcherPriority.Normal)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DispatcherSequencer"/> class.</summary>
    /// <param name="dispatcher">The dispatcher whose thread runs the scheduled work.</param>
    /// <param name="priority">Dispatcher priority used for posted drains.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public DispatcherSequencer(Dispatcher dispatcher, DispatcherPriority priority)
        : this(dispatcher, priority, null, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DispatcherSequencer"/> class.</summary>
    /// <param name="dispatcher">The dispatcher associated with this scheduler.</param>
    /// <param name="priority">The dispatcher priority.</param>
    /// <param name="post">Posts ready work, or null to use the dispatcher.</param>
    /// <param name="scheduleDelayed">Schedules delayed work, or null to use a dispatcher timer.</param>
    /// <exception cref="ArgumentNullException">The dispatcher is null.</exception>
    internal DispatcherSequencer(
        Dispatcher dispatcher,
        DispatcherPriority priority,
        Func<Action, bool>? post,
        Func<Action, TimeSpan, IDisposable>? scheduleDelayed)
    {
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        Priority = priority;
        _post = post;
        _scheduleDelayed = scheduleDelayed;
        _dispatch = new(RunDrain, DefaultScheduler.Instance);
    }

    /// <summary>Gets the shared scheduler for the WPF main (UI) thread.</summary>
    /// <remarks>
    /// Bound on first access to the running <see cref="System.Windows.Application"/>'s dispatcher, or to the calling
    /// thread's dispatcher when no application exists yet.
    /// </remarks>
    public static DispatcherSequencer Main => LazyMain.Value;

    /// <summary>Gets the dispatcher whose thread runs the scheduled work.</summary>
    public Dispatcher Dispatcher { get; }

    /// <summary>Gets the dispatcher priority used for posted drains.</summary>
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

    /// <summary>Resolves the dispatcher that owns the WPF main (UI) thread.</summary>
    /// <returns>The application's dispatcher, or the calling thread's dispatcher when no application exists yet.</returns>
    internal static Dispatcher ResolveMainDispatcher() =>
        System.Windows.Application.Current?.Dispatcher ?? Dispatcher.CurrentDispatcher;

    /// <summary>Posts the drain callback, through the test hook when one was supplied.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns>Whether the dispatcher accepted the callback.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool Post(Action drain) => _post is null ? PostToDispatcher(drain) : _post(drain);

    /// <summary>Schedules delayed work on a dispatcher timer, or through the test hook when one was supplied.</summary>
    /// <param name="work">The callback to run.</param>
    /// <param name="dueTime">The requested delay.</param>
    /// <returns>The timer cancellation handle.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime) =>
        _scheduleDelayed is null ? StartDispatcherTimer(work, dueTime) : _scheduleDelayed(work, dueTime);

    /// <summary>Runs one dispatcher batch.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RunDrain() => _dispatch.RunDrain(new DispatchHost(this));

    /// <summary>Posts a drain at the configured dispatcher priority.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns>True once the dispatcher accepts the callback.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private bool PostToDispatcher(Action drain)
    {
        _ = Dispatcher.BeginInvoke(drain, Priority);
        return true;
    }

    /// <summary>Starts a cancellable dispatcher timer.</summary>
    /// <param name="work">The callback to run.</param>
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
        return Disposable.Create(timer, static t => t.Stop());
    }

    /// <summary>Reaches this scheduler's dispatcher for its dispatch state.</summary>
    /// <param name="Owner">The scheduler.</param>
    private readonly record struct DispatchHost(DispatcherSequencer Owner) : IDispatchHost
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Post(Action drain) => Owner.Post(drain);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime) => Owner.ScheduleOnDispatcher(work, dueTime);
    }
}
