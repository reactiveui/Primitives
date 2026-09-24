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
    /// <summary>The shared main-thread scheduler, set once the application's dispatcher is available.</summary>
    private static DispatcherSequencer? _main;

    /// <summary>The scheduler for the calling thread's dispatcher, cached per thread.</summary>
    [ThreadStatic]
    private static DispatcherSequencer? _current;

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
    /// <exception cref="InvalidOperationException">No application exists yet and the calling thread has no dispatcher.</exception>
    /// <remarks>
    /// Bound once to the running <see cref="System.Windows.Application"/>'s dispatcher, from any thread. Until an
    /// application exists nothing is cached, and the calling thread's existing dispatcher is used instead.
    /// </remarks>
    public static DispatcherSequencer Main =>
        Volatile.Read(ref _main) ?? BindMain(System.Windows.Application.Current?.Dispatcher) ?? Current;

    /// <summary>Gets the scheduler for the calling thread's dispatcher.</summary>
    /// <exception cref="InvalidOperationException">The calling thread has no dispatcher.</exception>
    /// <remarks>
    /// Cached per thread, for applications that run UI on more than one thread. Never creates a dispatcher: a thread
    /// that has none cannot run the scheduled work.
    /// </remarks>
    public static DispatcherSequencer Current => _current ??= new(ResolveCurrentDispatcher());

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

    /// <summary>Caches the shared main-thread scheduler for the application's dispatcher, keeping the first one bound.</summary>
    /// <param name="applicationDispatcher">The application's dispatcher, or <see langword="null"/> when no application exists.</param>
    /// <returns>The shared scheduler, or <see langword="null"/> when <paramref name="applicationDispatcher"/> is <see langword="null"/>.</returns>
    internal static DispatcherSequencer? BindMain(Dispatcher? applicationDispatcher)
    {
        if (applicationDispatcher is null)
        {
            return null;
        }

        DispatcherSequencer created = new(applicationDispatcher);
        return Interlocked.CompareExchange(ref _main, created, null) ?? created;
    }

    /// <summary>Returns the calling thread's existing dispatcher without creating one.</summary>
    /// <returns>The calling thread's dispatcher.</returns>
    /// <exception cref="InvalidOperationException">The calling thread has no dispatcher.</exception>
    internal static Dispatcher ResolveCurrentDispatcher() =>
        Dispatcher.FromThread(Thread.CurrentThread)
        ?? throw new InvalidOperationException(
            "The calling thread has no WPF dispatcher. Use DispatcherSequencer.Main or Current from a UI thread, or after the Application is created.");

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
