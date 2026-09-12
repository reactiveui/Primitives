// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using System.Windows.Threading;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>WPF dispatcher scheduler that coalesces scheduled work onto a dispatcher drain.</summary>
/// <remarks>Callbacks run on the dispatcher thread at Priority; cancellation stops pending timers and suppresses unstarted actions.</remarks>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("DispatcherSequencer: Dispatcher = {Dispatcher}, Priority = {Priority}")]
public sealed class DispatcherSequencer : CoalescingDispatchScheduler
{
    /// <summary>Optional callback for posting ready work.</summary>
    private readonly Func<Action, bool>? _post;

    /// <summary>Optional callback for delayed work.</summary>
    private readonly Func<Action, TimeSpan, IDisposable>? _scheduleDelayed;

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
    }

    /// <summary>Gets the dispatcher whose thread runs the scheduled work.</summary>
    public Dispatcher Dispatcher { get; }

    /// <summary>Gets the dispatcher priority used for posted drains.</summary>
    public DispatcherPriority Priority { get; }

    /// <inheritdoc/>
    protected override bool Post(Action drain) => _post is null ? PostToDispatcher(drain) : _post(drain);

    /// <inheritdoc/>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    protected override IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime) =>
        _scheduleDelayed is null ? StartDispatcherTimer(work, dueTime) : _scheduleDelayed(work, dueTime);

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
}
