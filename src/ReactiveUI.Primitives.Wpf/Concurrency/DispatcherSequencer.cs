// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Windows.Threading;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>WPF dispatcher sequencer that coalesces scheduled work onto a dispatcher drain.</summary>
/// <remarks>Callbacks run at Priority in posted dispatcher batches without inline reentrancy; cancellation suppresses unstarted work.</remarks>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class DispatcherSequencer : ISequencer
{
    /// <summary>The shared main-thread sequencer, set once the application's dispatcher is available.</summary>
    private static DispatcherSequencer? _main;

    /// <summary>The sequencer for the calling thread's dispatcher, cached per thread.</summary>
    [ThreadStatic]
    private static DispatcherSequencer? _current;

    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

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
    /// <param name="dispatcher">The dispatcher associated with this sequencer.</param>
    /// <param name="priority">The dispatcher priority.</param>
    /// <param name="post">Posts ready work, or null to use the dispatcher.</param>
    /// <param name="scheduleDelayed">Schedules delayed work, or null to use a dispatcher timer.</param>
    /// <exception cref="ArgumentNullException">The dispatcher is null.</exception>
    internal DispatcherSequencer(
        Dispatcher dispatcher,
        DispatcherPriority priority,
        Func<Action, bool>? post,
        Action<IWorkItem, long>? scheduleDelayed)
    {
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        Priority = priority;
        _state = new(this, post ?? Post, RunDrain, scheduleDelayed ?? ScheduleDelayed);
    }

    /// <summary>Gets the shared sequencer for the WPF main (UI) thread.</summary>
    /// <exception cref="InvalidOperationException">No application exists yet and the calling thread has no dispatcher.</exception>
    /// <remarks>
    /// Bound once to the running <see cref="System.Windows.Application"/>'s dispatcher, from any thread. Until an
    /// application exists nothing is cached, and the calling thread's existing dispatcher is used instead.
    /// </remarks>
    public static DispatcherSequencer Main =>
        Volatile.Read(ref _main) ?? BindMain(System.Windows.Application.Current?.Dispatcher) ?? Current;

    /// <summary>Gets the sequencer for the calling thread's dispatcher.</summary>
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

    /// <summary>Caches the shared main-thread sequencer for the application's dispatcher, keeping the first one bound.</summary>
    /// <param name="applicationDispatcher">The application's dispatcher, or <see langword="null"/> when no application exists.</param>
    /// <returns>The shared sequencer, or <see langword="null"/> when <paramref name="applicationDispatcher"/> is <see langword="null"/>.</returns>
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

    /// <summary>Marshals the cached drain callback onto the dispatcher.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns><see langword="true"/>, since the dispatcher always accepts the work.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private bool Post(Action drain)
    {
        _ = Dispatcher.BeginInvoke(drain, Priority);
        return true;
    }

    /// <summary>Runs delayed work on a dispatcher timer so it executes directly on the dispatcher thread.</summary>
    /// <param name="item">Work item to execute at the due time.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private void ScheduleDelayed(IWorkItem item, long dueTimestamp)
    {
        DispatcherTimer timer =
            new(Priority, Dispatcher) { Interval = DispatchSequencerState.DelayUntil(dueTimestamp) };
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
