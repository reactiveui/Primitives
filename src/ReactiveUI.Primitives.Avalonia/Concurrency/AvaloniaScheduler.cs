// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Avalonia.Threading;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Avalonia UI-thread scheduler that coalesces scheduled work onto a dispatcher drain.</summary>
/// <remarks>Callbacks run at Priority in posted dispatcher batches without inline reentrancy; cancellation suppresses unstarted work.</remarks>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class AvaloniaScheduler : ISequencer
{
    /// <summary>Gets the shared scheduler for <see cref="Dispatcher.UIThread"/>.</summary>
    public static readonly AvaloniaScheduler Instance =
        new(Dispatcher.UIThread, DispatcherPriority.Background);

    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

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
        Func<Action, bool>? post,
        Action<IWorkItem, long>? scheduleDelayed)
    {
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        Priority = priority;
        _state = new(this, post ?? Post, RunDrain, scheduleDelayed ?? ScheduleDelayed);
    }

    /// <summary>Gets the dispatcher used to marshal work to the UI thread.</summary>
    public Dispatcher Dispatcher { get; }

    /// <summary>Gets the dispatcher priority used for posted drains and delayed work.</summary>
    public DispatcherPriority Priority { get; }

    /// <inheritdoc/>
    public DateTimeOffset Now => DispatchSequencerState.Now;

    /// <inheritdoc/>
    public long Timestamp => DispatchSequencerState.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item) => _state.Schedule(item);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

    /// <summary>Marshals the cached drain callback onto the dispatcher.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns><see langword="true"/>, since the dispatcher accepts the work.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private bool Post(Action drain)
    {
        Dispatcher.Post(drain, Priority);
        return true;
    }

    /// <summary>Runs delayed work on a dispatcher timer bound to the selected dispatcher.</summary>
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
