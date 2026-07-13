// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Windows.Threading;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>WPF dispatcher sequencer that coalesces scheduled work onto a dispatcher drain.</summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class DispatcherSequencer : ISequencer
{
    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>Initializes a new instance of the <see cref="DispatcherSequencer"/> class.</summary>
    /// <param name="dispatcher">The dispatcher.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public DispatcherSequencer(Dispatcher dispatcher)
        : this(dispatcher, DispatcherPriority.Normal)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DispatcherSequencer"/> class.</summary>
    /// <param name="dispatcher">The dispatcher.</param>
    /// <param name="priority">Dispatcher priority used for posted drains.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public DispatcherSequencer(Dispatcher dispatcher, DispatcherPriority priority)
    {
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        Priority = priority;
        _state = new(this, Post, RunDrain, ScheduleDelayed);
    }

    /// <summary>Gets the dispatcher.</summary>
    public Dispatcher Dispatcher { get; }

    /// <summary>Gets the dispatcher priority used for posted drains.</summary>
    public DispatcherPriority Priority { get; }

    /// <inheritdoc/>
    public DateTimeOffset Now => DispatchSequencerState.Now;

    /// <inheritdoc/>
    public long Timestamp => DispatchSequencerState.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public void Schedule(IWorkItem item) => _state.Schedule(item);

    /// <inheritdoc/>
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

    /// <summary>Marshals the cached drain callback onto the dispatcher.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns><see langword="true"/>, since the dispatcher always accepts the work.</returns>
    private bool Post(Action drain)
    {
        _ = Dispatcher.BeginInvoke(drain, Priority);
        return true;
    }

    /// <summary>Runs delayed work on a dispatcher timer so it executes directly on the dispatcher thread.</summary>
    /// <param name="item">Work item to execute at the due time.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
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

    /// <summary>Forwards the cached drain callback to the engine.</summary>
    private void RunDrain() => _state.RunDrain();
}
