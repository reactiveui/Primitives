// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.UI.Dispatching;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>WinUI dispatcher queue sequencer that coalesces scheduled work through a <see cref="DispatcherQueue"/>.</summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class DispatcherQueueSequencer : ISequencer
{
    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>Cached dispatcher queue handler used for the drain.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Maintainability",
        "SST1422:Move this field into the method that uses it",
        Justification = "Persistent lazy cache: the dispatcher queue handler is built once and reused across every post, so it cannot be a method local.")]
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
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public void Schedule(IWorkItem item) => _state.Schedule(item);

    /// <inheritdoc/>
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

    /// <summary>Marshals the cached drain callback through the dispatcher queue.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns><see langword="true"/> when the drain was enqueued.</returns>
    /// <exception cref="InvalidOperationException">The dispatcher queue is no longer accepting work.</exception>
    private bool Post(Action drain)
    {
        _handler ??= drain.Invoke;
        if (DispatcherQueue.TryEnqueue(Priority, _handler))
        {
            return true;
        }

        throw new InvalidOperationException("The dispatcher queue is no longer accepting work.");
    }

    /// <summary>Runs delayed work on a dispatcher queue timer so it executes directly on the dispatcher thread.</summary>
    /// <param name="item">Work item to execute at the due time.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
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

    /// <summary>Forwards the cached drain callback to the engine.</summary>
    private void RunDrain() => _state.RunDrain();
}
