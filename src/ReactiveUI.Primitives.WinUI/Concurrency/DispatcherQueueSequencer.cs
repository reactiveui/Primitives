// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.UI.Dispatching;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// WinUI dispatcher queue sequencer that coalesces scheduled work through a <see cref="DispatcherQueue"/>.
/// </summary>
/// <seealso cref="DispatchSequencerBase" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class DispatcherQueueSequencer : DispatchSequencerBase
{
    /// <summary>
    /// Cached dispatcher queue handler used for the base drain.
    /// </summary>
    private DispatcherQueueHandler? _handler;

    /// <summary>
    /// Initializes a new instance of the <see cref="DispatcherQueueSequencer"/> class.
    /// </summary>
    /// <param name="dispatcherQueue">The dispatcher queue used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcherQueue"/> is <see langword="null"/>.</exception>
    public DispatcherQueueSequencer(DispatcherQueue dispatcherQueue)
        : this(dispatcherQueue, DispatcherQueuePriority.Normal)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DispatcherQueueSequencer"/> class.
    /// </summary>
    /// <param name="dispatcherQueue">The dispatcher queue used to marshal work to the UI thread.</param>
    /// <param name="priority">Dispatcher queue priority used for posted drains.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcherQueue"/> is <see langword="null"/>.</exception>
    public DispatcherQueueSequencer(DispatcherQueue dispatcherQueue, DispatcherQueuePriority priority)
    {
        DispatcherQueue = dispatcherQueue ?? throw new ArgumentNullException(nameof(dispatcherQueue));
        Priority = priority;
    }

    /// <summary>
    /// Gets the dispatcher queue used to marshal work to the UI thread.
    /// </summary>
    public DispatcherQueue DispatcherQueue { get; }

    /// <summary>
    /// Gets the dispatcher queue priority used for posted drains.
    /// </summary>
    public DispatcherQueuePriority Priority { get; }

    /// <summary>
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    /// <exception cref="InvalidOperationException">The dispatcher queue is no longer accepting work.</exception>
    protected override bool Post(Action drain)
    {
        _handler ??= drain.Invoke;
        if (DispatcherQueue.TryEnqueue(Priority, _handler))
        {
            return true;
        }

        throw new InvalidOperationException("The dispatcher queue is no longer accepting work.");
    }

    /// <inheritdoc/>
    protected override void ScheduleDelayed(IWorkItem item, long dueTimestamp)
    {
        var timer = DispatcherQueue.CreateTimer();
        timer.Interval = DelayUntil(dueTimestamp);
        timer.IsRepeating = false;
        timer.Tick += (_, _) =>
        {
            timer.Stop();
            RunIfActive(item);
        };
        timer.Start();
    }
}
