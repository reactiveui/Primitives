// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using Microsoft.UI.Dispatching;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>WinUI dispatcher queue scheduler that coalesces scheduled work through a <see cref="DispatcherQueue"/>.</summary>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
public sealed class DispatcherQueueSequencer : CoalescingDispatchScheduler
{
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
    }

    /// <summary>Gets the dispatcher queue used to marshal work to the UI thread.</summary>
    public DispatcherQueue DispatcherQueue { get; }

    /// <summary>Gets the dispatcher queue priority used for posted drains.</summary>
    public DispatcherQueuePriority Priority { get; }

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
    protected override IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime)
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
}
