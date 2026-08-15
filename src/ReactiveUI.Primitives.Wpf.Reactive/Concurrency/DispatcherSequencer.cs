// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using System.Windows.Threading;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>WPF dispatcher scheduler that coalesces scheduled work onto a dispatcher drain.</summary>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("Dispatcher = {Dispatcher}, Priority = {Priority}")]
public sealed class DispatcherSequencer : CoalescingDispatchScheduler
{
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
    }

    /// <summary>Gets the dispatcher.</summary>
    public Dispatcher Dispatcher { get; }

    /// <summary>Gets the dispatcher priority used for posted drains.</summary>
    public DispatcherPriority Priority { get; }

    /// <inheritdoc/>
    protected override bool Post(Action drain)
    {
        _ = Dispatcher.BeginInvoke(drain, Priority);
        return true;
    }

    /// <inheritdoc/>
    protected override IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime)
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
