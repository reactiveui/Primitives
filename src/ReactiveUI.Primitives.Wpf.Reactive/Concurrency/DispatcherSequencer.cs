// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using System.Windows.Threading;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>WPF dispatcher scheduler that coalesces scheduled work onto a dispatcher drain.</summary>
/// <remarks>Work runs on the dispatcher's thread at <see cref="Priority"/> and delayed work fires on a
/// <see cref="DispatcherTimer"/>, so disposing the returned subscription stops that timer as well as suppressing work
/// that has not started.</remarks>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("DispatcherSequencer: Dispatcher = {Dispatcher}, Priority = {Priority}")]
public sealed class DispatcherSequencer : CoalescingDispatchScheduler
{
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
    {
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        Priority = priority;
    }

    /// <summary>Gets the dispatcher whose thread runs the scheduled work.</summary>
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
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
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
