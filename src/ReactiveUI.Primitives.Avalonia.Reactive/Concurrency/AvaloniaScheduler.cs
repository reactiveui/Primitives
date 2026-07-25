// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using Avalonia.Threading;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Avalonia UI-thread scheduler that coalesces scheduled work onto a dispatcher drain.</summary>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
public sealed class AvaloniaScheduler : CoalescingDispatchScheduler
{
    /// <summary>Gets the shared scheduler for <see cref="Dispatcher.UIThread"/>.</summary>
    public static readonly AvaloniaScheduler Instance =
        new(Dispatcher.UIThread, DispatcherPriority.Background);

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
    {
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        Priority = priority;
    }

    /// <summary>Gets the dispatcher used to marshal work to the UI thread.</summary>
    public Dispatcher Dispatcher { get; }

    /// <summary>Gets the dispatcher priority used for posted drains and delayed work.</summary>
    public DispatcherPriority Priority { get; }

    /// <inheritdoc/>
    protected override bool Post(Action drain)
    {
        Dispatcher.Post(drain, Priority);
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
        return Disposable.Create(timer, static value => value.Stop());
    }
}
