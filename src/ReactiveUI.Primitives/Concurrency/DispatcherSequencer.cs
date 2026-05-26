// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if WINDOWS

using System;
using System.Windows.Threading;
using ReactiveUI.Primitives.Disposables;
using static ReactiveUI.Primitives.Disposables.Disposable;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// DispatcherSequencer.
/// </summary>
/// <seealso cref="ReactiveUI.Primitives.Concurrency.ISequencer" />
public class DispatcherSequencer : ISequencer
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DispatcherSequencer"/> class.
    /// </summary>
    /// <param name="dispatcher">The dispatcher.</param>
    /// <exception cref="System.ArgumentNullException">dispatcher.</exception>
    public DispatcherSequencer(Dispatcher dispatcher) =>
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));

    /// <summary>
    /// Gets the dispatcher.
    /// </summary>
    /// <value>
    /// The dispatcher.
    /// </value>
    public Dispatcher Dispatcher { get; }

    /// <summary>
    /// Gets the scheduler's notion of current time.
    /// </summary>
    public DateTimeOffset Now => Sequencer.Now;

    /// <summary>
    /// Schedules an action to be executed.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    /// <exception cref="System.ArgumentNullException">action.</exception>
    public IDisposable Schedule<TState>(TState state, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var cancelable = new BooleanDisposable();
        Dispatcher.BeginInvoke(() =>
        {
            if (cancelable.IsDisposed)
            {
                return;
            }

            action(this, state);
        });
        return cancelable;
    }

    /// <summary>
    /// Schedules an action to be executed after dueTime.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    /// <exception cref="System.ArgumentNullException">action.</exception>
    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var timeSpan = Sequencer.Normalize(dueTime);
        var timer = new DispatcherTimer();
        timer.Tick += (_, _) =>
        {
            timer?.Stop();
            timer = null;
            action(this, state);
        };
        timer.Interval = timeSpan;
        timer.Start();
        return new AnonymousDisposable(() =>
        {
            timer?.Stop();
            timer = null;
        });
    }

    /// <summary>
    /// Schedules an action to be executed at dueTime.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<ISequencer, TState, IDisposable> action) =>
        Schedule(state, Sequencer.Normalize(dueTime - Now), action);
}
#endif
