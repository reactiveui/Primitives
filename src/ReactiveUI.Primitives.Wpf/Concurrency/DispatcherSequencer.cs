// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Threading;
using System.Windows.Threading;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// DispatcherSequencer.
/// </summary>
/// <seealso cref="ReactiveUI.Primitives.Concurrency.ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
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
    public DateTimeOffset Now
    {
        get
        {
#if NET8_0_OR_GREATER
            return TimeProvider.System.GetUtcNow();
#else
#pragma warning disable S6354 // TimeProvider is not available on supported .NET Framework target frameworks.
            return DateTimeOffset.UtcNow;
#pragma warning restore S6354
#endif
        }
    }

    /// <summary>
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

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

        var workItem = new SequencerWorkItem<DispatcherSequencer, TState>(this, state, action);
        Dispatcher.BeginInvoke((Action)workItem.Invoke);
        return workItem;
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
        if (timeSpan == TimeSpan.Zero)
        {
            return Schedule(state, action);
        }

        var workItem = new DispatcherTimerWorkItem<TState>(this, state, action, timeSpan);
        workItem.Start();
        return workItem;
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

    /// <summary>
    /// Disposable dispatcher timer work item.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    private sealed class DispatcherTimerWorkItem<TState> : IDisposable
    {
        /// <summary>
        /// Sequencer passed to the scheduled action.
        /// </summary>
        private readonly DispatcherSequencer _sequencer;

        /// <summary>
        /// State passed to the scheduled action.
        /// </summary>
        private readonly TState _state;

        /// <summary>
        /// Action invoked when the scheduled item runs.
        /// </summary>
        private readonly Func<ISequencer, TState, IDisposable> _action;

        /// <summary>
        /// Dispatcher timer used for delayed execution.
        /// </summary>
        private readonly DispatcherTimer _timer;

        /// <summary>
        /// Tracks cancellation.
        /// </summary>
        private int _isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="DispatcherTimerWorkItem{TState}"/> class.
        /// </summary>
        /// <param name="sequencer">Sequencer passed to the action.</param>
        /// <param name="state">State passed to the action.</param>
        /// <param name="action">Action to invoke.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        public DispatcherTimerWorkItem(DispatcherSequencer sequencer, TState state, Func<ISequencer, TState, IDisposable> action, TimeSpan dueTime)
        {
            _sequencer = sequencer;
            _state = state;
            _action = action;
            _timer = new DispatcherTimer { Interval = dueTime };
            _timer.Tick += OnTick;
        }

        /// <summary>
        /// Cancels the timer work item.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            {
                return;
            }

            _timer.Stop();
            _timer.Tick -= OnTick;
        }

        /// <summary>
        /// Starts the dispatcher timer.
        /// </summary>
        public void Start() => _timer.Start();

        /// <summary>
        /// Handles the timer tick.
        /// </summary>
        /// <param name="sender">The event source.</param>
        /// <param name="e">The event arguments.</param>
        private void OnTick(object? sender, EventArgs e)
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            {
                return;
            }

            _timer.Stop();
            _timer.Tick -= OnTick;
            _action(_sequencer, _state);
        }
    }
}
