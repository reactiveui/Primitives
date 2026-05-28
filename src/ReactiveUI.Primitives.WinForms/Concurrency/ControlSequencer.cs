// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Threading;
using System.Windows.Forms;
using FormsTimer = System.Windows.Forms.Timer;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Windows Forms sequencer that schedules work through a UI control.
/// </summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ControlSequencer : ISequencer
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ControlSequencer"/> class.
    /// </summary>
    /// <param name="control">The control used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="control"/> is <see langword="null"/>.</exception>
    public ControlSequencer(Control control) =>
        Control = control ?? throw new ArgumentNullException(nameof(control));

    /// <summary>
    /// Gets the control used to marshal work to the UI thread.
    /// </summary>
    public Control Control { get; }

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
    /// <returns>The disposable object used to cancel the scheduled action on a best-effort basis.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    public IDisposable Schedule<TState>(TState state, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var workItem = new ControlWorkItem<TState>(this, state, action);
        Control.BeginInvoke((MethodInvoker)workItem.Invoke);
        return workItem;
    }

    /// <summary>
    /// Schedules an action to be executed after dueTime.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action on a best-effort basis.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var normalized = Sequencer.Normalize(dueTime);
        if (normalized == TimeSpan.Zero)
        {
            return Schedule(state, action);
        }

        var workItem = new ControlTimerWorkItem<TState>(this, state, action, normalized);
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
    /// <returns>The disposable object used to cancel the scheduled action on a best-effort basis.</returns>
    public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<ISequencer, TState, IDisposable> action) =>
        Schedule(state, Sequencer.Normalize(dueTime - Now), action);

    /// <summary>
    /// Disposable control work item.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    private sealed class ControlWorkItem<TState> : IDisposable
    {
        /// <summary>
        /// Sequencer passed to the scheduled action.
        /// </summary>
        private readonly ControlSequencer _sequencer;

        /// <summary>
        /// State passed to the scheduled action.
        /// </summary>
        private readonly TState _state;

        /// <summary>
        /// Action invoked when the scheduled item runs.
        /// </summary>
        private readonly Func<ISequencer, TState, IDisposable> _action;

        /// <summary>
        /// Tracks cancellation.
        /// </summary>
        private int _isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ControlWorkItem{TState}"/> class.
        /// </summary>
        /// <param name="sequencer">Sequencer passed to the action.</param>
        /// <param name="state">State passed to the action.</param>
        /// <param name="action">Action to invoke.</param>
        public ControlWorkItem(ControlSequencer sequencer, TState state, Func<ISequencer, TState, IDisposable> action)
        {
            _sequencer = sequencer;
            _state = state;
            _action = action;
        }

        /// <summary>
        /// Cancels the work item.
        /// </summary>
        public void Dispose() => Interlocked.Exchange(ref _isDisposed, 1);

        /// <summary>
        /// Invokes the scheduled action if it has not been cancelled.
        /// </summary>
        public void Invoke()
        {
            if (Volatile.Read(ref _isDisposed) != 0)
            {
                return;
            }

            _action(_sequencer, _state);
        }
    }

    /// <summary>
    /// Disposable Windows Forms timer work item.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    private sealed class ControlTimerWorkItem<TState> : IDisposable
    {
        /// <summary>
        /// Sequencer passed to the scheduled action.
        /// </summary>
        private readonly ControlSequencer _sequencer;

        /// <summary>
        /// State passed to the scheduled action.
        /// </summary>
        private readonly TState _state;

        /// <summary>
        /// Action invoked when the scheduled item runs.
        /// </summary>
        private readonly Func<ISequencer, TState, IDisposable> _action;

        /// <summary>
        /// Windows Forms timer used for delayed execution.
        /// </summary>
        private readonly FormsTimer _timer;

        /// <summary>
        /// Tracks cancellation.
        /// </summary>
        private int _isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ControlTimerWorkItem{TState}"/> class.
        /// </summary>
        /// <param name="sequencer">Sequencer passed to the action.</param>
        /// <param name="state">State passed to the action.</param>
        /// <param name="action">Action to invoke.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        public ControlTimerWorkItem(ControlSequencer sequencer, TState state, Func<ISequencer, TState, IDisposable> action, TimeSpan dueTime)
        {
            _sequencer = sequencer;
            _state = state;
            _action = action;
            _timer = new FormsTimer
            {
                Interval = ToTimerInterval(dueTime),
            };

            _timer.Tick += OnTick;

            static int ToTimerInterval(TimeSpan normalizedDueTime)
            {
                var totalMilliseconds = normalizedDueTime.TotalMilliseconds;
                if (totalMilliseconds <= 1)
                {
                    return 1;
                }

                if (totalMilliseconds >= int.MaxValue)
                {
                    return int.MaxValue;
                }

                return (int)Math.Ceiling(totalMilliseconds);
            }
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
            _timer.Dispose();
        }

        /// <summary>
        /// Starts the Windows Forms timer.
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
            _timer.Dispose();
            _action(_sequencer, _state);
        }
    }
}
