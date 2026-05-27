// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Windows.Forms;
using ReactiveUI.Primitives.Disposables;
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

        var cancelable = new BooleanDisposable();
        Control.BeginInvoke((MethodInvoker)(() =>
        {
            if (cancelable.IsDisposed)
            {
                return;
            }

            action(this, state);
        }));

        return cancelable;
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

        var timer = new FormsTimer
        {
            Interval = ToTimerInterval(Sequencer.Normalize(dueTime)),
        };

        timer.Tick += (_, _) =>
        {
            timer.Stop();
            timer.Dispose();
            action(this, state);
        };
        timer.Start();

        return Disposable.Create(() =>
        {
            timer.Stop();
            timer.Dispose();
        });
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
    /// Converts the due time to a Windows Forms timer interval.
    /// </summary>
    /// <param name="dueTime">The normalized due time.</param>
    /// <returns>The timer interval in milliseconds.</returns>
    private static int ToTimerInterval(TimeSpan dueTime)
    {
        var totalMilliseconds = dueTime.TotalMilliseconds;
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
