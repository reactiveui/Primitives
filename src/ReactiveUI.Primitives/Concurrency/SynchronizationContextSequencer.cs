// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using Timer = System.Threading.Timer;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Sequencer that posts work through a <see cref="SynchronizationContext"/>.
/// </summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class SynchronizationContextSequencer : ISequencer
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SynchronizationContextSequencer"/> class.
    /// </summary>
    /// <param name="context">The synchronization context used to schedule work.</param>
    /// <exception cref="ArgumentNullException"><paramref name="context"/> is <see langword="null"/>.</exception>
    public SynchronizationContextSequencer(SynchronizationContext context) =>
        Context = context ?? throw new ArgumentNullException(nameof(context));

    /// <summary>
    /// Gets a sequencer for the current synchronization context.
    /// </summary>
    /// <exception cref="InvalidOperationException">There is no current synchronization context.</exception>
    public static SynchronizationContextSequencer Current =>
        new(SynchronizationContext.Current ?? throw new InvalidOperationException("There is no current synchronization context."));

    /// <summary>
    /// Gets the synchronization context used to schedule work.
    /// </summary>
    public SynchronizationContext Context { get; }

    /// <summary>
    /// Gets the scheduler's notion of current time.
    /// </summary>
    public DateTimeOffset Now => Sequencer.Now;

    /// <summary>
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public IDisposable Schedule<TState>(TState state, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var cancelable = new BooleanDisposable();
        Context.Post(
            _ =>
            {
                if (cancelable.IsDisposed)
                {
                    return;
                }

                action(this, state);
            },
            null);

        return cancelable;
    }

    /// <inheritdoc/>
    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var cancelable = new BooleanDisposable();
        Timer? timer = null;
        timer = new Timer(
            _ =>
            {
                if (cancelable.IsDisposed)
                {
                    return;
                }

                Context.Post(
                    __ =>
                    {
                        if (!cancelable.IsDisposed)
                        {
                            action(this, state);
                        }

                        timer?.Dispose();
                    },
                    null);
            },
            null,
            Sequencer.Normalize(dueTime),
            Timeout.InfiniteTimeSpan);

        return Disposable.Create(() =>
        {
            cancelable.Dispose();
            timer.Dispose();
        });
    }

    /// <inheritdoc/>
    public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<ISequencer, TState, IDisposable> action) =>
        Schedule(state, Sequencer.Normalize(dueTime - Now), action);
}
