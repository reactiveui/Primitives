// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Base class for virtual time schedulers using a priority queue for scheduled items.
/// </summary>
/// <typeparam name="TAbsolute">Absolute time representation type.</typeparam>
/// <typeparam name="TRelative">Relative time representation type.</typeparam>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public abstract class VirtualTimeSequencer<TAbsolute, TRelative> : VirtualTimeSequencerBase<TAbsolute, TRelative>
    where TAbsolute : IComparable<TAbsolute>
{
    private readonly SequencerQueue<TAbsolute> _queue = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="VirtualTimeSequencer{TAbsolute, TRelative}"/> class.
    /// Creates a new virtual time scheduler with the default value of TAbsolute as the initial clock value.
    /// </summary>
    protected VirtualTimeSequencer()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="VirtualTimeSequencer{TAbsolute, TRelative}"/> class.
    /// Creates a new virtual time scheduler.
    /// </summary>
    /// <param name="initialClock">Initial value for the clock.</param>
    /// <param name="comparer">Comparer to determine causality of events based on absolute time.</param>
    /// <exception cref="ArgumentNullException"><paramref name="comparer"/> is <c>null</c>.</exception>
    protected VirtualTimeSequencer(TAbsolute initialClock, IComparer<TAbsolute> comparer)
        : base(initialClock, comparer)
    {
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
    /// <exception cref="System.ArgumentNullException">action.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="action" /> is <c>null</c>.</exception>
    public override IDisposable ScheduleAbsolute<TState>(TState state, TAbsolute dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        ScheduledItem<TAbsolute, TState>? si = null;

        var run = new Func<ISequencer, TState, IDisposable>((scheduler, state1) =>
        {
            lock (_queue)
            {
                _queue.Remove(si!); // NB: Assigned before function is invoked.
            }

            return action(scheduler, state1);
        });

        si = new ScheduledItem<TAbsolute, TState>(this, state, run, dueTime, Comparer);

        lock (_queue)
        {
            _queue.Enqueue(si);
        }

        return si;
    }

    /// <summary>
    /// Gets the next scheduled item to be executed.
    /// </summary>
    /// <returns>The next scheduled item.</returns>
    protected override IScheduledItem<TAbsolute>? GetNext()
    {
        lock (_queue)
        {
            while (_queue.Count > 0)
            {
                var next = _queue.Peek();
                if (next.IsDisposed)
                {
                    _queue.Dequeue();
                }
                else
                {
                    return next;
                }
            }
        }

        return null;
    }
}
