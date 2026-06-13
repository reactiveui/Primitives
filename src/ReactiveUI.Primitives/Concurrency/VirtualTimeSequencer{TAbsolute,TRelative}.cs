// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Base class for virtual time schedulers using a priority queue for scheduled items.</summary>
/// <typeparam name="TAbsolute">Absolute time representation type.</typeparam>
/// <typeparam name="TRelative">Relative time representation type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public abstract class VirtualTimeSequencer<TAbsolute, TRelative> : VirtualTimeSequencerBase<TAbsolute, TRelative>
    where TAbsolute : IComparable<TAbsolute>
{
    /// <summary>Queue of scheduled virtual-time work.</summary>
    private readonly SequencerQueue<TAbsolute> _queue = new();

    /// <summary>Synchronization gate guarding the scheduled-work queue.</summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="VirtualTimeSequencer{TAbsolute, TRelative}"/> class.
    /// Creates a new virtual time scheduler with the default value of TAbsolute as the initial clock value.
    /// </summary>
    protected VirtualTimeSequencer()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="VirtualTimeSequencer{TAbsolute, TRelative}"/> class. Creates a new virtual time scheduler.</summary>
    /// <param name="initialClock">Initial value for the clock.</param>
    /// <param name="comparer">Comparer to determine causality of events based on absolute time.</param>
    /// <exception cref="ArgumentNullException"><paramref name="comparer"/> is <c>null</c>.</exception>
    protected VirtualTimeSequencer(TAbsolute initialClock, IComparer<TAbsolute> comparer)
        : base(initialClock, comparer)
    {
    }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Schedules an action to be executed at dueTime.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>
    /// The disposable object used to cancel the scheduled action (best effort).
    /// </returns>
    /// <exception cref="ArgumentNullException">action.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="action" /> is <c>null</c>.</exception>
    public override IDisposable ScheduleAbsolute<TState>(TState state, TAbsolute dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        var si = new VirtualScheduledItem<TState>(this, state, action, dueTime, Comparer);

        lock (_gate)
        {
            _queue.Enqueue(si);
        }

        return si;
    }

    /// <summary>Gets the next scheduled item to be executed.</summary>
    /// <returns>The next scheduled item.</returns>
    protected override IScheduledItem<TAbsolute>? GetNext()
    {
        lock (_gate)
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

    /// <summary>Removes an invoked scheduled item from the queue.</summary>
    /// <param name="scheduledItem">The item to remove.</param>
    private void Remove(ScheduledItem<TAbsolute> scheduledItem)
    {
        lock (_gate)
        {
            _queue.Remove(scheduledItem);
        }
    }

    /// <summary>Virtual-time scheduled item that removes itself without a per-schedule wrapper closure.</summary>
    /// <typeparam name="TState">The scheduled state type.</typeparam>
    private sealed class VirtualScheduledItem<TState> : ScheduledItem<TAbsolute>
    {
        /// <summary>The scheduler that owns the item.</summary>
        private readonly VirtualTimeSequencer<TAbsolute, TRelative> _owner;

        /// <summary>The scheduled state.</summary>
        private readonly TState _state;

        /// <summary>The scheduled action.</summary>
        private readonly Func<ISequencer, TState, IDisposable> _action;

        /// <summary>Initializes a new instance of the <see cref="VirtualScheduledItem{TState}"/> class.</summary>
        /// <param name="owner">The scheduler that owns the item.</param>
        /// <param name="state">The scheduled state.</param>
        /// <param name="action">The scheduled action.</param>
        /// <param name="dueTime">The absolute due time.</param>
        /// <param name="comparer">The due-time comparer.</param>
        internal VirtualScheduledItem(
            VirtualTimeSequencer<TAbsolute, TRelative> owner,
            TState state,
            Func<ISequencer, TState, IDisposable> action,
            TAbsolute dueTime,
            IComparer<TAbsolute> comparer)
            : base(dueTime, comparer)
        {
            _owner = owner;
            _state = state;
            _action = action;
        }

        /// <inheritdoc/>
        protected override IDisposable InvokeCore()
        {
            _owner.Remove(this);
            return _action(_owner, _state);
        }
    }
}
