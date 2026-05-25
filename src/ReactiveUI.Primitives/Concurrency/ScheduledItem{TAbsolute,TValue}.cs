// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Represents a scheduled work item based on the materialization of an ISequencer.Schedule method call.
/// </summary>
/// <typeparam name="TAbsolute">Absolute time representation type.</typeparam>
/// <typeparam name="TValue">Type of the state passed to the scheduled action.</typeparam>
public sealed class ScheduledItem<TAbsolute, TValue> : ScheduledItem<TAbsolute>
    where TAbsolute : IComparable<TAbsolute>
{
    private readonly ISequencer _scheduler;
    private readonly TValue _state;
    private readonly Func<ISequencer, TValue, IDisposable> _action;

    /// <summary>
    /// Initializes a new instance of the <see cref="ScheduledItem{TAbsolute, TValue}"/> class.
    /// Creates a materialized work item.
    /// </summary>
    /// <param name="scheduler">Recursive scheduler to invoke the scheduled action with.</param>
    /// <param name="state">State to pass to the scheduled action.</param>
    /// <param name="action">Scheduled action.</param>
    /// <param name="dueTime">Time at which to run the scheduled action.</param>
    /// <param name="comparer">Comparer used to compare work items based on their scheduled time.</param>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> or <paramref name="comparer"/> is <c>null</c>.</exception>
    public ScheduledItem(ISequencer scheduler, TValue state, Func<ISequencer, TValue, IDisposable> action, TAbsolute dueTime, IComparer<TAbsolute> comparer)
        : base(dueTime, comparer)
    {
        _scheduler = scheduler ?? throw new ArgumentNullException(nameof(scheduler));
        _state = state;
        _action = action ?? throw new ArgumentNullException(nameof(action));
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ScheduledItem{TAbsolute, TValue}"/> class.
    /// Creates a materialized work item.
    /// </summary>
    /// <param name="scheduler">Recursive scheduler to invoke the scheduled action with.</param>
    /// <param name="state">State to pass to the scheduled action.</param>
    /// <param name="action">Scheduled action.</param>
    /// <param name="dueTime">Time at which to run the scheduled action.</param>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> or <paramref name="action"/> is <c>null</c>.</exception>
    public ScheduledItem(ISequencer scheduler, TValue state, Func<ISequencer, TValue, IDisposable> action, TAbsolute dueTime)
        : this(scheduler, state, action, dueTime, Comparer<TAbsolute>.Default)
    {
    }

    /// <summary>
    /// Invokes the scheduled action with the supplied recursive scheduler and state.
    /// </summary>
    /// <returns>Cancellation resource returned by the scheduled action.</returns>
    protected override IDisposable InvokeCore() => _action(_scheduler, _state);
}
