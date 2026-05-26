// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Mutable latest-value signal with a ReactiveUI.Primitives name for reactive-property parity.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public partial class StateSignal<T> : BehaviorSignal<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="StateSignal{T}"/> class.
    /// </summary>
    /// <param name="initialValue">The initial current value.</param>
    public StateSignal(T initialValue)
        : base(initialValue)
    {
    }

    /// <summary>
    /// Gets the observable stream of current and subsequent values.
    /// </summary>
    public IObservable<T> Changed => this;

    /// <summary>
    /// Gets or sets the current value. Setting the value notifies observers even when equal to the previous value.
    /// </summary>
    public new T Value
    {
        get => base.Value;
        set => OnNext(value);
    }

    /// <summary>
    /// Emits the current value again without changing it.
    /// </summary>
    public void Refresh() => OnNext(Value);

    /// <summary>
    /// Creates a read-only projected state view that tracks this state until disposed.
    /// </summary>
    /// <typeparam name="TResult">The projected value type.</typeparam>
    /// <param name="selector">The projection to apply to each current value.</param>
    /// <returns>A read-only state view.</returns>
    public ReadOnlyState<TResult> ToReadOnlyState<TResult>(Func<T, TResult> selector)
    {
        if (selector == null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        return new ReadOnlyState<TResult>(this.Map(selector), selector(Value));
    }
}
