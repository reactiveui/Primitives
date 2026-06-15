// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>Mutable latest-value signal with a ReactiveUI.Primitives name for reactive-property parity.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class StateSignal<T> : ISignal<T>, IObserverRemovable<T>
{
    /// <summary>The latest-value signal state and mechanics; see <see cref="BehaviorSignalState{T}"/>.</summary>
    private BehaviorSignalState<T> _state;

    /// <summary>Initializes a new instance of the <see cref="StateSignal{T}"/> class.</summary>
    /// <param name="initialValue">The initial current value.</param>
    public StateSignal(T initialValue) => _state = new(initialValue);

    /// <summary>Gets the observable stream of current and subsequent values.</summary>
    public IObservable<T> Changed => this;

    /// <summary>Gets or sets the current value. Setting the value notifies observers even when equal to the previous value.</summary>
    public T Value
    {
        get => _state.GetValue();
        set => _state.OnNext(value);
    }

    /// <summary>Gets a value indicating whether this instance has observers.</summary>
    public bool HasObservers => _state.HasObservers;

    /// <summary>Gets a value indicating whether this instance is disposed.</summary>
    public bool IsDisposed => _state.IsDisposed;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Tries to get the current value, returning <see langword="false"/> when disposed.</summary>
    /// <param name="value">The current value, or <see langword="default"/> when disposed.</param>
    /// <returns><see langword="true"/> when a value is available.</returns>
    public bool TryGetValue(out T? value) => _state.TryGetValue(out value);

    /// <summary>Notifies all subscribed observers about the end of the sequence.</summary>
    public void OnCompleted() => _state.OnCompleted();

    /// <summary>Notifies all subscribed observers about the exception.</summary>
    /// <param name="error">The exception to send to all observers.</param>
    public void OnError(Exception error) => _state.OnError(error);

    /// <summary>Notifies all subscribed observers about the arrival of the specified element in the sequence.</summary>
    /// <param name="value">The value to send to all observers.</param>
    public void OnNext(T value) => _state.OnNext(value);

    /// <summary>Subscribes an observer to the signal.</summary>
    /// <param name="observer">The observer to subscribe.</param>
    /// <returns>A handle that unsubscribes the observer when disposed.</returns>
    public IDisposable Subscribe(IObserver<T> observer) => _state.Subscribe(this, observer);

    /// <summary>Releases the signal's observers and cached state.</summary>
    public void Dispose() => _state.Release();

    /// <summary>Emits the current value again without changing it.</summary>
    public void Refresh() => _state.OnNext(Value);

    /// <summary>Creates a read-only projected state view that tracks this state until disposed.</summary>
    /// <typeparam name="TResult">The projected value type.</typeparam>
    /// <param name="selector">The projection to apply to each current value.</param>
    /// <returns>A read-only state view.</returns>
    public ProjectedReadOnlyState<T, TResult> ToReadOnlyState<TResult>(Func<T, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(selector);

        return ProjectedReadOnlyState<T, TResult>.Create(this, selector);
    }

    /// <inheritdoc/>
    void IObserverRemovable<T>.RemoveObserver(IObserver<T> observer) => _state.RemoveObserver(observer);
}
