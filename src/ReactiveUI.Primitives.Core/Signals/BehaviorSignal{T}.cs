// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Signals;

/// <summary>A signal that replays its most recent value to new subscribers.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class BehaviorSignal<T> : ISignal<T>, IWitnessRemovable<T>
{
    /// <summary>The latest-value signal state and mechanics; see <see cref="BehaviorSignalState{T}"/>.</summary>
    private BehaviorSignalState<T> _state;

    /// <summary>Initializes a new instance of the <see cref="BehaviorSignal{T}"/> class.</summary>
    /// <param name="defaultValue">The default value.</param>
    public BehaviorSignal(T defaultValue) => _state = new(defaultValue);

    /// <summary>Gets the most recent value, which is the constructor's default until <see cref="OnNext"/> supplies one.</summary>
    /// <remarks>Completion freezes the value; a failure makes reads throw the terminal error and disposal makes them throw <see cref="ObjectDisposedException"/>.</remarks>
    public T Value => _state.GetValue();

    /// <summary>Gets a value indicating whether this instance has observers.</summary>
    public bool HasObservers => _state.HasObservers;

    /// <summary>Gets a value indicating whether this instance is disposed.</summary>
    public bool IsDisposed => _state.IsDisposed;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string? DebuggerDisplay => ToString();

    /// <summary>Tries to read the most recent value.</summary>
    /// <param name="value">The most recent value, or <see langword="default"/> when the signal is disposed.</param>
    /// <returns><see langword="true"/> when a value is available; <see langword="false"/> when the signal is disposed.</returns>
    /// <remarks>A read throws the terminal error after a failure, and is not atomic with a separate state check.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetValue(out T? value) => _state.TryGetValue(out value);

    /// <summary>Notifies all subscribed observers about the end of the sequence.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => _state.OnCompleted();

    /// <summary>Notifies all subscribed observers about the exception.</summary>
    /// <param name="error">The exception to send to all observers.</param>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <c>null</c>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => _state.OnError(error);

    /// <summary>Notifies all subscribed observers about the arrival of the specified element in the sequence.</summary>
    /// <param name="value">The value to send to all observers.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) => _state.OnNext(value);

    /// <summary>Subscribes an observer, replaying the current value or the terminal notification.</summary>
    /// <param name="observer">The observer to subscribe.</param>
    /// <returns>A handle that unsubscribes the observer when disposed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <c>null</c>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) => _state.Subscribe(this, observer);

    /// <summary>Drops the observers and the cached value, making later reads throw.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _state.Release();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void IWitnessRemovable<T>.RemoveObserver(IObserver<T> observer) => _state.RemoveObserver(observer);
}
