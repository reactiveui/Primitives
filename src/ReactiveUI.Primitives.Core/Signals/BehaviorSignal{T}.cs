// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>A signal that replays its most recent value to new subscribers.</summary>
/// <typeparam name="T">The Type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class BehaviorSignal<T> : ISignal<T>, IWitnessRemovable<T>
{
    /// <summary>The latest-value signal state and mechanics; see <see cref="BehaviorSignalState{T}"/>.</summary>
    private BehaviorSignalState<T> _state;

    /// <summary>Initializes a new instance of the <see cref="BehaviorSignal{T}"/> class.</summary>
    /// <param name="defaultValue">The default value.</param>
    public BehaviorSignal(T defaultValue) => _state = new(defaultValue);

    /// <summary>Gets the current value or throws an exception.</summary>
    /// <value>The initial value passed to the constructor until <see cref="OnNext"/> is called; after which, the last value passed to <see cref="OnNext"/>.</value>
    /// <remarks>
    /// <para><see cref="Value"/> is frozen after <see cref="OnCompleted"/> is called.</para>
    /// <para>After <see cref="OnError"/> is called, <see cref="Value"/> always throws the specified exception.</para>
    /// <para>An exception is always thrown after <see cref="Dispose()"/> is called.</para>
    /// <alert type="caller">
    /// Reading <see cref="Value"/> is a thread-safe operation, though there's a potential race condition when <see cref="OnNext"/> or <see cref="OnError"/> are being invoked concurrently.
    /// In some cases, it may be necessary for a caller to use external synchronization to avoid race conditions.
    /// </alert>
    /// </remarks>
    public T Value => _state.GetValue();

    /// <summary>Gets a value indicating whether this instance has observers.</summary>
    /// <value>
    ///   <c>true</c> if this instance has observers; otherwise, <c>false</c>.
    /// </value>
    public bool HasObservers => _state.HasObservers;

    /// <summary>Gets a value indicating whether this instance is disposed.</summary>
    /// <value>
    ///   <c>true</c> if this instance is disposed; otherwise, <c>false</c>.
    /// </value>
    public bool IsDisposed => _state.IsDisposed;

    /// <summary>Gets the string representation of this object for debugger display purposes.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string? DebuggerDisplay => ToString();

    /// <summary>Tries to get the current value or throws an exception.</summary>
    /// <param name="value">The initial value passed to the constructor until <see cref="OnNext"/> is called; after which, the last value passed to <see cref="OnNext"/>.</param>
    /// <returns>true if a value is available; false if the subject was disposed.</returns>
    /// <remarks>
    /// <para>The value returned from <see cref="TryGetValue"/> is frozen after <see cref="OnCompleted"/> is called.</para>
    /// <para>After <see cref="OnError"/> is called, <see cref="TryGetValue"/> always throws the specified exception.</para>
    /// <alert type="caller">
    /// Calling <see cref="TryGetValue"/> is a thread-safe operation, though there's a potential race condition when <see cref="OnNext"/> or <see cref="OnError"/> are being invoked concurrently.
    /// In some cases, it may be necessary for a caller to use external synchronization to avoid race conditions.
    /// </alert>
    /// </remarks>
    public bool TryGetValue(out T? value) => _state.TryGetValue(out value);

    /// <summary>Notifies all subscribed observers about the end of the sequence.</summary>
    public void OnCompleted() => _state.OnCompleted();

    /// <summary>Notifies all subscribed observers about the exception.</summary>
    /// <param name="error">The exception to send to all observers.</param>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <c>null</c>.</exception>
    public void OnError(Exception error) => _state.OnError(error);

    /// <summary>Notifies all subscribed observers about the arrival of the specified element in the sequence.</summary>
    /// <param name="value">The value to send to all observers.</param>
    public void OnNext(T value) => _state.OnNext(value);

    /// <summary>Subscribes an observer to the subject.</summary>
    /// <param name="observer">Observer to subscribe to the subject.</param>
    /// <returns>Disposable object that can be used to unsubscribe the observer from the subject.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <c>null</c>.</exception>
    public IDisposable Subscribe(IObserver<T> observer) => _state.Subscribe(this, observer);

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
    public void Dispose() => _state.Release();

    /// <inheritdoc/>
    void IWitnessRemovable<T>.RemoveObserver(IObserver<T> observer) => _state.RemoveObserver(observer);
}
