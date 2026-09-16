// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// A signal that accepts notifications from any number of threads and delivers them to its subscribers one at a time,
/// without holding a lock while they run.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// It wraps another signal, a <see cref="Signal{T}"/> unless one is supplied, which keeps the subscriptions and the
/// terminal state. Notifications reach that signal through a <see cref="SerializedWitness{T}"/>, so a subscriber that
/// blocks on another producer's thread cannot deadlock it.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SerializedSignal: HasObservers = {HasObservers}, IsDisposed = {IsDisposed}")]
public sealed class SerializedSignal<T> : ISignal<T>
{
    /// <summary>The wrapped signal that keeps the subscriptions.</summary>
    private readonly ISignal<T> _signal;

    /// <summary>Serializes notifications into the wrapped signal.</summary>
    private readonly SerializedWitness<T> _witness;

    /// <summary>Initializes a new instance of the <see cref="SerializedSignal{T}"/> class over a new <see cref="Signal{T}"/>.</summary>
    public SerializedSignal()
        : this(new Signal<T>())
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SerializedSignal{T}"/> class over an existing signal.</summary>
    /// <param name="signal">The signal whose notifications are serialized.</param>
    /// <exception cref="ArgumentNullException"><paramref name="signal"/> is <see langword="null"/>.</exception>
    public SerializedSignal(ISignal<T> signal)
    {
        ArgumentExceptionHelper.ThrowIfNull(signal);
        _signal = signal;
        _witness = new(signal);
    }

    /// <inheritdoc/>
    public bool HasObservers => _signal.HasObservers;

    /// <inheritdoc/>
    public bool IsDisposed => _signal.IsDisposed;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) => _witness.OnNext(value);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => _witness.OnError(error);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => _witness.OnCompleted();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) => _signal.Subscribe(observer);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _signal.Dispose();
}
