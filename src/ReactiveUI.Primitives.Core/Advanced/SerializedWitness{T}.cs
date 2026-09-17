// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>
/// Wraps an observer so notifications from any number of threads reach it one at a time, without holding a lock while it
/// runs.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// A notification that arrives while another thread is delivering is queued and delivered in arrival order; one raised by
/// the observer itself is delivered after it returns. The first terminal notification wins and nothing follows it.
/// <see cref="Post"/>, <see cref="PostError"/> and <see cref="PostCompleted"/> queue without delivering, so a caller can fix
/// the order under its own lock and call <see cref="Flush"/> after releasing it; a caller that posts should not also call
/// the observer methods, or a direct delivery can overtake a posted one.
/// </remarks>
[DebuggerDisplay("SerializedWitness: {_delivery}")]
public sealed class SerializedWitness<T> : IObserver<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>Serializes notifications into the downstream observer.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>Initializes a new instance of the <see cref="SerializedWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public SerializedWitness(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        _observer = observer;
    }

    /// <summary>Gets a value indicating whether the terminal notification has been taken for delivery.</summary>
    public bool IsTerminated => _delivery.IsTerminated;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) => _delivery.OnNext(_observer, value, new PendingDrain(this));

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => _delivery.OnError(error, new PendingDrain(this));

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => _delivery.OnCompleted(new PendingDrain(this));

    /// <summary>Queues a value without delivering it.</summary>
    /// <param name="value">The value to queue.</param>
    /// <returns><see langword="true"/> when the value was queued; <see langword="false"/> once a terminal notification is queued.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Post(T value) => _delivery.Post(value);

    /// <summary>Queues an error as the terminal notification without delivering it.</summary>
    /// <param name="error">The error.</param>
    /// <returns><see langword="true"/> when this is the first terminal notification.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool PostError(Exception error) => _delivery.PostError(error);

    /// <summary>Queues completion as the terminal notification without delivering it.</summary>
    /// <returns><see langword="true"/> when this is the first terminal notification.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool PostCompleted() => _delivery.PostCompleted();

    /// <summary>Delivers queued notifications on the calling thread, or hands them to the thread already delivering.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Flush() => _delivery.Flush(new PendingDrain(this));

    /// <summary>Claims delivery when nothing is queued or delivering; safe under the caller's lock.</summary>
    /// <returns><see langword="true"/> when the caller must deliver through <see cref="DeliverClaimed(T)"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryClaim() => _delivery.TryClaim();

    /// <summary>Delivers a value on a claimed delivery, then drains anything queued meanwhile.</summary>
    /// <param name="value">The value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void DeliverClaimed(T value) => _delivery.DeliverClaimed(_observer, value, new PendingDrain(this));

    /// <summary>Delivers values in order on a claimed delivery, then drains anything queued meanwhile.</summary>
    /// <param name="values">The values, delivered first to last.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void DeliverClaimed(T[] values) => _delivery.DeliverClaimed(_observer, values, new PendingDrain(this));

    /// <summary>Drains this witness's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The witness.</param>
    private readonly record struct PendingDrain(SerializedWitness<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer);
    }
}
