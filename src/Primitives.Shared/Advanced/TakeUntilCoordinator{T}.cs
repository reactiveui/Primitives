// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates serialized observer callbacks and subscription lifetime.</summary>
/// <typeparam name="T">The source value type.</typeparam>
/// <remarks>
/// Deliveries are serialized by a <see cref="SerializedDelivery{T}"/>, so no lock is held while the observer runs; the
/// first terminal notification wins and nothing follows it.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("TakeUntilCoordinator<T>: Observer = {_observer}")]
public sealed class TakeUntilCoordinator<T> : IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>Tracks the source and cancellation subscriptions.</summary>
    private readonly MultipleDisposable _subscriptions = [];

    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>Indicates whether the sequence has stopped.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="TakeUntilCoordinator{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public TakeUntilCoordinator(IObserver<T> observer) => _observer = observer;

    /// <summary>Gets a value indicating whether the sequence has stopped.</summary>
    internal bool IsStopped => Volatile.Read(ref _stopped) != 0;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _subscriptions.Dispose();

    /// <summary>Adds a subscription to the coordinator lifetime.</summary>
    /// <param name="subscription">The subscription to add.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Add(IDisposable subscription) => _subscriptions.Add(subscription);

    /// <summary>Forwards a source value when the sequence has not stopped.</summary>
    /// <param name="value">The source value.</param>
    internal void Next(T value)
    {
        if (IsStopped)
        {
            return;
        }

        _delivery.OnNext(_observer, value, new PendingDrain(this));
    }

    /// <summary>Completes the downstream observer once, after any value being delivered, and disposes all subscriptions.</summary>
    internal void Complete()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        _delivery.OnCompleted(new PendingDrain(this));
        _subscriptions.Dispose();
    }

    /// <summary>Sends an error to the downstream observer once, after any value being delivered, and disposes all subscriptions.</summary>
    /// <param name="exception">The exception to forward.</param>
    internal void Error(Exception exception)
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        _delivery.OnError(exception, new PendingDrain(this));
        _subscriptions.Dispose();
    }

    /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The coordinator.</param>
    private readonly record struct PendingDrain(TakeUntilCoordinator<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer);
    }
}
