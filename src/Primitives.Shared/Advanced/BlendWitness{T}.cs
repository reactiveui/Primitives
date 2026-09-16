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

/// <summary>Mediates concurrent merging for <see cref="BlendSignal{T}"/> and <see cref="EnumerableBlendSignal{T}"/>.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// Deliveries are serialized by a <see cref="SerializedDelivery{T}"/>, so no lock is held while the downstream observer
/// runs. Values that arrive while another thread is delivering are queued and delivered in arrival order; a value raised
/// by the delivering thread itself is delivered after the observer returns.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("BlendWitness: ActiveCount = {ActiveCount}, IsOuterCompleted = {IsOuterCompleted}, IsDone = {IsDone}")]
public sealed class BlendWitness<T> : IDisposable
{
    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>The number of active inner sources.</summary>
    private int _active;

    /// <summary>Whether the outer source completed, as 0 or 1.</summary>
    private int _outerCompleted;

    /// <summary>Initializes a new instance of the <see cref="BlendWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public BlendWitness(IObserver<T> observer) => Observer = observer;

    /// <summary>Gets the active subscriptions.</summary>
    private MultipleDisposable Subscriptions { get; } = [];

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <summary>Gets a value indicating whether the outer source completed.</summary>
    private bool IsOuterCompleted => Volatile.Read(ref _outerCompleted) != 0;

    /// <summary>Gets the number of active inner sources.</summary>
    private int ActiveCount => Volatile.Read(ref _active);

    /// <summary>Gets a value indicating whether the terminal notification has been taken for delivery.</summary>
    private bool IsDone => _delivery.IsTerminated;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Subscriptions.Dispose();

    /// <summary>Starts merging an outer observable of inner sources.</summary>
    /// <param name="sources">The outer source.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public BlendWitness<T> Run(IObservable<IObservable<T>> sources)
    {
        Subscriptions.Add(sources.Subscribe(OnSource, OnAnyError, OnOuterCompleted));
        return this;
    }

    /// <summary>Starts merging enumerable sources.</summary>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public BlendWitness<T> Run(IEnumerable<IObservable<T>> sources)
    {
        foreach (var source in sources)
        {
            OnSource(source);
        }

        OnOuterCompleted();
        return this;
    }

    /// <summary>Subscribes to a new inner source.</summary>
    /// <param name="source">The inner source.</param>
    private void OnSource(IObservable<T> source)
    {
        if (source is null)
        {
            OnAnyError(new InvalidOperationException("Blend source contained null."));
            return;
        }

        _ = Interlocked.Increment(ref _active);
        Subscriptions.Add(source.Subscribe(OnInnerNext, OnAnyError, OnInnerCompleted));
    }

    /// <summary>Forwards an inner value, directly when nothing else is delivering.</summary>
    /// <param name="value">The value to forward.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnInnerNext(T value) => _delivery.OnNext(Observer, value, new PendingDrain(this));

    /// <summary>Forwards the first terminal error.</summary>
    /// <param name="error">The error to forward.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnAnyError(Exception error) => _delivery.OnError(error, new PendingDrain(this));

    /// <summary>Marks one inner source complete.</summary>
    private void OnInnerCompleted()
    {
        _ = Interlocked.Decrement(ref _active);
        TryComplete();
    }

    /// <summary>Marks the outer source complete.</summary>
    private void OnOuterCompleted()
    {
        Volatile.Write(ref _outerCompleted, 1);
        TryComplete();
    }

    /// <summary>Delivers completion once the outer and all active inner sources are done.</summary>
    private void TryComplete()
    {
        if (IsDone || !IsOuterCompleted || ActiveCount != 0)
        {
            return;
        }

        _delivery.OnCompleted(new PendingDrain(this));
    }

    /// <summary>Drains this witness's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The witness.</param>
    private readonly record struct PendingDrain(BlendWitness<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner.Observer);
    }
}
