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

/// <summary>Mediates sequential concatenation for <see cref="ChainSignal{T}"/>.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// The gate only guards the queue and flags. Deliveries are serialized by a <see cref="SerializedDelivery{T}"/> and the
/// next inner source is subscribed after the gate is released, so no lock is held while the observer or an inner source
/// runs. A terminal notification raised before <see cref="Dispose"/> is still delivered; nothing raised after it is.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("ChainWitness: IsActive = {IsActive}, IsOuterCompleted = {IsOuterCompleted}")]
public sealed class ChainWitness<T> : IDisposable
{
    /// <summary>Guards the queue and flags; never held while user code runs.</summary>
    private readonly Lock _gate = new();

    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>Whether a terminal notification has been queued or the witness disposed; written under the gate.</summary>
    private bool _done;

    /// <summary>Initializes a new instance of the <see cref="ChainWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public ChainWitness(IObserver<T> observer) => Observer = observer;

    /// <summary>Gets queued sources awaiting subscription.</summary>
    private Queue<IObservable<T>> Queue { get; } = new();

    /// <summary>Gets active subscriptions.</summary>
    private MultipleDisposable Subscriptions { get; } = [];

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <summary>Gets or sets a value indicating whether an inner source is active.</summary>
    private bool IsActive { get; set; }

    /// <summary>Gets or sets a value indicating whether the outer source completed.</summary>
    private bool IsOuterCompleted { get; set; }

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (_gate)
        {
            Volatile.Write(ref _done, true);
            Queue.Clear();
        }

        Subscriptions.Dispose();
    }

    /// <summary>Starts concatenating an outer observable of sources.</summary>
    /// <param name="sources">The outer source.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public ChainWitness<T> Run(IObservable<IObservable<T>> sources)
    {
        Subscriptions.Add(sources.Subscribe(OnSource, OnError, OnOuterCompleted));
        return this;
    }

    /// <summary>Starts concatenating enumerable sources.</summary>
    /// <param name="sources">The sources to concatenate.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public ChainWitness<T> Run(IEnumerable<IObservable<T>> sources)
    {
        foreach (var source in sources)
        {
            OnSource(source);
        }

        OnOuterCompleted();
        return this;
    }

    /// <summary>Starts concatenating two fixed sources.</summary>
    /// <param name="first">The first source.</param>
    /// <param name="second">The second source.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public ChainWitness<T> Run(IObservable<T> first, IObservable<T> second)
    {
        lock (_gate)
        {
            Queue.Enqueue(first);
            Queue.Enqueue(second);
            IsOuterCompleted = true;
        }

        Drain();
        return this;
    }

    /// <summary>Queues a new inner source.</summary>
    /// <param name="source">The inner source.</param>
    private void OnSource(IObservable<T> source)
    {
        if (source is null)
        {
            OnError(new InvalidOperationException("Chain source contained null."));
            return;
        }

        lock (_gate)
        {
            Queue.Enqueue(source);
        }

        Drain();
    }

    /// <summary>Marks the outer source complete.</summary>
    private void OnOuterCompleted()
    {
        lock (_gate)
        {
            IsOuterCompleted = true;
        }

        Drain();
    }

    /// <summary>Marks the active inner source complete.</summary>
    private void OnInnerCompleted()
    {
        lock (_gate)
        {
            IsActive = false;
        }

        Drain();
    }

    /// <summary>Forwards an inner value unless the witness has terminated or been disposed.</summary>
    /// <param name="value">The value to forward.</param>
    private void OnInnerNext(T value)
    {
        if (Volatile.Read(ref _done))
        {
            return;
        }

        _delivery.OnNext(Observer, value, new PendingDrain(this));
    }

    /// <summary>Queues the first terminal error and stops subscribing further sources.</summary>
    /// <param name="error">The error to forward.</param>
    private void OnError(Exception error)
    {
        lock (_gate)
        {
            if (_done)
            {
                return;
            }

            Volatile.Write(ref _done, true);
            Queue.Clear();
            _ = _delivery.PostError(error);
        }

        _delivery.Flush(new PendingDrain(this));
    }

    /// <summary>Subscribes the next queued source, or completes when the outer source is done and nothing is left.</summary>
    private void Drain()
    {
        IObservable<T>? next = null;
        lock (_gate)
        {
            if (_done)
            {
                Queue.Clear();
                return;
            }

            if (IsActive)
            {
                return;
            }

            if (Queue.Count > 0)
            {
                IsActive = true;
                next = Queue.Dequeue();
            }
            else if (IsOuterCompleted)
            {
                Volatile.Write(ref _done, true);
                _ = _delivery.PostCompleted();
            }
            else
            {
                return;
            }
        }

        if (next is null)
        {
            _delivery.Flush(new PendingDrain(this));
            return;
        }

        Subscriptions.Add(next.Subscribe(OnInnerNext, OnError, OnInnerCompleted));
    }

    /// <summary>Drains this witness's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The witness.</param>
    private readonly record struct PendingDrain(ChainWitness<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner.Observer);
    }
}
