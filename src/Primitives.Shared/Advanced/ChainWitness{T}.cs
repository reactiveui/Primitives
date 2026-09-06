// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Mediates sequential concatenation for <see cref="ChainSignal{T}"/>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ChainWitness: IsActive = {IsActive}, IsOuterCompleted = {IsOuterCompleted}")]
public sealed class ChainWitness<T> : IDisposable
{
    /// <summary>Guards the queue and active/completed flags.</summary>
    private readonly Lock _gate = new();

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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Subscriptions.Dispose();

    /// <summary>Starts concatenating an outer observable of sources.</summary>
    /// <param name="sources">The outer source.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public ChainWitness<T> Run(IObservable<IObservable<T>> sources)
    {
        Subscriptions.Add(sources.Subscribe(OnSource, Observer.OnError, OnOuterCompleted));
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
            Observer.OnError(new InvalidOperationException("Chain source contained null."));
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

    /// <summary>Subscribes the next queued source or completes when drained.</summary>
    private void Drain()
    {
        IObservable<T>? next = null;
        lock (_gate)
        {
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
                Observer.OnCompleted();
                return;
            }
        }

        if (next is null)
        {
            return;
        }

        Subscriptions.Add(next.Subscribe(Observer.OnNext, Observer.OnError, OnInnerCompleted));
    }
}
