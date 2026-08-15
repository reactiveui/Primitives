// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Mediates concurrent merging for <see cref="BlendSignal{T}"/> and <see cref="EnumerableBlendSignal{T}"/>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ActiveCount = {ActiveCount}, IsOuterCompleted = {IsOuterCompleted}, IsDone = {IsDone}")]
public sealed class BlendWitness<T> : IDisposable
{
    /// <summary>Serializes downstream callbacks and guards counters.</summary>
    private readonly Lock _gate = new();

    /// <summary>Initializes a new instance of the <see cref="BlendWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public BlendWitness(IObserver<T> observer) => Observer = observer;

    /// <summary>Gets the active subscriptions.</summary>
    private MultipleDisposable Subscriptions { get; } = [];

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <summary>Gets or sets a value indicating whether the outer source completed.</summary>
    private bool IsOuterCompleted { get; set; }

    /// <summary>Gets or sets the number of active inner sources.</summary>
    private int ActiveCount { get; set; }

    /// <summary>Gets or sets a value indicating whether a terminal notification has been emitted.</summary>
    private bool IsDone { get; set; }

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

        lock (_gate)
        {
            ActiveCount++;
        }

        Subscriptions.Add(source.Subscribe(OnInnerNext, OnAnyError, OnInnerCompleted));
    }

    /// <summary>Forwards an inner value under the serialization gate.</summary>
    /// <param name="value">The value to forward.</param>
    private void OnInnerNext(T value)
    {
        lock (_gate)
        {
            if (!IsDone)
            {
                Observer.OnNext(value);
            }
        }
    }

    /// <summary>Forwards the first terminal error.</summary>
    /// <param name="error">The error to forward.</param>
    private void OnAnyError(Exception error)
    {
        lock (_gate)
        {
            if (IsDone)
            {
                return;
            }

            IsDone = true;
            Observer.OnError(error);
        }
    }

    /// <summary>Marks one inner source complete.</summary>
    private void OnInnerCompleted()
    {
        lock (_gate)
        {
            ActiveCount--;
        }

        TryComplete();
    }

    /// <summary>Marks the outer source complete.</summary>
    private void OnOuterCompleted()
    {
        lock (_gate)
        {
            IsOuterCompleted = true;
        }

        TryComplete();
    }

    /// <summary>Completes once the outer and all active inner sources are done.</summary>
    private void TryComplete()
    {
        lock (_gate)
        {
            if (IsDone || !IsOuterCompleted || ActiveCount != 0)
            {
                return;
            }

            IsDone = true;
            Observer.OnCompleted();
        }
    }
}
