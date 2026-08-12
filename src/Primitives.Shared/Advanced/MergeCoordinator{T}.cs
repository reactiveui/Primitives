// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates concurrent merge subscriptions.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class MergeCoordinator<T> : IDisposable
{
    /// <summary>Serializes downstream callbacks and counters.</summary>
    private readonly Lock _gate = new();

    /// <summary>Initializes a new instance of the <see cref="MergeCoordinator{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public MergeCoordinator(IObserver<T> observer) =>
        Observer = observer ?? throw new ArgumentNullException(nameof(observer));

    /// <summary>Gets the active subscriptions.</summary>
    private MultipleDisposable Subscriptions { get; } = [];

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <summary>Gets or sets a value indicating whether the outer source has completed.</summary>
    private bool OuterCompleted { get; set; }

    /// <summary>Gets or sets the number of active inner subscriptions.</summary>
    private int Active { get; set; }

    /// <summary>Gets or sets a value indicating whether a terminal notification has been emitted.</summary>
    private bool Done { get; set; }

    /// <inheritdoc/>
    public void Dispose() => Subscriptions.Dispose();

    /// <summary>Subscribes to enumerable sources.</summary>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>This coordinator as the subscription.</returns>
    public MergeCoordinator<T> Run(IEnumerable<IObservable<T>> sources)
    {
        foreach (var source in sources)
        {
            OnSource(source);
        }

        OnOuterCompleted();
        return this;
    }

    /// <summary>Subscribes to a pair of sources.</summary>
    /// <param name="first">The first source.</param>
    /// <param name="second">The second source.</param>
    /// <returns>This coordinator as the subscription.</returns>
    public MergeCoordinator<T> Run(IObservable<T> first, IObservable<T> second)
    {
        OnSource(first);
        OnSource(second);
        OnOuterCompleted();
        return this;
    }

    /// <summary>Subscribes a new inner source.</summary>
    /// <param name="source">The inner source.</param>
    public void OnSource(IObservable<T>? source)
    {
        if (source is null)
        {
            OnAnyError(new InvalidOperationException("Blend source contained null."));
            return;
        }

        lock (_gate)
        {
            Active++;
        }

        // A source is free to signal completion more than once. Latch per inner so a repeat cannot decrement
        // the active count a second time - that would drop the count on behalf of a sibling that is still
        // running and complete the merge early, losing everything the sibling had left to produce.
        var completed = 0;
        Subscriptions.Add(source.Subscribe(
            OnInnerNext,
            OnAnyError,
            () =>
            {
                if (Interlocked.Exchange(ref completed, 1) != 0)
                {
                    return;
                }

                OnInnerCompleted();
            }));
    }

    /// <summary>Forwards the first terminal error.</summary>
    /// <param name="error">The error to forward.</param>
    public void OnAnyError(Exception error)
    {
        lock (_gate)
        {
            if (Done)
            {
                return;
            }

            Done = true;
            Observer.OnError(error);
        }
    }

    /// <summary>Forwards an inner value.</summary>
    /// <param name="value">The value to forward.</param>
    private void OnInnerNext(T value)
    {
        lock (_gate)
        {
            if (!Done)
            {
                Observer.OnNext(value);
            }
        }
    }

    /// <summary>Marks one inner source complete.</summary>
    private void OnInnerCompleted()
    {
        lock (_gate)
        {
            Active--;
        }

        TryComplete();
    }

    /// <summary>Marks source enumeration complete.</summary>
    private void OnOuterCompleted()
    {
        lock (_gate)
        {
            OuterCompleted = true;
        }

        TryComplete();
    }

    /// <summary>Completes once enumeration and all inners are done.</summary>
    private void TryComplete()
    {
        lock (_gate)
        {
            if (Done || !OuterCompleted || Active != 0)
            {
                return;
            }

            Done = true;
            Observer.OnCompleted();
        }
    }
}
