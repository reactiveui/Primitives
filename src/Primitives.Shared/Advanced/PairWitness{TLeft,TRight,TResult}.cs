// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Mediates pair-by-index combination for <see cref="PairSignal{TLeft, TRight, TResult}"/>.</summary>
/// <typeparam name="TLeft">The left value type.</typeparam>
/// <typeparam name="TRight">The right value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
public sealed class PairWitness<TLeft, TRight, TResult>
{
    /// <summary>The synchronization gate.</summary>
    private readonly Lock _gate = new();

    /// <summary>Initializes a new instance of the <see cref="PairWitness{TLeft, TRight, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="selector">The result projection.</param>
    public PairWitness(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector)
    {
        Observer = observer;
        Selector = selector;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<TResult> Observer { get; }

    /// <summary>Gets the result projection.</summary>
    private Func<TLeft, TRight, TResult> Selector { get; }

    /// <summary>Gets queued left values.</summary>
    private Queue<TLeft> LeftQueue { get; } = new();

    /// <summary>Gets queued right values.</summary>
    private Queue<TRight> RightQueue { get; } = new();

    /// <summary>Gets or sets a value indicating whether the left source completed.</summary>
    private bool IsLeftCompleted { get; set; }

    /// <summary>Gets or sets a value indicating whether the right source completed.</summary>
    private bool IsRightCompleted { get; set; }

    /// <summary>Gets or sets a value indicating whether completion has been emitted.</summary>
    private bool IsCompleted { get; set; }

    /// <summary>Subscribes to both sources.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <returns>The subscriptions.</returns>
    public MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
        new(
            left.Subscribe(OnLeftNext, Observer.OnError, OnLeftCompleted),
            right.Subscribe(OnRightNext, Observer.OnError, OnRightCompleted));

    /// <summary>Queues a left value.</summary>
    /// <param name="value">The left value.</param>
    private void OnLeftNext(TLeft value)
    {
        lock (_gate)
        {
            LeftQueue.Enqueue(value);
        }

        Drain();
    }

    /// <summary>Queues a right value.</summary>
    /// <param name="value">The right value.</param>
    private void OnRightNext(TRight value)
    {
        lock (_gate)
        {
            RightQueue.Enqueue(value);
        }

        Drain();
    }

    /// <summary>Marks the left source complete.</summary>
    private void OnLeftCompleted()
    {
        lock (_gate)
        {
            IsLeftCompleted = true;
        }

        Drain();
    }

    /// <summary>Marks the right source complete.</summary>
    private void OnRightCompleted()
    {
        lock (_gate)
        {
            IsRightCompleted = true;
        }

        Drain();
    }

    /// <summary>Emits available pairs and completes when no more pairs can be formed.</summary>
    private void Drain()
    {
        lock (_gate)
        {
            if (IsCompleted)
            {
                return;
            }

            while (LeftQueue.Count != 0 && RightQueue.Count != 0)
            {
                Observer.OnNext(Selector(LeftQueue.Dequeue(), RightQueue.Dequeue()));
            }

            if ((!IsLeftCompleted || LeftQueue.Count != 0) && (!IsRightCompleted || RightQueue.Count != 0))
            {
                return;
            }

            IsCompleted = true;
            Observer.OnCompleted();
        }
    }
}
