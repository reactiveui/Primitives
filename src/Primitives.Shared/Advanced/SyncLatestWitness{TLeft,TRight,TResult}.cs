// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Mediates latest-value combination for <see cref="SyncLatestSignal{TLeft, TRight, TResult}"/>.</summary>
/// <typeparam name="TLeft">The left value type.</typeparam>
/// <typeparam name="TRight">The right value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
public sealed class SyncLatestWitness<TLeft, TRight, TResult>
{
    /// <summary>The synchronization gate.</summary>
    private readonly Lock _gate = new();

    /// <summary>Initializes a new instance of the <see cref="SyncLatestWitness{TLeft, TRight, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="selector">The result projection.</param>
    public SyncLatestWitness(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector)
    {
        Observer = observer;
        Selector = selector;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<TResult> Observer { get; }

    /// <summary>Gets the result projection.</summary>
    private Func<TLeft, TRight, TResult> Selector { get; }

    /// <summary>Gets or sets a value indicating whether the left source has produced a value.</summary>
    private bool HasLeft { get; set; }

    /// <summary>Gets or sets a value indicating whether the right source has produced a value.</summary>
    private bool HasRight { get; set; }

    /// <summary>Gets or sets a value indicating whether the left source completed.</summary>
    private bool IsLeftDone { get; set; }

    /// <summary>Gets or sets a value indicating whether the right source completed.</summary>
    private bool IsRightDone { get; set; }

    /// <summary>Gets or sets a value indicating whether completion has been emitted.</summary>
    private bool IsCompleted { get; set; }

    /// <summary>Gets or sets the latest left value.</summary>
    private TLeft? LatestLeft { get; set; }

    /// <summary>Gets or sets the latest right value.</summary>
    private TRight? LatestRight { get; set; }

    /// <summary>Subscribes to both sources.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <returns>The subscriptions.</returns>
    public MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
        new(
            left.Subscribe(OnLeftNext, Observer.OnError, OnLeftCompleted),
            right.Subscribe(OnRightNext, Observer.OnError, OnRightCompleted));

    /// <summary>Handles a left value.</summary>
    /// <param name="value">The left value.</param>
    private void OnLeftNext(TLeft value)
    {
        lock (_gate)
        {
            LatestLeft = value;
            HasLeft = true;
            if (!IsCompleted && TryProject(out var projected))
            {
                Observer.OnNext(projected);
            }
        }
    }

    /// <summary>Handles a right value.</summary>
    /// <param name="value">The right value.</param>
    private void OnRightNext(TRight value)
    {
        lock (_gate)
        {
            LatestRight = value;
            HasRight = true;
            if (!IsCompleted && TryProject(out var projected))
            {
                Observer.OnNext(projected);
            }
        }
    }

    /// <summary>Marks the left source complete.</summary>
    private void OnLeftCompleted()
    {
        lock (_gate)
        {
            IsLeftDone = true;
            if (IsCompleted || !IsRightDone)
            {
                return;
            }

            IsCompleted = true;
            Observer.OnCompleted();
        }
    }

    /// <summary>Marks the right source complete.</summary>
    private void OnRightCompleted()
    {
        lock (_gate)
        {
            IsRightDone = true;
            if (IsCompleted || !IsLeftDone)
            {
                return;
            }

            IsCompleted = true;
            Observer.OnCompleted();
        }
    }

    /// <summary>Projects the current latest values.</summary>
    /// <param name="result">The projected value.</param>
    /// <returns><see langword="true"/> when both sources have values.</returns>
    private bool TryProject(out TResult result)
    {
        if (!HasLeft || !HasRight)
        {
            result = default!;
            return false;
        }

        result = Selector(LatestLeft!, LatestRight!);
        return true;
    }
}
