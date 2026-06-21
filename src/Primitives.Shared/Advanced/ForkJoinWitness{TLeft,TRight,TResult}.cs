// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Mediates two-source fork-join completion for <see cref="ForkJoinSignal{TLeft, TRight, TResult}"/>.</summary>
/// <typeparam name="TLeft">The left value type.</typeparam>
/// <typeparam name="TRight">The right value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
public sealed class ForkJoinWitness<TLeft, TRight, TResult>
{
    /// <summary>The synchronization gate.</summary>
    private readonly Lock _gate = new();

    /// <summary>Initializes a new instance of the <see cref="ForkJoinWitness{TLeft, TRight, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="selector">The result projection.</param>
    public ForkJoinWitness(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector)
    {
        Observer = observer;
        Selector = selector;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<TResult> Observer { get; }

    /// <summary>Gets the result projection.</summary>
    private Func<TLeft, TRight, TResult> Selector { get; }

    /// <summary>Gets or sets a value indicating whether the left source produced a value.</summary>
    private bool HasLeft { get; set; }

    /// <summary>Gets or sets a value indicating whether the right source produced a value.</summary>
    private bool HasRight { get; set; }

    /// <summary>Gets or sets a value indicating whether the left source completed.</summary>
    private bool IsLeftDone { get; set; }

    /// <summary>Gets or sets a value indicating whether the right source completed.</summary>
    private bool IsRightDone { get; set; }

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

    /// <summary>Records a left value.</summary>
    /// <param name="value">The left value.</param>
    private void OnLeftNext(TLeft value)
    {
        lock (_gate)
        {
            HasLeft = true;
            LatestLeft = value;
        }
    }

    /// <summary>Records a right value.</summary>
    /// <param name="value">The right value.</param>
    private void OnRightNext(TRight value)
    {
        lock (_gate)
        {
            HasRight = true;
            LatestRight = value;
        }
    }

    /// <summary>Marks the left source complete.</summary>
    private void OnLeftCompleted()
    {
        if (!CompleteLeft(out var result, out var emit))
        {
            return;
        }

        Finish(result, emit);
    }

    /// <summary>Marks the right source complete.</summary>
    private void OnRightCompleted()
    {
        if (!CompleteRight(out var result, out var emit))
        {
            return;
        }

        Finish(result, emit);
    }

    /// <summary>Completes the left side and computes the result when both sides are done.</summary>
    /// <param name="result">The result to emit.</param>
    /// <param name="emit">Whether a result should be emitted.</param>
    /// <returns><see langword="true"/> when fork-join is ready to finish.</returns>
    private bool CompleteLeft(out TResult result, out bool emit)
    {
        lock (_gate)
        {
            IsLeftDone = true;
            return TryFinish(out result, out emit);
        }
    }

    /// <summary>Completes the right side and computes the result when both sides are done.</summary>
    /// <param name="result">The result to emit.</param>
    /// <param name="emit">Whether a result should be emitted.</param>
    /// <returns><see langword="true"/> when fork-join is ready to finish.</returns>
    private bool CompleteRight(out TResult result, out bool emit)
    {
        lock (_gate)
        {
            IsRightDone = true;
            return TryFinish(out result, out emit);
        }
    }

    /// <summary>Computes the result when both sources are complete.</summary>
    /// <param name="result">The result to emit.</param>
    /// <param name="emit">Whether a result should be emitted.</param>
    /// <returns><see langword="true"/> when both sources are complete.</returns>
    private bool TryFinish(out TResult result, out bool emit)
    {
        if (!IsLeftDone || !IsRightDone)
        {
            result = default!;
            emit = false;
            return false;
        }

        emit = HasLeft && HasRight;
        result = emit ? Selector(LatestLeft!, LatestRight!) : default!;
        return true;
    }

    /// <summary>Emits the result when present, then completes.</summary>
    /// <param name="result">The result to emit.</param>
    /// <param name="emit">Whether a result should be emitted.</param>
    private void Finish(TResult result, bool emit)
    {
        if (emit)
        {
            Observer.OnNext(result);
        }

        Observer.OnCompleted();
    }
}
