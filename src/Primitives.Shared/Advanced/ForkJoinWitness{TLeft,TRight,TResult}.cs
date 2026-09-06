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
[System.Diagnostics.DebuggerDisplay("ForkJoinWitness: HasLeft = {HasLeft}, HasRight = {HasRight}, IsDone = {IsDone}")]
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

    /// <summary>Gets or sets a value indicating whether a terminal notification has been emitted.</summary>
    private bool IsDone { get; set; }

    /// <summary>Subscribes to both sources.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <returns>The subscriptions.</returns>
    public MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
        new(
            left.Subscribe(OnLeftNext, OnError, OnLeftCompleted),
            right.Subscribe(OnRightNext, OnError, OnRightCompleted));

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
        lock (_gate)
        {
            if (IsDone)
            {
                return;
            }

            IsLeftDone = true;
            TryFinish();
        }
    }

    /// <summary>Marks the right source complete.</summary>
    private void OnRightCompleted()
    {
        lock (_gate)
        {
            if (IsDone)
            {
                return;
            }

            IsRightDone = true;
            TryFinish();
        }
    }

    /// <summary>Forwards the first source error and gates every later notification.</summary>
    /// <param name="error">The error to forward.</param>
    private void OnError(Exception error)
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

    /// <summary>Emits the result and completes once both sources are done.</summary>
    /// <remarks>Must be called while holding <see cref="_gate"/> so the terminal notification stays serialized.</remarks>
    private void TryFinish()
    {
        if (!IsLeftDone || !IsRightDone)
        {
            return;
        }

        IsDone = true;

        if (HasLeft && HasRight)
        {
            Observer.OnNext(Selector(LatestLeft!, LatestRight!));
        }

        Observer.OnCompleted();
    }
}
