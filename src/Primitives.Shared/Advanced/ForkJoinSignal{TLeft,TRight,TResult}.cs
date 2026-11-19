// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Internal;
#else
using ReactiveUI.Primitives.Internal;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits one result from the final values of two completed sources.</summary>
/// <typeparam name="TLeft">The left value type.</typeparam>
/// <typeparam name="TRight">The right value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ForkJoinSignal: Left = {Left}, Right = {Right}")]
public sealed class ForkJoinSignal<TLeft, TRight, TResult> : IObservable<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="ForkJoinSignal{TLeft, TRight, TResult}"/> class.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <param name="selector">The result projection.</param>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public ForkJoinSignal(IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(left);
        ArgumentExceptionHelper.ThrowIfNull(right);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        Left = left;
        Right = right;
        Selector = selector;
    }

    /// <summary>Gets the left source.</summary>
    private IObservable<TLeft> Left { get; }

    /// <summary>Gets the right source.</summary>
    private IObservable<TRight> Right { get; }

    /// <summary>Gets the result projection.</summary>
    private Func<TLeft, TRight, TResult> Selector { get; }

    /// <summary>Creates a fork-join sequence, using the range fast path when both sources are <see cref="RangeSignal"/>.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <param name="selector">The result projection.</param>
    /// <returns>The fork-join observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> Create(
        IObservable<TLeft> left,
        IObservable<TRight> right,
        Func<TLeft, TRight, TResult> selector) =>
        BinaryCoordinatorHelper.ValidateAndTryMatchRanges(left, right, selector, out var ranges)
            ? new RangeForkJoinSignal<TResult>(ranges.Left, ranges.Right, ranges.Selector)
            : new ForkJoinSignal<TLeft, TRight, TResult>(left, right, selector);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new ForkJoinWitness<TLeft, TRight, TResult>(observer, Selector).Run(Left, Right);
    }
}
