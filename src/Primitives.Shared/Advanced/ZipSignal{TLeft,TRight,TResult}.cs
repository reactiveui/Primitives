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

/// <summary>Signal that pairs values from two sources by index and projects each pair.</summary>
/// <typeparam name="TLeft">The left value type.</typeparam>
/// <typeparam name="TRight">The right value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ZipSignal: Left = {_left}, Right = {_right}")]
public sealed class ZipSignal<TLeft, TRight, TResult> : IObservable<TResult>
{
    /// <summary>The left source.</summary>
    private readonly IObservable<TLeft> _left;

    /// <summary>The right source.</summary>
    private readonly IObservable<TRight> _right;

    /// <summary>The projection function.</summary>
    private readonly Func<TLeft, TRight, TResult> _selector;

    /// <summary>Initializes a new instance of the <see cref="ZipSignal{TLeft, TRight, TResult}"/> class.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <param name="selector">The projection function.</param>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public ZipSignal(IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(left);
        ArgumentExceptionHelper.ThrowIfNull(right);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        _left = left;
        _right = right;
        _selector = selector;
    }

    /// <summary>Creates a paired sequence, using the range fast path when both sources are <see cref="RangeSignal"/>.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <param name="selector">The projection function.</param>
    /// <returns>The zipped observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/>, or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> Create(
        IObservable<TLeft> left,
        IObservable<TRight> right,
        Func<TLeft, TRight, TResult> selector) =>
        BinaryCoordinatorHelper.ValidateAndTryMatchRanges(left, right, selector, out var ranges)
            ? new RangeZipSignal<TResult>(ranges.Left, ranges.Right, ranges.Selector)
            : new ZipSignal<TLeft, TRight, TResult>(left, right, selector);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new PairWitness<TLeft, TRight, TResult>(observer, _selector).Run(_left, _right);
    }
}
