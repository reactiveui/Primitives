// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Fork-joins two synchronous integer ranges by projecting their final values.</summary>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Left = {Left.Start}+{Left.Count}, Right = {Right.Start}+{Right.Count}")]
public sealed class RangeForkJoinSignal<TResult> : IObservable<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="RangeForkJoinSignal{TResult}"/> class.</summary>
    /// <param name="left">The left range.</param>
    /// <param name="right">The right range.</param>
    /// <param name="selector">The result projection.</param>
    public RangeForkJoinSignal(RangeSignal left, RangeSignal right, Func<int, int, TResult> selector)
    {
        Left = left;
        Right = right;
        Selector = selector;
    }

    /// <summary>Gets the left range.</summary>
    private RangeSignal Left { get; }

    /// <summary>Gets the right range.</summary>
    private RangeSignal Right { get; }

    /// <summary>Gets the result projection.</summary>
    private Func<int, int, TResult> Selector { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        observer.OnNext(Selector(Left.Start + Left.Count - 1, Right.Start + Right.Count - 1));
        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }
}
