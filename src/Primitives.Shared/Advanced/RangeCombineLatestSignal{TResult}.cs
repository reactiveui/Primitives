// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Combines two synchronous integer ranges using System.Reactive <c>CombineLatest</c> semantics.</summary>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Left = {Left}, Right = {Right}")]
public sealed class RangeCombineLatestSignal<TResult> : IObservable<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="RangeCombineLatestSignal{TResult}"/> class.</summary>
    /// <param name="left">The left range.</param>
    /// <param name="right">The right range.</param>
    /// <param name="selector">The result projection.</param>
    public RangeCombineLatestSignal(RangeSignal left, RangeSignal right, Func<int, int, TResult> selector)
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<TResult> observer) =>
        RangeLatestSignalHelper.Subscribe(observer, Left, Right, Selector);
}
