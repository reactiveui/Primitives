// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Combines the latest values from two sources after both have produced a value.</summary>
/// <typeparam name="TLeft">The left value type.</typeparam>
/// <typeparam name="TRight">The right value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Left = {Left}, Right = {Right}")]
public sealed class SyncLatestSignal<TLeft, TRight, TResult> : IObservable<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="SyncLatestSignal{TLeft, TRight, TResult}"/> class.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <param name="selector">The result projection.</param>
    public SyncLatestSignal(IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
    {
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

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new SyncLatestWitness<TLeft, TRight, TResult>(observer, Selector).Run(Left, Right);
    }
}
