// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that pairs each left value with the latest value held from the right source.</summary>
/// <typeparam name="TLeft">The left value type.</typeparam>
/// <typeparam name="TRight">The right value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("LatchSignal: Left = {_left}, Right = {_right}")]
public sealed class LatchSignal<TLeft, TRight, TResult> : IObservable<TResult>
{
    /// <summary>The left (driving) source.</summary>
    private readonly IObservable<TLeft> _left;

    /// <summary>The right (latched) source.</summary>
    private readonly IObservable<TRight> _right;

    /// <summary>The projection function.</summary>
    private readonly Func<TLeft, TRight, TResult> _selector;

    /// <summary>Initializes a new instance of the <see cref="LatchSignal{TLeft, TRight, TResult}"/> class.</summary>
    /// <param name="left">The left (driving) source.</param>
    /// <param name="right">The right (latched) source.</param>
    /// <param name="selector">The projection function.</param>
    /// <exception cref="ArgumentNullException"><paramref name="left"/>, <paramref name="right"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public LatchSignal(IObservable<TLeft> left, IObservable<TRight> right, Func<TLeft, TRight, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(left);
        ArgumentExceptionHelper.ThrowIfNull(right);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        _left = left;
        _right = right;
        _selector = selector;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new LatchCoordinator<TLeft, TRight, TResult>(observer, _selector).Run(_left, _right);
    }
}
