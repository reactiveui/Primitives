// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates the general WithLatest projection for <c>Latch</c>.</summary>
/// <typeparam name="TLeft">The left value type.</typeparam>
/// <typeparam name="TRight">The right value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="selector">The projection function.</param>
internal sealed class LatchCoordinator<TLeft, TRight, TResult>(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector)
{
    /// <summary>Guards the latest-right state.</summary>
    private readonly Lock _gate = new();

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer = observer;

    /// <summary>The projection function.</summary>
    private readonly Func<TLeft, TRight, TResult> _selector = selector;

    /// <summary>A value indicating whether the right source has produced a value.</summary>
    private bool _hasRight;

    /// <summary>The latest right value.</summary>
    private TRight? _latestRight;

    /// <summary>Subscribes to both sources.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <returns>The subscription cleanup.</returns>
    internal MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
        new(
            right.Subscribe(OnRightNext, _observer.OnError, NoOp),
            left.Subscribe(OnLeftNext, _observer.OnError, _observer.OnCompleted));

    /// <summary>No-op completion handler for the right (latched) source.</summary>
    private static void NoOp()
    {
        // The right source's completion does not terminate the latch; only the left source does.
    }

    /// <summary>Stores the latest right value.</summary>
    /// <param name="value">The right value.</param>
    private void OnRightNext(TRight value)
    {
        lock (_gate)
        {
            _hasRight = true;
            _latestRight = value;
        }
    }

    /// <summary>Projects a left value with the latest right value when available.</summary>
    /// <param name="value">The left value.</param>
    private void OnLeftNext(TLeft value)
    {
        TRight rightValue;
        lock (_gate)
        {
            if (!_hasRight)
            {
                return;
            }

            rightValue = _latestRight!;
        }

        _observer.OnNext(_selector(value, rightValue));
    }
}
