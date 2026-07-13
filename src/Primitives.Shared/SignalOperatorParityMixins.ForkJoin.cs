// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>The ForkJoin operator: pairs the final value of each source once both have completed.</summary>
public static partial class LinqExtensions
{
    /// <summary>Dedicated signal for <c>ForkJoin</c>; runs the coordinator without a Create closure.</summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <param name="selector">The projection of the two final values.</param>
    private sealed class ForkJoinSignal<TLeft, TRight, TResult>(
        IObservable<TLeft> left,
        IObservable<TRight> right,
        Func<TLeft, TRight, TResult> selector) : IObservable<TResult>
    {
        /// <summary>The left source.</summary>
        private readonly IObservable<TLeft> _left = left;

        /// <summary>The right source.</summary>
        private readonly IObservable<TRight> _right = right;

        /// <summary>The projection of the two final values.</summary>
        private readonly Func<TLeft, TRight, TResult> _selector = selector;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new ForkJoinCoordinator<TLeft, TRight, TResult>(observer, _selector).Run(_left, _right);
        }
    }

    /// <summary>Range-specialized <c>ForkJoin</c>: emits the projection of each range's final value.</summary>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left source range.</param>
    /// <param name="right">The right source range.</param>
    /// <param name="selector">The projection of the two final values.</param>
    private sealed class RangeForkJoinSignal<TResult>(RangeSignal left, RangeSignal right, Func<int, int, TResult> selector) : IObservable<TResult>
    {
        /// <summary>The left source range.</summary>
        private readonly RangeSignal _left = left;

        /// <summary>The right source range.</summary>
        private readonly RangeSignal _right = right;

        /// <summary>The projection of the two final values.</summary>
        private readonly Func<int, int, TResult> _selector = selector;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            observer.OnNext(_selector(_left.Start + _left.Count - 1, _right.Start + _right.Count - 1));
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>Coordinates a two-source fork-join operation.</summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="selector">The projection function.</param>
    private sealed class ForkJoinCoordinator<TLeft, TRight, TResult>(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector)
    {
        /// <summary>The synchronization gate.</summary>
        private readonly Lock _gate = new();

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TResult> _observer = observer;

        /// <summary>The projection function.</summary>
        private readonly Func<TLeft, TRight, TResult> _selector = selector;

        /// <summary>A value indicating whether the left source produced a value.</summary>
        private bool _hasLeft;

        /// <summary>A value indicating whether the right source produced a value.</summary>
        private bool _hasRight;

        /// <summary>A value indicating whether the left source completed.</summary>
        private bool _leftDone;

        /// <summary>A value indicating whether the right source completed.</summary>
        private bool _rightDone;

        /// <summary>The latest left value.</summary>
        private TLeft? _latestLeft;

        /// <summary>The latest right value.</summary>
        private TRight? _latestRight;

        /// <summary>Subscribes to both fork-join sources.</summary>
        /// <param name="left">The left source.</param>
        /// <param name="right">The right source.</param>
        /// <returns>The subscription cleanup.</returns>
        internal MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
            new(
                left.Subscribe(OnLeftNext, _observer.OnError, OnLeftCompleted),
                right.Subscribe(OnRightNext, _observer.OnError, OnRightCompleted));

        /// <summary>Records a left value.</summary>
        /// <param name="value">The left value.</param>
        private void OnLeftNext(TLeft value)
        {
            lock (_gate)
            {
                _hasLeft = true;
                _latestLeft = value;
            }
        }

        /// <summary>Records a right value.</summary>
        /// <param name="value">The right value.</param>
        private void OnRightNext(TRight value)
        {
            lock (_gate)
            {
                _hasRight = true;
                _latestRight = value;
            }
        }

        /// <summary>Marks the left source as complete.</summary>
        private void OnLeftCompleted()
        {
            if (!CompleteLeft(out var result, out var emit))
            {
                return;
            }

            Finish(result, emit);
        }

        /// <summary>Marks the right source as complete.</summary>
        private void OnRightCompleted()
        {
            if (!CompleteRight(out var result, out var emit))
            {
                return;
            }

            Finish(result, emit);
        }

        /// <summary>Marks the left source complete and computes the result if both sources are complete.</summary>
        /// <param name="result">The result to emit.</param>
        /// <param name="emit">A value indicating whether a result should be emitted.</param>
        /// <returns><c>true</c> when fork-join is ready to finish; otherwise, <c>false</c>.</returns>
        private bool CompleteLeft(out TResult result, out bool emit)
        {
            lock (_gate)
            {
                _leftDone = true;
                return TryFinish(out result, out emit);
            }
        }

        /// <summary>Marks the right source complete and computes the result if both sources are complete.</summary>
        /// <param name="result">The result to emit.</param>
        /// <param name="emit">A value indicating whether a result should be emitted.</param>
        /// <returns><c>true</c> when fork-join is ready to finish; otherwise, <c>false</c>.</returns>
        private bool CompleteRight(out TResult result, out bool emit)
        {
            lock (_gate)
            {
                _rightDone = true;
                return TryFinish(out result, out emit);
            }
        }

        /// <summary>Computes the final result when both sources are complete.</summary>
        /// <param name="result">The result to emit.</param>
        /// <param name="emit">A value indicating whether a result should be emitted.</param>
        /// <returns><c>true</c> when both sources are complete; otherwise, <c>false</c>.</returns>
        private bool TryFinish(out TResult result, out bool emit)
        {
            if (!_leftDone || !_rightDone)
            {
                result = default!;
                emit = false;
                return false;
            }

            emit = _hasLeft && _hasRight;
            result = emit ? _selector(_latestLeft!, _latestRight!) : default!;
            return true;
        }

        /// <summary>Emits the final result and completes.</summary>
        /// <param name="result">The result to emit.</param>
        /// <param name="emit">A value indicating whether a result should be emitted.</param>
        private void Finish(TResult result, bool emit)
        {
            if (emit)
            {
                _observer.OnNext(result);
            }

            _observer.OnCompleted();
        }
    }
}
