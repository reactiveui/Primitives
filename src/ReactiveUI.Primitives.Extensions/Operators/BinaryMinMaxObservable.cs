// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Fast path for the common two-source min/max case.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="left">The first source.</param>
/// <param name="right">The second source.</param>
/// <param name="emitMaximum"><c>true</c> to emit the maximum; <c>false</c> to emit the minimum.</param>
internal sealed class BinaryMinMaxObservable<T>(IObservable<T> left, IObservable<T> right, bool emitMaximum) : IObservable<T>
    where T : struct, IComparable<T>
{
    /// <summary>The first source.</summary>
    private readonly IObservable<T> _left = InvalidOperationExceptionHelper.Check(left);

    /// <summary>The second source.</summary>
    private readonly IObservable<T> _right = InvalidOperationExceptionHelper.Check(right);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var sink = new Sink(observer, emitMaximum);
        return new DisposableBag(
            _left.Subscribe(new IndexedObserver(sink, isLeft: true)),
            _right.Subscribe(new IndexedObserver(sink, isLeft: false)));
    }

    /// <summary>Holds latest values and terminal state for two sources.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="emitMaximum"><c>true</c> for max; <c>false</c> for min.</param>
    private sealed class Sink(IObserver<T> downstream, bool emitMaximum)
    {
        /// <summary>The synchronization gate.</summary>
        private readonly Lock _gate = new();

        /// <summary>The latest left value.</summary>
        private T _leftValue;

        /// <summary>The latest right value.</summary>
        private T _rightValue;

        /// <summary>Whether the left source has produced a value.</summary>
        private bool _hasLeft;

        /// <summary>Whether the right source has produced a value.</summary>
        private bool _hasRight;

        /// <summary>Whether the left source has completed.</summary>
        private bool _leftCompleted;

        /// <summary>Whether the right source has completed.</summary>
        private bool _rightCompleted;

        /// <summary>Whether the sink is terminal.</summary>
        private bool _isDone;

        /// <summary>Handles a source value.</summary>
        /// <param name="isLeft"><c>true</c> for the left source.</param>
        /// <param name="value">The value.</param>
        public void OnNext(bool isLeft, T value)
        {
            lock (_gate)
            {
                if (_isDone)
                {
                    return;
                }

                if (isLeft)
                {
                    _leftValue = value;
                    _hasLeft = true;
                }
                else
                {
                    _rightValue = value;
                    _hasRight = true;
                }

                if (!_hasLeft || !_hasRight)
                {
                    return;
                }

                var compare = _leftValue.CompareTo(_rightValue);
                var useLeft = emitMaximum ? compare >= 0 : compare <= 0;
                downstream.OnNext(useLeft ? _leftValue : _rightValue);
            }
        }

        /// <summary>Handles a source error.</summary>
        /// <param name="error">The error.</param>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_isDone)
                {
                    return;
                }

                _isDone = true;
                downstream.OnError(error);
            }
        }

        /// <summary>Handles source completion.</summary>
        /// <param name="isLeft"><c>true</c> for the left source.</param>
        public void OnCompleted(bool isLeft)
        {
            lock (_gate)
            {
                if (_isDone)
                {
                    return;
                }

                if (isLeft)
                {
                    if (_leftCompleted)
                    {
                        return;
                    }

                    _leftCompleted = true;
                    if (!_hasLeft)
                    {
                        Complete();
                        return;
                    }
                }
                else
                {
                    if (_rightCompleted)
                    {
                        return;
                    }

                    _rightCompleted = true;
                    if (!_hasRight)
                    {
                        Complete();
                        return;
                    }
                }

                if (_leftCompleted && _rightCompleted)
                {
                    Complete();
                }
            }
        }

        /// <summary>Completes once.</summary>
        private void Complete()
        {
            _isDone = true;
            downstream.OnCompleted();
        }
    }

    /// <summary>Observer that labels left/right without per-callback closures.</summary>
    /// <param name="sink">The shared sink.</param>
    /// <param name="isLeft"><c>true</c> when observing the left source.</param>
    private sealed class IndexedObserver(Sink sink, bool isLeft) : IObserver<T>
    {
        /// <inheritdoc/>
        public void OnNext(T value) => sink.OnNext(isLeft, value);

        /// <inheritdoc/>
        public void OnError(Exception error) => sink.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => sink.OnCompleted(isLeft);
    }
}
