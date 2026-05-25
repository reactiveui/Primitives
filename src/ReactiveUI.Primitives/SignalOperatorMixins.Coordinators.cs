// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives;

/// <summary>
/// Coordinator helpers for multi-source signal operators.
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Coordinates race subscriptions and forwards only the winning source.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class RaceCoordinator<T> : IDisposable
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<T> _observer;

        /// <summary>
        /// The active subscriptions.
        /// </summary>
        private readonly MultipleDisposable _subscriptions = new();

        /// <summary>
        /// The winning source index.
        /// </summary>
        private int _winner = -1;

        /// <summary>
        /// The next source index.
        /// </summary>
        private int _index;

        /// <summary>
        /// Initializes a new instance of the <see cref="RaceCoordinator{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        internal RaceCoordinator(IObserver<T> observer) => _observer = observer;

        /// <summary>
        /// Releases the active subscriptions.
        /// </summary>
        public void Dispose() => _subscriptions.Dispose();

        /// <summary>
        /// Starts observing the candidate source streams.
        /// </summary>
        /// <param name="sources">The candidate source streams.</param>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal RaceCoordinator<T> Run(IObservable<IObservable<T>> sources)
        {
            _subscriptions.Add(sources.Subscribe(OnSource, _observer.OnError, OnOuterCompleted));
            return this;
        }

        /// <summary>
        /// Forwards a value from a candidate source.
        /// </summary>
        /// <param name="candidate">The candidate source index.</param>
        /// <param name="value">The value to forward.</param>
        private void OnNext(int candidate, T value)
        {
            if (!Win(candidate))
            {
                return;
            }

            _observer.OnNext(value);
        }

        /// <summary>
        /// Forwards an error from a candidate source.
        /// </summary>
        /// <param name="candidate">The candidate source index.</param>
        /// <param name="error">The error to forward.</param>
        private void OnError(int candidate, Exception error)
        {
            if (!Win(candidate))
            {
                return;
            }

            _observer.OnError(error);
        }

        /// <summary>
        /// Forwards completion from a candidate source.
        /// </summary>
        /// <param name="candidate">The candidate source index.</param>
        private void OnCompleted(int candidate)
        {
            if (!Win(candidate))
            {
                return;
            }

            _observer.OnCompleted();
        }

        /// <summary>
        /// Handles completion of the outer sequence.
        /// </summary>
        private void OnOuterCompleted()
        {
            // Race completion is controlled by the first inner source to win.
        }

        /// <summary>
        /// Subscribes to a candidate source.
        /// </summary>
        /// <param name="source">The source to observe.</param>
        private void OnSource(IObservable<T> source)
        {
            var current = Interlocked.Increment(ref _index) - 1;
            _subscriptions.Add(source.Subscribe(
                value => OnNext(current, value),
                error => OnError(current, error),
                () => OnCompleted(current)));
        }

        /// <summary>
        /// Attempts to make a candidate source the winner.
        /// </summary>
        /// <param name="candidate">The candidate source index.</param>
        /// <returns><c>true</c> when the candidate is the winning source; otherwise, <c>false</c>.</returns>
        private bool Win(int candidate)
        {
            var current = Volatile.Read(ref _winner);
            if (current == candidate)
            {
                return true;
            }

            if (current >= 0)
            {
                return false;
            }

            return Interlocked.CompareExchange(ref _winner, candidate, -1) == -1;
        }
    }

    /// <summary>
    /// Coordinates a two-source zip operation.
    /// </summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    private sealed class ZipCoordinator<TLeft, TRight, TResult>
    {
        /// <summary>
        /// The synchronization gate.
        /// </summary>
        private readonly OperatorGate _gate = new();

        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<TResult> _observer;

        /// <summary>
        /// The projection function.
        /// </summary>
        private readonly Func<TLeft, TRight, TResult> _selector;

        /// <summary>
        /// The queued left values.
        /// </summary>
        private readonly Queue<TLeft> _leftQueue = new();

        /// <summary>
        /// The queued right values.
        /// </summary>
        private readonly Queue<TRight> _rightQueue = new();

        /// <summary>
        /// A value indicating whether the left source completed.
        /// </summary>
        private bool _leftCompleted;

        /// <summary>
        /// A value indicating whether the right source completed.
        /// </summary>
        private bool _rightCompleted;

        /// <summary>
        /// Initializes a new instance of the <see cref="ZipCoordinator{TLeft, TRight, TResult}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="selector">The projection function.</param>
        internal ZipCoordinator(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector)
        {
            _observer = observer;
            _selector = selector;
        }

        /// <summary>
        /// Subscribes to both zip sources.
        /// </summary>
        /// <param name="left">The left source.</param>
        /// <param name="right">The right source.</param>
        /// <returns>The subscription cleanup.</returns>
        internal MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
            new(
                left.Subscribe(OnLeftNext, _observer.OnError, OnLeftCompleted),
                right.Subscribe(OnRightNext, _observer.OnError, OnRightCompleted));

        /// <summary>
        /// Queues a left value.
        /// </summary>
        /// <param name="value">The value to queue.</param>
        private void OnLeftNext(TLeft value)
        {
            lock (_gate.SyncRoot)
            {
                _leftQueue.Enqueue(value);
            }

            Drain();
        }

        /// <summary>
        /// Queues a right value.
        /// </summary>
        /// <param name="value">The value to queue.</param>
        private void OnRightNext(TRight value)
        {
            lock (_gate.SyncRoot)
            {
                _rightQueue.Enqueue(value);
            }

            Drain();
        }

        /// <summary>
        /// Marks the left source as complete.
        /// </summary>
        private void OnLeftCompleted()
        {
            lock (_gate.SyncRoot)
            {
                _leftCompleted = true;
            }

            Drain();
        }

        /// <summary>
        /// Marks the right source as complete.
        /// </summary>
        private void OnRightCompleted()
        {
            lock (_gate.SyncRoot)
            {
                _rightCompleted = true;
            }

            Drain();
        }

        /// <summary>
        /// Emits all currently available pairs.
        /// </summary>
        private void Drain()
        {
            while (TryTake(out var left, out var right))
            {
                _observer.OnNext(_selector(left, right));
            }
        }

        /// <summary>
        /// Attempts to remove the next available pair from the queues.
        /// </summary>
        /// <param name="left">The left value.</param>
        /// <param name="right">The right value.</param>
        /// <returns><c>true</c> when a pair was available; otherwise, <c>false</c>.</returns>
        private bool TryTake(out TLeft left, out TRight right)
        {
            lock (_gate.SyncRoot)
            {
                if (_leftQueue.Count != 0 && _rightQueue.Count != 0)
                {
                    left = _leftQueue.Dequeue();
                    right = _rightQueue.Dequeue();
                    return true;
                }

                if ((_leftCompleted && _leftQueue.Count == 0) || (_rightCompleted && _rightQueue.Count == 0))
                {
                    _observer.OnCompleted();
                }

                left = default!;
                right = default!;
                return false;
            }
        }
    }

    /// <summary>
    /// Coordinates a two-source combine-latest operation.
    /// </summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    private sealed class CombineLatestCoordinator<TLeft, TRight, TResult>
    {
        /// <summary>
        /// The synchronization gate.
        /// </summary>
        private readonly OperatorGate _gate = new();

        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<TResult> _observer;

        /// <summary>
        /// The projection function.
        /// </summary>
        private readonly Func<TLeft, TRight, TResult> _selector;

        /// <summary>
        /// A value indicating whether the left source has produced a value.
        /// </summary>
        private bool _hasLeft;

        /// <summary>
        /// A value indicating whether the right source has produced a value.
        /// </summary>
        private bool _hasRight;

        /// <summary>
        /// A value indicating whether the left source completed.
        /// </summary>
        private bool _leftDone;

        /// <summary>
        /// A value indicating whether the right source completed.
        /// </summary>
        private bool _rightDone;

        /// <summary>
        /// The latest left value.
        /// </summary>
        private TLeft? _latestLeft;

        /// <summary>
        /// The latest right value.
        /// </summary>
        private TRight? _latestRight;

        /// <summary>
        /// Initializes a new instance of the <see cref="CombineLatestCoordinator{TLeft, TRight, TResult}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="selector">The projection function.</param>
        internal CombineLatestCoordinator(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector)
        {
            _observer = observer;
            _selector = selector;
        }

        /// <summary>
        /// Subscribes to both combine-latest sources.
        /// </summary>
        /// <param name="left">The left source.</param>
        /// <param name="right">The right source.</param>
        /// <returns>The subscription cleanup.</returns>
        internal MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
            new(
                left.Subscribe(OnLeftNext, _observer.OnError, OnLeftCompleted),
                right.Subscribe(OnRightNext, _observer.OnError, OnRightCompleted));

        /// <summary>
        /// Handles a left value.
        /// </summary>
        /// <param name="value">The left value.</param>
        private void OnLeftNext(TLeft value)
        {
            if (!TryUpdateLeft(value, out var projected))
            {
                return;
            }

            _observer.OnNext(projected);
        }

        /// <summary>
        /// Handles a right value.
        /// </summary>
        /// <param name="value">The right value.</param>
        private void OnRightNext(TRight value)
        {
            if (!TryUpdateRight(value, out var projected))
            {
                return;
            }

            _observer.OnNext(projected);
        }

        /// <summary>
        /// Marks the left source as complete.
        /// </summary>
        private void OnLeftCompleted()
        {
            if (!CompleteLeft())
            {
                return;
            }

            _observer.OnCompleted();
        }

        /// <summary>
        /// Marks the right source as complete.
        /// </summary>
        private void OnRightCompleted()
        {
            if (!CompleteRight())
            {
                return;
            }

            _observer.OnCompleted();
        }

        /// <summary>
        /// Updates the latest left value.
        /// </summary>
        /// <param name="value">The new value.</param>
        /// <param name="result">The projected result.</param>
        /// <returns><c>true</c> when a result is available; otherwise, <c>false</c>.</returns>
        private bool TryUpdateLeft(TLeft value, out TResult result)
        {
            lock (_gate.SyncRoot)
            {
                _latestLeft = value;
                _hasLeft = true;
                return TryProject(out result);
            }
        }

        /// <summary>
        /// Updates the latest right value.
        /// </summary>
        /// <param name="value">The new value.</param>
        /// <param name="result">The projected result.</param>
        /// <returns><c>true</c> when a result is available; otherwise, <c>false</c>.</returns>
        private bool TryUpdateRight(TRight value, out TResult result)
        {
            lock (_gate.SyncRoot)
            {
                _latestRight = value;
                _hasRight = true;
                return TryProject(out result);
            }
        }

        /// <summary>
        /// Marks the left source as complete.
        /// </summary>
        /// <returns><c>true</c> when both sources are complete; otherwise, <c>false</c>.</returns>
        private bool CompleteLeft()
        {
            lock (_gate.SyncRoot)
            {
                _leftDone = true;
                return _rightDone;
            }
        }

        /// <summary>
        /// Marks the right source as complete.
        /// </summary>
        /// <returns><c>true</c> when both sources are complete; otherwise, <c>false</c>.</returns>
        private bool CompleteRight()
        {
            lock (_gate.SyncRoot)
            {
                _rightDone = true;
                return _leftDone;
            }
        }

        /// <summary>
        /// Projects the current latest values.
        /// </summary>
        /// <param name="result">The projected value.</param>
        /// <returns><c>true</c> when both sources have values; otherwise, <c>false</c>.</returns>
        private bool TryProject(out TResult result)
        {
            if (!_hasLeft || !_hasRight)
            {
                result = default!;
                return false;
            }

            result = _selector(_latestLeft!, _latestRight!);
            return true;
        }
    }

    /// <summary>
    /// Coordinates a switch operation.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class SwitchCoordinator<T> : IDisposable
    {
        /// <summary>
        /// The synchronization gate.
        /// </summary>
        private readonly OperatorGate _gate = new();

        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<T> _observer;

        /// <summary>
        /// The active subscriptions.
        /// </summary>
        private readonly MultipleDisposable _subscriptions = new();

        /// <summary>
        /// The active inner subscription.
        /// </summary>
        private readonly SingleReplaceableDisposable _innerSlot = new();

        /// <summary>
        /// A value indicating whether the outer source completed.
        /// </summary>
        private bool _outerCompleted;

        /// <summary>
        /// A value indicating whether an inner source is active.
        /// </summary>
        private bool _innerActive;

        /// <summary>
        /// The current inner source version.
        /// </summary>
        private int _version;

        /// <summary>
        /// Initializes a new instance of the <see cref="SwitchCoordinator{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        internal SwitchCoordinator(IObserver<T> observer) => _observer = observer;

        /// <summary>
        /// Releases the active subscriptions.
        /// </summary>
        public void Dispose()
        {
            _innerSlot.Dispose();
            _subscriptions.Dispose();
        }

        /// <summary>
        /// Subscribes to the outer source.
        /// </summary>
        /// <param name="sources">The outer source.</param>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal SwitchCoordinator<T> Run(IObservable<IObservable<T>> sources)
        {
            _subscriptions.Add(_innerSlot);
            _subscriptions.Add(sources.Subscribe(OnSource, _observer.OnError, OnOuterCompleted));
            return this;
        }

        /// <summary>
        /// Switches to a new inner source.
        /// </summary>
        /// <param name="source">The new inner source.</param>
        private void OnSource(IObservable<T> source)
        {
            int current;
            lock (_gate.SyncRoot)
            {
                current = ++_version;
                _innerActive = true;
            }

            _innerSlot.Create(source.Subscribe(
                value => OnNext(current, value),
                error => OnError(current, error),
                () => OnCompleted(current)));
        }

        /// <summary>
        /// Marks the outer source as complete.
        /// </summary>
        private void OnOuterCompleted()
        {
            lock (_gate.SyncRoot)
            {
                _outerCompleted = true;
            }

            TryComplete();
        }

        /// <summary>
        /// Forwards an inner value when it belongs to the current source.
        /// </summary>
        /// <param name="version">The inner version.</param>
        /// <param name="value">The value to forward.</param>
        private void OnNext(int version, T value)
        {
            if (!IsCurrent(version))
            {
                return;
            }

            _observer.OnNext(value);
        }

        /// <summary>
        /// Forwards an inner error when it belongs to the current source.
        /// </summary>
        /// <param name="version">The inner version.</param>
        /// <param name="error">The error to forward.</param>
        private void OnError(int version, Exception error)
        {
            if (!IsCurrent(version))
            {
                return;
            }

            _observer.OnError(error);
        }

        /// <summary>
        /// Completes an inner source when it belongs to the current source.
        /// </summary>
        /// <param name="version">The inner version.</param>
        private void OnCompleted(int version)
        {
            lock (_gate.SyncRoot)
            {
                if (version == _version)
                {
                    _innerActive = false;
                }
            }

            TryComplete();
        }

        /// <summary>
        /// Determines whether a version is the current inner source.
        /// </summary>
        /// <param name="version">The candidate version.</param>
        /// <returns><c>true</c> if the version is current; otherwise, <c>false</c>.</returns>
        private bool IsCurrent(int version)
        {
            lock (_gate.SyncRoot)
            {
                return version == _version;
            }
        }

        /// <summary>
        /// Completes the observer when both outer and inner sources are complete.
        /// </summary>
        private void TryComplete()
        {
            lock (_gate.SyncRoot)
            {
                if (_outerCompleted && !_innerActive)
                {
                    _observer.OnCompleted();
                }
            }
        }
    }
}
