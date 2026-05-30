// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives;

/// <summary>
/// Coordinator helpers for multi-source signal operators.
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Range-specialized WithLatest (Latch): emits each left range value paired with the right
    /// range's final value. A dedicated signal avoids the closure, delegate, CreateSafe wrapper,
    /// and safe-guard sink that <c>Signal.CreateSafe(observer =&gt; ...)</c> would allocate.
    /// </summary>
    /// <typeparam name="TResult">The result value type.</typeparam>
    private sealed class RangeWithLatestSignal<TResult> : IObservable<TResult>
    {
        /// <summary>The left source range.</summary>
        private readonly RangeSignal _left;

        /// <summary>The right source range (its final value is the latched value).</summary>
        private readonly RangeSignal _right;

        /// <summary>The result projection.</summary>
        private readonly Func<int, int, TResult> _selector;

        /// <summary>Initializes a new instance of the <see cref="RangeWithLatestSignal{TResult}"/> class.</summary>
        /// <param name="left">The left source range.</param>
        /// <param name="right">The right source range.</param>
        /// <param name="selector">The result projection.</param>
        internal RangeWithLatestSignal(RangeSignal left, RangeSignal right, Func<int, int, TResult> selector)
        {
            _left = left;
            _right = right;
            _selector = selector;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var rightValue = _right.Start + _right.Count - 1;
            for (var i = 0; i < _left.Count; i++)
            {
                observer.OnNext(_selector(_left.Start + i, rightValue));
            }

            observer.OnCompleted();
            return Disposable.Empty;
        }
    }

    /// <summary>
    /// Timeout signal with a direct subscription path.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class ExpireSignal<T> : IRequireCurrentThread<T>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The timeout period.
        /// </summary>
        private readonly TimeSpan _dueTime;

        /// <summary>
        /// The sequencer used to schedule the timeout.
        /// </summary>
        private readonly ISequencer _sequencer;

        /// <summary>
        /// Initializes a new instance of the <see cref="ExpireSignal{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        /// <param name="dueTime">The timeout period.</param>
        /// <param name="sequencer">The sequencer used to schedule the timeout.</param>
        public ExpireSignal(IObservable<T> source, TimeSpan dueTime, ISequencer sequencer)
        {
            _source = source;
            _dueTime = dueTime;
            _sequencer = sequencer;
        }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _sequencer == Sequencer.CurrentThread ||
            (_source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread());

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var coordinator = new ExpireCoordinator<T>(_source, _dueTime, _sequencer, observer);
            if (!IsRequiredSubscribeOnCurrentThread() || !Sequencer.CurrentThread.IsScheduleRequired)
            {
                return coordinator.Run();
            }

            var subscription = new SingleDisposable();
            Sequencer.CurrentThread.Schedule(() => subscription.Create(coordinator.Run()));
            return subscription;
        }
    }

    /// <summary>
    /// Coordinates timeout delivery with one active timer.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Disposable fields are released through interlocked exchange in Dispose.")]
    private sealed class ExpireCoordinator<T> : IObserver<T>, IDisposable
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The timeout period.
        /// </summary>
        private readonly TimeSpan _dueTime;

        /// <summary>
        /// The sequencer used to schedule the timeout.
        /// </summary>
        private readonly ISequencer _sequencer;

        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<T> _observer;

        /// <summary>
        /// The active source subscription.
        /// </summary>
        private IDisposable? _subscription;

        /// <summary>
        /// The active timeout timer.
        /// </summary>
        private IDisposable? _timer;

        /// <summary>
        /// A value indicating whether the timeout or source has terminated.
        /// </summary>
        private int _done;

        /// <summary>
        /// Initializes a new instance of the <see cref="ExpireCoordinator{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        /// <param name="dueTime">The timeout period.</param>
        /// <param name="sequencer">The sequencer used to schedule the timeout.</param>
        /// <param name="observer">The downstream observer.</param>
        public ExpireCoordinator(IObservable<T> source, TimeSpan dueTime, ISequencer sequencer, IObserver<T> observer)
        {
            _source = source;
            _dueTime = dueTime;
            _sequencer = sequencer;
            _observer = observer;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            var timer = Interlocked.Exchange(ref _timer, null);
            timer?.Dispose();

            var subscription = Interlocked.Exchange(ref _subscription, null);
            subscription?.Dispose();
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _done, 1) != 0)
            {
                return;
            }

            try
            {
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _done, 1) != 0)
            {
                return;
            }

            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _done) != 0)
            {
                return;
            }

            _observer.OnNext(value);
        }

        /// <summary>
        /// Starts observing the source and timeout timer.
        /// </summary>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal ExpireCoordinator<T> Run()
        {
            _timer = _sequencer.Schedule(this, _dueTime, static (_, coordinator) => coordinator.Timeout());
            _subscription = _source.Subscribe(this);
            if (Volatile.Read(ref _done) == 0)
            {
                return this;
            }

            Dispose();
            return this;
        }

        /// <summary>
        /// Emits the timeout error.
        /// </summary>
        /// <returns>An empty disposable.</returns>
        private IDisposable Timeout()
        {
            if (Interlocked.Exchange(ref _done, 1) != 0)
            {
                return Disposable.Empty;
            }

            try
            {
                _observer.OnError(new TimeoutException());
            }
            finally
            {
                Dispose();
            }

            return Disposable.Empty;
        }
    }

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
        private readonly Lock _gate = new();

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
        /// A value indicating whether completion has been emitted downstream.
        /// </summary>
        private bool _completed;

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
            lock (_gate)
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
            lock (_gate)
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
            lock (_gate)
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
            lock (_gate)
            {
                _rightCompleted = true;
            }

            Drain();
        }

        /// <summary>
        /// Emits all currently available pairs. The gate is held across the projection and the
        /// downstream callbacks so left and right threads cannot interleave emissions (the Rx
        /// serialization contract) and completion is delivered at most once.
        /// </summary>
        private void Drain()
        {
            lock (_gate)
            {
                if (_completed)
                {
                    return;
                }

                while (_leftQueue.Count != 0 && _rightQueue.Count != 0)
                {
                    var left = _leftQueue.Dequeue();
                    var right = _rightQueue.Dequeue();
                    _observer.OnNext(_selector(left, right));
                }

                if ((_leftCompleted && _leftQueue.Count == 0) || (_rightCompleted && _rightQueue.Count == 0))
                {
                    _completed = true;
                    _observer.OnCompleted();
                }
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
        private readonly Lock _gate = new();

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
        /// A value indicating whether completion has been emitted downstream.
        /// </summary>
        private bool _completed;

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
        /// Handles a left value. The gate is held across the projection and the downstream
        /// callback so left and right threads cannot interleave emissions (the Rx serialization
        /// contract).
        /// </summary>
        /// <param name="value">The left value.</param>
        private void OnLeftNext(TLeft value)
        {
            lock (_gate)
            {
                _latestLeft = value;
                _hasLeft = true;
                if (!_completed && TryProject(out var projected))
                {
                    _observer.OnNext(projected);
                }
            }
        }

        /// <summary>
        /// Handles a right value.
        /// </summary>
        /// <param name="value">The right value.</param>
        private void OnRightNext(TRight value)
        {
            lock (_gate)
            {
                _latestRight = value;
                _hasRight = true;
                if (!_completed && TryProject(out var projected))
                {
                    _observer.OnNext(projected);
                }
            }
        }

        /// <summary>
        /// Marks the left source as complete.
        /// </summary>
        private void OnLeftCompleted()
        {
            lock (_gate)
            {
                _leftDone = true;
                if (!_completed && _rightDone)
                {
                    _completed = true;
                    _observer.OnCompleted();
                }
            }
        }

        /// <summary>
        /// Marks the right source as complete.
        /// </summary>
        private void OnRightCompleted()
        {
            lock (_gate)
            {
                _rightDone = true;
                if (!_completed && _leftDone)
                {
                    _completed = true;
                    _observer.OnCompleted();
                }
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
        private readonly Lock _gate = new();

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
            lock (_gate)
            {
                current = _version + 1;

                // Publish the new version so the lock-free reader in IsCurrent observes it.
                Volatile.Write(ref _version, current);
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
            lock (_gate)
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
            lock (_gate)
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
        private bool IsCurrent(int version) => version == Volatile.Read(ref _version);

        /// <summary>
        /// Completes the observer when both outer and inner sources are complete.
        /// </summary>
        private void TryComplete()
        {
            lock (_gate)
            {
                if (_outerCompleted && !_innerActive)
                {
                    _observer.OnCompleted();
                }
            }
        }
    }
}
