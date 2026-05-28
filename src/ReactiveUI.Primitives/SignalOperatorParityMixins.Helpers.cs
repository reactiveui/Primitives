// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives;

/// <summary>
/// Private helper types for parity operators.
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Prepends a single value without composing through concat and return signals.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class PrependSignal<T> : Signals.Core.IInlineSignal<T>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The value emitted before source subscription.
        /// </summary>
        private readonly T _value;

        /// <summary>
        /// Initializes a new instance of the <see cref="PrependSignal{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        /// <param name="value">The prepended value.</param>
        internal PrependSignal(IObservable<T> source, T value)
        {
            _source = source;
            _value = value;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            observer.OnNext(_value);
            return _source.Subscribe(observer);
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            onNext(_value);
            return _source.Subscribe(onNext, onError, onCompleted);
        }

        /// <summary>
        /// Gets the source observable for operator fusion.
        /// </summary>
        /// <returns>The source observable.</returns>
        internal IObservable<T> GetSource() => _source;

        /// <summary>
        /// Gets the prepended value for operator fusion.
        /// </summary>
        /// <returns>The prepended value.</returns>
        internal T GetValue() => _value;
    }

    /// <summary>
    /// Prepends an enumerable without composing through concat and enumerable signals.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class StartWithEnumerableSignal<T> : Signals.Core.IInlineSignal<T>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// Values emitted before source subscription.
        /// </summary>
        private readonly IEnumerable<T> _values;

        /// <summary>
        /// Initializes a new instance of the <see cref="StartWithEnumerableSignal{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        /// <param name="values">Values emitted before source subscription.</param>
        internal StartWithEnumerableSignal(IObservable<T> source, IEnumerable<T> values)
        {
            _source = source;
            _values = values;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            foreach (var value in _values)
            {
                observer.OnNext(value);
            }

            return _source.Subscribe(observer);
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            foreach (var value in _values)
            {
                onNext(value);
            }

            return _source.Subscribe(onNext, onError, onCompleted);
        }
    }

    /// <summary>
    /// Fuses a single prepended value and a single appended value around a source subscription.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class PrependAppendSignal<T> : Signals.Core.IInlineSignal<T>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The value emitted before source subscription.
        /// </summary>
        private readonly T _prependValue;

        /// <summary>
        /// The value emitted after source completion.
        /// </summary>
        private readonly T _appendValue;

        /// <summary>
        /// Initializes a new instance of the <see cref="PrependAppendSignal{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        /// <param name="prependValue">The prepended value.</param>
        /// <param name="appendValue">The appended value.</param>
        internal PrependAppendSignal(IObservable<T> source, T prependValue, T appendValue)
        {
            _source = source;
            _prependValue = prependValue;
            _appendValue = appendValue;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            observer.OnNext(_prependValue);
            var sink = new AppendObserver<T>(observer, _appendValue);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            onNext(_prependValue);
            var sink = new AppendDelegateObserver<T>(onNext, onError, onCompleted, _appendValue);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>
    /// Appends a single value after source completion.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class AppendSignal<T> : IObservable<T>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The value emitted after source completion.
        /// </summary>
        private readonly T _value;

        /// <summary>
        /// Initializes a new instance of the <see cref="AppendSignal{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        /// <param name="value">The appended value.</param>
        internal AppendSignal(IObservable<T> source, T value)
        {
            _source = source;
            _value = value;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new AppendObserver<T>(observer, _value);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>
    /// Delegate-backed observer for fused prepend/append inline subscriptions.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class AppendDelegateObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The next callback.
        /// </summary>
        private readonly Action<T> _onNext;

        /// <summary>
        /// The error callback.
        /// </summary>
        private readonly Action<Exception> _onError;

        /// <summary>
        /// The completion callback.
        /// </summary>
        private readonly Action _onCompleted;

        /// <summary>
        /// The appended value.
        /// </summary>
        private readonly T _value;

        /// <summary>
        /// Initializes a new instance of the <see cref="AppendDelegateObserver{T}"/> class.
        /// </summary>
        /// <param name="onNext">The next callback.</param>
        /// <param name="onError">The error callback.</param>
        /// <param name="onCompleted">The completion callback.</param>
        /// <param name="value">The appended value.</param>
        internal AppendDelegateObserver(Action<T> onNext, Action<Exception> onError, Action onCompleted, T value)
        {
            _onNext = onNext;
            _onError = onError;
            _onCompleted = onCompleted;
            _value = value;
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            try
            {
                _onNext(value);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            try
            {
                _onError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            try
            {
                _onNext(_value);
                _onCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>
    /// Emits a default value when the source completes without values.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class DefaultIfEmptySignal<T> : IObservable<T>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// Value emitted for an empty source.
        /// </summary>
        private readonly T _defaultValue;

        /// <summary>
        /// Initializes a new instance of the <see cref="DefaultIfEmptySignal{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        /// <param name="defaultValue">Value emitted for an empty source.</param>
        internal DefaultIfEmptySignal(IObservable<T> source, T defaultValue)
        {
            _source = source;
            _defaultValue = defaultValue;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new DefaultIfEmptyObserver<T>(observer, _defaultValue);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>
    /// Abstract base class for observers that manage a single upstream subscription.
    /// </summary>
    /// <typeparam name="T">The type of elements observed.</typeparam>
    private abstract class SingleSourceObserver<T> : IObserver<T>, IDisposable
    {
        /// <summary>
        /// Disposed marker.
        /// </summary>
        private static readonly IDisposable DisposedSentinel = new DisposedMarker();

        /// <summary>
        /// Upstream subscription.
        /// </summary>
        private IDisposable? _subscription;

        /// <inheritdoc/>
        public abstract void OnNext(T value);

        /// <inheritdoc/>
        public abstract void OnError(Exception error);

        /// <inheritdoc/>
        public abstract void OnCompleted();

        /// <inheritdoc/>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Assigns the upstream subscription.
        /// </summary>
        /// <param name="subscription">The upstream subscription.</param>
        internal void SetSubscription(IDisposable subscription)
        {
            if (Interlocked.CompareExchange(ref _subscription, subscription, null) == null)
            {
                return;
            }

            subscription.Dispose();
        }

        /// <summary>
        /// Releases the upstream subscription.
        /// </summary>
        /// <param name="disposing">A value indicating whether managed resources should be disposed.</param>
        protected virtual void Dispose(bool disposing)
        {
            var subscription = Interlocked.Exchange(ref _subscription, DisposedSentinel);
            if (subscription == null || ReferenceEquals(subscription, DisposedSentinel) || !disposing)
            {
                return;
            }

            subscription.Dispose();
        }

        /// <summary>
        /// Disposable marker for disposed sinks.
        /// </summary>
        private sealed class DisposedMarker : IDisposable
        {
            /// <inheritdoc/>
            public void Dispose()
            {
            }
        }
    }

    /// <summary>
    /// Observer for append.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class AppendObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<T> _observer;

        /// <summary>
        /// The appended value.
        /// </summary>
        private readonly T _value;

        /// <summary>
        /// Initializes a new instance of the <see cref="AppendObserver{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="value">The appended value.</param>
        internal AppendObserver(IObserver<T> observer, T value)
        {
            _observer = observer;
            _value = value;
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            try
            {
                _observer.OnNext(value);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
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
        public override void OnCompleted()
        {
            try
            {
                _observer.OnNext(_value);
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>
    /// Observer for default-if-empty.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class DefaultIfEmptyObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<T> _observer;

        /// <summary>
        /// Value emitted for an empty source.
        /// </summary>
        private readonly T _defaultValue;

        /// <summary>
        /// A value indicating whether the source produced any values.
        /// </summary>
        private bool _seen;

        /// <summary>
        /// Initializes a new instance of the <see cref="DefaultIfEmptyObserver{T}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="defaultValue">Value emitted for an empty source.</param>
        internal DefaultIfEmptyObserver(IObserver<T> observer, T defaultValue)
        {
            _observer = observer;
            _defaultValue = defaultValue;
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            _seen = true;
            try
            {
                _observer.OnNext(value);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
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
        public override void OnCompleted()
        {
            try
            {
                if (!_seen)
                {
                    _observer.OnNext(_defaultValue);
                }

                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>
    /// Sample signal with a direct subscription path.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class ProbeSignal<T> : IRequireCurrentThread<T>
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The sample period.
        /// </summary>
        private readonly TimeSpan _period;

        /// <summary>
        /// The sequencer used to schedule ticks.
        /// </summary>
        private readonly ISequencer _sequencer;

        /// <summary>
        /// Initializes a new instance of the <see cref="ProbeSignal{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        /// <param name="period">The sample period.</param>
        /// <param name="sequencer">The sequencer used to schedule ticks.</param>
        internal ProbeSignal(IObservable<T> source, TimeSpan period, ISequencer sequencer)
        {
            _source = source;
            _period = period;
            _sequencer = sequencer;
        }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() => _sequencer == Sequencer.CurrentThread;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var coordinator = new ProbeCoordinator<T>(_source, _period, _sequencer, observer);
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
    /// Coordinates a sampled observable sequence without the anonymous signal wrapper.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Disposable fields are released through interlocked exchange in Dispose.")]
    private sealed class ProbeCoordinator<T> : IObserver<T>, IDisposable
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The sample period.
        /// </summary>
        private readonly TimeSpan _period;

        /// <summary>
        /// The sequencer used to schedule ticks.
        /// </summary>
        private readonly ISequencer _sequencer;

        /// <summary>
        /// The downstream observer.
        /// </summary>
        private readonly IObserver<T> _observer;

        /// <summary>
        /// The synchronization gate.
        /// </summary>
        private SpinLock _gate = new(false);

        /// <summary>
        /// The active source subscription.
        /// </summary>
        private IDisposable? _subscription;

        /// <summary>
        /// The active timer.
        /// </summary>
        private IDisposable? _timer;

        /// <summary>
        /// A value indicating whether a sample timer is active.
        /// </summary>
        private bool _timerActive;

        /// <summary>
        /// A value indicating whether a latest value is available.
        /// </summary>
        private bool _hasLatest;

        /// <summary>
        /// The latest value.
        /// </summary>
        private T? _latest;

        /// <summary>
        /// A value indicating whether the source has completed.
        /// </summary>
        private bool _done;

        /// <summary>
        /// A value indicating whether the coordinator has been disposed.
        /// </summary>
        private int _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ProbeCoordinator{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        /// <param name="period">The sample period.</param>
        /// <param name="sequencer">The sequencer used to schedule ticks.</param>
        /// <param name="observer">The downstream observer.</param>
        internal ProbeCoordinator(IObservable<T> source, TimeSpan period, ISequencer sequencer, IObserver<T> observer)
        {
            _source = source;
            _period = period;
            _sequencer = sequencer;
            _observer = observer;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            var timer = Interlocked.Exchange(ref _timer, null);
            timer?.Dispose();

            var subscription = Interlocked.Exchange(ref _subscription, null);
            subscription?.Dispose();
        }

        /// <summary>
        /// Records the latest source value.
        /// </summary>
        /// <param name="value">The source value.</param>
        public void OnNext(T value)
        {
            var shouldSchedule = false;
            var lockTaken = false;
            try
            {
                _gate.Enter(ref lockTaken);
                if (_done)
                {
                    return;
                }

                _hasLatest = true;
                _latest = value;
                shouldSchedule = !_timerActive;
                _timerActive = true;
            }
            finally
            {
                if (lockTaken)
                {
                    _gate.Exit();
                }
            }

            if (!shouldSchedule)
            {
                return;
            }

            ScheduleNext();
        }

        /// <summary>
        /// Forwards source errors and releases active resources.
        /// </summary>
        /// <param name="error">The source error.</param>
        public void OnError(Exception error)
        {
            _observer.OnError(error);
            Dispose();
        }

        /// <summary>
        /// Forwards completion and releases active resources.
        /// </summary>
        public void OnCompleted()
        {
            var lockTaken = false;
            try
            {
                _gate.Enter(ref lockTaken);
                _done = true;
            }
            finally
            {
                if (lockTaken)
                {
                    _gate.Exit();
                }
            }

            _observer.OnCompleted();
            Dispose();
        }

        /// <summary>
        /// Starts sampling the source.
        /// </summary>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal ProbeCoordinator<T> Run()
        {
            _subscription = _source.Subscribe(this);
            return this;
        }

        /// <summary>
        /// Schedules the next sample tick.
        /// </summary>
        private void ScheduleNext()
        {
            var timer = _sequencer.Schedule(this, _period, static (_, coordinator) => coordinator.Tick());
            if (Volatile.Read(ref _disposed) == 0)
            {
                Volatile.Write(ref _timer, timer);
                return;
            }

            timer.Dispose();
        }

        /// <summary>
        /// Handles a sample tick.
        /// </summary>
        /// <returns>An empty disposable.</returns>
        private IDisposable Tick()
        {
            if (!TryTake(out var value))
            {
                return Disposable.Empty;
            }

            _observer.OnNext(value);
            return Disposable.Empty;
        }

        /// <summary>
        /// Attempts to take the latest value.
        /// </summary>
        /// <param name="value">The latest value.</param>
        /// <returns><c>true</c> when a value should be emitted; otherwise, <c>false</c>.</returns>
        private bool TryTake(out T value)
        {
            var lockTaken = false;
            try
            {
                _gate.Enter(ref lockTaken);
                if (_done || !_hasLatest)
                {
                    _timerActive = false;
                    value = default!;
                    return false;
                }

                value = _latest!;
                _hasLatest = false;
                _timerActive = false;
                return true;
            }
            finally
            {
                if (lockTaken)
                {
                    _gate.Exit();
                }
            }
        }
    }

    /// <summary>
    /// Coordinates quiet-period emission with one active timer.
    /// </summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class CalmCoordinator<T> : IDisposable
    {
        /// <summary>
        /// The source observable.
        /// </summary>
        private readonly IObservable<T> _source;

        /// <summary>
        /// The normalized quiet period.
        /// </summary>
        private readonly TimeSpan _dueTime;

        /// <summary>
        /// The sequencer used to schedule quiet-period timers.
        /// </summary>
        private readonly ISequencer _sequencer;

        /// <summary>
        /// The synchronization gate.
        /// </summary>
        private readonly OperatorGate _gate = new();

        /// <summary>
        /// Active subscription and timer resources.
        /// </summary>
        private readonly MultipleDisposable _subscriptions = new();

        /// <summary>
        /// The active timer slot.
        /// </summary>
        private readonly SingleReplaceableDisposable _timer = new();

        /// <summary>
        /// The downstream observer.
        /// </summary>
        private IObserver<T>? _observer;

        /// <summary>
        /// The latest source value.
        /// </summary>
        private T? _latest;

        /// <summary>
        /// A value indicating whether a latest source value is waiting to be emitted.
        /// </summary>
        private bool _hasLatest;

        /// <summary>
        /// A value indicating whether the timer is active.
        /// </summary>
        private bool _timerActive;

        /// <summary>
        /// The virtual due time for the current quiet period.
        /// </summary>
        private DateTimeOffset _dueAt;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalmCoordinator{T}"/> class.
        /// </summary>
        /// <param name="source">The source observable.</param>
        /// <param name="dueTime">The quiet period.</param>
        /// <param name="sequencer">The sequencer used to schedule timers.</param>
        internal CalmCoordinator(IObservable<T> source, TimeSpan dueTime, ISequencer sequencer)
        {
            _source = source;
            _dueTime = Sequencer.Normalize(dueTime);
            _sequencer = sequencer;
            _dueAt = sequencer.Now;
        }

        /// <summary>
        /// The action to take when a timer fires.
        /// </summary>
        private enum TimerAction
        {
            /// <summary>
            /// No value is available.
            /// </summary>
            None,

            /// <summary>
            /// Emit the captured value.
            /// </summary>
            Emit,

            /// <summary>
            /// Reschedule for the remaining quiet period.
            /// </summary>
            Reschedule
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _timer.Dispose();
            _subscriptions.Dispose();
        }

        /// <summary>
        /// Starts quiet-period coordination.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal CalmCoordinator<T> Run(IObserver<T> observer)
        {
            _observer = observer;
            _subscriptions.Add(_timer);
            _subscriptions.Add(_source.Subscribe(OnNext, OnError, OnCompleted));
            return this;
        }

        /// <summary>
        /// Records a source value and schedules a timer when needed.
        /// </summary>
        /// <param name="value">The source value.</param>
        private void OnNext(T value)
        {
            var shouldSchedule = false;
            lock (_gate.SyncRoot)
            {
                _latest = value;
                _hasLatest = true;
                _dueAt = _sequencer.Now + _dueTime;
                if (!_timerActive)
                {
                    _timerActive = true;
                    shouldSchedule = true;
                }
            }

            if (!shouldSchedule)
            {
                return;
            }

            Schedule(_dueTime);
        }

        /// <summary>
        /// Forwards a terminal error and releases active resources.
        /// </summary>
        /// <param name="error">The terminal error.</param>
        private void OnError(Exception error)
        {
            _observer!.OnError(error);
            Dispose();
        }

        /// <summary>
        /// Forwards completion and releases active resources.
        /// </summary>
        private void OnCompleted()
        {
            _observer!.OnCompleted();
            Dispose();
        }

        /// <summary>
        /// Schedules the active timer.
        /// </summary>
        /// <param name="delay">The timer delay.</param>
        private void Schedule(TimeSpan delay) => _timer.Create(_sequencer.Schedule(delay, Tick));

        /// <summary>
        /// Handles a timer tick.
        /// </summary>
        private void Tick()
        {
            var action = GetTimerAction(out var delay, out var value);
            if (action == TimerAction.Reschedule)
            {
                Schedule(delay);
                return;
            }

            if (action != TimerAction.Emit)
            {
                return;
            }

            _observer!.OnNext(value);
        }

        /// <summary>
        /// Determines what the active timer should do.
        /// </summary>
        /// <param name="delay">The remaining delay when rescheduling is needed.</param>
        /// <param name="value">The value to emit.</param>
        /// <returns>The timer action.</returns>
        private TimerAction GetTimerAction(out TimeSpan delay, out T value)
        {
            lock (_gate.SyncRoot)
            {
                var remaining = _dueAt - _sequencer.Now;
                if (remaining > TimeSpan.Zero)
                {
                    delay = remaining;
                    value = default!;
                    return TimerAction.Reschedule;
                }

                _timerActive = false;
                delay = default;
                if (!_hasLatest)
                {
                    value = default!;
                    return TimerAction.None;
                }

                value = _latest!;
                _hasLatest = false;
                return TimerAction.Emit;
            }
        }
    }

    /// <summary>
    /// Coordinates a two-source fork-join operation.
    /// </summary>
    /// <typeparam name="TLeft">The left value type.</typeparam>
    /// <typeparam name="TRight">The right value type.</typeparam>
    /// <typeparam name="TResult">The result value type.</typeparam>
    private sealed class ForkJoinCoordinator<TLeft, TRight, TResult>
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
        /// A value indicating whether the left source produced a value.
        /// </summary>
        private bool _hasLeft;

        /// <summary>
        /// A value indicating whether the right source produced a value.
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
        /// Initializes a new instance of the <see cref="ForkJoinCoordinator{TLeft, TRight, TResult}"/> class.
        /// </summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="selector">The projection function.</param>
        internal ForkJoinCoordinator(IObserver<TResult> observer, Func<TLeft, TRight, TResult> selector)
        {
            _observer = observer;
            _selector = selector;
        }

        /// <summary>
        /// Subscribes to both fork-join sources.
        /// </summary>
        /// <param name="left">The left source.</param>
        /// <param name="right">The right source.</param>
        /// <returns>The subscription cleanup.</returns>
        internal MultipleDisposable Run(IObservable<TLeft> left, IObservable<TRight> right) =>
            new(
                left.Subscribe(OnLeftNext, _observer.OnError, OnLeftCompleted),
                right.Subscribe(OnRightNext, _observer.OnError, OnRightCompleted));

        /// <summary>
        /// Records a left value.
        /// </summary>
        /// <param name="value">The left value.</param>
        private void OnLeftNext(TLeft value)
        {
            lock (_gate.SyncRoot)
            {
                _hasLeft = true;
                _latestLeft = value;
            }
        }

        /// <summary>
        /// Records a right value.
        /// </summary>
        /// <param name="value">The right value.</param>
        private void OnRightNext(TRight value)
        {
            lock (_gate.SyncRoot)
            {
                _hasRight = true;
                _latestRight = value;
            }
        }

        /// <summary>
        /// Marks the left source as complete.
        /// </summary>
        private void OnLeftCompleted()
        {
            if (!CompleteLeft(out var result, out var emit))
            {
                return;
            }

            Finish(result, emit);
        }

        /// <summary>
        /// Marks the right source as complete.
        /// </summary>
        private void OnRightCompleted()
        {
            if (!CompleteRight(out var result, out var emit))
            {
                return;
            }

            Finish(result, emit);
        }

        /// <summary>
        /// Marks the left source complete and computes the result if both sources are complete.
        /// </summary>
        /// <param name="result">The result to emit.</param>
        /// <param name="emit">A value indicating whether a result should be emitted.</param>
        /// <returns><c>true</c> when fork-join is ready to finish; otherwise, <c>false</c>.</returns>
        private bool CompleteLeft(out TResult result, out bool emit)
        {
            lock (_gate.SyncRoot)
            {
                _leftDone = true;
                return TryFinish(out result, out emit);
            }
        }

        /// <summary>
        /// Marks the right source complete and computes the result if both sources are complete.
        /// </summary>
        /// <param name="result">The result to emit.</param>
        /// <param name="emit">A value indicating whether a result should be emitted.</param>
        /// <returns><c>true</c> when fork-join is ready to finish; otherwise, <c>false</c>.</returns>
        private bool CompleteRight(out TResult result, out bool emit)
        {
            lock (_gate.SyncRoot)
            {
                _rightDone = true;
                return TryFinish(out result, out emit);
            }
        }

        /// <summary>
        /// Computes the final result when both sources are complete.
        /// </summary>
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

        /// <summary>
        /// Emits the final result and completes.
        /// </summary>
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
